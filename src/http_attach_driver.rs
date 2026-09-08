//! HTTP attach driver (issue #100, ADR-0005 / holler-server ADR-0017):
//! drives an OpenCode session that **already exists** (typically inside a
//! Herdr pane on this same box) over that process's own HTTP control
//! surface, instead of spawning and owning a second one via ACP stdio the
//! way [`crate::acp_driver::AcpDriver`] does.
//!
//! This module never calls `session/new` (`POST /session` / `POST
//! /api/session`) and never execs anything — see [`HttpAttachDriver::attach`]
//! for the fail-closed existence check that enforces this.
//!
//! # Real OpenCode HTTP paths, pinned (verified live against `opencode
//! serve`, v1.18.20, by curling its own `/doc` OpenAPI spec and exercising
//! each call against a real running session)
//!
//! - **Existence check**: `GET {endpoint}/api/session/{session_id}` — `200`
//!   for a live session, clean `404` for a missing one. (The equivalent v1
//!   path, `GET {endpoint}/session/{session_id}`, also works identically;
//!   the v2 `/api/...` form is used here for consistency with the
//!   interrupt path below, which only exists under `/api/...`.)
//! - **Prompt**: `POST {endpoint}/session/{session_id}/prompt_async` with
//!   body `{"parts": [{"type": "text", "text": "..."}]}`, returns `204`
//!   immediately — genuinely fire-and-forget, so this never blocks the
//!   caller on a long-running turn. (The v2 `POST /api/session/{id}/prompt`
//!   form also exists and returns `200` with a body, but `prompt_async`'s
//!   `204`-immediately contract is the more directly non-blocking of the
//!   two, and is what this driver actually uses.)
//! - **Interrupt**: `POST {endpoint}/api/session/{session_id}/interrupt`,
//!   no body, returns `204`. This matches the path
//!   `crate::session_manager`'s existing ACP-fallback interrupt already
//!   uses — see [`HttpAttachDriver::interrupt`], which attach uses as its
//!   *primary* cancel path, not a fallback.
//! - **Observing the reply**: there is no per-session SSE stream that
//!   actually emits anything against this OpenCode version — `GET
//!   {endpoint}/api/session/{session_id}/event` accepts the connection but
//!   never sends a byte (verified: connected, waited several seconds after
//!   sending a real prompt, received nothing). The **global** event stream,
//!   `GET {endpoint}/event`, *does* work — it emits every session's events
//!   as newline-delimited `data: {json}\n\n` SSE frames, each carrying its
//!   own `properties.sessionID`, which this driver filters on. Real event
//!   types observed for one prompt/reply cycle, in order: `message.updated`
//!   (a new message, `properties.info.role` is `"user"` or `"assistant"`),
//!   `message.part.updated` (a text chunk, `properties.part.type ==
//!   "text"`, keyed to its message by `properties.part.messageID`),
//!   `session.status` (`properties.status.type` is `"busy"` or `"idle"`),
//!   and `session.idle` (a distinct, final "this turn is over" event).
//!
//! # Question/permission detection and reply (holler-client issue #133,
//! holler-server issue #382), pinned against the same running `opencode
//! serve` v1.18.20 by curling its `/doc` OpenAPI spec and its real
//! `/question`/`/permission` responses
//!
//! OpenCode has a real `question` tool (structured multiple-choice,
//! distinct from free-text prompting) and a separate tool-use
//! `permission` gate; both genuinely block the agent's turn until
//! answered. Neither is observable on the `/event` SSE stream this
//! driver already reads (verified: no `question.*`/`permission.*` event
//! ever appears there), so this driver **polls** instead, on
//! [`BLOCK_POLL_INTERVAL`]:
//!
//! - **List**: `GET {endpoint}/question` / `GET {endpoint}/permission` —
//!   global (all sessions), each item's JSON field is `id` (**not**
//!   `requestID` — that name is only the URL path parameter), plus
//!   `sessionID`, which this driver filters on since there is no
//!   per-session list endpoint that actually returns data (the v2-style
//!   `/api/session/{id}/question` path exists in the OpenAPI doc but
//!   returned empty even with a real question pending).
//! - **Question shape**: `{id, sessionID, questions: [{question, header,
//!   options: [{label, description}], multiple?, custom?}], tool?}`.
//!   Only single-question requests (`questions.len() == 1`, the common
//!   case) are answerable by [`HttpAttachDriver::answer`] today — a
//!   multi-question request's `choice` argument has nowhere unambiguous
//!   to go with a single CLI argument (scope cut, issue #133/#382).
//! - **Permission shape**: `{id, sessionID, permission, patterns,
//!   metadata, always, tool?}` — no options list; the real reply
//!   vocabulary is a fixed three-way enum (see below).
//! - **Question reply**: `POST {endpoint}/question/{id}/reply`, body
//!   `{"answers": [["<exact option label>"]]}` (one array of chosen
//!   labels per question — this driver only ever sends one, for the
//!   single supported question). 404s with `QuestionNotFoundError` for
//!   an unknown/already-answered id.
//! - **Permission reply**: `POST {endpoint}/permission/{id}/reply`, body
//!   `{"reply": "once" | "always" | "reject"}` — **not** the
//!   `{"answers": [[...]]}` shape questions use; permission's vocabulary
//!   is closed and unrelated to any options list. 404s with
//!   `PermissionNotFoundError` for an unknown/already-answered id.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::acp_driver::{DriverError, DriverEvent, DriverStatus, DriverStopReason};
use crate::config::SessionConfig;
use crate::debug::{self, DebugConfig};

/// How often the background loop polls `GET {endpoint}/question` and
/// `GET {endpoint}/permission` for an item matching this session (issue
/// #133/#382) — see the module docs for why polling, not SSE, is used.
/// Frequent enough that a real question/permission is detected promptly
/// without perceptible lag to an operator watching the roster, cheap
/// enough (two GETs to a local loopback process) not to matter. `pub`
/// so regression tests can wait a precise multiple of it rather than
/// guessing a sleep duration.
pub const BLOCK_POLL_INTERVAL: Duration = Duration::from_millis(750);

enum Command {
    Prompt(String),
}

/// Which real OpenCode endpoint a [`PendingBlock`] answers against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockKind {
    Question,
    Permission,
}

/// One currently-pending question or permission for this driver's
/// session, as last observed by the background poll loop (issue
/// #133/#382/#139). `options` is only populated for a question — one
/// inner `Vec` per question in the request, in order (almost always a
/// single entry; more than one when OpenCode asks several questions in
/// one request) — the exact labels [`HttpAttachDriver::answer`] resolves
/// each of a comma-separated `choice`'s parts against. A permission's
/// reply vocabulary is the fixed `once`/`always`/`reject` enum, not an
/// options list, so it stays empty for that kind.
#[derive(Debug, Clone)]
struct PendingBlock {
    kind: BlockKind,
    id: String,
    options: Vec<Vec<String>>,
}

/// A running attach driver for one already-existing OpenCode session.
///
/// Mirrors [`crate::acp_driver::AcpDriver`]'s public shape
/// (`prompt`/`next_event`/`session_id`/`shutdown`) so
/// [`crate::session_manager::SessionManager`]'s prompt/interrupt/busy-queue
/// logic does not need a second code path — see `session_manager`'s
/// `SessionDriver` enum, which wraps whichever of the two a session's
/// `mode` selects.
pub struct HttpAttachDriver {
    command_tx: mpsc::UnboundedSender<Command>,
    event_rx: mpsc::UnboundedReceiver<DriverEvent>,
    connection: tokio::task::JoinHandle<()>,
    client: reqwest::Client,
    endpoint: String,
    session_id: String,
    /// Set just before sending an HTTP interrupt, read (and cleared) by the
    /// background loop the next time it observes the turn actually end
    /// (`session.idle`), so that end can be reported as
    /// [`DriverStopReason::Cancelled`] rather than [`DriverStopReason::EndTurn`].
    /// OpenCode's event stream has no cancellation-specific event of its
    /// own to key off instead.
    interrupt_requested: Arc<AtomicBool>,
    /// The question/permission currently blocking this session's turn,
    /// if any, as last observed by the background poll loop (issue
    /// #133/#382). Read directly by [`HttpAttachDriver::answer`]
    /// (a direct POST, bypassing the background loop entirely — the
    /// same pattern [`HttpAttachDriver::interrupt`] already uses) and
    /// written by the background loop's poll tick.
    pending_block: Arc<Mutex<Option<PendingBlock>>>,
    debug: DebugConfig,
}

impl HttpAttachDriver {
    /// Attaches to `config`'s already-existing OpenCode session.
    ///
    /// Fails closed (never `session/new`, never exec `command`) if
    /// `endpoint`/`session_id` are missing (defensive — [`crate::config`]
    /// already validates this at load time) or if the existence check
    /// 404s / the endpoint cannot be reached at all.
    pub async fn attach(config: &SessionConfig, cfg: DebugConfig) -> Result<Self, DriverError> {
        let endpoint = config
            .endpoint
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DriverError::AttachSessionNotFound {
                endpoint: String::new(),
                session_id: config.session_id.clone().unwrap_or_default(),
                detail: "no endpoint configured".to_string(),
            })?;
        let session_id = config
            .session_id
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| DriverError::AttachSessionNotFound {
                endpoint: endpoint.clone(),
                session_id: String::new(),
                detail: "no session_id configured".to_string(),
            })?;

        let client = reqwest::Client::new();
        let existence_url = format!(
            "{}/api/session/{}",
            endpoint.trim_end_matches('/'),
            session_id
        );
        let response = client.get(&existence_url).send().await.map_err(|err| {
            DriverError::AttachSessionNotFound {
                endpoint: endpoint.clone(),
                session_id: session_id.clone(),
                detail: format!("could not reach endpoint: {err}"),
            }
        })?;
        if !response.status().is_success() {
            return Err(DriverError::AttachSessionNotFound {
                endpoint: endpoint.clone(),
                session_id: session_id.clone(),
                detail: format!("HTTP {}", response.status()),
            });
        }

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let interrupt_requested = Arc::new(AtomicBool::new(false));
        let pending_block = Arc::new(Mutex::new(None));

        let loop_client = client.clone();
        let loop_endpoint = endpoint.clone();
        let loop_session_id = session_id.clone();
        let loop_interrupt_flag = interrupt_requested.clone();
        let loop_pending_block = pending_block.clone();
        let connection = tokio::spawn(async move {
            run_http_loop(
                loop_client,
                loop_endpoint,
                loop_session_id,
                command_rx,
                event_tx,
                loop_interrupt_flag,
                loop_pending_block,
                cfg,
            )
            .await;
        });

        Ok(Self {
            command_tx,
            event_rx,
            connection,
            client,
            endpoint,
            session_id,
            interrupt_requested,
            pending_block,
            debug: cfg,
        })
    }

    /// Sends a prompt via `prompt_async`. Returns once the request has been
    /// handed to the background task, not once the turn completes — the
    /// same non-blocking contract as [`crate::acp_driver::AcpDriver::prompt`].
    pub fn prompt(&self, text: impl Into<String>) -> Result<(), DriverError> {
        self.command_tx
            .send(Command::Prompt(text.into()))
            .map_err(|_| DriverError::Disconnected)
    }

    /// Sends an HTTP interrupt directly. This is attach mode's *primary*
    /// cancel path (issue #100) — there is no ACP channel to prefer over
    /// it, unlike spawn mode's ACP-cancel-then-HTTP-fallback dance.
    pub async fn interrupt(&self) -> Result<(), DriverError> {
        self.interrupt_requested.store(true, Ordering::SeqCst);
        let url = format!(
            "{}/api/session/{}/interrupt",
            self.endpoint.trim_end_matches('/'),
            self.session_id
        );
        debug::outgoing(self.debug, "http_attach", "interrupt")
            .field("session", self.session_id.clone())
            .emit();
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|err| DriverError::Http(err.to_string()))?;
        debug::incoming(self.debug, "http_attach", "interrupt")
            .field("status", response.status().as_str())
            .emit();
        if !response.status().is_success() {
            return Err(DriverError::Http(format!(
                "interrupt returned HTTP {}",
                response.status()
            )));
        }
        Ok(())
    }

    /// Answers whichever question/permission is currently blocking this
    /// session's turn (holler-server issue #382), resolving `choice`
    /// against the real pending request's shape (an option index or
    /// exact label for a question; `once`/`always`/`reject`, plus common
    /// aliases, for a permission) and POSTing the reply directly —
    /// bypassing the background poll loop entirely, the same pattern
    /// [`interrupt`](Self::interrupt) already uses for its own direct
    /// POST. `Err(DriverError::NoPendingAnswer(_))` covers every
    /// rejection: nothing pending, an out-of-range index, an unmatched
    /// label, or an unrecognized permission reply.
    pub async fn answer(&self, choice: String) -> Result<(), DriverError> {
        let pending = self
            .pending_block
            .lock()
            .expect("pending-block mutex poisoned")
            .clone();
        let Some(pending) = pending else {
            return Err(DriverError::NoPendingAnswer(format!(
                "no question or permission is pending for session {}",
                self.session_id
            )));
        };

        match pending.kind {
            BlockKind::Permission => {
                let Some(reply) = normalize_permission_reply(&choice) else {
                    return Err(DriverError::NoPendingAnswer(format!(
                        "invalid permission choice {choice:?}; expected one of \
                         once/allow, always, reject/deny"
                    )));
                };
                let url = format!(
                    "{}/permission/{}/reply",
                    self.endpoint.trim_end_matches('/'),
                    pending.id
                );
                let body = serde_json::json!({ "reply": reply });
                debug::outgoing(self.debug, "http_attach", "answer")
                    .field("session", self.session_id.clone())
                    .field("kind", "permission")
                    .field("reply", reply)
                    .emit();
                let response = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|err| DriverError::Http(err.to_string()))?;
                debug::incoming(self.debug, "http_attach", "answer")
                    .field("status", response.status().as_str())
                    .emit();
                if !response.status().is_success() {
                    return Err(DriverError::Http(format!(
                        "permission reply returned HTTP {}",
                        response.status()
                    )));
                }
            }
            BlockKind::Question => {
                let Some(labels) = resolve_question_choices(&choice, &pending.options) else {
                    return Err(DriverError::NoPendingAnswer(format!(
                        "invalid question choice {choice:?}; expected {} comma-separated \
                         choice(s), each an option index or an exact label, matching this \
                         request's questions in order: {:?}",
                        pending.options.len(),
                        pending.options
                    )));
                };
                let url = format!(
                    "{}/question/{}/reply",
                    self.endpoint.trim_end_matches('/'),
                    pending.id
                );
                let answers: Vec<Vec<&str>> =
                    labels.iter().map(|label| vec![label.as_str()]).collect();
                let body = serde_json::json!({ "answers": answers });
                debug::outgoing(self.debug, "http_attach", "answer")
                    .field("session", self.session_id.clone())
                    .field("kind", "question")
                    .field("reply", labels.join(","))
                    .emit();
                let response = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|err| DriverError::Http(err.to_string()))?;
                debug::incoming(self.debug, "http_attach", "answer")
                    .field("status", response.status().as_str())
                    .emit();
                if !response.status().is_success() {
                    return Err(DriverError::Http(format!(
                        "question reply returned HTTP {}",
                        response.status()
                    )));
                }
            }
        }

        // Deliberately NOT clearing `pending_block` here: the background
        // poll loop is the single source of truth for it (so a
        // real-but-unanswered/replaced question the server still has
        // right after this reply is never lost to a race), and it will
        // observe this id is gone on its very next tick and emit the
        // unblocked transition itself. A second `answer` landing before
        // that tick would just get OpenCode's own 404
        // (`QuestionNotFoundError`/`PermissionNotFoundError`) surfaced
        // as `DriverError::Http`, which is an accurate report — nothing
        // is pending to apply it to anymore.
        Ok(())
    }

    /// The next event from the attached session. Returns `None` once the
    /// connection has closed and all buffered events are drained.
    pub async fn next_event(&mut self) -> Option<DriverEvent> {
        self.event_rx.recv().await
    }

    /// The OpenCode session id this driver is attached to.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Detaches from this session WITHOUT touching it (issue #101): drops
    /// the command channel and stops this driver's own background event
    /// listener. No HTTP `DELETE`, no process kill (there is none — this
    /// driver never owned a process), no `session/new` replacement. The
    /// attached OpenCode process is completely unaffected by this call —
    /// it keeps running exactly as it was, e.g. still visible in its Herdr
    /// pane.
    pub async fn shutdown(self) -> Result<(), DriverError> {
        drop(self.command_tx);
        self.connection.abort();
        let _ = self.connection.await;
        Ok(())
    }
}

/// Real HTTP existence probe for one attach session — `true` iff `GET
/// {endpoint}/api/session/{session_id}` returns success, right now (issue
/// #102). This is the same check [`HttpAttachDriver::attach`] performs
/// before it will drive a session, extracted standalone because `support
/// opencode-http`/`status`/`presence` (issue #102) only need a yes/no
/// answer, not a driver — asking for one just to throw it away would spawn
/// this session's background event-listener task for no reason. One real
/// GET, no retry loop: "is my configured id real right now," not "wait
/// until it becomes real."
pub async fn probe_session_exists(endpoint: &str, session_id: &str) -> bool {
    let client = reqwest::Client::new();
    let url = format!("{}/api/session/{}", endpoint.trim_end_matches('/'), session_id);
    match client.get(&url).send().await {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

/// One real session as returned by `GET {endpoint}/session` (issue #109's
/// `holler attach sessions`/`holler attach init` — verified live against
/// `opencode serve` v1.18.20: a real session object carries at least these
/// fields, plus several this driver doesn't need, which `serde`'s default
/// unknown-field-ignoring behavior leaves alone).
#[derive(Debug, Clone, Deserialize)]
pub struct OcSessionSummary {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub time: OcSessionTime,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct OcSessionTime {
    /// Unix epoch milliseconds. Used to pick the most recently active
    /// session (`holler attach init`'s no-`--session` default) — sorting by
    /// this rather than trusting the endpoint's own array order, since
    /// nothing about `GET /session`'s contract promises an order.
    #[serde(default)]
    pub updated: i64,
}

/// Lists every real session at `endpoint`, right now, newest-updated first
/// (issue #109). One real GET, no retry loop — same "ask once, fail closed"
/// shape as [`probe_session_exists`].
pub async fn list_sessions(endpoint: &str) -> Result<Vec<OcSessionSummary>, String> {
    let client = reqwest::Client::new();
    let url = format!("{}/session", endpoint.trim_end_matches('/'));
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|err| format!("could not reach {endpoint}: {err}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "listing sessions at {endpoint} returned HTTP {}",
            response.status()
        ));
    }
    let mut sessions: Vec<OcSessionSummary> = response
        .json()
        .await
        .map_err(|err| format!("could not parse session list from {endpoint}: {err}"))?;
    sessions.sort_by_key(|s| std::cmp::Reverse(s.time.updated));
    Ok(sessions)
}

/// Probes every attach-mode session in `registry` for real, right now,
/// returning the names of the ones whose configured `endpoint`/`session_id`
/// currently answer. Spawn sessions are never included here — they're
/// confirmed via [`crate::config::SessionRegistry::confirmed_harnesses`]'s
/// PATH/executable check instead (issue #99's explicit "attach sessions are
/// not confirmed by PATH-checking `opencode`" rule, and its mirror: spawn
/// sessions are not confirmed by an HTTP probe they have nothing to do
/// with). An attach session missing `endpoint`/`session_id` is skipped
/// (defensive — [`crate::config`] already rejects this at load time, so it
/// should never actually happen) rather than probing a malformed URL.
pub async fn confirmed_attach_sessions(
    registry: &crate::config::SessionRegistry,
) -> Vec<String> {
    let mut confirmed = Vec::new();
    for session in registry.sessions().iter().filter(|s| s.is_attach()) {
        let (Some(endpoint), Some(session_id)) = (&session.endpoint, &session.session_id) else {
            continue;
        };
        if endpoint.is_empty() || session_id.is_empty() {
            continue;
        }
        if probe_session_exists(endpoint, session_id).await {
            confirmed.push(session.name.clone());
        }
    }
    confirmed
}

/// A single global SSE event's shape this driver cares about. OpenCode's
/// event payload has many more fields/variants than this; `serde`'s
/// default behavior (ignore unknown fields on a struct, unless `deny_unknown_fields`
/// is set, which this does not set) means this stays forward-compatible
/// with fields this driver doesn't need.
#[derive(Debug, Deserialize)]
struct OcEvent {
    #[serde(rename = "type")]
    kind: String,
    properties: OcEventProperties,
}

#[derive(Debug, Default, Deserialize)]
struct OcEventProperties {
    #[serde(rename = "sessionID")]
    session_id: Option<String>,
    info: Option<OcMessageInfo>,
    part: Option<OcPart>,
    status: Option<OcStatus>,
}

#[derive(Debug, Deserialize)]
struct OcMessageInfo {
    id: String,
    role: String,
}

#[derive(Debug, Deserialize)]
struct OcPart {
    #[serde(rename = "type")]
    kind: String,
    text: Option<String>,
    #[serde(rename = "messageID")]
    message_id: String,
}

#[derive(Debug, Deserialize)]
struct OcStatus {
    #[serde(rename = "type")]
    kind: String,
}

/// One pending item from `GET {endpoint}/question` (issue #133/#382),
/// per OpenCode's real `QuestionRequest` schema (`/doc`'s OpenAPI spec,
/// v1.18.20) — the JSON field is `id`, not `requestID` (that name is
/// only the URL path parameter on the reply/reject endpoints).
#[derive(Debug, Deserialize)]
struct OcQuestionRequest {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
    questions: Vec<OcQuestionInfo>,
}

#[derive(Debug, Deserialize)]
struct OcQuestionInfo {
    options: Vec<OcQuestionOption>,
}

#[derive(Debug, Deserialize)]
struct OcQuestionOption {
    label: String,
}

/// One pending item from `GET {endpoint}/permission` (issue #133/#382),
/// per OpenCode's real `PermissionRequest` schema. Only `id`/`sessionID`
/// are needed here — the reply vocabulary (`once`/`always`/`reject`) is
/// fixed, not derived from anything else in this shape.
#[derive(Debug, Deserialize)]
struct OcPermissionRequest {
    id: String,
    #[serde(rename = "sessionID")]
    session_id: String,
}

/// Maps a `holler-server answer` `choice` to OpenCode's real permission
/// reply enum (`once`/`always`/`reject`), accepting a few obvious
/// operator-facing aliases since the wire's `choice` is free text.
fn normalize_permission_reply(choice: &str) -> Option<&'static str> {
    match choice.trim().to_ascii_lowercase().as_str() {
        "once" | "allow" | "approve" | "yes" | "y" => Some("once"),
        "always" => Some("always"),
        "reject" | "deny" | "no" | "n" => Some("reject"),
        _ => None,
    }
}

/// Resolves one `choice` segment against a single question's real option
/// labels: a 0-based numeric index into `options`, or an exact
/// (case-insensitive) label match. Returns the real label OpenCode
/// expects on the wire (`options`' own casing), not the caller's input.
fn resolve_question_choice(choice: &str, options: &[String]) -> Option<String> {
    if let Ok(index) = choice.trim().parse::<usize>() {
        if let Some(label) = options.get(index) {
            return Some(label.clone());
        }
    }
    options
        .iter()
        .find(|label| label.eq_ignore_ascii_case(choice.trim()))
        .cloned()
}

/// Resolves a `holler-server answer` `choice` against every pending
/// question in order (issue #139): comma-separated for more than one
/// question (`"Yes,2,No"` for a 3-question request), a bare single value
/// for the overwhelmingly common one-question case (no comma required).
/// Fails closed — `None` — the moment the segment count doesn't match
/// `options.len()`, or any individual segment doesn't resolve against its
/// own question's options; a partial answer is never sent.
fn resolve_question_choices(choice: &str, options: &[Vec<String>]) -> Option<Vec<String>> {
    let segments: Vec<&str> = choice.split(',').collect();
    if segments.len() != options.len() {
        return None;
    }
    segments
        .iter()
        .zip(options.iter())
        .map(|(segment, question_options)| resolve_question_choice(segment, question_options))
        .collect()
}

/// Drives one attached session: sends prompts via `prompt_async`, and
/// concurrently reads the global SSE event stream, translating this
/// session's own events into [`DriverEvent`]s. Returns once the command
/// channel closes ([`HttpAttachDriver`] was dropped or
/// [`HttpAttachDriver::shutdown`] called).
#[allow(clippy::too_many_arguments)]
async fn run_http_loop(
    client: reqwest::Client,
    endpoint: String,
    session_id: String,
    mut command_rx: mpsc::UnboundedReceiver<Command>,
    event_tx: mpsc::UnboundedSender<DriverEvent>,
    interrupt_requested: Arc<AtomicBool>,
    pending_block: Arc<Mutex<Option<PendingBlock>>>,
    cfg: DebugConfig,
) {
    // message id -> role, so a `message.part.updated` (which carries no
    // role of its own) can be attributed correctly. Only assistant text is
    // surfaced as `DriverEvent::Update` -- otherwise the operator's own
    // echoed prompt would come back as if the agent had said it.
    let mut message_roles: HashMap<String, String> = HashMap::new();

    let event_url = format!("{}/event", endpoint.trim_end_matches('/'));
    debug::local(cfg, "http_attach", "sse")
        .field("event", "connecting")
        .emit();
    let response = match client.get(&event_url).send().await {
        Ok(resp) => resp,
        Err(_) => {
            // connection lost before it ever started; nothing to drive
            debug::warn(cfg, "http_attach", "sse")
                .field("event", "connect_failed")
                .emit();
            return;
        }
    };
    debug::local(cfg, "http_attach", "sse")
        .field("event", "connected")
        .emit();
    let mut byte_stream = response.bytes_stream();
    let mut buffer = String::new();

    let mut block_poll = tokio::time::interval(BLOCK_POLL_INTERVAL);
    block_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    None => return,
                    Some(Command::Prompt(text)) => {
                        let prompt_url = format!(
                            "{}/session/{}/prompt_async",
                            endpoint.trim_end_matches('/'),
                            session_id
                        );
                        let body = serde_json::json!({
                            "parts": [{"type": "text", "text": text}]
                        });
                        debug::outgoing(cfg, "http_attach", "prompt")
                            .field("session", session_id.clone())
                            .emit();
                        match client.post(&prompt_url).json(&body).send().await {
                            Ok(response) => {
                                debug::incoming(cfg, "http_attach", "prompt")
                                    .field("status", response.status().as_str())
                                    .emit();
                            }
                            Err(_) => {
                                // The attached OpenCode became unreachable mid-session.
                                // There is no subprocess to detect exiting the way
                                // AcpDriver's connection task would -- surface this the
                                // same way a lost ACP connection does: stop producing
                                // events. The caller's next_event() will observe the
                                // channel close in that case.
                                debug::incoming(cfg, "http_attach", "prompt")
                                    .field("event", "error")
                                    .emit();
                                return;
                            }
                        }
                        let _ = event_tx.send(DriverEvent::Status(DriverStatus::Working));
                    }
                }
            }
            chunk = byte_stream.next() => {
                let Some(Ok(bytes)) = chunk else { return };
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                while let Some(pos) = buffer.find("\n\n") {
                    let frame = buffer[..pos].to_string();
                    buffer.drain(..pos + 2);
                    for line in frame.lines() {
                        let Some(json_str) = line.strip_prefix("data: ") else { continue };
                        let Ok(event) = serde_json::from_str::<OcEvent>(json_str) else { continue };
                        if event.properties.session_id.as_deref() != Some(session_id.as_str()) {
                            continue; // another session's event on the shared global stream
                        }
                        debug::incoming(cfg, "http_attach", "sse_event")
                            .field("event_type", event.kind.clone())
                            .emit();
                        handle_event(event, &mut message_roles, &event_tx, &interrupt_requested);
                    }
                }
            }
            _ = block_poll.tick() => {
                poll_pending_block(&client, &endpoint, &session_id, &pending_block, &event_tx, cfg).await;
            }
        }
    }
}

/// One poll tick of the question/permission detection loop (issue
/// #133/#382): `GET`s both global lists, keeps whichever item (if any)
/// matches this session, and — only on a real transition — updates
/// `pending_block` and emits [`DriverEvent::Status`]. A poll that finds
/// nothing new (same pending id as last tick, or still nothing pending)
/// is silent: no duplicate `Blocked`/`Working` events every tick.
async fn poll_pending_block(
    client: &reqwest::Client,
    endpoint: &str,
    session_id: &str,
    pending_block: &Arc<Mutex<Option<PendingBlock>>>,
    event_tx: &mpsc::UnboundedSender<DriverEvent>,
    cfg: DebugConfig,
) {
    let found = poll_pending_question(client, endpoint, session_id, cfg)
        .await
        .or(poll_pending_permission(client, endpoint, session_id, cfg).await);

    let previous_id = pending_block
        .lock()
        .expect("pending-block mutex poisoned")
        .as_ref()
        .map(|p| p.id.clone());
    let found_id = found.as_ref().map(|p| p.id.clone());
    if previous_id == found_id {
        return; // no real transition -- same pending item, or still none
    }

    let became_blocked = found.is_some();
    *pending_block.lock().expect("pending-block mutex poisoned") = found;
    if became_blocked {
        debug::local(cfg, "http_attach", "answer")
            .field("event", "blocked")
            .field("session", session_id.to_string())
            .emit();
        let _ = event_tx.send(DriverEvent::Status(DriverStatus::Blocked));
    } else {
        // Whatever was pending is gone (answered by us or by another
        // channel, or timed out on OpenCode's own side) -- the turn is
        // presumably continuing; the next real SSE `session.idle` (if
        // any) still settles the final status precisely.
        debug::local(cfg, "http_attach", "answer")
            .field("event", "unblocked")
            .field("session", session_id.to_string())
            .emit();
        let _ = event_tx.send(DriverEvent::Status(DriverStatus::Working));
    }
}

async fn poll_pending_question(
    client: &reqwest::Client,
    endpoint: &str,
    session_id: &str,
    _cfg: DebugConfig,
) -> Option<PendingBlock> {
    let url = format!("{}/question", endpoint.trim_end_matches('/'));
    let response = client.get(&url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let requests: Vec<OcQuestionRequest> = response.json().await.ok()?;
    let request = requests.into_iter().find(|r| r.session_id == session_id)?;
    if request.questions.is_empty() {
        // Malformed on OpenCode's side (a question request with no
        // questions in it) -- nothing to resolve `answer` against, and
        // reporting `Blocked` for a request with nothing to answer would
        // just leave an operator stuck with no way to clear it.
        return None;
    }
    // Issue #133/#382/#139: one entry per question, in order, so `answer`
    // can resolve a comma-separated `choice` against each independently.
    let options: Vec<Vec<String>> = request
        .questions
        .iter()
        .map(|q| q.options.iter().map(|o| o.label.clone()).collect())
        .collect();
    Some(PendingBlock {
        kind: BlockKind::Question,
        id: request.id,
        options,
    })
}

async fn poll_pending_permission(
    client: &reqwest::Client,
    endpoint: &str,
    session_id: &str,
    _cfg: DebugConfig,
) -> Option<PendingBlock> {
    let url = format!("{}/permission", endpoint.trim_end_matches('/'));
    let response = client.get(&url).send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    let requests: Vec<OcPermissionRequest> = response.json().await.ok()?;
    let request = requests.into_iter().find(|r| r.session_id == session_id)?;
    Some(PendingBlock {
        kind: BlockKind::Permission,
        id: request.id,
        options: Vec::new(),
    })
}

fn handle_event(
    event: OcEvent,
    message_roles: &mut HashMap<String, String>,
    event_tx: &mpsc::UnboundedSender<DriverEvent>,
    interrupt_requested: &Arc<AtomicBool>,
) {
    match event.kind.as_str() {
        "message.updated" => {
            if let Some(info) = event.properties.info {
                message_roles.insert(info.id, info.role);
            }
        }
        "message.part.updated" => {
            if let Some(part) = event.properties.part {
                if part.kind == "text" {
                    let is_assistant = message_roles
                        .get(&part.message_id)
                        .is_some_and(|role| role == "assistant");
                    if is_assistant {
                        if let Some(text) = part.text {
                            let _ = event_tx.send(DriverEvent::Update(text));
                        }
                    }
                }
            }
        }
        "session.status" => {
            if let Some(status) = event.properties.status {
                if status.kind == "busy" {
                    let _ = event_tx.send(DriverEvent::Status(DriverStatus::Working));
                }
                // Deliberately not reacting to status.kind == "idle" here:
                // the distinct "session.idle" event below is the single,
                // unambiguous "this turn is over" signal. Reacting to both
                // would double-emit DriverEvent::StopReason for one real
                // turn ending.
            }
        }
        "session.idle" => {
            let was_cancelled = interrupt_requested.swap(false, Ordering::SeqCst);
            let _ = event_tx.send(DriverEvent::Status(DriverStatus::Idle));
            let reason = if was_cancelled {
                DriverStopReason::Cancelled
            } else {
                DriverStopReason::EndTurn
            };
            let _ = event_tx.send(DriverEvent::StopReason(reason));
        }
        _ => {}
    }
}

#[cfg(test)]
mod live_smoke_tests {
    //! Opt-in, `#[ignore]`d tests against a REAL `opencode serve` (issue
    //! #100's own "verify against the actual installed OpenCode" mandate).
    //! Not part of the default `cargo test` run since they need a real
    //! `opencode` binary and a free port; run explicitly with
    //! `cargo test --lib -- --ignored live_smoke`.
    use super::*;
    use crate::config::SessionMode;

    fn opencode_available() -> bool {
        std::process::Command::new("opencode")
            .arg("--version")
            .output()
            .is_ok()
    }

    async fn start_real_opencode(port: u16) -> (std::process::Child, String) {
        let mut child = std::process::Command::new("opencode")
            .args(["serve", "--port", &port.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("opencode serve must be spawnable for this live smoke test");
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            if let Ok(resp) = client
                .post(format!("{base}/session"))
                .json(&serde_json::json!({}))
                .send()
                .await
            {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    let sid = body["id"].as_str().unwrap().to_string();
                    return (child, sid);
                }
            }
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("opencode serve on port {port} never became ready");
    }

    #[tokio::test]
    #[ignore]
    async fn live_smoke_attach_prompt_interrupt_shutdown_leaves_session_alive() {
        assert!(opencode_available(), "this test requires a real `opencode` binary on PATH");
        let port = 41099;
        let (mut child, session_id) = start_real_opencode(port).await;
        let endpoint = format!("http://127.0.0.1:{port}");

        let config = SessionConfig {
            name: "alpha".to_string(),
            harness: "opencode".to_string(),
            mode: SessionMode::Attach,
            endpoint: Some(endpoint.clone()),
            session_id: Some(session_id.clone()),
            ..Default::default()
        };

        let mut driver = HttpAttachDriver::attach(&config, DebugConfig::default())
            .await
            .expect("attach to a real, existing session must succeed");
        assert_eq!(driver.session_id(), session_id);

        driver.prompt("say hi").expect("prompt should queue");
        // Drain a few events -- real content depends on a configured model
        // (this dev box has none), so this only asserts the protocol
        // mechanics work: at least one event arrives before we move on.
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), driver.next_event())
            .await
            .expect("at least one event should arrive within 5s")
            .expect("event channel should not be closed yet");
        println!("first event after prompt: {first:?}");

        driver.interrupt().await.expect("interrupt HTTP call must succeed");

        driver.shutdown().await.expect("shutdown must succeed");

        // The whole point of #101: the real OpenCode process is untouched.
        let client = reqwest::Client::new();
        let still_alive = client
            .get(format!("{endpoint}/api/session/{session_id}"))
            .send()
            .await
            .expect("real opencode should still answer after our driver shut down")
            .status()
            .is_success();
        assert!(still_alive, "attached session must survive our own shutdown");

        let _ = child.kill();
        let _ = child.wait();
    }

    #[tokio::test]
    #[ignore]
    async fn live_smoke_attach_to_missing_session_fails_closed() {
        assert!(opencode_available(), "this test requires a real `opencode` binary on PATH");
        let port = 41098;
        let (mut child, _real_session_id) = start_real_opencode(port).await;
        let endpoint = format!("http://127.0.0.1:{port}");

        let config = SessionConfig {
            name: "alpha".to_string(),
            harness: "opencode".to_string(),
            mode: SessionMode::Attach,
            endpoint: Some(endpoint),
            session_id: Some("ses_this_does_not_exist".to_string()),
            ..Default::default()
        };

        match HttpAttachDriver::attach(&config, DebugConfig::default()).await {
            Err(DriverError::AttachSessionNotFound { .. }) => {}
            Err(other) => panic!("expected AttachSessionNotFound, got a different error: {other}"),
            Ok(_) => panic!("attach to a missing session_id must fail closed, not succeed"),
        }

        let _ = child.kill();
        let _ = child.wait();
    }
}
