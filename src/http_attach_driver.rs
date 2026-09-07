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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::acp_driver::{DriverError, DriverEvent, DriverStatus, DriverStopReason};
use crate::config::SessionConfig;

enum Command {
    Prompt(String),
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
}

impl HttpAttachDriver {
    /// Attaches to `config`'s already-existing OpenCode session.
    ///
    /// Fails closed (never `session/new`, never exec `command`) if
    /// `endpoint`/`session_id` are missing (defensive — [`crate::config`]
    /// already validates this at load time) or if the existence check
    /// 404s / the endpoint cannot be reached at all.
    pub async fn attach(config: &SessionConfig) -> Result<Self, DriverError> {
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

        let loop_client = client.clone();
        let loop_endpoint = endpoint.clone();
        let loop_session_id = session_id.clone();
        let loop_interrupt_flag = interrupt_requested.clone();
        let connection = tokio::spawn(async move {
            run_http_loop(
                loop_client,
                loop_endpoint,
                loop_session_id,
                command_rx,
                event_tx,
                loop_interrupt_flag,
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
        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|err| DriverError::Http(err.to_string()))?;
        if !response.status().is_success() {
            return Err(DriverError::Http(format!(
                "interrupt returned HTTP {}",
                response.status()
            )));
        }
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

/// Drives one attached session: sends prompts via `prompt_async`, and
/// concurrently reads the global SSE event stream, translating this
/// session's own events into [`DriverEvent`]s. Returns once the command
/// channel closes ([`HttpAttachDriver`] was dropped or
/// [`HttpAttachDriver::shutdown`] called).
async fn run_http_loop(
    client: reqwest::Client,
    endpoint: String,
    session_id: String,
    mut command_rx: mpsc::UnboundedReceiver<Command>,
    event_tx: mpsc::UnboundedSender<DriverEvent>,
    interrupt_requested: Arc<AtomicBool>,
) {
    // message id -> role, so a `message.part.updated` (which carries no
    // role of its own) can be attributed correctly. Only assistant text is
    // surfaced as `DriverEvent::Update` -- otherwise the operator's own
    // echoed prompt would come back as if the agent had said it.
    let mut message_roles: HashMap<String, String> = HashMap::new();

    let event_url = format!("{}/event", endpoint.trim_end_matches('/'));
    let response = match client.get(&event_url).send().await {
        Ok(resp) => resp,
        Err(_) => return, // connection lost before it ever started; nothing to drive
    };
    let mut byte_stream = response.bytes_stream();
    let mut buffer = String::new();

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
                        if client.post(&prompt_url).json(&body).send().await.is_err() {
                            // The attached OpenCode became unreachable mid-session.
                            // There is no subprocess to detect exiting the way
                            // AcpDriver's connection task would -- surface this the
                            // same way a lost ACP connection does: stop producing
                            // events. The caller's next_event() will observe the
                            // channel close in that case.
                            return;
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
                        handle_event(event, &mut message_roles, &event_tx, &interrupt_requested);
                    }
                }
            }
        }
    }
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

        let mut driver = HttpAttachDriver::attach(&config)
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

        match HttpAttachDriver::attach(&config).await {
            Err(DriverError::AttachSessionNotFound { .. }) => {}
            Err(other) => panic!("expected AttachSessionNotFound, got a different error: {other}"),
            Ok(_) => panic!("attach to a missing session_id must fail closed, not succeed"),
        }

        let _ = child.kill();
        let _ = child.wait();
    }
}
