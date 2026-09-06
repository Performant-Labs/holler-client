//! Live WebSocket session against holler-server: `auth` → `hello` →
//! heartbeat, with exponential-backoff reconnect (issue #24).
//!
//! # Scope: resume with a credential, not redeem a token
//!
//! This module **only** maintains a session using a credential that is
//! already persisted (from a prior `join`, however it was obtained — see
//! `holler-server` ADR 0003, `docs/protocol/v1.md` §4: "Reconnect uses
//! the credential again."). It never sends a join token.
//!
//! It is not the real implementation behind `holler join`'s redeem step
//! — that's [`crate::join`]. The redeem gap this module used to describe
//! (no wire frame for turning a join token into a credential) is now
//! closed at the protocol level: `holler-server` ADR 0015 adds a
//! dedicated `join`/`join_ok` frame pair (`docs/protocol/v1.md` §4.1),
//! and the implementation on both sides is in flight (or done — check
//! `crate::join`'s own module doc for current status). This module still
//! doesn't send a join token itself; it only resumes with an
//! already-persisted credential, same as before.
//!
//! # `hello`'s own `token_id` field is still unset
//!
//! `docs/protocol/v1.md` §3/§6 says the client envelope `from` is the
//! **public `token_id`** — [`connect_and_auth`] sends it correctly on
//! `auth` (issue #47; [`crate::credential::PersistedCredential`] now
//! persists it). `hello`'s own `body.token_id` field (distinct from
//! `body.client_id`) is left unset (an `Option`, so this is wire-legal)
//! since nothing consumes it server-side yet — a smaller, independent
//! gap, worth a future story rather than a blocker here.
//!
//! # Heartbeat and backoff numbers
//!
//! [`heartbeat_interval`] is 15s by default, matching `holler-server`'s
//! `research/dropped-connections` memo (`docs/research-dropped-connections.md`):
//! "3 missed heartbeats = dead" is a near-universal convention (SSH's
//! `ClientAliveCountMax`/`ServerAliveCountMax` default, Buzz's own
//! `SLOW_CLIENT_GRACE_LIMIT`), which is why [`stale_after`] derives as
//! `heartbeat_interval() * 3` (45s by default) — the same threshold
//! `holler-server`'s roster (issue #32) uses to move a session from
//! `connected` to `reconnecting`. This was originally guessed at 20s
//! before the memo was found; fixed to avoid a healthy client flapping
//! into `reconnecting` on the server's roster between heartbeats. The
//! backoff schedule in [`backoff_with_full_jitter`] (base 1s, cap 30s,
//! full jitter) is the same memo's recommendation, grounded in AWS's
//! well-known "Exponential Backoff and Jitter" post.
//!
//! # Live state is file-based, not a control socket
//!
//! `holler run` (the process that owns the live socket) and `holler
//! status` / `holler detach` (separate process invocations) share no
//! memory. Rather than add a control socket or PID-signal protocol for
//! this first story, [`ConnectionStateStore`] persists the live state to
//! a small JSON file that `run` updates on every transition and the
//! other commands read. Staleness (a `run` process that died without
//! cleaning up) is handled by an age check ([`current_state`][ConnectionStateStore::current_state]'s
//! `max_age`), not a PID-liveness check — `/proc` isn't portable to
//! macOS and a real cross-platform liveness probe is more machinery than
//! this story's scope justifies. `detach` uses the same mechanism in
//! reverse: it drops a marker file that the live `run` process polls for
//! and, on seeing it, closes its socket and exits.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::acp_driver::{DriverEvent, DriverStatus};
use crate::config::{SessionConfig, SessionRegistry};
use crate::credential::{resolve_state_dir, CredentialError, STATE_DIR_ENV};
use crate::debug::{self, DebugConfig};
use crate::proto::{
    self, Body, ErrorBody, InterruptBody, PromptBody, CODE_SESSION_UNAVAILABLE,
    CODE_UNAUTHENTICATED, CODE_UNKNOWN_SESSION,
};
use crate::query;
use crate::reply_coalescer::ReplyCoalescer;
use crate::session_manager::{ManagerError, SessionManager};
use crate::status::SessionStatus;

/// Every session's event-forwarding channel, keyed by session name —
/// see [`SessionManager::take_event_channels`] for why the wire layer
/// owns these directly rather than going through
/// [`SessionManager::next_event`].
pub type EventChannels = HashMap<String, mpsc::UnboundedReceiver<DriverEvent>>;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// How often this client sends its own `ping` (and the window it gives
/// the server to `pong` back before declaring the connection dead).
/// 15s to match holler-server's roster/presence TTL (issue #32), which
/// assumes 3 missed heartbeats at this interval = 45s before marking a
/// session `reconnecting` (`research/dropped-connections`,
/// docs/research-dropped-connections.md: "3 missed = dead" is a
/// near-universal convention — SSH's ClientAliveCountMax/
/// ServerAliveCountMax default, Buzz's own SLOW_CLIENT_GRACE_LIMIT).
/// This value was originally guessed at 20s before that research memo
/// was found; fixed here so a healthy client doesn't flap into
/// "reconnecting" on the server's roster between heartbeats.
///
/// Overridable via `HOLLER_HEARTBEAT_INTERVAL_MS` (parsed once per call,
/// not cached) purely so an integration test can shrink [`stale_after`]'s
/// window instead of sleeping out a real 45s to prove `mark_connected` is
/// refreshed on every heartbeat (issue #50) rather than once at connect.
/// Production code never sets this; the default is unchanged.
pub fn heartbeat_interval() -> Duration {
    std::env::var("HOLLER_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(15))
}
/// Exponential-backoff base delay (first retry is `random(0, BASE)`).
pub const BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Backoff never waits longer than this between reconnect attempts.
pub const BACKOFF_CAP: Duration = Duration::from_secs(30);
/// A persisted live-state file older than this is treated as stale (the
/// `run` process that wrote it likely died without cleaning up), i.e.
/// "not connected" — three missed heartbeats' worth of grace. See
/// [`heartbeat_interval`] for why this is a function, not a `const`.
pub fn stale_after() -> Duration {
    heartbeat_interval() * 3
}
/// How often `run`'s session loop polls for a detach request while a
/// connection is live.
const DETACH_POLL_INTERVAL: Duration = Duration::from_millis(500);

const CONNECTION_STATE_FILE: &str = "connection_state.json";
const DETACH_REQUEST_FILE: &str = "detach_request";

/// The live connection state a `holler status` invocation (a different
/// process than the one running the socket) can observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveState {
    /// No credential, no live/staged connection, or a state file too
    /// stale to trust.
    Disconnected,
    /// A connection attempt (first-ever or a retry after a drop) is in
    /// flight but not yet past `auth`+`hello`.
    Connecting,
    /// `auth`+`hello` completed; the socket is up.
    Connected,
    /// A previously-live connection dropped and a reconnect attempt is
    /// pending or in backoff.
    Reconnecting,
}

#[derive(Serialize, Deserialize)]
struct PersistedLiveState {
    state: String,
    updated_at: String,
}

/// File-based handoff of live connection state between the `holler run`
/// process and other `holler` invocations (`status`, `detach`). See the
/// module docs for why this is a file, not a control socket.
pub struct ConnectionStateStore {
    state_path: PathBuf,
    detach_path: PathBuf,
}

impl ConnectionStateStore {
    /// Opens the store in the same directory `holler join`'s
    /// [`crate::credential::CredentialStore`] uses.
    pub fn open() -> Result<Self, CredentialError> {
        let dir = resolve_state_dir(std::env::var(STATE_DIR_ENV).ok(), std::env::var("HOME").ok())?;
        Ok(ConnectionStateStore {
            state_path: dir.join(CONNECTION_STATE_FILE),
            detach_path: dir.join(DETACH_REQUEST_FILE),
        })
    }

    /// Opens a store rooted at an explicit directory, bypassing
    /// environment resolution. Used by tests for an isolated,
    /// temp-dir-backed store.
    #[cfg(test)]
    pub fn at_dir(dir: PathBuf) -> Self {
        ConnectionStateStore {
            state_path: dir.join(CONNECTION_STATE_FILE),
            detach_path: dir.join(DETACH_REQUEST_FILE),
        }
    }

    fn write_state(&self, state: &str) -> io::Result<()> {
        if let Some(parent) = self.state_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let persisted = PersistedLiveState {
            state: state.to_string(),
            updated_at: now_ts(),
        };
        let contents = serde_json::to_string(&persisted)
            .unwrap_or_else(|_| r#"{"state":"disconnected","updated_at":""}"#.to_string());
        std::fs::write(&self.state_path, contents)
    }

    /// Records that a connection attempt has started (first try or a
    /// retry after a drop).
    pub fn mark_connecting(&self, is_retry: bool) {
        let _ = self.write_state(if is_retry { "reconnecting" } else { "connecting" });
    }

    /// Records that `auth`+`hello` completed and the socket is live.
    pub fn mark_connected(&self) {
        let _ = self.write_state("connected");
    }

    /// Removes the live-state file: "no live/staged connection". Never
    /// an error to call when nothing is persisted (mirrors
    /// [`crate::credential::CredentialStore::delete`]'s idempotence).
    pub fn clear(&self) {
        let _ = std::fs::remove_file(&self.state_path);
    }

    /// Resolves the current [`LiveState`] for `holler status`: missing,
    /// unreadable, unparseable, or older-than-`max_age` all read as
    /// [`LiveState::Disconnected`] rather than erroring — a status
    /// document must always render something.
    pub fn current_state(&self, max_age: Duration) -> LiveState {
        let contents = match std::fs::read_to_string(&self.state_path) {
            Ok(c) => c,
            Err(_) => return LiveState::Disconnected,
        };
        let persisted: PersistedLiveState = match serde_json::from_str(&contents) {
            Ok(p) => p,
            Err(_) => return LiveState::Disconnected,
        };
        let updated_at = match OffsetDateTime::parse(&persisted.updated_at, &Rfc3339) {
            Ok(t) => t,
            Err(_) => return LiveState::Disconnected,
        };
        // Compare as Unix seconds rather than `time::Duration` arithmetic,
        // so this never has to reconcile `time`'s duration type with
        // `std::time::Duration` (`max_age`'s type).
        let age_secs = OffsetDateTime::now_utc().unix_timestamp() - updated_at.unix_timestamp();
        if age_secs < 0 || age_secs as u64 >= max_age.as_secs() {
            return LiveState::Disconnected;
        }
        match persisted.state.as_str() {
            "connecting" => LiveState::Connecting,
            "connected" => LiveState::Connected,
            "reconnecting" => LiveState::Reconnecting,
            _ => LiveState::Disconnected,
        }
    }

    /// Drops a marker asking the live `run` process (if any) to close
    /// its connection and exit.
    pub fn request_detach(&self) -> io::Result<()> {
        if let Some(parent) = self.detach_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.detach_path, b"")
    }

    /// Whether a detach has been requested (polled by the session loop).
    pub fn is_detach_requested(&self) -> bool {
        self.detach_path.exists()
    }

    /// Clears the detach marker. Idempotent.
    pub fn clear_detach_request(&self) {
        let _ = std::fs::remove_file(&self.detach_path);
    }
}

fn now_ts() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// Why [`connect_and_auth`] or the session loop ended.
#[derive(Debug)]
pub enum ConnectError {
    /// The server rejected the credential (`error`/`unauthenticated`),
    /// or the socket closed in a way that means the same is true.
    /// **Terminal**: retrying with the same credential will not
    /// succeed, so [`run`] does not apply backoff-and-retry for this —
    /// see "wrong/revoked credential surfaces as a clear failure" in the
    /// issue.
    Unauthenticated(String),
    /// Any other failure to reach or complete the handshake (connect
    /// refused, malformed frame, timeout, socket closed mid-handshake).
    /// **Transient**: [`run`] retries with backoff.
    Transport(String),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Unauthenticated(msg) => write!(f, "authentication failed: {msg}"),
            ConnectError::Transport(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ConnectError {}

/// Sessions whose harness is confirmed runnable right now (ADR-0001:
/// "advertise only what is real") — the one filter `hello`'s `sessions`
/// list and `presence`'s session rows both apply.
fn confirmed_sessions<'a>(
    registry: &'a SessionRegistry,
    confirmed: &[String],
) -> Vec<&'a SessionConfig> {
    registry
        .sessions()
        .iter()
        .filter(|s| confirmed.contains(&s.harness))
        .collect()
}

/// Builds this connection's `presence` session rows (issue #49): one
/// `{name, harness, busy}` row — the same shape `holler status` reports
/// ([`SessionStatus`]) — per confirmed session, `busy` read live from
/// `manager` when one is running. A session with no live
/// [`SessionManager`] entry (spawning it failed; see
/// `crate::main`/`run_run`) reports `busy: false` — hello's own sessions
/// list already only advertises this same confirmed set, so this never
/// claims a session is real when nothing backs it.
async fn build_presence_sessions(
    registry: &SessionRegistry,
    confirmed: &[String],
    manager: Option<&SessionManager>,
) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for session in confirmed_sessions(registry, confirmed) {
        let busy = match manager {
            Some(m) => m.is_busy(&session.name).await.unwrap_or(false),
            None => false,
        };
        let status = SessionStatus {
            name: session.name.clone(),
            harness: session.harness.clone(),
            busy,
        };
        rows.push(serde_json::to_value(status).expect("SessionStatus always serializes"));
    }
    rows
}

/// Dials `server_url`, sends `auth` with the persisted credential, and
/// waits for the server's `hello` (spec §4: `connect -> auth -> hello
/// (both ways)`). Sends this client's own `hello`, then a `presence`
/// frame (issue #49; holler-server issue #52: every (re)connect
/// re-advertises a complete, fresh session list — never assumes the
/// server remembers anything from before a drop) once the server's hello
/// has arrived. Never sends or logs the credential in the clear beyond
/// this one `auth` frame.
#[allow(clippy::too_many_arguments)]
async fn connect_and_auth(
    server_url: &str,
    credential: &str,
    token_id: &str,
    client_id: &str,
    hostname: &str,
    registry: &SessionRegistry,
    session_manager: Option<&SessionManager>,
    cfg: DebugConfig,
) -> Result<WsStream, ConnectError> {
    let (mut ws, _response) = tokio_tungstenite::connect_async(server_url)
        .await
        .map_err(|e| ConnectError::Transport(format!("connect to {server_url} failed: {e}")))?;

    let auth = proto::auth_envelope(token_id, credential);
    let raw = proto::encode(&auth).expect("v1 auth envelope always serializes");
    debug::outgoing(cfg, "auth")
        .id(&auth.id)
        .peer(token_id)
        .frame(|| debug::redact_secret(&raw, credential))
        .emit();
    ws.send(Message::Text(raw.into()))
        .await
        .map_err(|e| ConnectError::Transport(format!("failed to send auth: {e}")))?;

    let reply_raw = match ws.next().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        Some(Ok(_)) => {
            return Err(ConnectError::Transport(
                "unexpected non-text frame while awaiting hello".to_string(),
            ))
        }
        Some(Err(e)) => {
            return Err(ConnectError::Transport(format!(
                "socket error awaiting hello: {e}"
            )))
        }
        None => {
            return Err(ConnectError::Transport(
                "connection closed awaiting hello".to_string(),
            ))
        }
    };
    let envelope = proto::decode(&reply_raw)
        .map_err(|e| ConnectError::Transport(format!("malformed frame awaiting hello: {e}")))?;
    debug::incoming(cfg, envelope.msg_type.as_wire_str())
        .id(&envelope.id)
        .peer(&envelope.from)
        .frame(|| reply_raw.clone())
        .emit();
    match envelope.body {
        Body::Hello(_) => {}
        Body::Error(ErrorBody { code, message, .. }) if code == CODE_UNAUTHENTICATED => {
            return Err(ConnectError::Unauthenticated(
                message.unwrap_or_else(|| "authentication failed".to_string()),
            ));
        }
        Body::Error(ErrorBody { code, message, .. }) => {
            return Err(ConnectError::Transport(format!(
                "server error {code}: {}",
                message.unwrap_or_default()
            )));
        }
        other => {
            return Err(ConnectError::Transport(format!(
                "expected `hello` after auth, got {:?}",
                std::mem::discriminant(&other)
            )))
        }
    }

    // "Advertise only what is real" (ADR-0001): only genuinely-confirmed
    // runnable harnesses, and only sessions whose harness is one of them.
    let confirmed = registry.confirmed_harnesses();
    let sessions = confirmed_sessions(registry, &confirmed)
        .into_iter()
        .map(|s| proto::HelloSession {
            name: s.name.clone(),
            harness: s.harness.clone(),
        })
        .collect();
    let features = query::CLIENT_FEATURES
        .iter()
        .map(|s| s.to_string())
        .collect();
    let hello = proto::client_hello(
        client_id,
        hostname,
        client_id,
        features,
        confirmed.clone(),
        sessions,
    );
    let raw = proto::encode(&hello).expect("v1 hello envelope always serializes");
    debug::outgoing(cfg, "hello")
        .id(&hello.id)
        .peer(client_id)
        .field("hostname", hostname)
        .frame(|| raw.clone())
        .emit();
    ws.send(Message::Text(raw.into()))
        .await
        .map_err(|e| ConnectError::Transport(format!("failed to send client hello: {e}")))?;

    let presence_rows = build_presence_sessions(registry, &confirmed, session_manager).await;
    let presence = proto::client_presence(client_id, presence_rows);
    let raw = proto::encode(&presence).expect("v1 presence envelope always serializes");
    debug::outgoing(cfg, "presence")
        .id(&presence.id)
        .peer(client_id)
        .frame(|| raw.clone())
        .emit();
    ws.send(Message::Text(raw.into()))
        .await
        .map_err(|e| ConnectError::Transport(format!("failed to send client presence: {e}")))?;

    Ok(ws)
}

/// Why the session loop returned.
enum LoopExit {
    /// `holler detach` asked us to close and stop; the caller should not
    /// reconnect.
    Detached,
    /// The connection ended (peer close, socket error, missed
    /// heartbeat); the caller should reconnect with backoff.
    Dropped(String),
}

/// Waits for the next [`DriverEvent`] from *any* session in `channels`,
/// tagged with that session's name. `channels` holds the raw receivers
/// [`SessionManager::take_event_channels`] handed over — see that
/// method's docs for why polling them directly here (rather than through
/// [`SessionManager::next_event`]) is what lets an idle session's events
/// never block another session's prompt/interrupt handling.
///
/// Never resolves if `channels` is empty (nothing to ever produce an
/// event) — callers select! this alongside other branches, so that's
/// exactly "this branch never wins," not a hang.
async fn recv_any_event(channels: &mut EventChannels) -> Option<(String, DriverEvent)> {
    if channels.is_empty() {
        return std::future::pending().await;
    }
    std::future::poll_fn(|cx| {
        for (name, rx) in channels.iter_mut() {
            if let std::task::Poll::Ready(Some(event)) = rx.poll_recv(cx) {
                return std::task::Poll::Ready(Some((name.clone(), event)));
            }
            // Pending, or this session's channel closed for good: keep
            // checking the others rather than resolving `None` for the
            // whole batch.
        }
        std::task::Poll::Pending
    })
    .await
}

/// Sends one `reply` frame carrying `chunks` — several streamed updates
/// coalesced into a single frame (issue #83).
///
/// `ReplyBody` has always had a `chunks` field for this, and holler-server
/// joins `text` and `chunks` in arrival order (spec §10), so a batch is
/// reassembled identically to the one-frame-per-update stream it replaces.
/// `text` is left `None`: everything rides in `chunks`.
///
/// Returns `false` if the socket write failed, so the caller can drop the
/// connection. An empty non-terminal batch sends nothing — only a `done`
/// frame is worth sending with no text.
#[allow(clippy::too_many_arguments)]
async fn send_reply_chunks(
    ws: &mut WsStream,
    cfg: DebugConfig,
    reply_id: &str,
    client_id: &str,
    session: &str,
    chunks: Vec<String>,
    done: bool,
) -> bool {
    if chunks.is_empty() && !done {
        return true;
    }
    let coalesced = chunks.len();
    // The actual reply content — the reason a human is watching this log
    // at all. Joined before the wire-shape split below consumes `chunks`,
    // so it reflects this frame's text regardless of which wire field
    // ends up carrying it. Never a secret, so it belongs at `quiet`, not
    // locked behind `noisy`'s full frame dump.
    let preview = chunks.join("");
    // A batch of one keeps the pre-coalescing wire shape exactly: `text`
    // set, `chunks` empty. Only a genuine batch uses `chunks`, so the
    // common short-reply case is byte-identical to before and stays
    // readable to any peer that only looks at `text`.
    let (text, chunks) = if chunks.len() == 1 {
        (chunks.into_iter().next(), Vec::new())
    } else {
        (None, chunks)
    };
    let reply = proto::reply(reply_id, client_id, session, text, chunks, done);
    let Ok(raw) = proto::encode(&reply) else {
        return true;
    };
    let mut outbound = debug::outgoing(cfg, "reply")
        .id(reply_id)
        .peer(client_id)
        .field("session", session)
        .field("chunks", coalesced.to_string())
        .field("done", done.to_string());
    if !preview.is_empty() {
        outbound = outbound.field("text", preview);
    }
    outbound.frame(|| raw.clone()).emit();
    ws.send(Message::Text(raw.into())).await.is_ok()
}

/// Services one live connection: answers `ping` with `pong` (including
/// hostname, per the issue), sends this client's own heartbeat `ping` on
/// [`heartbeat_interval`], polls for a detach request, and (issue #49)
/// dispatches inbound `prompt`/`interrupt` to `session_manager` while
/// streaming its `reply`/`ack` frames back out. Returns when the
/// connection ends for any reason.
#[allow(clippy::too_many_arguments)]
async fn session_loop(
    mut ws: WsStream,
    client_id: &str,
    hostname: &str,
    registry: &SessionRegistry,
    state: &ConnectionStateStore,
    session_manager: Option<&SessionManager>,
    event_channels: &mut EventChannels,
    cfg: DebugConfig,
) -> LoopExit {
    state.mark_connected();

    let mut heartbeat = tokio::time::interval(heartbeat_interval());
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // interval's first tick fires immediately; skip it

    let mut detach_poll = tokio::time::interval(DETACH_POLL_INTERVAL);
    detach_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut awaiting_pong = false;
    // The envelope `id` of the most recent `prompt` dispatched to each
    // session, so this session's streamed `reply` frames can reuse the
    // request id (spec §3: "Replies reuse the request id"). Scoped to
    // this one connection — a turn that outlives a reconnect loses this
    // correlation, which is consistent with holler-server issue #52's
    // "ask again" contract (nothing about a pre-drop turn's identity is
    // assumed to survive the drop).
    let mut last_prompt_id: HashMap<String, String> = HashMap::new();
    // Issue #83: batch streamed updates rather than sending one frame per
    // ACP event, where ~130 bytes of every ~200-byte frame is the same
    // invariant preamble repeated per chunk.
    let mut coalescer = ReplyCoalescer::default();

    loop {
        // Recomputed each iteration and captured by value, so the flush
        // branch's future never borrows `coalescer` across the await —
        // the arm body needs it mutably.
        let flush_at = coalescer.next_deadline();

        tokio::select! {
            incoming = ws.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(envelope) = proto::decode(&text) else {
                            // A single malformed frame from an otherwise-good
                            // server is not worth tearing the session down
                            // over; the dispatcher (issue #30) can be
                            // stricter once it exists.
                            continue;
                        };
                        // A prompt's text is the whole reason this frame
                        // exists, not incidental metadata — surface it at
                        // `quiet`, not only inside the `noisy` frame dump.
                        // Read before the match below moves `envelope.body`.
                        let prompt_preview = match &envelope.body {
                            Body::Prompt(PromptBody { text, .. }) => Some(text.as_str()),
                            _ => None,
                        };
                        let mut inbound = debug::incoming(cfg, envelope.msg_type.as_wire_str())
                            .id(&envelope.id)
                            .peer(&envelope.from);
                        if let Some(t) = prompt_preview {
                            inbound = inbound.field("text", t);
                        }
                        inbound.frame(|| text.to_string()).emit();
                        match envelope.body {
                            Body::Ping(_) => {
                                let pong = proto::pong_reply(&envelope.id, client_id, hostname);
                                let Ok(raw) = proto::encode(&pong) else { continue };
                                debug::outgoing(cfg, "pong")
                                    .id(&envelope.id)
                                    .peer(client_id)
                                    .frame(|| raw.clone())
                                    .emit();
                                if ws.send(Message::Text(raw.into())).await.is_err() {
                                    return LoopExit::Dropped("failed to send pong".to_string());
                                }
                            }
                            Body::Pong(_) => {
                                awaiting_pong = false;
                            }
                            Body::Error(ErrorBody { code, message, .. }) => {
                                return LoopExit::Dropped(format!(
                                    "server error {code}: {}",
                                    message.unwrap_or_default()
                                ));
                            }
                            Body::Query(q) => {
                                // Inside this loop the socket is, by
                                // definition, live — no need to consult
                                // `state`'s file for `LiveState`.
                                let reply = match query::dispatch(
                                    &q,
                                    envelope.v,
                                    Some(client_id),
                                    registry,
                                    hostname,
                                    LiveState::Connected,
                                ) {
                                    Ok(body) => proto::query_ok_reply(&envelope.id, client_id, body),
                                    Err(err) => proto::error_reply(
                                        &envelope.id,
                                        client_id,
                                        err.code(),
                                        Some(&q.cmd),
                                        &err.to_string(),
                                    ),
                                };
                                let Ok(raw) = proto::encode(&reply) else { continue };
                                debug::outgoing(cfg, reply.msg_type.as_wire_str())
                                    .id(&envelope.id)
                                    .peer(client_id)
                                    .field("cmd", q.cmd.as_str())
                                    .frame(|| raw.clone())
                                    .emit();
                                if ws.send(Message::Text(raw.into())).await.is_err() {
                                    return LoopExit::Dropped("failed to send query reply".to_string());
                                }
                            }
                            Body::Prompt(PromptBody { session, text, .. }) => {
                                let reply = match session_manager {
                                    Some(manager) => match manager.prompt(&session, text) {
                                        Ok(()) => {
                                            last_prompt_id.insert(session.clone(), envelope.id.clone());
                                            None
                                        }
                                        Err(ManagerError::UnknownSession(_)) => Some(proto::error_reply(
                                            &envelope.id,
                                            client_id,
                                            CODE_UNKNOWN_SESSION,
                                            None,
                                            &format!("no such session: {session}"),
                                        )),
                                        Err(other) => Some(proto::error_reply(
                                            &envelope.id,
                                            client_id,
                                            CODE_SESSION_UNAVAILABLE,
                                            None,
                                            &other.to_string(),
                                        )),
                                    },
                                    None => Some(proto::error_reply(
                                        &envelope.id,
                                        client_id,
                                        CODE_UNKNOWN_SESSION,
                                        None,
                                        &format!("no such session: {session}"),
                                    )),
                                };
                                if let Some(reply) = reply {
                                    let Ok(raw) = proto::encode(&reply) else { continue };
                                    debug::outgoing(cfg, "error")
                                        .id(&envelope.id)
                                        .peer(client_id)
                                        .field("reason", "unroutable prompt")
                                        .frame(|| raw.clone())
                                        .emit();
                                    if ws.send(Message::Text(raw.into())).await.is_err() {
                                        return LoopExit::Dropped("failed to send prompt error reply".to_string());
                                    }
                                }
                            }
                            Body::Interrupt(InterruptBody { session }) => {
                                let reply = match session_manager {
                                    Some(manager) => match manager.interrupt(&session).await {
                                        Ok(_outcome) => proto::ack_reply(&envelope.id, client_id),
                                        Err(ManagerError::UnknownSession(_)) => proto::error_reply(
                                            &envelope.id,
                                            client_id,
                                            CODE_UNKNOWN_SESSION,
                                            None,
                                            &format!("no such session: {session}"),
                                        ),
                                        Err(other) => proto::error_reply(
                                            &envelope.id,
                                            client_id,
                                            CODE_SESSION_UNAVAILABLE,
                                            None,
                                            &other.to_string(),
                                        ),
                                    },
                                    None => proto::error_reply(
                                        &envelope.id,
                                        client_id,
                                        CODE_UNKNOWN_SESSION,
                                        None,
                                        &format!("no such session: {session}"),
                                    ),
                                };
                                let Ok(raw) = proto::encode(&reply) else { continue };
                                debug::outgoing(cfg, reply.msg_type.as_wire_str())
                                    .id(&envelope.id)
                                    .peer(client_id)
                                    .field("session", session.as_str())
                                    .frame(|| raw.clone())
                                    .emit();
                                if ws.send(Message::Text(raw.into())).await.is_err() {
                                    return LoopExit::Dropped("failed to send interrupt reply".to_string());
                                }
                            }
                            // `hello`/`presence`/`reply`/`ack`/anything
                            // else: nothing this client acts on when it
                            // arrives inbound.
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return LoopExit::Dropped("connection closed by peer".to_string());
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        // WebSocket protocol-level ping (distinct from
                        // Holler's own application-level `ping` body).
                        if ws.send(Message::Pong(payload)).await.is_err() {
                            return LoopExit::Dropped("failed to answer WS-level ping".to_string());
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Binary(_) | Message::Frame(_))) => {
                        // Spec §13: binary frames are not part of v1. The
                        // server fails closed on these; we just ignore —
                        // we are not the side enforcing the wire contract.
                    }
                    Some(Err(e)) => {
                        return LoopExit::Dropped(format!("socket read error: {e}"));
                    }
                }
            }
            _ = heartbeat.tick() => {
                if awaiting_pong {
                    return LoopExit::Dropped(
                        "no pong received within one heartbeat interval".to_string(),
                    );
                }
                let ping = proto::heartbeat_ping(client_id, hostname);
                let Ok(raw) = proto::encode(&ping) else { continue };
                debug::outgoing(cfg, "ping")
                    .id(&ping.id)
                    .peer(client_id)
                    .field("kind", "heartbeat")
                    .frame(|| raw.clone())
                    .emit();
                if ws.send(Message::Text(raw.into())).await.is_err() {
                    return LoopExit::Dropped("failed to send heartbeat ping".to_string());
                }
                awaiting_pong = true;
                // Issue #50: `mark_connected()` at the top of this
                // function only stamps `updated_at` once, at connect
                // time. A connection healthy for longer than
                // `stale_after()` (with no reconnect to re-stamp it) would
                // otherwise read as `Disconnected` from
                // `ConnectionStateStore::current_state` even while very
                // much alive — which made `holler detach`'s "is there
                // anything live to detach" guard skip `request_detach()`
                // entirely. Refreshing here, every heartbeat, keeps the
                // persisted timestamp within one `heartbeat_interval()` of
                // now for as long as the connection is actually healthy.
                state.mark_connected();
            }
            _ = detach_poll.tick() => {
                if state.is_detach_requested() {
                    let _ = ws.close(None).await;
                    return LoopExit::Detached;
                }
            }
            tagged = recv_any_event(event_channels) => {
                let Some((name, event)) = tagged else { continue };
                // Reuse the id of the `prompt` that started this turn
                // (spec §3); a session that somehow never had one
                // (e.g. a turn already in flight before this connection
                // — not something a fresh `holler run` can produce
                // today) still gets a well-formed, if uncorrelated, id.
                let reply_id = last_prompt_id
                    .get(&name)
                    .cloned()
                    .unwrap_or_else(proto::new_id);
                match event {
                    DriverEvent::Update(text) => {
                        // Buffered, not sent: the flush branch below or the
                        // turn's end releases it. Only a byte-cap overflow
                        // sends immediately.
                        if let Some(batch) = coalescer.push(&name, text, Instant::now()) {
                            if !send_reply_chunks(
                                &mut ws, cfg, &reply_id, client_id, &name, batch, false,
                            )
                            .await
                            {
                                return LoopExit::Dropped("failed to send reply".to_string());
                            }
                        }
                    }
                    DriverEvent::StopReason(_) => {
                        // Straggling chunks ride out *with* the terminal
                        // frame: nothing is dropped, and `done` is never
                        // delayed behind the debounce timer.
                        let tail = coalescer.take(&name);
                        last_prompt_id.remove(&name);
                        if !send_reply_chunks(
                            &mut ws, cfg, &reply_id, client_id, &name, tail, true,
                        )
                        .await
                        {
                            return LoopExit::Dropped("failed to send reply".to_string());
                        }
                    }
                    // Presence/busy tracking is answered by the
                    // `presence` frame at (re)connect time, not streamed
                    // mid-turn — see module docs on issue #52's
                    // "ask again" contract.
                    DriverEvent::Status(DriverStatus::Working | DriverStatus::Idle | DriverStatus::Blocked) => {}
                }
            }
            // Debounce expiry: release whichever sessions' windows have
            // closed. Never wins when nothing is buffered (`pending`
            // never resolves), so an idle connection has no extra wakeups.
            _ = async move {
                match flush_at {
                    Some(deadline) => {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {
                for (session, chunks) in coalescer.due(Instant::now()) {
                    let reply_id = last_prompt_id
                        .get(&session)
                        .cloned()
                        .unwrap_or_else(proto::new_id);
                    if !send_reply_chunks(
                        &mut ws, cfg, &reply_id, client_id, &session, chunks, false,
                    )
                    .await
                    {
                        return LoopExit::Dropped("failed to send reply".to_string());
                    }
                }
            }
        }
    }
}

/// AWS "Full Jitter" backoff: `random(0, min(cap, base * 2^attempt))`,
/// `attempt` counting consecutive failures so far (the first retry uses
/// `attempt == 1`, i.e. `2^0`). See module docs for why these numbers
/// are a judgment call rather than a measured recommendation.
fn backoff_with_full_jitter(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let shift = attempt.saturating_sub(1).min(20); // cap the shift so 2^shift can't overflow u64 millis
    let exp_millis = (base.as_millis() as u64).saturating_mul(1u64 << shift);
    let capped_millis = exp_millis.min(cap.as_millis() as u64);
    let jitter_millis = random_u64() % (capped_millis + 1);
    Duration::from_millis(jitter_millis)
}

/// Non-cryptographic randomness for backoff jitter. `RandomState`'s
/// per-instance keys are seeded from OS entropy (`std`'s
/// `sys::hashmap_random_keys`); good enough for "don't have every
/// reconnecting client retry in lockstep", the only property jitter
/// needs here, without adding a `rand`-family dependency for it.
fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    RandomState::new().build_hasher().finish()
}

/// Sleeps for `delay`, but wakes early (returning `true`) if a detach is
/// requested mid-sleep — so `holler detach` isn't left waiting out a
/// full (possibly 30s) backoff window.
async fn sleep_or_detach(delay: Duration, state: &ConnectionStateStore) -> bool {
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return false;
        }
        tokio::time::sleep((deadline - now).min(DETACH_POLL_INTERVAL)).await;
        if state.is_detach_requested() {
            return true;
        }
    }
}

/// Runs the connect → session → reconnect-with-backoff loop until either
/// a detach is requested (`Ok(())`) or the credential is rejected
/// (`Err(ConnectError::Unauthenticated)`). Never returns `Err` for a
/// merely transient failure — those are retried internally.
// This is the one public entry point threading together everything a live
// connection needs (identity, target, local session state); splitting it
// into a config struct for a single-caller function would add a layer of
// indirection without a second call site to justify it.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    server_url: &str,
    credential: &str,
    token_id: &str,
    client_id: &str,
    hostname: &str,
    registry: &SessionRegistry,
    state: &ConnectionStateStore,
    session_manager: Option<&SessionManager>,
    event_channels: &mut EventChannels,
    cfg: DebugConfig,
) -> Result<(), ConnectError> {
    let mut attempt: u32 = 0;

    loop {
        if state.is_detach_requested() {
            state.clear_detach_request();
            state.clear();
            return Ok(());
        }

        state.mark_connecting(attempt > 0);
        match connect_and_auth(
            server_url,
            credential,
            token_id,
            client_id,
            hostname,
            registry,
            session_manager,
            cfg,
        )
        .await
        {
            Ok(ws) => {
                attempt = 0; // a successful handshake resets the backoff schedule
                match session_loop(
                    ws,
                    client_id,
                    hostname,
                    registry,
                    state,
                    session_manager,
                    event_channels,
                    cfg,
                )
                .await
                {
                    LoopExit::Detached => {
                        debug::local(cfg, "conn")
                            .field("event", "detached")
                            .emit();
                        state.clear_detach_request();
                        state.clear();
                        return Ok(());
                    }
                    LoopExit::Dropped(reason) => {
                        // Always emitted, at every debug level: this is
                        // the line an operator alerts on, so it must not
                        // vanish just because `--debug` is off.
                        debug::warn(cfg, "conn")
                            .field("event", "dropped")
                            .field("reason", reason.as_str())
                            .emit();
                    }
                }
            }
            Err(ConnectError::Unauthenticated(msg)) => {
                state.clear();
                return Err(ConnectError::Unauthenticated(msg));
            }
            Err(ConnectError::Transport(msg)) => {
                debug::warn(cfg, "conn")
                    .field("event", "connect_failed")
                    .field("reason", msg.as_str())
                    .emit();
            }
        }

        attempt += 1;
        state.mark_connecting(true);
        let delay = backoff_with_full_jitter(attempt, BACKOFF_BASE, BACKOFF_CAP);
        if sleep_or_detach(delay, state).await {
            state.clear_detach_request();
            state.clear();
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_by_cap() {
        for attempt in 1..=50 {
            let delay = backoff_with_full_jitter(attempt, BACKOFF_BASE, BACKOFF_CAP);
            assert!(delay <= BACKOFF_CAP, "attempt {attempt} exceeded cap: {delay:?}");
        }
    }

    #[test]
    fn backoff_grows_then_caps() {
        // Attempt 1: random in [0, 1s]. Attempt 6: 2^5 * 1s = 32s, capped
        // to [0, 30s]. Sample many draws and check the observed max grows
        // between early attempts, then saturates at the cap.
        let max_delay = |attempt: u32| -> Duration {
            (0..200)
                .map(|_| backoff_with_full_jitter(attempt, BACKOFF_BASE, BACKOFF_CAP))
                .max()
                .unwrap()
        };
        let early = max_delay(1);
        let later = max_delay(6);
        assert!(later > early, "backoff should grow with attempt count");
        assert!(later <= BACKOFF_CAP);
    }

    #[test]
    fn live_state_missing_file_is_disconnected() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        assert_eq!(store.current_state(stale_after()), LiveState::Disconnected);
    }

    #[test]
    fn live_state_round_trips_connected() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        store.mark_connected();
        assert_eq!(store.current_state(stale_after()), LiveState::Connected);
    }

    #[test]
    fn live_state_round_trips_reconnecting() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        store.mark_connecting(true);
        assert_eq!(store.current_state(stale_after()), LiveState::Reconnecting);
    }

    #[test]
    fn live_state_round_trips_connecting() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        store.mark_connecting(false);
        assert_eq!(store.current_state(stale_after()), LiveState::Connecting);
    }

    #[test]
    fn stale_live_state_reads_as_disconnected() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        store.mark_connected();
        // Zero max_age: the just-written timestamp is immediately "too old".
        assert_eq!(
            store.current_state(Duration::from_secs(0)),
            LiveState::Disconnected
        );
    }

    #[test]
    fn clear_removes_state_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        store.mark_connected();
        store.clear();
        assert_eq!(store.current_state(stale_after()), LiveState::Disconnected);
        store.clear(); // idempotent
    }

    #[test]
    fn detach_request_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStateStore::at_dir(dir.path().to_path_buf());
        assert!(!store.is_detach_requested());
        store.request_detach().unwrap();
        assert!(store.is_detach_requested());
        store.clear_detach_request();
        assert!(!store.is_detach_requested());
        store.clear_detach_request(); // idempotent
    }
}
