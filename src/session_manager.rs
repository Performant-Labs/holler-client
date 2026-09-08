//! Session runtime (issues #27, #28, #100): owns one live `SessionDriver`
//! per configured session -- an [`AcpDriver`] (spawn mode) or an
//! [`crate::http_attach_driver::HttpAttachDriver`] (attach mode), selected
//! by [`crate::config::SessionConfig::mode`] -- and gives it the two
//! behaviors a future network-facing story (#24) needs to expose over the
//! wire:
//!
//! - **Interrupt mapping** (#27, #100): for a spawn session,
//!   [`SessionManager::interrupt`] sends ACP `session/cancel`, falling back
//!   to `POST {base_url}/api/session/{id}/interrupt` only if the ACP
//!   connection can no longer carry that notification. For an attach
//!   session there is no ACP channel at all, so the same HTTP interrupt is
//!   used directly and unconditionally as the *primary* path.
//! - **Busy-turn policy** (#28): [`SessionManager::prompt`] queues a prompt
//!   sent while a turn is already in flight, and drains the queue (one at a
//!   time) as turns complete. `interrupt` only cancels the current turn —
//!   the queue survives and the next entry starts once that turn is
//!   confirmed done via its [`DriverEvent::StopReason`].
//!
//! # Why one type covers both issues
//!
//! Both track the same in-flight-turn state on a session. Splitting them
//! would risk two independent, incompatible answers to "what does turn
//! state look like"; this module has exactly one `busy` flag and one queue.
//!
//! # Detecting "ACP cancel unsupported"
//!
//! ACP v1 makes `session/cancel` a **baseline-mandatory** capability — "all
//! Agents MUST support `session/new`, `session/prompt`, `session/cancel`,
//! and `session/update`" — and the `agent-client-protocol` crate sends it
//! as a fire-and-forget notification (`CancelNotification`), not a request,
//! so there is no response to inspect and no capability flag to check: a
//! spec-conformant agent can never advertise "cancel unsupported", and a
//! notification send cannot itself carry back a "method not found" style
//! error the way a request would.
//!
//! Given that, this module treats [`AcpDriver::cancel`] failing (today,
//! only [`DriverError::Disconnected`] — the driver's background connection
//! has already ended, e.g. the agent process died) as the trigger for the
//! HTTP fallback, on the theory that the fallback's job is exactly to reach
//! the agent through a *different* channel when the ACP one is gone. This
//! is a deliberate, documented compromise, not a literal reading of "if
//! unsupported" from the issue text — ACP v1 has no such signal to read.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;

use crate::acp_driver::{AcpDriver, DriverError, DriverEvent, DriverStatus};
use crate::config::{SessionConfig, SessionMode, SessionRegistry};
use crate::debug::{self, DebugConfig};
use crate::http_attach_driver::HttpAttachDriver;

/// How long [`run_session`] waits, after an interrupt has been
/// successfully acked while a turn was in flight, for that turn's
/// [`DriverEvent::StopReason`] to actually arrive before giving up on it
/// and forcibly clearing local `busy`/queue state itself (issue #131).
///
/// A well-behaved agent reports a cancelled turn's completion quickly, so
/// this is generous, not a tight race. It exists for the case a real
/// interrupt mid-tool-call was observed to trigger: OpenCode's HTTP
/// interrupt endpoint acks (204) unconditionally, but a genuinely
/// cancelled turn's assistant message can be left without ever reaching a
/// terminal state on OpenCode's side, so no `session.idle` (and therefore
/// no `StopReason`) is ever emitted for it. Without this timeout, `busy`
/// would then never clear and every later prompt for that session would
/// queue forever with zero signal back to the caller.
pub const INTERRUPT_STALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Wraps whichever transport a session's `mode` selects (issue #100,
/// ADR-0005), so [`run_session`]'s prompt/interrupt/busy-queue loop stays
/// a single implementation instead of forking per transport. Both variants
/// expose the same shape as [`AcpDriver`] itself
/// (`prompt`/`next_event`/`shutdown`).
enum SessionDriver {
    /// `mode = "spawn"` / omitted: this process owns the ACP child.
    Acp(AcpDriver),
    /// `mode = "attach"`: an already-running OpenCode, driven over HTTP.
    /// Never the parent of the process it talks to.
    Http(HttpAttachDriver),
}

impl SessionDriver {
    async fn for_config(config: &SessionConfig, cfg: DebugConfig) -> Result<Self, DriverError> {
        match config.mode {
            SessionMode::Spawn => AcpDriver::spawn(config).await.map(SessionDriver::Acp),
            SessionMode::Attach => HttpAttachDriver::attach(config, cfg)
                .await
                .map(SessionDriver::Http),
        }
    }

    fn prompt(&self, text: impl Into<String>) -> Result<(), DriverError> {
        match self {
            SessionDriver::Acp(d) => d.prompt(text),
            SessionDriver::Http(d) => d.prompt(text),
        }
    }

    /// Answers whichever question/permission is currently blocking this
    /// session's turn (holler-server issue #382). Only attach-mode
    /// sessions can service this today: ACP v1's analogous request
    /// (`session/request_permission`) is a real inbound RPC call this
    /// driver would need to intercept and hold open pending a reply,
    /// which `AcpDriver` does not do yet (see its `DriverStatus::Blocked`
    /// doc comment) — out of scope here, so a spawn-mode session reports
    /// a clear, typed [`DriverError::AnswerUnsupported`] rather than
    /// silently doing nothing.
    async fn answer(&self, choice: String) -> Result<(), DriverError> {
        match self {
            SessionDriver::Acp(_) => Err(DriverError::AnswerUnsupported),
            SessionDriver::Http(d) => d.answer(choice).await,
        }
    }

    async fn next_event(&mut self) -> Option<DriverEvent> {
        match self {
            SessionDriver::Acp(d) => d.next_event().await,
            SessionDriver::Http(d) => d.next_event().await,
        }
    }

    /// Transport-aware teardown (issue #101). `Acp` keeps today's child
    /// teardown (waits for the spawned ACP subprocess). `Http` drops its
    /// own event-listener task only -- no HTTP delete, no process kill (it
    /// owns no process), no `session/new` replacement. The attached
    /// OpenCode session is completely untouched.
    async fn shutdown(self) -> Result<(), DriverError> {
        match self {
            SessionDriver::Acp(d) => d.shutdown().await,
            SessionDriver::Http(d) => d.shutdown().await,
        }
    }
}

/// Errors from a [`SessionManager`] operation.
#[derive(Debug)]
pub enum ManagerError {
    /// No session with this name is registered with this manager.
    UnknownSession(String),
    /// Spawning a session's [`AcpDriver`] failed.
    Driver(DriverError),
    /// The HTTP interrupt fallback request itself failed. Carries the
    /// underlying `reqwest::Error`'s message rather than the error itself,
    /// so this module doesn't need to expose that crate's error type.
    Http(String),
    /// The session's background task has already ended (e.g. the driver's
    /// connection closed), so this call could not be serviced.
    Disconnected,
}

impl std::fmt::Display for ManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerError::UnknownSession(name) => write!(f, "no such session: {name}"),
            ManagerError::Driver(err) => write!(f, "driver error: {err}"),
            ManagerError::Http(message) => write!(f, "HTTP interrupt fallback failed: {message}"),
            ManagerError::Disconnected => write!(f, "session manager connection has closed"),
        }
    }
}

impl std::error::Error for ManagerError {}

/// Which channel actually carried a successful interrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelChannel {
    /// ACP `session/cancel`.
    Acp,
    /// The HTTP fallback (`POST /api/session/{id}/interrupt`).
    Http,
}

/// The result of a well-formed [`SessionManager::interrupt`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptOutcome {
    /// No turn was in flight; nothing was cancelled. A clean, well-defined
    /// no-op rather than an error — interrupting an idle session isn't a
    /// misuse of the API, it just has nothing to do.
    NoTurnInFlight,
    /// A turn was in flight and a cancellation was sent via this channel.
    /// This reports that the cancel request/notification was sent, not
    /// that the agent has confirmed the turn stopped — observe
    /// [`DriverEvent::StopReason`] via [`SessionManager::next_event`] for
    /// that.
    Cancelled(CancelChannel),
}

/// A command sent from a [`SessionManager`] handle to one session's
/// background task.
enum ManagerCommand {
    Prompt(String),
    Interrupt(oneshot::Sender<Result<InterruptOutcome, ManagerError>>),
    /// Answer whichever question/permission is currently blocking this
    /// session's turn (holler-server issue #382).
    Answer(String, oneshot::Sender<Result<(), ManagerError>>),
    /// Whether a turn is currently in flight (issue #49: presence's
    /// `busy` field). Answered synchronously from `run_session`'s own
    /// `busy` flag, the same one `Prompt`/`Interrupt` already consult.
    IsBusy(oneshot::Sender<bool>),
    /// Whether this session is currently blocked on a question/permission
    /// (issue #139: `session_blocked`'s reconnect-time resync). Answered
    /// from `run_session`'s own `blocked` flag, set/cleared by the same
    /// `DriverEvent::Status` transitions `crate::connection`'s live push
    /// reacts to.
    IsBlocked(oneshot::Sender<bool>),
}

/// One configured session's live runtime: its background task handle, the
/// command channel to reach it, and the event channel it forwards driver
/// events on.
struct SessionHandle {
    command_tx: mpsc::UnboundedSender<ManagerCommand>,
    /// `None` once [`SessionManager::take_event_channels`] has taken it —
    /// see that method's docs for why the wire layer needs to own these
    /// directly rather than go through [`SessionManager::next_event`].
    event_rx: Option<mpsc::UnboundedReceiver<DriverEvent>>,
    task: tokio::task::JoinHandle<()>,
}

/// Owns one running [`AcpDriver`] per session in a [`SessionRegistry`], and
/// layers interrupt mapping (#27) and busy-turn queueing (#28) on top.
///
/// This is a pure Rust API layer with no networking of its own: a future
/// story (#24) is expected to translate Holler wire `prompt`/`interrupt`
/// messages into calls on this type.
pub struct SessionManager {
    handles: HashMap<String, SessionHandle>,
}

impl SessionManager {
    /// Spawns an [`AcpDriver`] for every session in `registry` and starts
    /// each one's background task.
    ///
    /// `http_fallback_base_url` is the base URL of the agent's own HTTP
    /// control surface (e.g. `http://127.0.0.1:4096`), shared across every
    /// session spawned here — the issue's `POST /api/session/{id}/interrupt`
    /// path is appended per call, with `{id}` filled in from the specific
    /// session's [`AcpDriver::session_id`]. `None` disables the HTTP
    /// fallback entirely: a session whose ACP cancel notification cannot be
    /// delivered then reports [`ManagerError::Disconnected`] from
    /// [`interrupt`](Self::interrupt) instead of attempting one.
    pub async fn spawn(
        registry: &SessionRegistry,
        http_fallback_base_url: Option<String>,
        cfg: DebugConfig,
    ) -> Result<Self, ManagerError> {
        let mut handles = HashMap::with_capacity(registry.sessions().len());
        for config in registry.sessions() {
            let driver = SessionDriver::for_config(config, cfg)
                .await
                .map_err(ManagerError::Driver)?;
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let http = http_fallback_base_url
                .clone()
                .map(|base_url| (reqwest::Client::new(), base_url));
            let task = tokio::spawn(run_session(driver, http, command_rx, event_tx, cfg));
            handles.insert(
                config.name.clone(),
                SessionHandle {
                    command_tx,
                    event_rx: Some(event_rx),
                    task,
                },
            );
        }
        Ok(SessionManager { handles })
    }

    /// Sends a prompt to the named session: delivered immediately if no
    /// turn is in flight, queued (and delivered once earlier turns finish)
    /// otherwise. Returns once the request has been handed to the
    /// session's background task, not once any turn it starts completes.
    pub fn prompt(&self, name: &str, text: impl Into<String>) -> Result<(), ManagerError> {
        let handle = self
            .handles
            .get(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        handle
            .command_tx
            .send(ManagerCommand::Prompt(text.into()))
            .map_err(|_| ManagerError::Disconnected)
    }

    /// Interrupts the named session's current turn, if any. See
    /// [`InterruptOutcome`] for what a clean `Ok` return means, and the
    /// module docs for how the ACP-vs-HTTP channel choice is made.
    pub async fn interrupt(&self, name: &str) -> Result<InterruptOutcome, ManagerError> {
        let handle = self
            .handles
            .get(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .command_tx
            .send(ManagerCommand::Interrupt(reply_tx))
            .map_err(|_| ManagerError::Disconnected)?;
        reply_rx.await.map_err(|_| ManagerError::Disconnected)?
    }

    /// Answers whichever question/permission is currently blocking the
    /// named session's turn (holler-server issue #382). Only attach-mode
    /// sessions can service this today — a spawn-mode session reports
    /// [`ManagerError::Driver`] wrapping [`DriverError::AnswerUnsupported`].
    pub async fn answer(&self, name: &str, choice: String) -> Result<(), ManagerError> {
        let handle = self
            .handles
            .get(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .command_tx
            .send(ManagerCommand::Answer(choice, reply_tx))
            .map_err(|_| ManagerError::Disconnected)?;
        reply_rx.await.map_err(|_| ManagerError::Disconnected)?
    }

    /// The next event from the named session's driven session. Returns
    /// `Ok(None)` once that session's connection has closed and all
    /// buffered events are drained, **or** once
    /// [`take_event_channels`](Self::take_event_channels) has taken this
    /// session's receiver for direct use elsewhere.
    pub async fn next_event(&mut self, name: &str) -> Result<Option<DriverEvent>, ManagerError> {
        let handle = self
            .handles
            .get_mut(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        match handle.event_rx.as_mut() {
            Some(rx) => Ok(rx.recv().await),
            None => Ok(None),
        }
    }

    /// Whether the named session currently has a turn in flight — the
    /// same `busy` flag [`prompt`](Self::prompt)/[`interrupt`](Self::interrupt)
    /// already consult inside `run_session`, surfaced for issue #49's
    /// `presence` frame (`crate::connection`).
    pub async fn is_busy(&self, name: &str) -> Result<bool, ManagerError> {
        let handle = self
            .handles
            .get(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .command_tx
            .send(ManagerCommand::IsBusy(reply_tx))
            .map_err(|_| ManagerError::Disconnected)?;
        reply_rx.await.map_err(|_| ManagerError::Disconnected)
    }

    /// Whether the named session is currently blocked on a question or
    /// tool-use permission (issue #139) — the same `blocked` flag
    /// `crate::connection`'s live `session_blocked` push already reacts
    /// to, surfaced here so a fresh (re)connection can resync a session
    /// that was already blocked before the previous connection dropped
    /// (a `DriverEvent::Status(Blocked)` only fires on the *transition*
    /// into blocked, which a reconnect does not repeat).
    pub async fn is_blocked(&self, name: &str) -> Result<bool, ManagerError> {
        let handle = self
            .handles
            .get(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .command_tx
            .send(ManagerCommand::IsBlocked(reply_tx))
            .map_err(|_| ManagerError::Disconnected)?;
        reply_rx.await.map_err(|_| ManagerError::Disconnected)
    }

    /// The name of every session this manager is driving.
    pub fn session_names(&self) -> Vec<String> {
        self.handles.keys().cloned().collect()
    }

    /// Takes ownership of every session's event receiver, keyed by
    /// session name, for a caller that needs to drain **all** of them
    /// concurrently.
    ///
    /// [`next_event`](Self::next_event) requires `&mut self` (a `HashMap`
    /// lookup needs it), so polling several sessions' events at once
    /// through it would mean serializing every session behind one
    /// `&mut` — starving an idle session's prompt/interrupt handling
    /// behind another session's still-open-ended event wait. The wire
    /// layer (`crate::connection`) instead takes the raw receivers once,
    /// up front, and polls them itself; [`prompt`](Self::prompt) and
    /// [`interrupt`](Self::interrupt) only ever need `&self` (they just
    /// send a command), so this doesn't block them.
    ///
    /// After this call, [`next_event`](Self::next_event) for any session
    /// whose receiver was taken here returns `Ok(None)`.
    pub fn take_event_channels(&mut self) -> HashMap<String, mpsc::UnboundedReceiver<DriverEvent>> {
        self.handles
            .iter_mut()
            .filter_map(|(name, handle)| handle.event_rx.take().map(|rx| (name.clone(), rx)))
            .collect()
    }

    /// Ends every session and waits for each background task (and its
    /// driven `AcpDriver`, and that driver's spawned agent subprocess) to
    /// shut down. Best-effort: a per-session driver shutdown error is not
    /// surfaced, since one session's teardown failure shouldn't stop the
    /// rest from being cleaned up.
    pub async fn shutdown(self) {
        for (_, handle) in self.handles {
            drop(handle.command_tx);
            let _ = handle.task.await;
        }
    }
}

/// Drives one session's commands to completion: forwards prompts (queueing
/// while a turn is in flight), forwards driver events, and services
/// interrupts. Returns once the command channel closes (the
/// [`SessionManager`] was dropped or [`SessionManager::shutdown`] was
/// called) or the driven [`AcpDriver`]'s connection ends.
async fn run_session(
    mut driver: SessionDriver,
    http: Option<(reqwest::Client, String)>,
    mut command_rx: mpsc::UnboundedReceiver<ManagerCommand>,
    event_tx: mpsc::UnboundedSender<DriverEvent>,
    cfg: DebugConfig,
) {
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut busy = false;
    // Issue #139: mirrors the driver's own `DriverStatus::Blocked`
    // transitions, so `is_blocked` (a reconnect-time resync query) has
    // something to answer without waiting for a fresh transition.
    let mut blocked = false;
    // Once the driven ACP connection ends, `driver.next_event()` has
    // nothing left to ever produce — polling it further would just spin.
    // From that point on this task only services `Interrupt` (so a turn
    // that was in flight when the connection died can still be reported
    // through the HTTP fallback: see `attempt_cancel`, which reliably
    // observes `driver.cancel()` failing once this happens) and silently
    // drops further `Prompt`s (nothing left to deliver them to).
    let mut driver_alive = true;
    // Armed the moment an interrupt is successfully acked while `busy`;
    // disarmed as soon as either the interrupted turn's real `StopReason`
    // arrives or this deadline itself fires. See `INTERRUPT_STALL_TIMEOUT`'s
    // docs (issue #131) for why the ack alone cannot be trusted to mean the
    // turn is over, and why this can't just wait unboundedly for the event.
    let mut interrupt_deadline: Option<Pin<Box<Sleep>>> = None;

    loop {
        if driver_alive {
            tokio::select! {
                command = command_rx.recv() => {
                    match command {
                        None => break,
                        Some(ManagerCommand::Prompt(text)) => {
                            if busy {
                                queue.push_back(text);
                            } else if driver.prompt(text).is_ok() {
                                busy = true;
                            } else {
                                driver_alive = false;
                            }
                        }
                        Some(ManagerCommand::Interrupt(reply_tx)) => {
                            let result = if busy {
                                let http_ref = http.as_ref().map(|(client, url)| (client, url.as_str()));
                                let outcome = attempt_cancel(&driver, http_ref, cfg)
                                    .await
                                    .map(InterruptOutcome::Cancelled);
                                if outcome.is_ok() {
                                    debug::local(cfg, "session_manager", "interrupt")
                                        .field("event", "stall_timeout_armed")
                                        .emit();
                                    interrupt_deadline =
                                        Some(Box::pin(tokio::time::sleep(INTERRUPT_STALL_TIMEOUT)));
                                }
                                outcome
                            } else {
                                Ok(InterruptOutcome::NoTurnInFlight)
                            };
                            let _ = reply_tx.send(result);
                        }
                        Some(ManagerCommand::Answer(choice, reply_tx)) => {
                            let result = driver.answer(choice).await.map_err(ManagerError::Driver);
                            let _ = reply_tx.send(result);
                        }
                        Some(ManagerCommand::IsBusy(reply_tx)) => {
                            let _ = reply_tx.send(busy);
                        }
                        Some(ManagerCommand::IsBlocked(reply_tx)) => {
                            let _ = reply_tx.send(blocked);
                        }
                    }
                }
                event = driver.next_event() => {
                    match event {
                        None => driver_alive = false,
                        Some(driver_event) => {
                            let turn_ended = matches!(driver_event, DriverEvent::StopReason(_));
                            match &driver_event {
                                DriverEvent::Status(DriverStatus::Blocked) => blocked = true,
                                DriverEvent::Status(DriverStatus::Working | DriverStatus::Idle) => {
                                    blocked = false
                                }
                                _ => {}
                            }
                            let _ = event_tx.send(driver_event);
                            if turn_ended {
                                interrupt_deadline = None;
                                busy = false;
                                if let Some(next_text) = queue.pop_front() {
                                    if driver.prompt(next_text).is_ok() {
                                        busy = true;
                                    } else {
                                        driver_alive = false;
                                    }
                                }
                            }
                        }
                    }
                }
                _ = async {
                    match interrupt_deadline.as_mut() {
                        Some(deadline) => deadline.await,
                        None => std::future::pending::<()>().await,
                    }
                }, if interrupt_deadline.is_some() => {
                    // The interrupted turn never reported completion --
                    // treat the earlier ack as authoritative rather than
                    // leaving this session's queue wedged forever waiting
                    // for a `StopReason` that may never arrive (issue #131).
                    debug::warn(cfg, "session_manager", "interrupt")
                        .field("event", "stall_timeout_forced_clear")
                        .emit();
                    interrupt_deadline = None;
                    busy = false;
                    if let Some(next_text) = queue.pop_front() {
                        if driver.prompt(next_text).is_ok() {
                            busy = true;
                        } else {
                            driver_alive = false;
                        }
                    }
                }
            }
        } else {
            match command_rx.recv().await {
                None => break,
                Some(ManagerCommand::Prompt(_)) => {}
                Some(ManagerCommand::Interrupt(reply_tx)) => {
                    let result = if busy {
                        let http_ref = http.as_ref().map(|(client, url)| (client, url.as_str()));
                        attempt_cancel(&driver, http_ref, cfg)
                            .await
                            .map(InterruptOutcome::Cancelled)
                    } else {
                        Ok(InterruptOutcome::NoTurnInFlight)
                    };
                    let _ = reply_tx.send(result);
                }
                Some(ManagerCommand::Answer(choice, reply_tx)) => {
                    let result = driver.answer(choice).await.map_err(ManagerError::Driver);
                    let _ = reply_tx.send(result);
                }
                Some(ManagerCommand::IsBusy(reply_tx)) => {
                    let _ = reply_tx.send(busy);
                }
                Some(ManagerCommand::IsBlocked(reply_tx)) => {
                    let _ = reply_tx.send(blocked);
                }
            }
        }
    }
    let _ = driver.shutdown().await;
}

/// For an ACP-spawned session: sends `session/cancel`, falling back to the
/// HTTP interrupt endpoint only when that notification could not be
/// delivered at all (see the module docs for why that's this module's
/// chosen trigger). For an attach session there is no ACP channel at
/// all -- HTTP interrupt is used directly and unconditionally, as the
/// *primary* path (issue #100), never a fallback reached after a fake ACP
/// disconnect.
async fn attempt_cancel(
    driver: &SessionDriver,
    http: Option<(&reqwest::Client, &str)>,
    cfg: DebugConfig,
) -> Result<CancelChannel, ManagerError> {
    match driver {
        SessionDriver::Acp(acp) => match acp.cancel() {
            Ok(()) => {
                debug::local(cfg, "session_manager", "interrupt")
                    .field("event", "acp_cancel")
                    .emit();
                Ok(CancelChannel::Acp)
            }
            Err(DriverError::Disconnected) => {
                debug::local(cfg, "session_manager", "interrupt")
                    .field("event", "http_interrupt_fallback")
                    .emit();
                let (client, base_url) = http.ok_or(ManagerError::Disconnected)?;
                let url = format!(
                    "{}/api/session/{}/interrupt",
                    base_url.trim_end_matches('/'),
                    acp.session_id()
                );
                client
                    .post(url)
                    .send()
                    .await
                    .map_err(|err| ManagerError::Http(err.to_string()))?;
                Ok(CancelChannel::Http)
            }
            Err(other) => Err(ManagerError::Driver(other)),
        },
        SessionDriver::Http(http_driver) => {
            debug::local(cfg, "session_manager", "interrupt")
                .field("event", "http_interrupt_primary")
                .emit();
            http_driver
                .interrupt()
                .await
                .map_err(ManagerError::Driver)?;
            Ok(CancelChannel::Http)
        }
    }
}
