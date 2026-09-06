//! Session runtime (issues #27, #28): owns one live [`AcpDriver`] per
//! configured session and gives it the two behaviors a future
//! network-facing story (#24) needs to expose over the wire:
//!
//! - **Interrupt mapping** (#27): [`SessionManager::interrupt`] sends ACP
//!   `session/cancel`. If the ACP connection can no longer carry that
//!   notification, it falls back to `POST {base_url}/api/session/{id}/interrupt`
//!   against the agent's own HTTP control surface.
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

use tokio::sync::{mpsc, oneshot};

use crate::acp_driver::{AcpDriver, DriverError, DriverEvent};
use crate::config::SessionRegistry;

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
}

/// One configured session's live runtime: its background task handle, the
/// command channel to reach it, and the event channel it forwards driver
/// events on.
struct SessionHandle {
    command_tx: mpsc::UnboundedSender<ManagerCommand>,
    event_rx: mpsc::UnboundedReceiver<DriverEvent>,
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
    ) -> Result<Self, ManagerError> {
        let mut handles = HashMap::with_capacity(registry.sessions().len());
        for config in registry.sessions() {
            let driver = AcpDriver::spawn(config).await.map_err(ManagerError::Driver)?;
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let http = http_fallback_base_url
                .clone()
                .map(|base_url| (reqwest::Client::new(), base_url));
            let task = tokio::spawn(run_session(driver, http, command_rx, event_tx));
            handles.insert(
                config.name.clone(),
                SessionHandle {
                    command_tx,
                    event_rx,
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

    /// The next event from the named session's driven session. Returns
    /// `Ok(None)` once that session's connection has closed and all
    /// buffered events are drained.
    pub async fn next_event(&mut self, name: &str) -> Result<Option<DriverEvent>, ManagerError> {
        let handle = self
            .handles
            .get_mut(name)
            .ok_or_else(|| ManagerError::UnknownSession(name.to_string()))?;
        Ok(handle.event_rx.recv().await)
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
    mut driver: AcpDriver,
    http: Option<(reqwest::Client, String)>,
    mut command_rx: mpsc::UnboundedReceiver<ManagerCommand>,
    event_tx: mpsc::UnboundedSender<DriverEvent>,
) {
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut busy = false;
    // Once the driven ACP connection ends, `driver.next_event()` has
    // nothing left to ever produce — polling it further would just spin.
    // From that point on this task only services `Interrupt` (so a turn
    // that was in flight when the connection died can still be reported
    // through the HTTP fallback: see `attempt_cancel`, which reliably
    // observes `driver.cancel()` failing once this happens) and silently
    // drops further `Prompt`s (nothing left to deliver them to).
    let mut driver_alive = true;

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
                                attempt_cancel(&driver, http_ref)
                                    .await
                                    .map(InterruptOutcome::Cancelled)
                            } else {
                                Ok(InterruptOutcome::NoTurnInFlight)
                            };
                            let _ = reply_tx.send(result);
                        }
                    }
                }
                event = driver.next_event() => {
                    match event {
                        None => driver_alive = false,
                        Some(driver_event) => {
                            let turn_ended = matches!(driver_event, DriverEvent::StopReason(_));
                            let _ = event_tx.send(driver_event);
                            if turn_ended {
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
            }
        } else {
            match command_rx.recv().await {
                None => break,
                Some(ManagerCommand::Prompt(_)) => {}
                Some(ManagerCommand::Interrupt(reply_tx)) => {
                    let result = if busy {
                        let http_ref = http.as_ref().map(|(client, url)| (client, url.as_str()));
                        attempt_cancel(&driver, http_ref)
                            .await
                            .map(InterruptOutcome::Cancelled)
                    } else {
                        Ok(InterruptOutcome::NoTurnInFlight)
                    };
                    let _ = reply_tx.send(result);
                }
            }
        }
    }
    let _ = driver.shutdown().await;
}

/// Sends ACP `session/cancel`; falls back to the HTTP interrupt endpoint
/// only when that notification could not be delivered at all. See the
/// module docs for why that is the trigger this module uses.
async fn attempt_cancel(
    driver: &AcpDriver,
    http: Option<(&reqwest::Client, &str)>,
) -> Result<CancelChannel, ManagerError> {
    match driver.cancel() {
        Ok(()) => Ok(CancelChannel::Acp),
        Err(DriverError::Disconnected) => {
            let (client, base_url) = http.ok_or(ManagerError::Disconnected)?;
            let url = format!(
                "{}/api/session/{}/interrupt",
                base_url.trim_end_matches('/'),
                driver.session_id()
            );
            client
                .post(url)
                .send()
                .await
                .map_err(|err| ManagerError::Http(err.to_string()))?;
            Ok(CancelChannel::Http)
        }
        Err(other) => Err(ManagerError::Driver(other)),
    }
}
