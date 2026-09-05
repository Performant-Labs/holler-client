//! Generic ACP driver (issue #26): spawns a configured ACP v1 agent
//! subprocess for one session and drives `session/new` + `session/prompt`
//! against it over stdio, using the official `agent-client-protocol` crate.
//!
//! This is deliberately not an OpenCode-specific adapter: the spawned
//! `command` comes from [`SessionConfig`], and OpenCode (`opencode acp`) is
//! only the v1 *default* value for that field, not something this module
//! knows about.
//!
//! # Scope
//!
//! This module does not build Holler wire envelopes (`Envelope`,
//! `ReplyBody`, etc. live in the separate holler-server repo, and this
//! crate has no WebSocket/wire code yet — issue #24). [`DriverEvent`] is a
//! local, ACP-shaped seam: issue #24 is expected to translate it into
//! Holler's actual wire protocol. It is intentionally not named or shaped
//! like a Holler wire type.
//!
//! # Pinning ACP v1
//!
//! `agent-client-protocol` v2.1.0 also has an unstable, opt-in v2 draft
//! protocol gated behind the `unstable_protocol_v2` Cargo feature. This
//! crate does not enable that feature, and this driver only ever sends
//! [`InitializeRequest::new(ProtocolVersion::V1)`], so the connection is
//! pinned to stable ACP v1 both at compile time and on the wire.
//!
//! # A fixture bug this story found and fixed
//!
//! `tests/stub-acp` (issue #32) predates this story's research into the
//! real crate and originally emitted wire shapes it called "documented,
//! made-up-but-reasonable" guesses — because ACP v1 was not otherwise
//! pinned in this repo yet. Two of those guesses turned out not to be
//! schema-conformant: it had no `initialize` handler, and its
//! `session/update` notification's `params` didn't match the real,
//! internally-tagged `SessionNotification`/`SessionUpdate` shape. Both are
//! required for this driver's typed, official-SDK approach to work at all
//! (the SDK's dispatcher matches `session/update` to `SessionNotification`
//! unconditionally and silently drops it on a parse failure, since a
//! notification has no request `id` to report an error back on — so the
//! old shape wasn't just technically non-conformant, it was silently
//! invisible to any client built on this crate). This story fixed
//! `tests/stub-acp` in place rather than reimplementing JSON-RPC dispatch
//! by hand to accommodate the old shape.

use agent_client_protocol::schema::v1::{
    CancelNotification, ContentBlock, InitializeRequest, SessionNotification, SessionUpdate,
    StopReason,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::util::MatchDispatch;
use agent_client_protocol::{
    AcpAgent, AcpAgentConfig, Agent, Client, ConnectionTo, SessionMessage,
};
use tokio::sync::{mpsc, oneshot};

use crate::config::SessionConfig;

/// An event surfaced from the driven ACP session.
///
/// This is a local, Rust-native seam — not a Holler wire type. A future
/// story (#24) is expected to consume it and translate it into an actual
/// Holler `reply`/presence envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverEvent {
    /// Text content streamed from the agent during a turn, extracted from a
    /// `session/update`'s `AgentMessageChunk`/`AgentThoughtChunk` text
    /// content blocks. Other update kinds (tool calls, plans, mode
    /// changes, ...) are not surfaced yet; extend this enum as Holler
    /// grows support for them.
    Update(String),
    /// Presence, as far as ACP v1's session lifecycle exposes it.
    Status(DriverStatus),
    /// The turn's stop reason, once `session/prompt` completes.
    StopReason(DriverStopReason),
}

/// Presence derived from the ACP v1 prompt-turn lifecycle.
///
/// ACP v1's base session lifecycle has no distinct "blocked" signal — the
/// closest analog is an agent-initiated permission request
/// (`session/request_permission`), which blocks the turn on a client
/// decision. This driver does not yet handle permission requests (out of
/// scope for issue #26; a future story can add it), so [`Blocked`] is
/// reserved but never emitted today.
///
/// [`Blocked`]: DriverStatus::Blocked
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverStatus {
    /// A `session/prompt` turn is in flight.
    Working,
    /// No turn is in flight.
    Idle,
    /// Reserved: would indicate a pending agent-initiated request (e.g.
    /// permission) blocking the turn. Never emitted today; see the
    /// enum-level doc comment.
    Blocked,
}

/// A turn's stop reason, mirrored from [`StopReason`] so callers of this
/// module don't need to depend on `agent_client_protocol` directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverStopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    /// A stop reason ACP has added since this enum was last updated.
    /// [`StopReason`] is `#[non_exhaustive]`, so this keeps `From` total
    /// without this module needing a release for every new variant.
    Unknown,
}

impl From<StopReason> for DriverStopReason {
    fn from(reason: StopReason) -> Self {
        match reason {
            StopReason::EndTurn => Self::EndTurn,
            StopReason::MaxTokens => Self::MaxTokens,
            StopReason::MaxTurnRequests => Self::MaxTurnRequests,
            StopReason::Refusal => Self::Refusal,
            StopReason::Cancelled => Self::Cancelled,
            _ => Self::Unknown,
        }
    }
}

/// Errors from spawning or driving an ACP session.
#[derive(Debug)]
pub enum DriverError {
    /// `SessionConfig.command` was empty; there is no program to spawn.
    EmptyCommand,
    /// The agent subprocess or ACP protocol connection failed. Carries the
    /// underlying `agent_client_protocol::Error`'s message rather than the
    /// error itself, so this module doesn't need to expose that crate's
    /// error type as part of its own API.
    Acp(String),
    /// The driver's background connection task has already ended (e.g. the
    /// agent process exited), so this call could not be serviced.
    Disconnected,
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverError::EmptyCommand => write!(f, "session config has an empty command"),
            DriverError::Acp(message) => write!(f, "ACP driver error: {message}"),
            DriverError::Disconnected => write!(f, "ACP driver connection has closed"),
        }
    }
}

impl std::error::Error for DriverError {}

/// A command sent from the driver handle to its background connection task.
enum Command {
    Prompt(String),
    Cancel,
}

/// A running driver for one configured session: a spawned ACP agent
/// subprocess with an established ACP v1 session, driven by
/// [`AcpDriver::prompt`] and [`AcpDriver::cancel`], and observed via
/// [`AcpDriver::next_event`].
#[derive(Debug)]
pub struct AcpDriver {
    command_tx: mpsc::UnboundedSender<Command>,
    event_rx: mpsc::UnboundedReceiver<DriverEvent>,
    connection: tokio::task::JoinHandle<Result<(), agent_client_protocol::Error>>,
}

impl AcpDriver {
    /// Spawns `config.command` and establishes one ACP v1 session against
    /// it (`initialize` then `session/new`). Returns once the session is
    /// ready for [`prompt`](Self::prompt) calls.
    pub async fn spawn(config: &SessionConfig) -> Result<Self, DriverError> {
        let (program, args) = config
            .command
            .split_first()
            .ok_or(DriverError::EmptyCommand)?;
        let agent = AcpAgent::new(AcpAgentConfig::new(program).args(args.iter().cloned()));

        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let connection = tokio::spawn(async move {
            Client
                .builder()
                .connect_with(agent, move |cx: ConnectionTo<Agent>| async move {
                    cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;

                    cx.build_session_cwd()?
                        .block_task()
                        .run_until(async |mut session| {
                            // Signal readiness only once the session
                            // actually exists, so a caller's first
                            // `prompt()` is never raced against session
                            // setup.
                            let _ = ready_tx.send(());
                            run_command_loop(&mut session, command_rx, event_tx).await
                        })
                        .await
                })
                .await
        });

        // If anything above failed before the session was established,
        // `ready_tx` is dropped without sending and `ready_rx` errors;
        // join the task to recover the actual error instead of reporting a
        // generic "disconnected".
        match ready_rx.await {
            Ok(()) => Ok(Self {
                command_tx,
                event_rx,
                connection,
            }),
            Err(_) => Err(match connection.await {
                Ok(Err(err)) => DriverError::Acp(err.to_string()),
                Ok(Ok(())) => DriverError::Disconnected,
                Err(join_err) => DriverError::Acp(format!("driver task panicked: {join_err}")),
            }),
        }
    }

    /// Sends `session/prompt` with the given text. Returns once the
    /// request has been queued to the background connection, not once the
    /// turn completes — observe [`DriverEvent::StopReason`] via
    /// [`next_event`](Self::next_event) for that.
    ///
    /// A prompt sent while another is still in flight is dropped: ACP v1
    /// sessions handle one turn at a time, and this driver does not queue
    /// prompts.
    pub fn prompt(&self, text: impl Into<String>) -> Result<(), DriverError> {
        self.command_tx
            .send(Command::Prompt(text.into()))
            .map_err(|_| DriverError::Disconnected)
    }

    /// Sends `session/cancel` for this driver's session, per ACP v1's
    /// cancellation protocol.
    pub fn cancel(&self) -> Result<(), DriverError> {
        self.command_tx
            .send(Command::Cancel)
            .map_err(|_| DriverError::Disconnected)
    }

    /// The next event from the driven session. Returns `None` once the
    /// connection has closed and all buffered events are drained.
    pub async fn next_event(&mut self) -> Option<DriverEvent> {
        self.event_rx.recv().await
    }

    /// Ends the session and waits for the background connection (and its
    /// spawned agent subprocess) to shut down.
    pub async fn shutdown(self) -> Result<(), DriverError> {
        drop(self.command_tx);
        match self.connection.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(DriverError::Acp(err.to_string())),
            Err(join_err) => Err(DriverError::Acp(format!(
                "driver task panicked: {join_err}"
            ))),
        }
    }
}

/// Drives one session's commands to completion: sends prompts, forwards
/// `session/update` text and stop reasons as [`DriverEvent`]s, and handles
/// `session/cancel`. Returns once the command channel closes (the
/// [`AcpDriver`] handle was dropped or [`AcpDriver::shutdown`] was called).
async fn run_command_loop(
    session: &mut agent_client_protocol::ActiveSession<'_, Agent>,
    mut command_rx: mpsc::UnboundedReceiver<Command>,
    event_tx: mpsc::UnboundedSender<DriverEvent>,
) -> Result<(), agent_client_protocol::Error> {
    let mut prompt_in_flight = false;
    loop {
        if prompt_in_flight {
            tokio::select! {
                command = command_rx.recv() => {
                    match command {
                        Some(Command::Prompt(_)) => continue,
                        Some(Command::Cancel) => send_cancel(session)?,
                        None => return Ok(()),
                    }
                }
                update = session.read_update() => {
                    match update? {
                        SessionMessage::SessionMessage(dispatch) => {
                            if let Some(text) = extract_update_text(dispatch).await? {
                                let _ = event_tx.send(DriverEvent::Update(text));
                            }
                        }
                        SessionMessage::StopReason(reason) => {
                            prompt_in_flight = false;
                            let _ = event_tx.send(DriverEvent::Status(DriverStatus::Idle));
                            let _ = event_tx.send(DriverEvent::StopReason(reason.into()));
                        }
                        // SessionMessage is #[non_exhaustive]; no other kind
                        // of session message exists to translate today.
                        _ => {}
                    }
                }
            }
        } else {
            match command_rx.recv().await {
                Some(Command::Prompt(text)) => {
                    session.send_prompt(text)?;
                    prompt_in_flight = true;
                    let _ = event_tx.send(DriverEvent::Status(DriverStatus::Working));
                }
                Some(Command::Cancel) => send_cancel(session)?,
                None => return Ok(()),
            }
        }
    }
}

fn send_cancel(
    session: &agent_client_protocol::ActiveSession<'_, Agent>,
) -> Result<(), agent_client_protocol::Error> {
    session
        .connection()
        .send_notification_to(Agent, CancelNotification::new(session.session_id().clone()))
}

/// Extracts text content from a session-scoped dispatch, if it is a
/// `session/update` notification carrying a text chunk.
///
/// Mirrors `agent_client_protocol::ActiveSession::read_to_string`'s own
/// match-and-ignore pattern: a `session/update` whose method matches but
/// whose payload fails to parse as [`SessionNotification`] is a real
/// protocol error (propagated); any other message kind, or a text-less
/// update (e.g. a tool call), is not.
async fn extract_update_text(
    dispatch: agent_client_protocol::Dispatch,
) -> Result<Option<String>, agent_client_protocol::Error> {
    let mut text = None;
    MatchDispatch::new(dispatch)
        .if_notification(async |notif: SessionNotification| {
            text = update_text(&notif.update);
            Ok(())
        })
        .await
        .otherwise_ignore()?;
    Ok(text)
}

fn update_text(update: &SessionUpdate) -> Option<String> {
    let content = match update {
        SessionUpdate::AgentMessageChunk(chunk) | SessionUpdate::AgentThoughtChunk(chunk) => {
            &chunk.content
        }
        _ => return None,
    };
    match content {
        ContentBlock::Text(text_content) => Some(text_content.text.clone()),
        _ => None,
    }
}
