//! `holler` CLI: `join` / `detach` / `status` / `run` (issues #23, #24, #16).
//!
//! Both `join` (a one-shot `join`/`join_ok` exchange) and `run` (the
//! long-lived session) open a real Holler WebSocket — see
//! [`holler_client::join::WsJoinTransport`] and
//! [`holler_client::connection`] respectively.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

use holler_client::config;
use holler_client::connection::{self, ConnectionStateStore, LiveState};
use holler_client::credential::{CredentialStore, PersistedCredential};
use holler_client::join::{JoinTransport, WsJoinTransport};
use holler_client::proto::{self, QueryBody};
use holler_client::query;
use holler_client::server_address::ServerAddress;

/// How long `holler detach` waits for a live `holler run` process to
/// notice the detach marker and close its own socket before this
/// process gives up waiting and deletes the credential anyway (detach's
/// contract — "no credential left behind" — always holds either way).
const DETACH_WAIT_BUDGET: Duration = Duration::from_secs(3);
const DETACH_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Parser)]
#[command(name = "holler", about = "Thin client for the Holler talk circuit")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Body config file (`[[session]]` entries). With no config, this
    /// process has zero local sessions — every session is explicit, there
    /// is no default.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Redeem a join token against a server and persist this client's identity.
    Join {
        /// Server URL, e.g. ws://host:41807 or ws://[::1] (port defaults to 41807).
        #[arg(long)]
        server: String,
        /// One-time join token, as `<token_id>:<secret>` (both printed by
        /// `holler token mint` on the server). Never persisted or sent
        /// again after this call.
        #[arg(long)]
        token: String,
    },
    /// Connect to the joined server and maintain a live session: auth,
    /// hello, heartbeat, reconnect with backoff (issue #24). Runs in the
    /// foreground until detached (`holler detach`) or interrupted
    /// (Ctrl-C).
    Run,
    /// Disconnect (closing any live connection a `holler run` process
    /// holds) and delete the local credential.
    Detach,
    /// Print this client's status document (local `query status`).
    Status,
    /// Print whether this process supports a protocol feature or harness,
    /// right now (local `query support <feature>`). Never asks a model.
    Support {
        /// A protocol feature id (`ping`, `query`, ...) or harness id
        /// (`opencode`, `claude`, ...).
        feature: String,
    },
    /// Print the full capability document: status plus every known
    /// feature/harness's support answer (local `query caps`).
    Caps,
    /// General local query form: `holler query <cmd> [args...]`, e.g.
    /// `holler query protocol` or `holler query protocol 2`.
    Query {
        cmd: String,
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = cli.config.clone();
    let result = match cli.command {
        Command::Join { server, token } => run_join(&server, &token),
        Command::Run => run_run(config.as_deref()),
        Command::Detach => run_detach(),
        Command::Status => run_query_local("status", &[], config.as_deref()),
        Command::Support { feature } => {
            run_query_local("support", std::slice::from_ref(&feature), config.as_deref())
        }
        Command::Caps => run_query_local("caps", &[], config.as_deref()),
        Command::Query { cmd, args } => run_query_local(&cmd, &args, config.as_deref()),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn current_hostname() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

fn run_join(server: &str, token: &str) -> Result<(), String> {
    let address = ServerAddress::parse(server).map_err(|e| e.to_string())?;
    let hostname = current_hostname();

    let identity = WsJoinTransport
        .redeem(&address, token, &hostname)
        .map_err(|e| e.to_string())?;

    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let persisted = PersistedCredential {
        client_id: identity.client_id,
        credential: identity.credential,
        server: address.to_canonical_url(),
        hostname,
    };
    store.save(&persisted).map_err(|e| e.to_string())?;

    println!(
        "joined {} as client_id={}",
        persisted.server, persisted.client_id
    );
    Ok(())
}

fn run_run(config: Option<&std::path::Path>) -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let credential = store
        .load()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "not joined; run `holler join` first".to_string())?;

    let registry = config::load(config).map_err(|e| e.to_string())?;
    let state = ConnectionStateStore::open().map_err(|e| e.to_string())?;
    // A prior `run` in this state dir may have died without clearing a
    // detach marker or its own live-state file; start from a clean slate.
    state.clear_detach_request();
    state.clear();

    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    let result = runtime.block_on(async {
        tokio::select! {
            res = connection::run(
                &credential.server,
                &credential.credential,
                &credential.client_id,
                &credential.hostname,
                &registry,
                &state,
            ) => res,
            _ = tokio::signal::ctrl_c() => {
                // Cancelling the `run` future here drops the socket
                // (an unclean close, not a graceful WS close frame) —
                // acceptable for a Ctrl-C shutdown; clearing the state
                // file is what actually matters for `holler status`.
                Ok(())
            }
        }
    });
    state.clear();
    result.map_err(|e| e.to_string())
}

fn run_detach() -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let was_joined = store.load().map_err(|e| e.to_string())?.is_some();

    let state = ConnectionStateStore::open().map_err(|e| e.to_string())?;
    if state.current_state(connection::STALE_AFTER) != LiveState::Disconnected {
        state.request_detach().map_err(|e| e.to_string())?;
        let deadline = std::time::Instant::now() + DETACH_WAIT_BUDGET;
        while std::time::Instant::now() < deadline
            && state.current_state(connection::STALE_AFTER) != LiveState::Disconnected
        {
            std::thread::sleep(DETACH_POLL_INTERVAL);
        }
        // Whether or not the live `run` process noticed in time, detach's
        // contract is "no credential left behind" — clean up either way.
        state.clear_detach_request();
        state.clear();
    }

    store.delete().map_err(|e| e.to_string())?;
    if was_joined {
        println!("detached");
    } else {
        println!("not joined; nothing to detach");
    }
    Ok(())
}

/// Answers a local `query` — `status`/`support`/`caps`/`protocol` — the
/// same way [`holler_client::connection`] answers one arriving on the wire
/// (`crate::query::dispatch`), so `holler status` and a server's inbound
/// `query`/`cmd=status` are provably the same document, not two
/// independently-maintained ones.
fn run_query_local(
    cmd: &str,
    args: &[String],
    config: Option<&std::path::Path>,
) -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let credential = store.load().map_err(|e| e.to_string())?;
    let hostname = current_hostname();
    let registry = config::load(config).map_err(|e| e.to_string())?;
    let live = ConnectionStateStore::open()
        .map(|s| s.current_state(connection::STALE_AFTER))
        .unwrap_or(LiveState::Disconnected);

    let query = QueryBody {
        cmd: cmd.to_string(),
        args: args.to_vec(),
    };
    let body = query::dispatch(
        &query,
        proto::PROTOCOL_VERSION,
        credential.as_ref().map(|c| c.client_id.as_str()),
        &registry,
        &hostname,
        live,
    )
    .map_err(|e| e.to_string())?;

    let json = serde_json::to_string_pretty(&body).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}
