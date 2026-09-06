//! `holler` CLI: `join` / `detach` / `status` / `run` (issues #23, #24).
//!
//! `run` is this crate's first process that actually opens a Holler
//! WebSocket (see [`holler_client::connection`]); `join`'s redeem step
//! is still a stub (see [`holler_client::join`]) — that gap is real and
//! is not this story's to close, see that module's doc comment.

use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};

use holler_client::config::SessionRegistry;
use holler_client::connection::{self, ConnectionStateStore, LiveState};
use holler_client::credential::{CredentialStore, PersistedCredential};
use holler_client::join::{JoinTransport, StubJoinTransport};
use holler_client::server_address::ServerAddress;
use holler_client::status;

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
}

#[derive(Subcommand)]
enum Command {
    /// Redeem a join token against a server and persist this client's identity.
    Join {
        /// Server URL, e.g. ws://host:41807 or ws://[::1] (port defaults to 41807).
        #[arg(long)]
        server: String,
        /// One-time join token. Never persisted or sent again after this call.
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
    /// Print this client's status document.
    Status,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Join { server, token } => run_join(&server, &token),
        Command::Run => run_run(),
        Command::Detach => run_detach(),
        Command::Status => run_status(),
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

    let identity = StubJoinTransport
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

fn run_run() -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let credential = store
        .load()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "not joined; run `holler join` first".to_string())?;

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

fn run_status() -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let credential = store.load().map_err(|e| e.to_string())?;
    let registry = SessionRegistry::defaults();
    let hostname = current_hostname();
    let live = ConnectionStateStore::open()
        .map(|s| s.current_state(connection::STALE_AFTER))
        .unwrap_or(LiveState::Disconnected);

    let document = status::build(credential.as_ref(), &registry, hostname, live);
    let json = serde_json::to_string_pretty(&document).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}
