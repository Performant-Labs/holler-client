//! `holler` CLI: `join` / `detach` / `status` (issue #23).
//!
//! This binary does not open a Holler WebSocket — `join`'s redeem is a
//! stub (see [`holler_client::join`]) and `detach` has no live connection
//! to close. Real wire traffic is issue #24.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use holler_client::config::SessionRegistry;
use holler_client::credential::{CredentialStore, PersistedCredential};
use holler_client::join::{JoinTransport, StubJoinTransport};
use holler_client::server_address::ServerAddress;
use holler_client::status;

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
    /// Disconnect and delete the local credential.
    Detach,
    /// Print this client's status document.
    Status,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Join { server, token } => run_join(&server, &token),
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

fn run_detach() -> Result<(), String> {
    let store = CredentialStore::open().map_err(|e| e.to_string())?;
    let was_joined = store.load().map_err(|e| e.to_string())?.is_some();
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

    let document = status::build(credential.as_ref(), &registry, hostname);
    let json = serde_json::to_string_pretty(&document).map_err(|e| e.to_string())?;
    println!("{json}");
    Ok(())
}
