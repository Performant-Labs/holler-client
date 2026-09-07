//! Network Failure & Recovery: a real server restart while a client is
//! connected -- proves the client's own reconnect-with-backoff loop
//! (src/connection.rs, issue #24) actually recovers a real dropped
//! connection, not just the backoff math in isolation (that math has its
//! own pre-existing unit-test coverage, referenced by a separate,
//! non-integration-test catalog case).
//!
//! Same `HOLLER_SERVER_BIN`-gated pattern as `interop_smoke_test.rs` --
//! see that file's doc comment for the full local run command.

mod support;

use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn server_bin() -> Option<PathBuf> {
    env::var_os("HOLLER_SERVER_BIN").map(PathBuf::from)
}

const PEPPER: &str = "harness-test-pepper-not-a-real-secret";

/// Spawns `holler-server serve --listen <listen>` against `state_dir`,
/// reads its announced address off stdout (same parsing as
/// `interop_smoke_test.rs`), and drains the rest of stdout on a
/// background thread so the child never blocks on a full pipe buffer.
fn spawn_server(bin: &Path, state_dir: &Path, listen: &str) -> (Child, String) {
    let mut server = Command::new(bin)
        .env("HOLLER_STATE_DIR", state_dir)
        .env("HOLLER_SERVER_PEPPER", PEPPER)
        .args(["serve", "--listen", listen])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holler-server serve");

    let stdout = server.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + support::DEFAULT_TIMEOUT;
    let addr = loop {
        if Instant::now() >= deadline {
            let _ = server.kill();
            panic!("holler-server serve did not announce a listen address in time");
        }
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            let _ = server.kill();
            panic!("holler-server serve exited before announcing a listen address");
        }
        if let Some(rest) = line.trim().strip_prefix("holler-server listening on: ") {
            if let Some(a) = rest.split(',').next().and_then(|s| s.trim().strip_prefix("ws://")) {
                break a.to_string();
            }
        }
    };
    std::thread::spawn(move || {
        let mut discard = String::new();
        while reader.read_line(&mut discard).unwrap_or(0) > 0 {
            discard.clear();
        }
    });
    (server, addr)
}

fn mint(bin: &Path, state_dir: &Path) -> (String, String) {
    let mint_out = Command::new(bin)
        .env("HOLLER_STATE_DIR", state_dir)
        .env("HOLLER_SERVER_PEPPER", PEPPER)
        .args(["token", "mint", "--label", "network-smoke"])
        .output()
        .expect("failed to run token mint");
    assert!(mint_out.status.success());
    let mint_stdout = String::from_utf8_lossy(&mint_out.stdout);
    let token_id = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("token_id:"))
        .map(str::trim)
        .expect("mint output missing token_id")
        .to_string();
    let secret = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("secret:"))
        .map(str::trim)
        .expect("mint output missing secret")
        .to_string();
    (token_id, secret)
}

/// hlrclnt-1602: kill the server the client is connected to, confirm the
/// client notices (`connected: false`), bring a fresh server process back
/// on the same address and state dir, confirm the client's own
/// reconnect-with-backoff loop picks the connection back up with zero
/// client-side intervention.
#[test]
#[ignore = "needs HOLLER_SERVER_BIN pointing at a built holler-server binary; \
            run with `cargo test --test network -- --ignored`, see \
            interop_smoke_test.rs's doc comment for the full command"]
fn server_restart_client_reconnects_and_reports_connected_again() {
    let server_bin =
        server_bin().expect("HOLLER_SERVER_BIN not set -- this test is #[ignore]d for exactly this reason");

    let server_state = support::StateDir::new();
    let (mut server, addr) = spawn_server(&server_bin, server_state.path(), "127.0.0.1:0");
    let (token_id, secret) = mint(&server_bin, server_state.path());

    let client_state = support::StateDir::new();
    support::join(&client_state, &format!("ws://{addr}"), &token_id, &secret);
    let run = support::RunHandle::start(&client_state);

    let connected = support::wait_for(support::DEFAULT_TIMEOUT, || {
        support::status_json(&client_state)
            .get("connected")
            .and_then(|v| v.as_bool())
            .filter(|&c| c)
    });
    assert!(connected.is_some(), "client never reported connected: true before the restart");

    // A real "mid-flight network blip" / crash -- not a graceful shutdown.
    let _ = server.kill();
    let _ = server.wait();

    let dropped = support::wait_for(support::DEFAULT_TIMEOUT, || {
        support::status_json(&client_state)
            .get("connected")
            .and_then(|v| v.as_bool())
            .filter(|&c| !c)
    });
    assert!(dropped.is_some(), "client never noticed the server going away");

    // Same HOLLER_STATE_DIR (so the token store the client's credential
    // was verified against is still there) and the SAME address (so the
    // client doesn't need to be told anything new) -- the only thing that
    // should recover this is the client's own reconnect loop.
    let (mut server2, addr2) = spawn_server(&server_bin, server_state.path(), &addr);
    assert_eq!(addr2, addr, "restarted server did not bind the same address");

    let reconnected = support::wait_for(Duration::from_secs(20), || {
        support::status_json(&client_state)
            .get("connected")
            .and_then(|v| v.as_bool())
            .filter(|&c| c)
    });
    assert!(reconnected.is_some(), "client never reconnected after the server came back");

    run.stop(&client_state, Duration::from_secs(5));
    let _ = server2.kill();
    let _ = server2.wait();
}
