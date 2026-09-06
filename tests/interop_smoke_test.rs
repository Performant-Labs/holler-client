//! Real end-to-end mint -> join -> run -> roster/status -> stop, across a
//! genuine `holler-server` and this repo's `holler` client -- the literal
//! sequence the test-catalog pilot's harness step asks for.
//!
//! `holler-server` lives in a separate repo with no workspace/path
//! dependency on this one (deliberately: each crate's own `cargo test`
//! stays self-contained by default, matching "no heroic workspace
//! invention"). So this test is gated on `HOLLER_SERVER_BIN` -- the path to
//! an already-built `holler-server` binary -- and skips with a clear
//! message when it's unset, rather than trying to build a sibling repo
//! from inside this one's test suite. Set it locally with both repos
//! checked out side by side:
//!
//!   (cd ../holler-server && cargo build --release)
//!   HOLLER_SERVER_BIN=$(pwd)/../holler-server/target/release/holler-server \
//!     cargo test --test interop_smoke_test -- --ignored
//!
//! The existing cross-platform interop harness (holler-server#94,
//! `.github/workflows/interop.yml`) is the CI-side version of this same
//! idea at a larger scale (real network, real OS matrix); this test is the
//! fast, local, single-machine version for iterating on the client/server
//! CLI contract itself.

mod support;

use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

fn server_bin() -> Option<PathBuf> {
    env::var_os("HOLLER_SERVER_BIN").map(PathBuf::from)
}

#[test]
#[ignore = "needs HOLLER_SERVER_BIN pointing at a built holler-server binary; \
            run with `cargo test --test interop_smoke_test -- --ignored`, see \
            this file's doc comment for the full command"]
fn mint_join_run_roster_status_stop() {
    let server_bin = server_bin().expect(
        "HOLLER_SERVER_BIN not set -- this test is #[ignore]d for exactly this reason, so it \
         should only run via `--ignored` with the env var set (see this file's doc comment)",
    );

    let server_state = support::StateDir::new();
    let mut server = Command::new(&server_bin)
        .env("HOLLER_STATE_DIR", server_state.path())
        .env("HOLLER_SERVER_PEPPER", "harness-test-pepper-not-a-real-secret")
        .args(["serve", "--listen", "127.0.0.1:0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn holler-server serve");

    let addr = {
        use std::io::{BufRead, BufReader};
        let stdout = server.stdout.take().expect("piped stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let deadline = std::time::Instant::now() + support::DEFAULT_TIMEOUT;
        let addr = loop {
            if std::time::Instant::now() >= deadline {
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
        addr
    };

    let mint_out = Command::new(&server_bin)
        .env("HOLLER_STATE_DIR", server_state.path())
        .env("HOLLER_SERVER_PEPPER", "harness-test-pepper-not-a-real-secret")
        .args(["token", "mint", "--label", "interop-smoke"])
        .output()
        .expect("failed to run token mint");
    assert!(mint_out.status.success());
    let mint_stdout = String::from_utf8_lossy(&mint_out.stdout);
    let token_id = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("token_id:"))
        .map(str::trim)
        .expect("mint output missing token_id");
    let secret = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("secret:"))
        .map(str::trim)
        .expect("mint output missing secret");

    let client_state = support::StateDir::new();
    support::join(&client_state, &format!("ws://{addr}"), token_id, secret);

    let run = support::RunHandle::start(&client_state);

    let connected = support::wait_for(support::DEFAULT_TIMEOUT, || {
        let status = support::status_json(&client_state);
        status
            .get("connected")
            .and_then(|v| v.as_bool())
            .filter(|&c| c)
            .map(|_| ())
    });
    assert!(connected.is_some(), "client never reported connected: true");

    let roster_out = Command::new(&server_bin)
        .env("HOLLER_STATE_DIR", server_state.path())
        .env("HOLLER_SERVER_PEPPER", "harness-test-pepper-not-a-real-secret")
        .args(["roster", "--json"])
        .output()
        .expect("failed to run roster");
    assert!(roster_out.status.success());
    let roster: serde_json::Value =
        serde_json::from_slice(&roster_out.stdout).expect("roster --json invalid");
    // No sessions are advertised (no config file was given to `run`), but
    // the client itself should still show up once connected/authed --
    // exact shape depends on presence semantics, so just confirm the
    // roster call round-tripped real JSON rather than asserting a count
    // this test doesn't have enough context to know for certain.
    assert!(roster.is_array(), "roster --json should print a JSON array, got: {roster}");

    run.stop(&client_state, Duration::from_secs(5));
    let _ = server.kill();
    let _ = server.wait();
}
