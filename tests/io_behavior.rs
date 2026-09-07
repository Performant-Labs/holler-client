//! Unix I/O & Stdio Behavior group (hlrclnt-1300-1399, holler-server#98).
//! Redirection, piping, /dev/null, no-TTY, and signal handling for the
//! `holler` client binary.

mod support;

use std::process::Stdio;
use std::time::Duration;

use support::StateDir;

/// hlrclnt-1300: `holler status`'s stdout, redirected to a file, is clean
/// JSON with nothing else mixed in -- works even when never joined (it
/// reports local state either way), so no server is needed for this case.
#[test]
fn stdout_redirect_captures_clean_json() {
    let state = StateDir::new();
    let out = support::holler_cmd(&state)
        .arg("status")
        .output()
        .expect("failed to run holler status");
    assert!(out.status.success());
    assert!(
        out.stderr.is_empty(),
        "stderr should be empty on the success path, got: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _: serde_json::Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("stdout was not clean JSON ({e}): {:?}", out.stdout));
}

/// hlrclnt-1301: piping stdout into a downstream reader that closes early
/// must not hang the writer.
#[test]
fn piped_output_does_not_hang_on_closed_downstream() {
    let state = StateDir::new();
    let mut child = support::holler_cmd(&state)
        .arg("status")
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn holler status");

    drop(child.stdout.take());

    support::wait_for(Duration::from_secs(5), || child.try_wait().ok().flatten())
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("holler status hung after its stdout pipe was closed early");
        });
}

/// hlrclnt-1302: redirecting stdout+stderr to /dev/null does not hang or
/// error.
#[test]
fn redirect_to_dev_null_does_not_hang() {
    let state = StateDir::new();
    let out = support::holler_cmd(&state)
        .arg("status")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .expect("failed to run holler status with /dev/null stdio");
    assert!(out.status.success());
}

/// hlrclnt-1303: no controlling TTY / stdin already closed must not block.
#[test]
fn no_tty_stdin_closed_does_not_block() {
    let state = StateDir::new();
    let out = support::holler_cmd(&state)
        .arg("status")
        .stdin(Stdio::null())
        .output()
        .expect("failed to run holler status with stdin closed");
    assert!(out.status.success());
}

/// hlrclnt-1304 (dev-convenience only -- see holler-server#98's catalog
/// entry for why this is filed as a MANUAL case despite this real,
/// runnable test existing): SIGINT on a live `holler run` process. Client
/// `run`'s own `tokio::select!` treats `ctrl_c()` as an intentional,
/// accepted unclean-close path (see `src/main.rs`'s `run_run` doc comment)
/// -- confirmed here as exit 0, distinct from the server's graceful
/// shutdown announcement (the client has no equivalent message, by
/// design; only the state file being cleared is the documented contract).
///
/// Gated exactly like `interop_smoke_test.rs` (needs a real `holler-server`
/// binary this repo has no workspace/path dependency on) -- run manually:
///
///   (cd ../holler-server && cargo build --release)
///   HOLLER_SERVER_BIN=$(pwd)/../holler-server/target/release/holler-server \
///     cargo test --test io_behavior -- --ignored
#[test]
#[ignore = "needs HOLLER_SERVER_BIN pointing at a built holler-server binary; \
            see this test's doc comment for the full command"]
fn sigint_on_live_run_exits_cleanly() {
    use std::env;
    use std::io::{BufRead, BufReader};
    use std::path::PathBuf;
    use std::process::Command;

    let server_bin: PathBuf = env::var_os("HOLLER_SERVER_BIN")
        .map(PathBuf::from)
        .expect("HOLLER_SERVER_BIN not set -- see this test's doc comment");

    let server_state = StateDir::new();
    let mut server = Command::new(&server_bin)
        .env("HOLLER_STATE_DIR", server_state.path())
        .env("HOLLER_SERVER_PEPPER", "harness-test-pepper-not-a-real-secret")
        .args(["serve", "--listen", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holler-server serve");

    let addr = {
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
                if let Some(a) = rest.split(',').next().and_then(|s| s.trim().strip_prefix("ws://"))
                {
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
        .args(["token", "mint", "--label", "io-sigint-test"])
        .output()
        .expect("failed to run holler-server token mint");
    let mint_stdout = String::from_utf8_lossy(&mint_out.stdout);
    let token_id = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("token_id:").map(str::trim))
        .expect("mint output missing token_id");
    let secret = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("secret:").map(str::trim))
        .expect("mint output missing secret");

    let client_state = StateDir::new();
    support::join(&client_state, &format!("ws://{addr}"), token_id, secret);

    let mut client = support::holler_cmd(&client_state)
        .arg("run")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holler run");

    // Give it a moment to actually reach the tokio::select! await point
    // rather than racing SIGINT against startup.
    std::thread::sleep(Duration::from_millis(300));

    // SAFETY: `client.id()` names a live child process this test owns
    // exclusively; SIGINT does not touch memory.
    unsafe {
        libc::kill(client.id() as libc::pid_t, libc::SIGINT);
    }

    let status = support::wait_for(Duration::from_secs(5), || client.try_wait().ok().flatten())
        .unwrap_or_else(|| {
            let _ = client.kill();
            panic!("holler run did not exit within 5s of SIGINT");
        });
    assert!(status.success(), "SIGINT on `holler run` should exit 0, got {status:?}");

    let _ = server.kill();
    let _ = server.wait();
}

/// hlrclnt-1305 (Unix I/O & Stdio Behavior): unlike SIGINT (above),
/// `holler run` does NOT currently catch SIGTERM at all -- `run_run`'s
/// `tokio::select!` only arms `tokio::signal::ctrl_c()`, which is SIGINT
/// specifically, not SIGTERM. A bare SIGTERM therefore hits the OS's
/// default disposition (immediate termination, no in-process cleanup
/// code ever runs) rather than the graceful, `state.clear()`-ing exit
/// SIGINT gets. Confirmed empirically before writing this test: sending
/// SIGTERM to a live `holler run` kills it (exit status "terminated by
/// signal 15") and leaves `connection_state.json` un-cleared, unlike a
/// clean SIGINT/normal exit. This pins that CURRENT gap -- same pattern
/// as `hlrsvr-1305` on the server side -- it does not fix it. If this
/// test ever fails because SIGTERM starts exiting cleanly, the gap has
/// been closed: update this test (and hlrclnt-1305's issue) to require
/// the clean-exit outcome instead.
///
/// Gated exactly like `sigint_on_live_run_exits_cleanly` above -- needs a
/// real `holler-server` binary this repo has no workspace/path dependency
/// on. Run manually:
///
///   (cd ../holler-server && cargo build --release)
///   HOLLER_SERVER_BIN=$(pwd)/../holler-server/target/release/holler-server \
///     cargo test --test io_behavior -- --ignored
#[test]
#[ignore = "needs HOLLER_SERVER_BIN pointing at a built holler-server binary; \
            see this test's doc comment for the full command"]
fn sigterm_on_live_run_is_not_caught_and_kills_it_uncleanly() {
    use std::env;
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;
    use std::process::Command;

    let server_bin: PathBuf = env::var_os("HOLLER_SERVER_BIN")
        .map(PathBuf::from)
        .expect("HOLLER_SERVER_BIN not set -- see this test's doc comment");

    let server_state = StateDir::new();
    let mut server = Command::new(&server_bin)
        .env("HOLLER_STATE_DIR", server_state.path())
        .env("HOLLER_SERVER_PEPPER", "harness-test-pepper-not-a-real-secret")
        .args(["serve", "--listen", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holler-server serve");

    let addr = {
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
                if let Some(a) = rest.split(',').next().and_then(|s| s.trim().strip_prefix("ws://"))
                {
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
        .args(["token", "mint", "--label", "io-sigterm-test"])
        .output()
        .expect("failed to run holler-server token mint");
    let mint_stdout = String::from_utf8_lossy(&mint_out.stdout);
    let token_id = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("token_id:").map(str::trim))
        .expect("mint output missing token_id");
    let secret = mint_stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("secret:").map(str::trim))
        .expect("mint output missing secret");

    let client_state = StateDir::new();
    support::join(&client_state, &format!("ws://{addr}"), token_id, secret);

    let mut client = support::holler_cmd(&client_state)
        .arg("run")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn holler run");

    std::thread::sleep(Duration::from_millis(300));

    // SAFETY: `client.id()` names a live child process this test owns
    // exclusively; SIGTERM does not touch memory.
    unsafe {
        libc::kill(client.id() as libc::pid_t, libc::SIGTERM);
    }

    let status = support::wait_for(Duration::from_secs(5), || client.try_wait().ok().flatten())
        .unwrap_or_else(|| {
            let _ = client.kill();
            panic!("holler run did not exit within 5s of SIGTERM");
        });
    assert_eq!(
        status.signal(),
        Some(libc::SIGTERM),
        "expected today's gap: SIGTERM is not caught, so the process dies \
         by that signal rather than exiting cleanly -- got {status:?}"
    );

    let _ = server.kill();
    let _ = server.wait();
}
