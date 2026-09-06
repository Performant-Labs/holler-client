//! Shared test-support harness for `holler-client` integration tests
//! (holler-server#98's test-catalog pilot). Real subprocess orchestration
//! against the built `holler` binary and its actual CLI surface.
//!
//! Scope note: this module only drives `holler-client` itself (state dir,
//! `join`, `run`, `status`, `detach`). `join`/`run` need a real server to
//! talk to; a genuine cross-repo smoke test (a real `holler-server` plus
//! this client) is gated behind the `HOLLER_SERVER_BIN` env var in
//! `interop_smoke_test.rs` rather than reimplementing server startup here —
//! see that file for why.
//!
//! Not every test binary in this repo uses every helper here -- `dead_code`
//! is allowed at module scope for that reason, the same way a shared
//! test-support module does in most crates.
#![allow(dead_code)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use assert_cmd::cargo::CommandCargoExt;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// A fresh, empty `HOLLER_STATE_DIR` for one test.
pub struct StateDir(tempfile::TempDir);

impl StateDir {
    pub fn new() -> Self {
        Self(tempfile::tempdir().expect("failed to create ephemeral HOLLER_STATE_DIR"))
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }
}

impl Default for StateDir {
    fn default() -> Self {
        Self::new()
    }
}

/// Poll `check` every 20ms until it returns `Some(_)` or `timeout` elapses.
pub fn wait_for<T>(timeout: Duration, mut check: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = check() {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn holler_cmd(state_dir: &StateDir) -> Command {
    let mut cmd = Command::cargo_bin("holler").expect(
        "holler binary not built -- run `cargo build` (or `cargo test`, which builds it \
         automatically) before invoking harness helpers directly",
    );
    cmd.env("HOLLER_STATE_DIR", state_dir.path());
    cmd
}

/// `holler join --server <url> --token <token_id>:<secret>` against
/// `state_dir` -- the real one-shot redemption a human runs once per
/// machine. Panics with the real stderr on failure.
pub fn join(state_dir: &StateDir, server_url: &str, token_id: &str, secret: &str) {
    let out = holler_cmd(state_dir)
        .args(["join", "--server", server_url, "--token", &format!("{token_id}:{secret}")])
        .output()
        .expect("failed to run `holler join`");
    assert!(
        out.status.success(),
        "`holler join` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A live `holler run` subprocess (the long-lived, foreground session
/// process) against an already-`join`ed `state_dir`.
pub struct RunHandle {
    child: Child,
}

impl RunHandle {
    /// Spawns `holler run` in the background. stdout/stderr go to
    /// `Stdio::null()` -- `run`'s own query interface is a separate
    /// `holler status` invocation (see `status_json` below), not its
    /// stdout stream, so there's nothing a test needs to read from the
    /// process itself and no pipe-buffer deadlock risk to guard against.
    /// Doesn't block on any particular readiness signal (unlike the
    /// server's `serve`, `run` doesn't announce one) -- callers should
    /// poll `status_json`/`wait_for` for the specific condition they need
    /// (e.g. `connected: true`) rather than assuming readiness at return.
    pub fn start(state_dir: &StateDir) -> Self {
        let child = holler_cmd(state_dir)
            .arg("run")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn `holler run`");
        Self { child }
    }

    /// Graceful stop: `holler detach` against the same state dir (the
    /// real, documented way to end a session -- it signals the live `run`
    /// process and removes the local credential), then a forced kill if
    /// the process hasn't exited within `timeout`. Windows has no SIGINT
    /// equivalent for `run`, so `detach` (not a signal) is the portable
    /// graceful path on every OS -- both live in this one method.
    pub fn stop(mut self, state_dir: &StateDir, timeout: Duration) {
        let _ = holler_cmd(state_dir).arg("detach").output();
        let deadline = Instant::now() + timeout;
        loop {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `holler status` against `state_dir` -- always prints JSON (no `--json`
/// flag needed, unlike the server's query commands).
pub fn status_json(state_dir: &StateDir) -> serde_json::Value {
    let out = holler_cmd(state_dir)
        .arg("status")
        .output()
        .expect("failed to run `holler status`");
    assert!(
        out.status.success(),
        "`holler status` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "`holler status` did not print valid JSON ({e}):\n{}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}
