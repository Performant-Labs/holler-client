//! Logging & Debug Levels group (holler-server#98, hlrclnt-1200 range).
//!
//! `holler-client` has no `RUST_LOG` -- verbosity is its own two-axis
//! `--debug`/`--log-format` contract (`src/debug.rs`), which already has
//! extensive unit coverage for parsing/precedence (see that module's own
//! `mod tests`; several `hlrclnt-12xx` cases point straight at those real
//! functions instead of duplicating them here). This file holds the two
//! properties that can only be proven at the real-process level: that an
//! invalid value actually fails the process closed, and that debug output
//! actually lands on stderr, not stdout.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;

fn holler() -> Command {
    Command::cargo_bin("holler").expect("holler binary not built")
}

/// hlrclnt-1200: an invalid `--debug` value fails the whole process
/// closed -- non-zero exit, a clear message on stderr, nothing on
/// stdout -- for any subcommand.
#[test]
fn invalid_debug_value_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    holler()
        .env("HOLLER_STATE_DIR", dir.path())
        .args(["--debug=bogus", "status"])
        .assert()
        .failure()
        .code(1)
        .stdout("")
        .stderr(predicates::str::contains(
            "invalid debug level \"bogus\" from --debug (expected one of: none, quiet, noisy)",
        ));
}

/// hlrclnt-1203: at `--debug=noisy`, log lines go to stderr only. `run`
/// emits its startup `logging_started` line before doing any I/O
/// (including the "not joined" check), so this is provable without a
/// real join or server -- both the log line and the resulting error land
/// on stderr, stdout stays completely empty.
#[test]
fn noisy_debug_logs_go_to_stderr_not_stdout() {
    let dir = tempfile::tempdir().unwrap();
    holler()
        .env("HOLLER_STATE_DIR", dir.path())
        .args(["--debug=noisy", "run"])
        .assert()
        .failure()
        .stdout("")
        .stderr(
            predicates::str::contains("logging_started")
                .and(predicates::str::contains("not joined")),
        );
}
