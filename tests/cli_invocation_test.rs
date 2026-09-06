//! Invocation-group test cases (hlrclnt-1000 / hlrclnt-1001 / hlrclnt-1002,
//! holler-server#98). Exercises the built `holler` binary as a real
//! subprocess -- clap's own generated behavior, not this crate's code
//! directly, so a real process invocation is the only thing that
//! actually proves it.

use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn holler() -> Command {
    Command::cargo_bin("holler").expect("holler binary not built")
}

/// hlrclnt-1000: `--version` and `-V` report the crate's real version.
#[rstest]
#[case("--version")]
#[case("-V")]
fn version_flag_prints_crate_version(#[case] flag: &str) {
    holler()
        .arg(flag)
        .assert()
        .success()
        .stdout(format!("holler {}\n", env!("CARGO_PKG_VERSION")));
}

/// hlrclnt-1001: `--help` prints the full subcommand list to stdout and
/// exits 0.
#[test]
fn help_flag_prints_full_usage() {
    let assert = holler().arg("--help").assert().success().stderr(predicate::str::is_empty());

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();
    for needle in ["join", "run", "detach", "status", "support", "caps", "query", "--config", "--debug"] {
        assert!(
            stdout.contains(needle),
            "--help output should list `{needle}`, got:\n{stdout}"
        );
    }
}

/// hlrclnt-1002: an invocation with no subcommand, or an unrecognized one,
/// fails closed -- usage/error text on stderr only, exit code 2 -- rather
/// than silently doing something, printing to stdout, or hanging.
#[rstest]
#[case::bare(&[][..], &["Usage: holler"])]
#[case::unknown_subcommand(&["bogus"][..], &["unrecognized subcommand", "bogus"])]
fn bare_and_unknown_subcommand_fail_closed(#[case] args: &[&str], #[case] expect_stderr_contains: &[&str]) {
    let assert = holler()
        .args(args)
        .assert()
        .failure()
        .code(2)
        .stdout(predicate::str::is_empty());

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    for needle in expect_stderr_contains {
        assert!(stderr.contains(needle), "expected stderr to contain `{needle}`, got:\n{stderr}");
    }
}
