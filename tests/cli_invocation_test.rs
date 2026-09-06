//! Invocation-group test cases (hlrclnt-1000 / hlrclnt-1001 / hlrclnt-1002,
//! holler-server#98). Exercises the built `holler` binary as a real
//! subprocess -- clap's own generated behavior, not this crate's code
//! directly, so a real process invocation is the only thing that
//! actually proves it.

use std::process::Command;

fn holler() -> Command {
    Command::new(env!("CARGO_BIN_EXE_holler"))
}

/// hlrclnt-1000: `--version` and `-V` report the crate's real version.
#[test]
fn version_flag_prints_crate_version() {
    let expected = format!("holler {}\n", env!("CARGO_PKG_VERSION"));

    for flag in ["--version", "-V"] {
        let out = holler().arg(flag).output().expect("failed to run holler");
        assert!(out.status.success(), "`{flag}` should exit 0");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            expected,
            "`{flag}` stdout should be exactly `holler <version>`"
        );
    }
}

/// hlrclnt-1001: `--help` prints the full subcommand list to stdout and
/// exits 0.
#[test]
fn help_flag_prints_full_usage() {
    let out = holler().arg("--help").output().expect("failed to run holler");
    assert!(out.status.success(), "`--help` should exit 0");

    let stdout = String::from_utf8_lossy(&out.stdout);
    for subcommand in ["join", "run", "detach", "status", "support", "caps", "query"] {
        assert!(
            stdout.contains(subcommand),
            "--help output should list `{subcommand}`, got:\n{stdout}"
        );
    }
    for global_opt in ["--config", "--debug"] {
        assert!(
            stdout.contains(global_opt),
            "--help output should list the global `{global_opt}` option, got:\n{stdout}"
        );
    }
    assert!(
        String::from_utf8_lossy(&out.stderr).is_empty(),
        "--help should not write to stderr"
    );
}

/// hlrclnt-1002: an invocation with no subcommand, or an unrecognized
/// one, fails closed -- usage/error text on stderr (never stdout),
/// non-zero exit -- rather than silently doing something or hanging.
#[test]
fn bare_and_unknown_subcommand_fail_closed() {
    let bare = holler().output().expect("failed to run holler");
    assert!(!bare.status.success(), "bare invocation must not exit 0");
    assert_eq!(
        bare.status.code(),
        Some(2),
        "bare invocation should exit with clap's usage-error code 2"
    );
    assert!(
        bare.stdout.is_empty(),
        "bare invocation must not print to stdout"
    );
    assert!(
        String::from_utf8_lossy(&bare.stderr).contains("Usage: holler"),
        "bare invocation's stderr should show usage"
    );

    let unknown = holler()
        .arg("bogus")
        .output()
        .expect("failed to run holler");
    assert!(!unknown.status.success(), "an unknown subcommand must not exit 0");
    assert_eq!(
        unknown.status.code(),
        Some(2),
        "an unknown subcommand should exit with clap's usage-error code 2"
    );
    assert!(
        unknown.stdout.is_empty(),
        "an unknown subcommand must not print to stdout"
    );
    let unknown_stderr = String::from_utf8_lossy(&unknown.stderr);
    assert!(
        unknown_stderr.contains("unrecognized subcommand") && unknown_stderr.contains("bogus"),
        "an unknown subcommand's stderr should name it, got:\n{unknown_stderr}"
    );
}
