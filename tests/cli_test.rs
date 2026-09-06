//! Integration test for `holler join`/`detach`/`status` (issue #23),
//! driven through the actual built `holler` binary so it exercises real
//! argument parsing, exit codes, and stdout the way an operator sees them.

use std::process::Command;

fn holler() -> Command {
    Command::new(env!("CARGO_BIN_EXE_holler"))
}

/// A fresh, isolated `HOLLER_STATE_DIR` per test so parallel tests never
/// share a credential file.
struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        Env {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn cmd(&self) -> Command {
        let mut cmd = holler();
        cmd.env("HOLLER_STATE_DIR", self.dir.path());
        cmd
    }
}

#[test]
fn status_before_join_reports_not_joined() {
    let env = Env::new();
    let out = env.cmd().arg("status").output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(!stdout.contains("client_id"));
    assert!(stdout.contains("\"role\": \"client\""));
}

#[test]
fn join_then_status_then_detach_then_status() {
    let env = Env::new();
    let token = "hlr_join_integrationtesttoken";

    // join
    let join_out = env
        .cmd()
        .args(["join", "--server", "ws://example.com:41807", "--token", token])
        .output()
        .unwrap();
    assert!(join_out.status.success(), "{join_out:?}");
    let join_stdout = String::from_utf8(join_out.stdout).unwrap();
    assert!(join_stdout.contains("client_id=cli_"));
    assert!(!join_stdout.contains(token));

    // status shows the joined identity
    let status_out = env.cmd().arg("status").output().unwrap();
    assert!(status_out.status.success());
    let status_stdout = String::from_utf8(status_out.stdout).unwrap();
    assert!(status_stdout.contains("\"client_id\""));
    assert!(status_stdout.contains("cli_"));
    assert!(!status_stdout.contains(token));

    // credential.json on disk holds no join token, and status never echoes it
    let credential_contents =
        std::fs::read_to_string(env.dir.path().join("credential.json")).unwrap();
    assert!(!credential_contents.contains(token));
    assert!(credential_contents.contains("cli_"));

    // detach deletes the credential
    let detach_out = env.cmd().arg("detach").output().unwrap();
    assert!(detach_out.status.success(), "{detach_out:?}");
    assert!(!env.dir.path().join("credential.json").exists());

    // status now reports not joined again
    let status_out2 = env.cmd().arg("status").output().unwrap();
    assert!(status_out2.status.success());
    let status_stdout2 = String::from_utf8(status_out2.stdout).unwrap();
    assert!(!status_stdout2.contains("client_id"));
}

#[test]
fn detach_without_join_is_not_an_error() {
    let env = Env::new();
    let out = env.cmd().arg("detach").output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("not joined"));
}

#[test]
fn join_rejects_bad_server_url() {
    let env = Env::new();
    let out = env
        .cmd()
        .args(["join", "--server", "not-a-url", "--token", "hlr_join_x"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.to_lowercase().contains("ws://") || stderr.to_lowercase().contains("scheme"));
}

#[test]
fn join_accepts_ipv6_bracketed_server_and_default_port() {
    let env = Env::new();
    let out = env
        .cmd()
        .args(["join", "--server", "ws://[::1]", "--token", "hlr_join_ipv6"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("ws://[::1]:41807"));
}

#[test]
fn join_never_prints_the_token_even_on_help() {
    let out = holler().args(["join", "--help"]).output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(!stdout.contains("hlr_"));
}

#[test]
fn rejoin_replaces_previous_identity() {
    let env = Env::new();
    env.cmd()
        .args(["join", "--server", "ws://example.com", "--token", "hlr_join_first"])
        .output()
        .unwrap();
    let first_status = String::from_utf8(env.cmd().arg("status").output().unwrap().stdout).unwrap();

    env.cmd()
        .args(["join", "--server", "ws://example.com", "--token", "hlr_join_second"])
        .output()
        .unwrap();
    let second_status =
        String::from_utf8(env.cmd().arg("status").output().unwrap().stdout).unwrap();

    assert_ne!(first_status, second_status);
}
