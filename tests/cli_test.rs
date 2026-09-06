//! Integration test for `holler join`/`detach`/`status`/`support`/`caps`/
//! `query` (issues #23, #30), driven through the actual built `holler`
//! binary so it exercises real argument parsing, exit codes, and stdout
//! the way an operator sees them.

use std::process::Command;

fn holler() -> Command {
    Command::new(env!("CARGO_BIN_EXE_holler"))
}

/// A fresh, isolated `HOLLER_STATE_DIR` per test so parallel tests never
/// share a credential file, with `$PATH` deterministically controlled
/// (empty by default) so harness-confirmation answers never depend on
/// what happens to be installed on the machine running these tests.
struct Env {
    dir: tempfile::TempDir,
    path_dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        Env {
            dir: tempfile::tempdir().unwrap(),
            path_dir: tempfile::tempdir().unwrap(),
        }
    }

    fn with_fake_executable(self, name: &str) -> Self {
        let exe = self.path_dir.path().join(name);
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&exe).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&exe, perms).unwrap();
        }
        self
    }

    fn cmd(&self) -> Command {
        let mut cmd = holler();
        cmd.env("HOLLER_STATE_DIR", self.dir.path());
        cmd.env("PATH", self.path_dir.path());
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

#[test]
fn support_reports_true_for_an_implemented_protocol_feature() {
    let env = Env::new();
    let out = env.cmd().args(["support", "ping"]).output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("\"ok\": true"));
    assert!(stdout.contains("\"kind\": \"feature\""));
}

#[test]
fn support_reports_false_for_an_unconfirmed_harness() {
    let env = Env::new(); // empty $PATH: "opencode" is configured but not runnable
    let out = env.cmd().args(["support", "opencode"]).output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("\"ok\": false"));
    assert!(stdout.contains("\"reason\": \"no adapter\""));
}

#[test]
fn support_reports_true_for_a_confirmed_runnable_harness() {
    let env = Env::new().with_fake_executable("opencode");
    let out = env.cmd().args(["support", "opencode"]).output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("\"ok\": true"));
    assert!(stdout.contains("\"kind\": \"harness\""));
}

#[test]
fn caps_lists_every_known_protocol_feature_and_harness() {
    let env = Env::new();
    let out = env.cmd().arg("caps").output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    let body: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(body["cmd"], "caps");
    assert!(body["capabilities"]["query"].is_object());
    assert!(body["capabilities"]["opencode"].is_object());
}

#[test]
fn query_protocol_reports_this_binarys_min_max() {
    let env = Env::new();
    let out = env.cmd().args(["query", "protocol"]).output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(body["cmd"], "protocol");
    assert_eq!(body["min"], 1);
    assert_eq!(body["max"], 1);
}

#[test]
fn query_protocol_with_arg_answers_can_you_speak_n() {
    let env = Env::new();
    let out = env.cmd().args(["query", "protocol", "2"]).output().unwrap();
    assert!(out.status.success(), "{out:?}");
    let body: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(body["ok"], false);
    assert_eq!(body["asked"], 2);
}

#[test]
fn query_unknown_cmd_is_a_clear_cli_failure() {
    let env = Env::new();
    let out = env.cmd().args(["query", "summarize"]).output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.to_lowercase().contains("unknown"));
}
