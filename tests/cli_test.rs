//! Integration test for `holler join`/`detach`/`status`/`support`/`caps`/
//! `query` (issues #23, #30, #16), driven through the actual built `holler`
//! binary so it exercises real argument parsing, exit codes, and stdout
//! the way an operator sees them.
//!
//! `join` now performs a real WebSocket round-trip (`docs/protocol/v1.md`
//! §4.1, ADR 0015) via [`holler_client::join::WsJoinTransport`], so every
//! test that actually redeems a token spins up a minimal local WS test
//! server standing in for holler-server — the same pattern
//! `tests/connection_test.rs` uses for `holler run`. Each server binds
//! `127.0.0.1:0` / `[::1]:0` (an OS-assigned free port), never a fixed one.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use holler_client::proto::{self, Body, JoinBody, JoinOkBody};

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

    /// Spawns `holler join --server <server> --token <token>` without
    /// waiting for it — the caller drives a fake server against it, then
    /// collects output itself.
    fn spawn_join(&self, server: &str, token: &str) -> Child {
        self.cmd()
            .args(["join", "--server", server, "--token", token])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn `holler join`")
    }
}

async fn bind_local() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, format!("ws://127.0.0.1:{port}"))
}

async fn bind_local_ipv6() -> (TcpListener, String) {
    let listener = TcpListener::bind("[::1]:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, format!("ws://[::1]:{port}"))
}

async fn accept_ws(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

async fn next_envelope(ws: &mut WebSocketStream<TcpStream>) -> Option<proto::Envelope> {
    match ws.next().await {
        Some(Ok(Message::Text(t))) => proto::decode(&t).ok(),
        _ => None,
    }
}

async fn send_envelope(ws: &mut WebSocketStream<TcpStream>, env: &proto::Envelope) {
    let raw = proto::encode(env).unwrap();
    ws.send(Message::Text(raw.into())).await.unwrap();
}

async fn expect_join(ws: &mut WebSocketStream<TcpStream>) -> (String, JoinBody) {
    let envelope = next_envelope(ws).await.expect("expected a `join` frame");
    match envelope.body {
        Body::Join(body) => (envelope.from, body),
        other => panic!("expected `join`, got {other:?}"),
    }
}

fn join_ok_envelope(reply_id: &str, client_id: &str, credential: &str) -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::JoinOk,
        id: reply_id.to_string(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::JoinOk(JoinOkBody {
            client_id: client_id.to_string(),
            credential: credential.to_string(),
        }),
    }
}

/// Waits (off the async runtime, since [`Child::wait_with_output`] is
/// blocking) for the join process to exit and collects its output.
fn finish_join(child: Child) -> std::process::Output {
    child.wait_with_output().expect("failed to wait on `holler join`")
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

#[tokio::test]
async fn join_then_status_then_detach_then_status() {
    let env = Env::new();
    let secret = "hlr_join_integrationtesttoken";
    let token = format!("tok_integration:{secret}");

    // join: a real WS round-trip against a local test-double server.
    let (listener, url) = bind_local().await;
    let child = env.spawn_join(&url, &token);

    let mut ws = accept_ws(&listener).await;
    let (from, body) = expect_join(&mut ws).await;
    assert_eq!(from, "tok_integration");
    assert_eq!(body.secret, secret);
    assert!(!body.hostname.is_empty());

    let reply = join_ok_envelope("ignored", "cli_integrationtest", "hlr_live_integrationtestcred");
    send_envelope(&mut ws, &reply).await;

    // `join` is a one-shot bootstrap (spec §4.1): the client must close
    // its end after the reply, not linger or continue into `hello`.
    // Deliberately don't close our (server) side first, so this proves
    // the *client* closes it rather than just observing our own close.
    match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {}
        other => panic!("expected the client to close the connection after `join_ok`, got {other:?}"),
    }

    let join_out = finish_join(child);
    assert!(join_out.status.success(), "{join_out:?}");
    let join_stdout = String::from_utf8(join_out.stdout).unwrap();
    assert!(join_stdout.contains("client_id=cli_integrationtest"));
    assert!(!join_stdout.contains(secret));
    assert!(!join_stdout.contains(&token));

    // status shows the joined identity
    let status_out = env.cmd().arg("status").output().unwrap();
    assert!(status_out.status.success());
    let status_stdout = String::from_utf8(status_out.stdout).unwrap();
    assert!(status_stdout.contains("\"client_id\""));
    assert!(status_stdout.contains("cli_integrationtest"));
    assert!(!status_stdout.contains(secret));

    // credential.json on disk holds no join secret, and status never echoes it
    let credential_contents =
        std::fs::read_to_string(env.dir.path().join("credential.json")).unwrap();
    assert!(!credential_contents.contains(secret));
    assert!(!credential_contents.contains(&token));
    assert!(credential_contents.contains("cli_integrationtest"));

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
    // No network is ever touched here: URL parsing fails before dialing.
    let env = Env::new();
    let out = env
        .cmd()
        .args(["join", "--server", "not-a-url", "--token", "tok_x:hlr_join_x"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.to_lowercase().contains("ws://") || stderr.to_lowercase().contains("scheme"));
}

#[test]
fn join_rejects_a_token_with_no_token_id() {
    // No network is ever touched here either: the malformed token is
    // rejected before a socket is opened.
    let env = Env::new();
    let out = env
        .cmd()
        .args(["join", "--server", "ws://127.0.0.1:1", "--token", "hlr_join_x"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.to_lowercase().contains("token_id"), "{stderr:?}");
}

#[tokio::test]
async fn join_accepts_ipv6_bracketed_server() {
    let env = Env::new();
    let secret = "hlr_join_ipv6";
    let token = format!("tok_ipv6:{secret}");

    let (listener, url) = bind_local_ipv6().await;
    let child = env.spawn_join(&url, &token);

    let mut ws = accept_ws(&listener).await;
    expect_join(&mut ws).await;
    send_envelope(
        &mut ws,
        &join_ok_envelope("ignored", "cli_ipv6", "hlr_live_ipv6cred"),
    )
    .await;
    let _ = ws.close(None).await;

    let out = finish_join(child);
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains(&url), "expected {stdout:?} to contain {url:?}");
}

#[test]
fn join_never_prints_the_token_even_on_help() {
    let out = holler().args(["join", "--help"]).output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(!stdout.contains("hlr_"));
}

#[tokio::test]
async fn join_reports_join_failed_error_clearly() {
    let env = Env::new();
    let token = "tok_bad:hlr_join_bad";

    let (listener, url) = bind_local().await;
    let child = env.spawn_join(&url, token);

    let mut ws = accept_ws(&listener).await;
    expect_join(&mut ws).await;
    let error_envelope = proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Error,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Error(proto::ErrorBody {
            code: proto::CODE_JOIN_FAILED.to_string(),
            cmd: None,
            message: Some("secret already bound".to_string()),
        }),
    };
    send_envelope(&mut ws, &error_envelope).await;
    let _ = ws.close(None).await;

    let out = finish_join(child);
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("secret already bound"), "{stderr:?}");
    assert!(!stderr.contains("hlr_join_bad"), "must never log the secret");

    // Nothing was persisted for a failed join.
    assert!(!env.dir.path().join("credential.json").exists());
}

#[test]
fn join_connection_refused_is_a_clear_error_not_a_hang() {
    let env = Env::new();
    // Bind then drop immediately: very likely nothing is listening on
    // this port for the duration of the test.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let out = env
        .cmd()
        .args([
            "join",
            "--server",
            &format!("ws://127.0.0.1:{port}"),
            "--token",
            "tok_x:hlr_join_x",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(!stderr.is_empty());
}

#[tokio::test]
async fn rejoin_replaces_previous_identity() {
    let env = Env::new();

    let (listener1, url1) = bind_local().await;
    let child1 = env.spawn_join(&url1, "tok_first:hlr_join_first");
    let mut ws1 = accept_ws(&listener1).await;
    expect_join(&mut ws1).await;
    send_envelope(
        &mut ws1,
        &join_ok_envelope("ignored", "cli_first", "hlr_live_firstcred"),
    )
    .await;
    let _ = ws1.close(None).await;
    assert!(finish_join(child1).status.success());
    let first_status = String::from_utf8(env.cmd().arg("status").output().unwrap().stdout).unwrap();

    let (listener2, url2) = bind_local().await;
    let child2 = env.spawn_join(&url2, "tok_second:hlr_join_second");
    let mut ws2 = accept_ws(&listener2).await;
    expect_join(&mut ws2).await;
    send_envelope(
        &mut ws2,
        &join_ok_envelope("ignored", "cli_second", "hlr_live_secondcred"),
    )
    .await;
    let _ = ws2.close(None).await;
    assert!(finish_join(child2).status.success());
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
