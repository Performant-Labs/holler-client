//! Integration tests for the live WebSocket session (issue #24), driven
//! through the actual built `holler` binary (`holler run`) against a
//! minimal local WS test server built in this file with `tokio-tungstenite`
//! directly — not the holler-server repo's own `tests/wire/` harness,
//! which assumes a fixed port (`ws://127.0.0.1:41807`) this file must not
//! collide with. Every server here binds `127.0.0.1:0` (an OS-assigned
//! free port) instead.
//!
//! `holler join`'s redeem step is still a stub (issue #23; see
//! `src/join.rs`), so these tests never run `holler join` against this
//! fake server — they write `credential.json` directly, exactly the
//! shape a real join would have persisted, which is exactly what "resume
//! with the credential" (this story's actual scope) means to exercise.

use std::io::Read as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use holler_client::proto::{self, Body, ErrorBody, HelloBody, PingBody, Role};

fn holler() -> Command {
    Command::new(env!("CARGO_BIN_EXE_holler"))
}

/// A fresh, isolated `HOLLER_STATE_DIR` per test.
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

    /// Writes `credential.json` directly — standing in for a completed
    /// `holler join` (whose real network redeem doesn't exist yet; see
    /// module docs), since this story's scope is resuming with an
    /// already-persisted credential.
    fn write_credential(&self, server_url: &str, credential: &str, client_id: &str, hostname: &str) {
        let contents = serde_json::json!({
            "client_id": client_id,
            "credential": credential,
            "server": server_url,
            "hostname": hostname,
        });
        std::fs::write(
            self.dir.path().join("credential.json"),
            serde_json::to_string_pretty(&contents).unwrap(),
        )
        .unwrap();
    }

    fn status_json(&self) -> Value {
        let out = self.cmd().arg("status").output().unwrap();
        assert!(out.status.success(), "{out:?}");
        serde_json::from_slice(&out.stdout).unwrap()
    }

    /// Polls `holler status` until `predicate` accepts the parsed
    /// document, or panics after `budget`. A hang here is a test
    /// failure, not a silent pass.
    fn wait_for_status(&self, budget: Duration, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = std::time::Instant::now() + budget;
        loop {
            let doc = self.status_json();
            if predicate(&doc) {
                return doc;
            }
            if std::time::Instant::now() >= deadline {
                panic!("status never matched predicate within {budget:?}; last seen: {doc}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn spawn_run(env: &Env) -> Child {
    env.cmd()
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn `holler run`")
}

fn spawn_run_capturing_stderr(env: &Env) -> Child {
    env.cmd()
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn `holler run`")
}

fn kill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Waits for `child` to exit on its own within `budget`; kills it and
/// returns `None` on timeout (a timeout is the test's failure to report,
/// never a silent hang).
fn wait_for_exit(child: &mut Child, budget: Duration) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn bind_local() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, format!("ws://127.0.0.1:{port}"))
}

async fn accept_ws(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

/// Reads the next `Message::Text` frame, skipping WS-level ping/pong
/// control frames, treating close/EOF/error as "no frame".
async fn next_text(ws: &mut WebSocketStream<TcpStream>) -> Option<String> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => return Some(t.to_string()),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => return None,
        }
    }
}

async fn next_envelope(ws: &mut WebSocketStream<TcpStream>) -> Option<proto::Envelope> {
    let raw = next_text(ws).await?;
    proto::decode(&raw).ok()
}

async fn send_envelope(ws: &mut WebSocketStream<TcpStream>, env: &proto::Envelope) {
    let raw = proto::encode(env).unwrap();
    ws.send(Message::Text(raw.into())).await.unwrap();
}

async fn expect_auth(ws: &mut WebSocketStream<TcpStream>) -> proto::AuthBody {
    let envelope = next_envelope(ws).await.expect("expected an `auth` frame");
    match envelope.body {
        Body::Auth(a) => a,
        other => panic!("expected `auth`, got {other:?}"),
    }
}

fn server_hello_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Hello,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Hello(HelloBody {
            protocol: 1,
            protocol_min: 1,
            protocol_max: 1,
            role: Role::Server,
            hostname: "test-server".to_string(),
            token_id: None,
            client_id: None,
            harnesses: Vec::new(),
            harnesses_known: Vec::new(),
            harnesses_confirmed: Vec::new(),
            features: vec!["ping".to_string()],
            sessions: Vec::new(),
        }),
    }
}

fn ping_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Ping,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Ping(PingBody {
            hostname: Some("test-server".to_string()),
        }),
    }
}

fn unauthenticated_error_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Error,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Error(ErrorBody {
            code: proto::CODE_UNAUTHENTICATED.to_string(),
            cmd: None,
            message: Some("credential revoked".to_string()),
        }),
    }
}

const STATUS_BUDGET: Duration = Duration::from_secs(5);

#[tokio::test]
async fn auth_then_hello_round_trip_and_status_reports_connected() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "cli_test1", "test-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    let auth = expect_auth(&mut ws).await;
    assert_eq!(auth.credential, "hlr_live_good");
    send_envelope(&mut ws, &server_hello_envelope()).await;

    // The client's own `hello`, per spec §4 ("each side sends hello").
    let client_hello = next_envelope(&mut ws).await.expect("expected client `hello`");
    match client_hello.body {
        Body::Hello(hello) => {
            assert_eq!(hello.role, Role::Client);
            assert_eq!(hello.hostname, "test-host");
            assert_eq!(hello.client_id.as_deref(), Some("cli_test1"));
        }
        other => panic!("expected `hello`, got {other:?}"),
    }

    let status = env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    assert_eq!(status["reconnecting"], false);
    assert_eq!(status["client_id"], "cli_test1");

    kill(child);
}

#[tokio::test]
async fn ping_from_server_is_answered_with_pong_including_hostname() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "cli_test2", "pong-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws).await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws).await.expect("expected client `hello`"); // drain it

    send_envelope(&mut ws, &ping_envelope()).await;
    let reply = next_envelope(&mut ws).await.expect("expected a `pong` reply");
    match reply.body {
        Body::Pong(pong) => assert_eq!(pong.hostname.as_deref(), Some("pong-host")),
        other => panic!("expected `pong`, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn reconnect_with_backoff_triggers_and_eventually_succeeds() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "cli_test3", "reconnect-host");

    let child = spawn_run(&env);

    // First connection: complete the handshake, then drop it without
    // warning (a real dropped-connection simulation).
    {
        let mut ws = accept_ws(&listener).await;
        expect_auth(&mut ws).await;
        send_envelope(&mut ws, &server_hello_envelope()).await;
        let _ = ws.close(None).await;
    }

    env.wait_for_status(STATUS_BUDGET, |doc| doc["reconnecting"] == true);

    // Second connection: the client's backoff loop retries against the
    // same listener; complete the handshake again.
    let mut ws2 = accept_ws(&listener).await;
    expect_auth(&mut ws2).await;
    send_envelope(&mut ws2, &server_hello_envelope()).await;

    let status = env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    assert_eq!(status["reconnecting"], false);

    kill(child);
}

#[tokio::test]
async fn wrong_credential_surfaces_as_a_clear_failure_not_a_retry_loop() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_bad", "cli_test4", "bad-host");

    let mut child = spawn_run_capturing_stderr(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws).await;
    send_envelope(&mut ws, &unauthenticated_error_envelope()).await;

    let status = wait_for_exit(&mut child, Duration::from_secs(5))
        .expect("`holler run` should exit promptly on an unauthenticated error, not retry forever");
    assert!(!status.success(), "expected a non-zero exit for a rejected credential");

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr was piped")
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.to_lowercase().contains("authentication failed"),
        "stderr should clearly report the auth failure, got: {stderr:?}"
    );
    assert!(!stderr.contains("hlr_live_bad"), "must never log the credential");

    // The credential is left in place (an operator decision, not this
    // story's to make); the connection is not.
    let status_doc = env.status_json();
    assert_eq!(status_doc["connected"], false);
    assert_eq!(status_doc["reconnecting"], false);
    assert_eq!(status_doc["client_id"], "cli_test4");
}

#[tokio::test]
async fn detach_closes_a_live_connection_and_the_run_process_exits() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "cli_test5", "detach-host");

    let mut child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws).await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws).await.expect("expected client `hello`");

    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    let detach_out = env.cmd().arg("detach").output().unwrap();
    assert!(detach_out.status.success(), "{detach_out:?}");
    assert!(String::from_utf8(detach_out.stdout).unwrap().contains("detached"));

    let status = wait_for_exit(&mut child, Duration::from_secs(5))
        .expect("`holler run` should exit once detach is requested");
    assert!(status.success(), "expected a clean exit on detach");
    assert!(!env.dir.path().join("credential.json").exists());
}
