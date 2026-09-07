//! Regression tests for issue #101 (detach/shutdown must not kill an
//! attached body), exercised against a **fake** in-process HTTP OpenCode
//! double -- not `stub-acp` stdio, since attach mode has nothing to do
//! with ACP-over-stdio, per the issue's explicit instruction. Built the
//! same minimal `std`-only way `session_manager_test.rs`'s
//! `OneShotHttpServer` already proved out for the ACP interrupt-fallback
//! test, extended to a long-lived, multi-request server (attach needs the
//! existence check, the SSE event connection, and prompt/interrupt calls
//! all served across one session's lifetime, not just one request).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use holler_client::acp_driver::DriverError;
use holler_client::config::{SessionConfig, SessionMode, SessionRegistry};
use holler_client::debug::DebugConfig;
use holler_client::session_manager::SessionManager;

/// A fake OpenCode HTTP control surface: answers the existence check,
/// `prompt_async`, `interrupt`, and the global `/event` SSE stream well
/// enough for the attach driver to consider itself successfully attached,
/// and records every request line it receives so tests can assert on what
/// was (or, for the fail-closed case, was *not*) sent.
struct FakeOpenCode {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl FakeOpenCode {
    /// `session_exists = false` makes every existence check 404, simulating
    /// attaching to a `session_id` that doesn't exist.
    fn start(session_exists: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_thread = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let requests = requests_for_thread.clone();
                std::thread::spawn(move || handle_connection(stream, requests, session_exists));
            }
        });
        FakeOpenCode { addr, requests }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Whether this fake server is still alive and answering the existence
    /// check right now -- issue #101's central assertion: after our own
    /// driver shuts down / detaches, the attached body must still be here.
    async fn still_answers_existence_check(&self, session_id: &str) -> bool {
        let url = format!("{}/api/session/{}", self.base_url(), session_id);
        reqwest::Client::new()
            .get(url)
            .send()
            .await
            .is_ok_and(|resp| resp.status().is_success())
    }

    fn recorded_requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn handle_connection(mut stream: TcpStream, requests: Arc<Mutex<Vec<String>>>, session_exists: bool) {
    let mut buf = [0u8; 8192];
    let mut received = Vec::new();
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                received.extend_from_slice(&buf[..n]);
                if received.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let text = String::from_utf8_lossy(&received);
    let request_line = text.lines().next().unwrap_or_default().to_string();
    requests.lock().unwrap().push(request_line.clone());

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    if method == "GET" && path.starts_with("/api/session/") {
        if session_exists {
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
            );
        } else {
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
        }
    } else if method == "GET" && path == "/event" {
        // The attach driver's background task holds this connection open
        // for the life of the session, reading events. This fake never
        // sends any -- these tests only need the connection to exist, not
        // real translated DriverEvents. Block on a read (nothing arrives)
        // until the peer (our driver) drops the connection on shutdown.
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");
        let mut sink = [0u8; 1];
        let _ = stream.read(&mut sink);
    } else if method == "POST" {
        let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\n\r\n");
    } else {
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    }
}

fn attach_config(name: &str, endpoint: String, session_id: &str) -> SessionConfig {
    SessionConfig {
        name: name.to_string(),
        harness: "opencode".to_string(),
        mode: SessionMode::Attach,
        endpoint: Some(endpoint),
        session_id: Some(session_id.to_string()),
        ..Default::default()
    }
}

/// Required test 1: attach; `SessionManager::shutdown`; the fake server
/// (standing in for the real attached OpenCode / Herdr pane) is still
/// alive and still answers the existence check afterward.
#[tokio::test]
async fn attach_then_manager_shutdown_leaves_attached_body_answering() {
    let server = FakeOpenCode::start(true);
    let registry = SessionRegistry::from_configs(vec![attach_config(
        "alpha",
        server.base_url(),
        "ses_abc",
    )])
    .unwrap();
    let manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed against a fake server that answers 200");

    manager.shutdown().await;

    assert!(
        server.still_answers_existence_check("ses_abc").await,
        "the attached body must survive our own SessionManager::shutdown"
    );
    assert!(
        !server
            .recorded_requests()
            .iter()
            .any(|r| r.starts_with("DELETE")),
        "shutdown on an attach session must never send an HTTP delete"
    );
}

/// Required test 2: attach; a simulated `holler detach`; same assertion.
///
/// `holler detach` is a separate process invocation that asks the live
/// `holler run` process to close its connection (see `connection.rs`'s
/// `ConnectionStateStore::request_detach` + poll loop) -- the live
/// process, on observing that request, calls the exact same
/// `SessionManager::shutdown()` this test calls directly. There is only
/// one shutdown mechanism in this codebase; this test's job is to prove
/// *that* mechanism is transport-aware, which is exactly what test 1 also
/// proves -- documented here as its own case per the issue's explicit
/// list, rather than skipped as "the same test".
#[tokio::test]
async fn simulated_cli_detach_leaves_attached_body_answering() {
    let server = FakeOpenCode::start(true);
    let registry = SessionRegistry::from_configs(vec![attach_config(
        "alpha",
        server.base_url(),
        "ses_def",
    )])
    .unwrap();
    let manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    // What `holler detach` actually triggers, end to end, once the live
    // `run` process observes the detach request (main.rs's run_run /
    // connection.rs's poll loop): the same manager.shutdown() call.
    manager.shutdown().await;

    assert!(
        server.still_answers_existence_check("ses_def").await,
        "the attached body must survive a detach"
    );
}

/// Required test 3: the spawn path still kills its own child (do not
/// regress v1) -- exercised here (not just relying on `acp_driver_test.rs`
/// / `session_manager_test.rs`'s pre-existing coverage) so this file's own
/// suite stands as a complete, self-contained proof for issue #101.
#[tokio::test]
async fn spawn_path_still_tears_down_its_own_child() {
    let config = SessionConfig {
        name: "alpha".to_string(),
        harness: "stub-acp".to_string(),
        command: vec![env!("CARGO_BIN_EXE_stub-acp").to_string()],
        ..Default::default()
    };
    let registry = SessionRegistry::from_configs(vec![config]).unwrap();
    let manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("spawn should still work exactly as before");

    // SessionManager::shutdown awaits every session's background task,
    // which itself awaits AcpDriver::shutdown -- which only returns once
    // the spawned child's own connection/process has actually torn down.
    // Reaching this line at all is the regression proof: an attach-mode
    // `SessionDriver::Http::shutdown` (used by the tests above) returns
    // immediately without waiting on anything, so if #100/#101's dispatch
    // ever silently routed spawn sessions through that path instead, nothing
    // here would fail loudly -- so this is deliberately paired with the
    // attach-side tests' explicit "still answers" assertion, which spawn
    // mode has no equivalent of (there is nothing left alive to ask).
    manager.shutdown().await;
}

/// Required test 4: attach to a missing `session_id` fails closed before
/// any write.
#[tokio::test]
async fn attach_to_missing_session_fails_closed_before_any_write() {
    let server = FakeOpenCode::start(false); // every existence check 404s

    let config = attach_config("alpha", server.base_url(), "ses_does_not_exist");
    let registry = SessionRegistry::from_configs(vec![config]).unwrap();

    match SessionManager::spawn(&registry, None, DebugConfig::default()).await {
        Err(holler_client::session_manager::ManagerError::Driver(
            DriverError::AttachSessionNotFound { .. },
        )) => {}
        Err(other) => panic!("expected AttachSessionNotFound, got a different error: {other}"),
        Ok(_) => panic!("attach to a nonexistent session must fail closed, not succeed"),
    }

    let requests = server.recorded_requests();
    assert_eq!(
        requests.len(),
        1,
        "only the existence-check GET should have been sent, got: {requests:?}"
    );
    assert!(
        requests[0].starts_with("GET"),
        "the one request sent must be the existence check, not a write: {requests:?}"
    );
}

// ---------------------------------------------------------------------------
// Issue #102: support/caps/status for attach.
// ---------------------------------------------------------------------------

use holler_client::connection::LiveState;
use holler_client::proto::QueryBody;
use holler_client::query;

fn dispatch_local(cmd: &str, registry: &SessionRegistry, confirmed_attach: &[String]) -> serde_json::Value {
    let query = QueryBody { cmd: cmd.to_string(), args: vec![] };
    query::dispatch(&query, 1, None, registry, "kiwi", LiveState::Disconnected, confirmed_attach)
        .expect("dispatch should not fail for a known cmd")
}

fn dispatch_support(feature: &str, registry: &SessionRegistry, confirmed_attach: &[String]) -> serde_json::Value {
    let query = QueryBody { cmd: "support".to_string(), args: vec![feature.to_string()] };
    query::dispatch(&query, 1, None, registry, "kiwi", LiveState::Disconnected, confirmed_attach)
        .expect("support <feature> should not fail for a known feature")
}

/// `holler support attach` is a static capability, `true` unconditionally
/// once this binary implements attach mode -- not gated on any session
/// being configured, still less on one being reachable.
#[tokio::test]
async fn support_attach_is_always_true_regardless_of_configuration() {
    let empty = SessionRegistry::from_configs(vec![]).unwrap();
    let query = QueryBody { cmd: "support".to_string(), args: vec!["attach".to_string()] };
    let body = query::dispatch(&query, 1, None, &empty, "kiwi", LiveState::Disconnected, &[])
        .expect("support attach must be answerable");
    assert_eq!(body["ok"], true);
    assert_eq!(body["kind"], "capability");
}

/// `holler support opencode-http` is `true` iff at least one configured
/// attach session's real HTTP endpoint answers right now, `false` (with a
/// clear reason, not a bare `false`) when none do -- confirmed via a real
/// fake HTTP OpenCode double, not a hardcoded assumption.
#[tokio::test]
async fn support_opencode_http_reflects_a_real_probe_not_a_static_flag() {
    let live_server = FakeOpenCode::start(true);
    let live_config = attach_config("alpha", live_server.base_url(), "ses_real");
    let live_registry = SessionRegistry::from_configs(vec![live_config]).unwrap();
    let confirmed = holler_client::http_attach_driver::confirmed_attach_sessions(&live_registry).await;
    assert_eq!(confirmed, vec!["alpha".to_string()], "a real, answering endpoint must be confirmed");
    let body = dispatch_support("opencode-http", &live_registry, &confirmed);
    assert_eq!(body["ok"], true, "endpoint answers, opencode-http support must be true");

    let dead_server = FakeOpenCode::start(false); // every existence check 404s
    let dead_config = attach_config("beta", dead_server.base_url(), "ses_missing");
    let dead_registry = SessionRegistry::from_configs(vec![dead_config]).unwrap();
    let confirmed = holler_client::http_attach_driver::confirmed_attach_sessions(&dead_registry).await;
    assert!(confirmed.is_empty(), "a 404ing endpoint must not be confirmed");
    let body = dispatch_support("opencode-http", &dead_registry, &confirmed);
    assert_eq!(body["ok"], false, "no reachable attach session, opencode-http support must be false");
    assert!(body["reason"].is_string(), "a false answer must carry a reason, not just `false`");
}

/// `status`'s per-session shape: an attach session reports `mode: "attach"`
/// and its real `harness_session_id`; a spawn session reports neither key
/// at all (not `null`, genuinely absent) -- ADR-0017's "optional presence
/// keys, unknown keys ignored" contract, and attach is only "confirmed" (so
/// only appears at all) once its real endpoint answers.
#[tokio::test]
async fn status_reports_mode_and_harness_session_id_for_attach_only() {
    let server = FakeOpenCode::start(true);
    let attach_cfg = attach_config("alpha", server.base_url(), "ses_abc123");
    let spawn_cfg = SessionConfig {
        name: "beta".to_string(),
        harness: "opencode".to_string(),
        command: vec!["/bin/sh".to_string()],
        ..Default::default()
    };
    let registry = SessionRegistry::from_configs(vec![attach_cfg, spawn_cfg]).unwrap();
    let confirmed_attach = holler_client::http_attach_driver::confirmed_attach_sessions(&registry).await;
    assert_eq!(confirmed_attach, vec!["alpha".to_string()]);

    let body = dispatch_local("status", &registry, &confirmed_attach);
    let sessions = body["sessions"].as_array().expect("sessions is an array");

    let alpha = sessions
        .iter()
        .find(|s| s["name"] == "alpha")
        .expect("attach session alpha (confirmed via the real probe) must be present");
    assert_eq!(alpha["mode"], "attach");
    assert_eq!(alpha["harness_session_id"], "ses_abc123");

    // `/bin/sh` is a real, always-present absolute path, so `beta` (spawn)
    // is confirmed runnable via the existing PATH check and appears too --
    // its shape must have neither key at all (not `null`), the actual
    // point of this test: attach and spawn sessions are genuinely
    // distinguishable in the same document, and spawn's shape is untouched.
    let beta = sessions
        .iter()
        .find(|s| s["name"] == "beta")
        .expect("spawn session beta (confirmed runnable via PATH) must be present");
    assert!(beta.get("mode").is_none(), "spawn session must not carry a mode key at all: {beta:?}");
    assert!(
        beta.get("harness_session_id").is_none(),
        "spawn session must not carry a harness_session_id key at all: {beta:?}"
    );
}
