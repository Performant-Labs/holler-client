//! Regression tests for holler-client issue #133 / holler-server issue
//! #382: the HTTP attach driver detecting a pending OpenCode
//! question/permission and answering it. Built the same minimal
//! `std`-only fake-server pattern `tests/http_attach_driver_test.rs`
//! already uses, extended to serve `GET /question` / `GET /permission`
//! (mutable, so a test can inject a pending item) and record the reply
//! bodies POSTed to `/question/{id}/reply` / `/permission/{id}/reply`.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use holler_client::acp_driver::{DriverError, DriverEvent, DriverStatus};
use holler_client::config::{SessionConfig, SessionMode, SessionRegistry};
use holler_client::debug::DebugConfig;
use holler_client::http_attach_driver::BLOCK_POLL_INTERVAL;
use holler_client::session_manager::{ManagerError, SessionManager};

/// A fake OpenCode HTTP control surface, extended beyond
/// `tests/http_attach_driver_test.rs`'s `FakeOpenCode` with mutable
/// `/question`/`/permission` lists and body-recording POST handling —
/// needed here (and not there) because this file actually inspects what
/// gets POSTed as a reply, not just which path was hit.
struct FakeOpenCode {
    addr: SocketAddr,
    pending_question: Arc<Mutex<Option<Value>>>,
    pending_permission: Arc<Mutex<Option<Value>>>,
    replies: Arc<Mutex<Vec<(String, Value)>>>,
}

impl FakeOpenCode {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let pending_question = Arc::new(Mutex::new(None));
        let pending_permission = Arc::new(Mutex::new(None));
        let replies = Arc::new(Mutex::new(Vec::new()));
        let (q, p, r) = (pending_question.clone(), pending_permission.clone(), replies.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (q, p, r) = (q.clone(), p.clone(), r.clone());
                std::thread::spawn(move || handle_connection(stream, q, p, r));
            }
        });
        FakeOpenCode { addr, pending_question, pending_permission, replies }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn set_pending_question(&self, id: &str, options: &[&str]) {
        *self.pending_question.lock().unwrap() = Some(json!({
            "id": id,
            "sessionID": "ses_test",
            "questions": [{
                "question": "Which way?",
                "header": "Which way?",
                "options": options.iter().map(|o| json!({"label": o, "description": ""})).collect::<Vec<_>>(),
            }],
        }));
    }

    fn set_pending_permission(&self, id: &str) {
        *self.pending_permission.lock().unwrap() = Some(json!({
            "id": id,
            "sessionID": "ses_test",
            "permission": "bash",
            "patterns": [],
            "metadata": {},
            "always": [],
        }));
    }

    fn recorded_replies(&self) -> Vec<(String, Value)> {
        self.replies.lock().unwrap().clone()
    }
}

fn handle_connection(
    mut stream: TcpStream,
    pending_question: Arc<Mutex<Option<Value>>>,
    pending_permission: Arc<Mutex<Option<Value>>>,
    replies: Arc<Mutex<Vec<(String, Value)>>>,
) {
    let mut buf = [0u8; 8192];
    let mut received = Vec::new();
    let headers_end = loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                received.extend_from_slice(&buf[..n]);
                if let Some(pos) = find_subslice(&received, b"\r\n\r\n") {
                    break pos;
                }
            }
        }
    };
    let head = String::from_utf8_lossy(&received[..headers_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default().to_string();
    let content_length: usize = lines
        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().to_string()))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut body = received[headers_end + 4..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    let body_json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    let write_json = |stream: &mut TcpStream, status: &str, body: &Value| {
        let payload = body.to_string();
        let _ = stream.write_all(
            format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                payload.len(),
                payload
            )
            .as_bytes(),
        );
    };

    if method == "GET" && path.starts_with("/api/session/") {
        write_json(&mut stream, "200 OK", &json!({}));
    } else if method == "GET" && path == "/event" {
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n");
        let mut sink = [0u8; 1];
        let _ = stream.read(&mut sink);
    } else if method == "GET" && path == "/question" {
        let list = match pending_question.lock().unwrap().clone() {
            Some(q) => vec![q],
            None => vec![],
        };
        write_json(&mut stream, "200 OK", &Value::Array(list));
    } else if method == "GET" && path == "/permission" {
        let list = match pending_permission.lock().unwrap().clone() {
            Some(p) => vec![p],
            None => vec![],
        };
        write_json(&mut stream, "200 OK", &Value::Array(list));
    } else if method == "POST" && path.starts_with("/question/") && path.ends_with("/reply") {
        let id = path.trim_start_matches("/question/").trim_end_matches("/reply").trim_end_matches('/').to_string();
        replies.lock().unwrap().push((path.to_string(), body_json));
        *pending_question.lock().unwrap() = None;
        let _ = id;
        write_json(&mut stream, "200 OK", &Value::Bool(true));
    } else if method == "POST" && path.starts_with("/permission/") && path.ends_with("/reply") {
        replies.lock().unwrap().push((path.to_string(), body_json));
        *pending_permission.lock().unwrap() = None;
        write_json(&mut stream, "200 OK", &Value::Bool(true));
    } else if method == "POST" {
        let _ = stream.write_all(b"HTTP/1.1 204 No Content\r\n\r\n");
    } else {
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn attach_config(name: &str, endpoint: String) -> SessionConfig {
    SessionConfig {
        name: name.to_string(),
        harness: "opencode".to_string(),
        mode: SessionMode::Attach,
        endpoint: Some(endpoint),
        session_id: Some("ses_test".to_string()),
        ..Default::default()
    }
}

/// Detection: a pending question surfaces as `DriverEvent::Status(Blocked)`
/// within one poll interval, and `answer` by numeric index POSTs the
/// exact real option label back, not the caller's raw input.
#[tokio::test]
async fn answer_by_index_replies_with_the_real_option_label_and_unblocks() {
    let server = FakeOpenCode::start();
    server.set_pending_question("que_1", &["Yes", "No"]);
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let mut manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Blocked)),
        "a pending question must surface as Blocked within one poll tick"
    );

    manager
        .answer("alpha", "1".to_string())
        .await
        .expect("answering the pending question by index should succeed");

    let replies = server.recorded_replies();
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert_eq!(replies[0].0, "/question/que_1/reply");
    assert_eq!(replies[0].1, json!({"answers": [["No"]]}));

    manager.shutdown().await;
}

/// `answer` also accepts the exact label (case-insensitively) instead of
/// an index.
#[tokio::test]
async fn answer_by_label_is_case_insensitive() {
    let server = FakeOpenCode::start();
    server.set_pending_question("que_2", &["Yes", "No"]);
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let mut manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    let _ = manager.next_event("alpha").await.unwrap(); // Blocked

    manager
        .answer("alpha", "yes".to_string())
        .await
        .expect("answering by exact (case-insensitive) label should succeed");

    let replies = server.recorded_replies();
    assert_eq!(replies[0].1, json!({"answers": [["Yes"]]}));

    manager.shutdown().await;
}

/// A pending permission is answered via the real, distinct
/// `{"reply": "once"|"always"|"reject"}` shape — not the question shape —
/// with common aliases (`allow`) normalized to OpenCode's real enum.
#[tokio::test]
async fn answer_replies_to_a_pending_permission_with_the_real_enum_value() {
    let server = FakeOpenCode::start();
    server.set_pending_permission("per_1");
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let mut manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Blocked))
    );

    manager
        .answer("alpha", "allow".to_string())
        .await
        .expect("answering the pending permission should succeed");

    let replies = server.recorded_replies();
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert_eq!(replies[0].0, "/permission/per_1/reply");
    assert_eq!(replies[0].1, json!({"reply": "once"}));

    manager.shutdown().await;
}

/// No pending question/permission at all: `answer` fails closed with a
/// precise, typed error rather than silently doing nothing or hanging.
#[tokio::test]
async fn answer_with_nothing_pending_fails_closed() {
    let server = FakeOpenCode::start();
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    match manager.answer("alpha", "once".to_string()).await {
        Err(ManagerError::Driver(DriverError::NoPendingAnswer(_))) => {}
        other => panic!("expected NoPendingAnswer, got {other:?}"),
    }

    assert!(server.recorded_replies().is_empty(), "no reply should ever be POSTed");

    manager.shutdown().await;
}

/// An out-of-range index / unmatched label is rejected the same way,
/// without ever POSTing a reply.
#[tokio::test]
async fn answer_with_an_unresolvable_choice_fails_closed_before_any_post() {
    let server = FakeOpenCode::start();
    server.set_pending_question("que_3", &["Yes", "No"]);
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let mut manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    let _ = manager.next_event("alpha").await.unwrap(); // Blocked

    match manager.answer("alpha", "maybe".to_string()).await {
        Err(ManagerError::Driver(DriverError::NoPendingAnswer(_))) => {}
        other => panic!("expected NoPendingAnswer, got {other:?}"),
    }
    assert!(server.recorded_replies().is_empty());

    manager.shutdown().await;
}

/// Once answered, the next poll tick confirms the block is gone and
/// reports the session unblocked again.
#[tokio::test]
async fn answering_clears_the_blocked_status_on_the_next_poll() {
    let server = FakeOpenCode::start();
    server.set_pending_question("que_4", &["Yes", "No"]);
    let registry = SessionRegistry::from_configs(vec![attach_config("alpha", server.base_url())]).unwrap();
    let mut manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("attach should succeed");

    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Blocked))
    );

    manager.answer("alpha", "0".to_string()).await.expect("answer should succeed");

    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Working)),
        "the next poll tick must observe the question is gone and report unblocked"
    );

    manager.shutdown().await;
}

/// A spawn-mode (ACP) session cannot answer today (out of scope, see
/// `SessionDriver::answer`'s doc comment) — reported as a clear, typed
/// error rather than silently doing nothing.
#[tokio::test]
async fn answer_on_a_spawn_session_is_unsupported() {
    let config = SessionConfig {
        name: "alpha".to_string(),
        harness: "stub-acp".to_string(),
        command: vec![env!("CARGO_BIN_EXE_stub-acp").to_string()],
        ..Default::default()
    };
    let registry = SessionRegistry::from_configs(vec![config]).unwrap();
    let manager = SessionManager::spawn(&registry, None, DebugConfig::default())
        .await
        .expect("spawn should still work");

    match manager.answer("alpha", "once".to_string()).await {
        Err(ManagerError::Driver(DriverError::AnswerUnsupported)) => {}
        other => panic!("expected AnswerUnsupported, got {other:?}"),
    }

    manager.shutdown().await;
}

/// Sanity check on the poll interval this file waits on, so a future
/// change to `BLOCK_POLL_INTERVAL` doesn't silently make these tests
/// flaky by racing a too-short implicit sleep.
#[test]
fn block_poll_interval_is_reasonably_fast_for_tests() {
    assert!(BLOCK_POLL_INTERVAL.as_millis() <= 2000);
}
