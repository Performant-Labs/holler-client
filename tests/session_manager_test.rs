//! Integration tests for `session_manager` (issues #27, #28), exercised
//! against the real `stub-acp` binary the same way `acp_driver_test.rs`
//! (issue #26) is — real child processes, real ACP v1 traffic, no mocking.
//!
//! # Simulating a "long turn" without wall-clock delays
//!
//! `stub-acp` answers every `session/prompt` synchronously and fast (issue
//! #26's own report notes it has no artificial delay). Rather than add one,
//! these tests win the race a different way: `SessionManager::prompt`'s
//! `busy` flag flips to `true` synchronously, inside the session's
//! background task, the moment it forwards a prompt to the driver — before
//! that prompt's turn has actually completed on the wire. So issuing a
//! second `prompt()` (or an `interrupt()`) immediately after the first,
//! without draining any events in between, reliably lands while the first
//! turn is still considered in flight from the manager's point of view,
//! regardless of how fast the stub actually replies. This is the
//! alternative the story brief allowed in place of an artificial stub
//! delay, and it is what "in flight" means throughout these tests: the
//! manager hasn't yet observed that turn's `StopReason`, not that the OS
//! process is provably still computing.

use holler_client::acp_driver::{DriverEvent, DriverStatus, DriverStopReason};
use holler_client::config::SessionConfig;
use holler_client::session_manager::{CancelChannel, InterruptOutcome, ManagerError, SessionManager};
use std::io::{Read, Write};
use std::net::TcpListener;

fn stub_acp_session(name: &str) -> SessionConfig {
    SessionConfig {
        name: name.to_string(),
        harness: "stub-acp".to_string(),
        command: vec![env!("CARGO_BIN_EXE_stub-acp").to_string()],
        interrupt: None,
    }
}

async fn spawn_manager(names: &[&str]) -> SessionManager {
    let registry = holler_client::config::SessionRegistry::from_configs(
        names.iter().map(|n| stub_acp_session(n)).collect(),
    )
    .expect("distinct names never collide");
    SessionManager::spawn(&registry, None)
        .await
        .expect("manager should spawn stub-acp for every session")
}

/// Drains one full turn's events for `name`, asserting the exact sequence
/// stub-acp's own contract promises (mirrors `acp_driver_test.rs`'s
/// `expect_turn`).
async fn expect_turn(manager: &mut SessionManager, name: &str, expected: DriverStopReason) {
    assert_eq!(
        manager.next_event(name).await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Working))
    );
    assert_eq!(
        manager.next_event(name).await.unwrap(),
        Some(DriverEvent::Update("PONG".to_string()))
    );
    assert_eq!(
        manager.next_event(name).await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Idle))
    );
    assert_eq!(
        manager.next_event(name).await.unwrap(),
        Some(DriverEvent::StopReason(expected))
    );
}

#[tokio::test]
async fn interrupt_with_no_turn_in_flight_is_a_clean_no_op() {
    let manager = spawn_manager(&["alpha"]).await;

    let outcome = manager
        .interrupt("alpha")
        .await
        .expect("interrupting an idle session is not an error");
    assert_eq!(outcome, InterruptOutcome::NoTurnInFlight);

    manager.shutdown().await;
}

#[tokio::test]
async fn interrupt_on_in_flight_turn_cancels_and_session_stays_promptable() {
    let mut manager = spawn_manager(&["alpha"]).await;

    manager.prompt("alpha", "hello").expect("prompt should send");
    // Interrupt immediately, before draining any events: `busy` is already
    // true (see module doc comment on "long turn" simulation above).
    let outcome = manager.interrupt("alpha").await.expect("interrupt should succeed");
    assert_eq!(outcome, InterruptOutcome::Cancelled(CancelChannel::Acp));

    // The turn stub-acp was already computing still finishes and reports
    // some stop reason (stub-acp has no real concurrency: whether it lands
    // as `end_turn` or `cancelled` depends on exactly when the cancel
    // notification is read relative to the in-progress prompt, which is
    // stub-acp's own well-documented one-shot-flag quirk, not something
    // this manager controls). What matters here is what comes next.
    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Working))
    );
    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Update("PONG".to_string()))
    );
    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Idle))
    );
    let first_stop = manager.next_event("alpha").await.unwrap();
    assert!(matches!(first_stop, Some(DriverEvent::StopReason(_))));

    // Session remains promptable after the interrupted turn completes. Its
    // own stop reason again depends on stub-acp's one-shot cancel-flag
    // quirk (whether the flag was consumed by the first turn already, or
    // is still pending and lands on this one instead).
    let second_stop_reason = if first_stop == Some(DriverEvent::StopReason(DriverStopReason::Cancelled)) {
        DriverStopReason::EndTurn
    } else {
        DriverStopReason::Cancelled
    };
    manager
        .prompt("alpha", "still there?")
        .expect("prompt should send");
    expect_turn(&mut manager, "alpha", second_stop_reason).await;

    manager.shutdown().await;
}

#[tokio::test]
async fn interrupting_one_session_does_not_affect_its_sibling() {
    let mut manager = spawn_manager(&["alpha", "beta"]).await;

    manager.prompt("alpha", "hello").expect("prompt should send");
    manager.prompt("beta", "hello").expect("prompt should send");

    let outcome = manager
        .interrupt("alpha")
        .await
        .expect("interrupt should succeed");
    assert_eq!(outcome, InterruptOutcome::Cancelled(CancelChannel::Acp));

    // beta's turn was never touched: it must complete as a normal,
    // uncancelled end_turn, isolated from alpha's interrupt.
    expect_turn(&mut manager, "beta", DriverStopReason::EndTurn).await;

    // Drain alpha's (possibly cancelled) turn so shutdown doesn't race it.
    let _ = manager.next_event("alpha").await.unwrap(); // Working
    let _ = manager.next_event("alpha").await.unwrap(); // Update
    let _ = manager.next_event("alpha").await.unwrap(); // Idle
    let stop_reason = manager.next_event("alpha").await.unwrap();
    assert!(matches!(stop_reason, Some(DriverEvent::StopReason(_))));

    manager.shutdown().await;
}

#[tokio::test]
async fn second_prompt_while_busy_is_queued_and_delivered_after_first_turn() {
    let mut manager = spawn_manager(&["alpha"]).await;

    manager.prompt("alpha", "first").expect("prompt should send");
    // Queued immediately: busy is already true from the send above.
    manager.prompt("alpha", "second").expect("prompt should send");

    // Only one turn's worth of events shows up before the queued prompt is
    // dispatched -- draining exactly one turn proves "second" wasn't
    // dropped or run concurrently, since a second `Working` status would
    // otherwise appear here instead of after this first turn's StopReason.
    expect_turn(&mut manager, "alpha", DriverStopReason::EndTurn).await;

    // The queued prompt is now delivered as its own, separate turn.
    expect_turn(&mut manager, "alpha", DriverStopReason::EndTurn).await;

    manager.shutdown().await;
}

#[tokio::test]
async fn interrupt_during_queued_prompt_only_cancels_current_turn_and_queue_still_drains() {
    let mut manager = spawn_manager(&["alpha"]).await;

    manager.prompt("alpha", "first").expect("prompt should send");
    manager.prompt("alpha", "second").expect("prompt should send"); // queued

    let outcome = manager
        .interrupt("alpha")
        .await
        .expect("interrupt should succeed");
    assert_eq!(outcome, InterruptOutcome::Cancelled(CancelChannel::Acp));

    // First turn completes (with whatever stop reason stub-acp's one-shot
    // cancel-flag quirk assigns it -- see the in-flight interrupt test).
    let _ = manager.next_event("alpha").await.unwrap(); // Working
    let _ = manager.next_event("alpha").await.unwrap(); // Update
    let _ = manager.next_event("alpha").await.unwrap(); // Idle
    let first_stop = manager.next_event("alpha").await.unwrap();
    assert!(matches!(first_stop, Some(DriverEvent::StopReason(_))));

    // The queue survived the interrupt and still drains as its own turn.
    let second_stop_reason = if first_stop == Some(DriverEvent::StopReason(DriverStopReason::Cancelled)) {
        // stub-acp's cancel flag was consumed by the first turn already.
        DriverStopReason::EndTurn
    } else {
        // stub-acp's cancel flag is still pending and will be consumed by
        // this next turn instead.
        DriverStopReason::Cancelled
    };
    expect_turn(&mut manager, "alpha", second_stop_reason).await;

    manager.shutdown().await;
}

#[tokio::test]
async fn unknown_session_is_a_well_defined_error_not_a_panic() {
    let manager = spawn_manager(&["alpha"]).await;

    let err = manager
        .interrupt("nonexistent")
        .await
        .expect_err("unknown session name must error, not panic");
    assert!(matches!(err, ManagerError::UnknownSession(name) if name == "nonexistent"));

    let err = manager
        .prompt("nonexistent", "hi")
        .expect_err("unknown session name must error, not panic");
    assert!(matches!(err, ManagerError::UnknownSession(name) if name == "nonexistent"));

    manager.shutdown().await;
}

/// A minimal single-request HTTP/1.1 server: accepts one connection, reads
/// until it has the full request line plus headers (a blank line), records
/// the request line, and replies `200 OK`. No dependency beyond `std`,
/// since this only needs to prove the fallback's URL and method are right.
struct OneShotHttpServer {
    request_line: std::sync::mpsc::Receiver<String>,
    addr: std::net::SocketAddr,
}

impl OneShotHttpServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = match listener.accept() {
                Ok(pair) => pair,
                Err(_) => return,
            };
            let mut buf = [0u8; 4096];
            let mut received = Vec::new();
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
                if received.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&received);
            let request_line = text.lines().next().unwrap_or_default().to_string();
            let _ = tx.send(request_line);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
        });
        OneShotHttpServer {
            request_line: rx,
            addr,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn recv_request_line(&self) -> String {
        self.request_line
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("HTTP fallback request should arrive within 5s")
    }
}

#[tokio::test]
async fn acp_cancel_unavailable_falls_back_to_http_interrupt() {
    let server = OneShotHttpServer::start();

    let registry = holler_client::config::SessionRegistry::from_configs(vec![stub_acp_session(
        "alpha",
    )])
    .unwrap();
    let mut manager = SessionManager::spawn(&registry, Some(server.base_url()))
        .await
        .expect("manager should spawn stub-acp");

    // stub-acp exits immediately on this sentinel prompt instead of
    // replying, simulating a crashed/dead agent mid-turn -- see
    // tests/stub-acp/src/main.rs's CRASH_SENTINEL doc comment. This
    // deterministically makes the ACP connection unusable, which is this
    // module's documented trigger for the HTTP fallback (see
    // session_manager.rs's module docs on why ACP v1 offers no capability
    // signal for "cancel unsupported").
    manager
        .prompt("alpha", "__stub_acp_simulate_crash__")
        .expect("prompt should send");

    // "Working" is emitted synchronously the moment the prompt is
    // dispatched, well before the stub has had a chance to exit.
    assert_eq!(
        manager.next_event("alpha").await.unwrap(),
        Some(DriverEvent::Status(DriverStatus::Working))
    );

    // Give the crashed child's stdout EOF time to propagate through the
    // driver's connection task before interrupting: `AcpDriver::cancel()`
    // only fails once that task has actually observed the closed pipe and
    // torn itself down. This is a generous, deterministic wall-clock wait,
    // not a race retry -- local process exit and pipe EOF detection is
    // normally on the order of low single-digit milliseconds.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let outcome = manager
        .interrupt("alpha")
        .await
        .expect("interrupt should fall back to HTTP once the ACP connection is gone");
    assert_eq!(outcome, InterruptOutcome::Cancelled(CancelChannel::Http));

    let request_line = server.recv_request_line();
    assert_eq!(request_line, "POST /api/session/alpha/interrupt HTTP/1.1");

    manager.shutdown().await;
}
