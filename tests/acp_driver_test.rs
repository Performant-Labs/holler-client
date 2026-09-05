//! Integration test for `acp_driver` (issue #26), exercised against the
//! real `stub-acp` binary built by this crate (issue #32) rather than a
//! mock — the driver's whole job is spawning a real child process and
//! speaking real ACP v1 JSON-RPC over its stdio.

use holler_client::acp_driver::{AcpDriver, DriverEvent, DriverStatus, DriverStopReason};
use holler_client::config::SessionConfig;

fn stub_acp_session(name: &str) -> SessionConfig {
    SessionConfig {
        name: name.to_string(),
        harness: "stub-acp".to_string(),
        command: vec![env!("CARGO_BIN_EXE_stub-acp").to_string()],
        interrupt: None,
    }
}

/// Drains events up to and including the next `StopReason`, asserting the
/// exact sequence stub-acp's own contract promises: a `Working` status, the
/// canned `"PONG"` update, an `Idle` status, then the stop reason.
async fn expect_turn(driver: &mut AcpDriver, expected_stop_reason: DriverStopReason) {
    assert_eq!(
        driver.next_event().await,
        Some(DriverEvent::Status(DriverStatus::Working))
    );
    assert_eq!(
        driver.next_event().await,
        Some(DriverEvent::Update("PONG".to_string()))
    );
    assert_eq!(
        driver.next_event().await,
        Some(DriverEvent::Status(DriverStatus::Idle))
    );
    assert_eq!(
        driver.next_event().await,
        Some(DriverEvent::StopReason(expected_stop_reason))
    );
}

#[tokio::test]
async fn drives_session_new_prompt_cancel_prompt_against_stub_acp() {
    let mut driver = AcpDriver::spawn(&stub_acp_session("alpha"))
        .await
        .expect("driver should spawn stub-acp and establish a session");

    driver.prompt("hello").expect("prompt should send");
    expect_turn(&mut driver, DriverStopReason::EndTurn).await;

    driver.cancel().expect("cancel should send");

    // stub-acp arms a one-shot "cancelled" flag consumed by the *next*
    // session/prompt, then reverts to normal — modelling "turn cancelled,
    // session still promptable" without real concurrency. Assert both
    // halves of that contract.
    driver.prompt("hello again").expect("prompt should send");
    expect_turn(&mut driver, DriverStopReason::Cancelled).await;

    driver
        .prompt("hello once more")
        .expect("prompt should send");
    expect_turn(&mut driver, DriverStopReason::EndTurn).await;

    driver.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn rejects_empty_command() {
    let config = SessionConfig {
        name: "empty".to_string(),
        harness: "stub-acp".to_string(),
        command: vec![],
        interrupt: None,
    };

    let err = AcpDriver::spawn(&config)
        .await
        .expect_err("an empty command has no program to spawn");
    assert!(matches!(
        err,
        holler_client::acp_driver::DriverError::EmptyCommand
    ));
}
