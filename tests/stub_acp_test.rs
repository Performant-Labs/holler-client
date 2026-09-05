//! Integration test for `stub-acp` (issue #32): spawns the built
//! binary as a real child process and drives it over stdio exactly
//! as the wire harness will, using plain `std::process` (no tokio,
//! no bash, no Playwright — matching holler-server's own canary
//! style in `tests/wire/selftest.rs`).

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

struct StubAcp {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl StubAcp {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_stub-acp"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawning stub-acp must not fail");
        let stdin = child.stdin.take().expect("child stdin was piped");
        let stdout = BufReader::new(child.stdout.take().expect("child stdout was piped"));
        StubAcp {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, msg: &Value) {
        let mut line = serde_json::to_vec(msg).expect("request serializes");
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .expect("write to stub-acp stdin");
        self.stdin.flush().expect("flush stub-acp stdin");
    }

    fn recv(&mut self) -> Value {
        let mut line = String::new();
        let n = self
            .stdout
            .read_line(&mut line)
            .expect("read from stub-acp stdout");
        assert!(n > 0, "stub-acp closed stdout before sending a reply");
        serde_json::from_str(line.trim_end()).expect("stub-acp line is valid JSON")
    }
}

impl Drop for StubAcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn session_new_prompt_cancel_prompt_again() {
    let mut stub = StubAcp::spawn();

    stub.send(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "session/new",
        "params": { "sessionId": "alpha" }
    }));
    let new_reply = stub.recv();
    assert_eq!(new_reply["id"], 1);
    assert_eq!(new_reply["result"]["sessionId"], "alpha");

    stub.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/prompt",
        "params": { "sessionId": "alpha", "prompt": "hello" }
    }));
    let update = stub.recv();
    assert_eq!(update["method"], "session/update");
    assert_eq!(update["params"]["sessionId"], "alpha");
    assert_eq!(update["params"]["update"]["content"]["text"], "PONG");
    let prompt_reply = stub.recv();
    assert_eq!(prompt_reply["id"], 2);
    assert_eq!(prompt_reply["result"]["stopReason"], "end_turn");

    stub.send(&json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "session/cancel",
        "params": { "sessionId": "alpha" }
    }));
    let cancel_reply = stub.recv();
    assert_eq!(cancel_reply["id"], 3);
    assert_eq!(cancel_reply["result"]["sessionId"], "alpha");
    assert_eq!(cancel_reply["result"]["cancelled"], true);

    // Session must still be promptable after cancel, and the cancel
    // is reflected as this turn's stopReason.
    stub.send(&json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "session/prompt",
        "params": { "sessionId": "alpha", "prompt": "still there?" }
    }));
    let update_after_cancel = stub.recv();
    assert_eq!(update_after_cancel["method"], "session/update");
    assert_eq!(
        update_after_cancel["params"]["update"]["content"]["text"],
        "PONG"
    );
    let prompt_after_cancel = stub.recv();
    assert_eq!(prompt_after_cancel["id"], 4);
    assert_eq!(prompt_after_cancel["result"]["stopReason"], "cancelled");

    // And the session keeps working normally on a subsequent turn.
    stub.send(&json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "session/prompt",
        "params": { "sessionId": "alpha", "prompt": "one more" }
    }));
    let _ = stub.recv(); // session/update
    let final_reply = stub.recv();
    assert_eq!(final_reply["id"], 5);
    assert_eq!(final_reply["result"]["stopReason"], "end_turn");
}

#[test]
fn unnamed_sessions_are_named_alpha_then_beta() {
    let mut stub = StubAcp::spawn();

    stub.send(&json!({ "jsonrpc": "2.0", "id": 1, "method": "session/new" }));
    let first = stub.recv();
    assert_eq!(first["result"]["sessionId"], "alpha");

    stub.send(&json!({ "jsonrpc": "2.0", "id": 2, "method": "session/new" }));
    let second = stub.recv();
    assert_eq!(second["result"]["sessionId"], "beta");
}
