//! stub-acp: a fake ACP v1 agent for the same-machine wire harness
//! (issue #32). It is a test fixture, not a protocol implementation —
//! it speaks just enough JSON-RPC 2.0 to stand in for a real ACP
//! agent (e.g. OpenCode) while the client's stdio/child-process and
//! Holler-wire-protocol code is exercised end to end.
//!
//! Transport: newline-delimited JSON-RPC 2.0 on stdin/stdout, one
//! object per line. No `Content-Length` framing (the issue is
//! explicit about this), no Unix sockets. Output is written with
//! `io::stdout().lock()` + `write_all` and an explicit `\n`, not
//! `println!`, so the line terminator is exact and binary-safe on
//! every platform, including Windows (`stub-acp.exe`).
//!
//! ACP v1 was not pinned in this repo when this stub was first written
//! (issue #32), so its wire shapes were originally documented,
//! made-up-but-reasonable guesses. Issue #26 pinned the real protocol
//! (the `agent-client-protocol`/`agent-client-protocol-schema` crates)
//! and found two of those guesses were not actually schema-conformant:
//! the client-side handshake (`initialize`) was missing, and
//! `session/update`'s payload didn't match the real, internally-tagged
//! `SessionNotification`/`SessionUpdate` shape. The real crate's
//! dispatcher matches `session/update` to `SessionNotification`
//! unconditionally and silently drops it on a parse failure (there's no
//! request `id` to error back on), so a non-conformant shape here is
//! invisible to any client built on that crate, not just wrong. Both
//! are now fixed to match the real schema, verified against the actual
//! crate (see `src/acp_driver.rs`) rather than guessed:
//!
//! - `initialize` params: `{"protocolVersion": 1, ...}` (other fields
//!   optional/defaulted per schema). Responds with
//!   `{"protocolVersion": 1}` (a minimal `InitializeResponse`; the
//!   other fields all default in the real schema).
//! - `session/new` params: optional `{"sessionId": "alpha" | "beta" | ...}`.
//!   If a name is given, it becomes the session id verbatim. If
//!   omitted, sessions are auto-named `"alpha"`, then `"beta"`, then
//!   `"session-2"`, `"session-3"`, ... The chosen id is returned as
//!   `{"sessionId": ...}`.
//! - `session/prompt` params: `{"sessionId": ..., "prompt": [...]}`.
//!   Before replying to the request, the stub emits one
//!   `session/update` notification with a fixed canned reply — a
//!   `SessionNotification` whose `update` is an
//!   `AgentMessageChunk`/`ContentBlock::Text` carrying the constant
//!   text `"PONG"` (`{"sessionId": ..., "update": {"sessionUpdate":
//!   "agent_message_chunk", "content": {"type": "text", "text":
//!   "PONG"}}}`) — rather than echoing the prompt text, so fixture
//!   assertions can compare against an exact constant. It then
//!   responds to the request with `{"stopReason": "end_turn"}`, or
//!   `{"stopReason": "cancelled"}` if a `session/cancel` is still
//!   pending for this session (see below).
//! - `session/cancel` params: `{"sessionId": ...}`. May arrive as a
//!   request (has `id`, gets a `{"sessionId": ..., "cancelled": true}`
//!   response) or as a notification (no `id`, no response). Since
//!   this stub answers each request synchronously, there is never a
//!   turn actually "in flight" for cancel to interrupt; instead,
//!   cancel arms a one-shot flag that is consumed by that session's
//!   *next* `session/prompt`, marking that turn's `stopReason` as
//!   `"cancelled"`. The flag is then cleared, so the session goes on
//!   answering normally — modelling "turn cancelled, session still
//!   promptable" without needing real concurrency.
//! - Unknown methods that arrive as requests get a JSON-RPC
//!   `-32601 Method not found` error response; as notifications they
//!   are silently ignored, per JSON-RPC 2.0 semantics.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

/// Per-session state the stub tracks across messages.
#[derive(Default)]
struct Session {
    /// Set by `session/cancel`; consumed by the next `session/prompt`.
    cancel_pending: bool,
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut sessions: HashMap<String, Session> = HashMap::new();

    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("stub-acp: ignoring unparseable line: {e}");
                continue;
            }
        };
        handle_message(&msg, &mut sessions, &mut stdout);
    }
}

fn handle_message(msg: &Value, sessions: &mut HashMap<String, Session>, out: &mut impl Write) {
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let request_id = msg.get("id").filter(|v| !v.is_null()).cloned();
    let params = msg.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "initialize" => {
            if let Some(id) = request_id {
                respond(out, id, json!({ "protocolVersion": 1 }));
            }
        }
        "session/new" => {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| next_auto_name(sessions));
            sessions.entry(session_id.clone()).or_default();
            if let Some(id) = request_id {
                respond(out, id, json!({ "sessionId": session_id }));
            }
        }
        "session/prompt" => {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let cancelled = sessions
                .get_mut(&session_id)
                .map(|s| std::mem::take(&mut s.cancel_pending))
                .unwrap_or(false);

            notify(
                out,
                "session/update",
                json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": { "type": "text", "text": "PONG" },
                    },
                }),
            );

            let stop_reason = if cancelled { "cancelled" } else { "end_turn" };
            if let Some(id) = request_id {
                respond(out, id, json!({ "stopReason": stop_reason }));
            }
        }
        "session/cancel" => {
            let session_id = params
                .get("sessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if let Some(s) = sessions.get_mut(&session_id) {
                s.cancel_pending = true;
            }
            if let Some(id) = request_id {
                respond(
                    out,
                    id,
                    json!({ "sessionId": session_id, "cancelled": true }),
                );
            }
        }
        other => {
            if let Some(id) = request_id {
                respond_error(out, id, -32601, format!("method not found: {other}"));
            }
        }
    }
}

/// `"alpha"` for the first unnamed session, `"beta"` for the second,
/// `"session-N"` (N = current session count) after that.
fn next_auto_name(sessions: &HashMap<String, Session>) -> String {
    if !sessions.contains_key("alpha") {
        "alpha".to_string()
    } else if !sessions.contains_key("beta") {
        "beta".to_string()
    } else {
        format!("session-{}", sessions.len())
    }
}

fn respond(out: &mut impl Write, id: Value, result: Value) {
    write_line(
        out,
        &json!({ "jsonrpc": "2.0", "id": id, "result": result }),
    );
}

fn respond_error(out: &mut impl Write, id: Value, code: i32, message: String) {
    write_line(
        out,
        &json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
    );
}

fn notify(out: &mut impl Write, method: &str, params: Value) {
    write_line(
        out,
        &json!({ "jsonrpc": "2.0", "method": method, "params": params }),
    );
}

fn write_line(out: &mut impl Write, value: &Value) {
    let mut bytes = serde_json::to_vec(value).expect("Value serialization cannot fail");
    bytes.push(b'\n');
    out.write_all(&bytes).expect("stdout write_all");
    out.flush().expect("stdout flush");
}
