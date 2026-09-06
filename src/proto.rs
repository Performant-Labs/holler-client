//! Minimal Holler v1 wire codec for this client (issue #24).
//!
//! Mirrors holler-server's `src/proto/mod.rs` wire format — one JSON
//! object per WebSocket text frame, envelope `type` discriminates a
//! **bare-object** `body` (no serde enum tag) — but implements only the
//! message bodies this story actually sends or receives: `auth`, `hello`,
//! `ping`, `pong`, `error`. There is no shared crate between the two
//! repos (see `src/join.rs`'s doc comment), so this is modeled
//! independently, not imported.
//!
//! `query` / `query_ok` are added here too (issue #30: answer `status` /
//! `caps` / `support` / `protocol`). Every other v1 type (`prompt`,
//! `reply`, `interrupt`, `presence`, `ack`) still decodes to
//! [`Body::Unknown`] rather than failing — this story does not implement
//! prompt routing, so a server that sends one of those must not crash or
//! wedge this connection — it is simply a frame this binary does not act
//! on yet.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// This binary's protocol version (spec §2): `min = 1`, `max = 1`.
pub const PROTOCOL_VERSION: u32 = 1;

/// `auth` missing, malformed, or the credential does not verify (spec §11).
pub const CODE_UNAUTHENTICATED: &str = "unauthenticated";
/// `query.cmd` is not one of `status`/`caps`/`support`/`protocol` (spec §11).
pub const CODE_UNKNOWN_CMD: &str = "unknown_cmd";
/// `join`'s secret was not found, already bound, invalidated, revoked, or
/// expired (spec §4.1, §11; ADR 0015).
pub const CODE_JOIN_FAILED: &str = "join_failed";
/// `support` asked with no argument, or `protocol`'s argument isn't a
/// positive integer (spec §7, §11).
pub const CODE_UNKNOWN_FEATURE: &str = "unknown_feature";

/// Who is speaking (spec §6).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    #[serde(rename = "client")]
    Client,
    #[serde(rename = "server")]
    Server,
}

/// The v1 message types this client's codec can name. Anything not in
/// this list still decodes (as [`Body::Unknown`]) via [`MessageType::from_wire`]
/// returning `None` — see [`decode`].
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
pub enum MessageType {
    #[serde(rename = "join")]
    Join,
    #[serde(rename = "join_ok")]
    JoinOk,
    #[serde(rename = "auth")]
    Auth,
    #[serde(rename = "hello")]
    Hello,
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "query")]
    Query,
    #[serde(rename = "query_ok")]
    QueryOk,
    /// Any other v1 type (`prompt`, `reply`, `interrupt`, `presence`,
    /// `ack`). Carries the original wire string so a log line can still
    /// name it.
    #[serde(other)]
    Unknown,
}

impl MessageType {
    fn from_wire(s: &str) -> Self {
        match s {
            "join" => Self::Join,
            "join_ok" => Self::JoinOk,
            "auth" => Self::Auth,
            "hello" => Self::Hello,
            "ping" => Self::Ping,
            "pong" => Self::Pong,
            "error" => Self::Error,
            "query" => Self::Query,
            "query_ok" => Self::QueryOk,
            _ => Self::Unknown,
        }
    }

    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::JoinOk => "join_ok",
            Self::Auth => "auth",
            Self::Hello => "hello",
            Self::Ping => "ping",
            Self::Pong => "pong",
            Self::Error => "error",
            Self::Query => "query",
            Self::QueryOk => "query_ok",
            Self::Unknown => "unknown",
        }
    }
}

/// One WebSocket text frame: the JSON envelope (spec §3).
#[derive(Clone, PartialEq, Debug)]
pub struct Envelope {
    pub v: u32,
    pub msg_type: MessageType,
    pub id: String,
    pub ts: String,
    pub from: String,
    pub body: Body,
}

/// `join` body (spec §4.1): the one-time join secret plus this machine's
/// hostname. `secret` must never be logged or persisted beyond the single
/// redeem call that sends this — see [`crate::join`]'s module docs.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct JoinBody {
    pub secret: String,
    pub hostname: String,
}

/// `join_ok` body (spec §4.1): the durable identity a successful `join`
/// redeems. The server closes the connection right after sending this —
/// see [`crate::join::JoinTransport`].
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct JoinOkBody {
    pub client_id: String,
    pub credential: String,
}

/// `auth` body: the client credential (spec §4 — "not the join secret").
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct AuthBody {
    pub credential: String,
}

/// A named session in a client `hello` (spec §6). This client never
/// sends any (no live session wiring yet — issue #30+), but the field
/// still needs a type to deserialize the server's `hello` shape.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct HelloSession {
    pub name: String,
    pub harness: String,
}

/// `hello` body (spec §6).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct HelloBody {
    pub protocol: u32,
    pub protocol_min: u32,
    pub protocol_max: u32,
    pub role: Role,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub harnesses: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub harnesses_known: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub harnesses_confirmed: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sessions: Vec<HelloSession>,
}

/// `ping` body (spec §10): empty or `{hostname}`.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct PingBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// `pong` body (spec §10): empty or `{hostname}`.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
pub struct PongBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// `error` body (spec §11).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct ErrorBody {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `query` body (spec §7): a command plus string args. **Not** a prompt —
/// never routed to a model (ADR-0001; `crate::query` is the dispatcher).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct QueryBody {
    pub cmd: String,
    #[serde(default)]
    pub args: Vec<String>,
}

/// One body variant per [`MessageType`], plus [`Body::Unknown`] for any
/// v1 type this client doesn't act on yet (see module docs).
#[derive(Clone, PartialEq, Debug)]
pub enum Body {
    Join(JoinBody),
    JoinOk(JoinOkBody),
    Auth(AuthBody),
    Hello(HelloBody),
    Ping(PingBody),
    Pong(PongBody),
    Error(ErrorBody),
    Query(QueryBody),
    /// `query_ok`'s shape depends on `cmd` (spec §7: `status`/`caps`/
    /// `support`/`protocol` each answer differently), so it is kept as raw
    /// JSON rather than a typed enum — built by [`crate::query::dispatch`]
    /// and sent via [`query_ok_reply`]. This client only ever *sends*
    /// `query_ok`, never needs to parse one it receives (it never issues a
    /// remote `query` of its own — see `crate::query` module docs), so
    /// there's no round-trip need for a typed variant here.
    QueryOk(Value),
    /// An undecoded frame body of a type this client doesn't implement
    /// (e.g. `prompt`), kept as raw JSON so nothing is lost from a log.
    Unknown(Value),
}

/// Failures from [`decode`].
#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
    /// Envelope `v` is present but not `1` (spec §2).
    UnsupportedVersion(u32),
    /// Frame is not valid JSON, or JSON that does not fit the envelope
    /// schema.
    Malformed(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnsupportedVersion(v) => {
                write!(f, "unsupported protocol version {v} (v1 requires v == 1)")
            }
            DecodeError::Malformed(msg) => write!(f, "malformed frame: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Serialize an envelope to its wire form (one WebSocket text frame).
pub fn encode(envelope: &Envelope) -> serde_json::Result<String> {
    let body: Value = match &envelope.body {
        Body::Join(b) => serde_json::to_value(b)?,
        Body::JoinOk(b) => serde_json::to_value(b)?,
        Body::Auth(b) => serde_json::to_value(b)?,
        Body::Hello(b) => serde_json::to_value(b)?,
        Body::Ping(b) => serde_json::to_value(b)?,
        Body::Pong(b) => serde_json::to_value(b)?,
        Body::Error(b) => serde_json::to_value(b)?,
        Body::Query(b) => serde_json::to_value(b)?,
        Body::QueryOk(v) => v.clone(),
        Body::Unknown(v) => v.clone(),
    };
    let out = serde_json::json!({
        "v": envelope.v,
        "type": envelope.msg_type.as_wire_str(),
        "id": envelope.id,
        "ts": envelope.ts,
        "from": envelope.from,
        "body": body,
    });
    serde_json::to_string(&out)
}

/// Parse one WebSocket text frame into an [`Envelope`], enforcing the v1
/// invariant (`v == 1`, spec §2). An unrecognized `type` decodes as
/// [`Body::Unknown`] rather than an error — see module docs.
pub fn decode(raw: &str) -> Result<Envelope, DecodeError> {
    let value: Value =
        serde_json::from_str(raw).map_err(|e| DecodeError::Malformed(e.to_string()))?;
    let obj = match value {
        Value::Object(o) => o,
        _ => return Err(DecodeError::Malformed("envelope must be a JSON object".into())),
    };

    let type_str = match obj.get("type") {
        Some(Value::String(s)) => s.clone(),
        _ => return Err(DecodeError::Malformed("missing or non-string `type`".into())),
    };
    let msg_type = MessageType::from_wire(&type_str);

    let v = match obj.get("v").and_then(|v| v.as_u64()) {
        Some(v) => v as u32,
        None => return Err(DecodeError::Malformed("missing or non-numeric `v`".into())),
    };
    if v != 1 {
        return Err(DecodeError::UnsupportedVersion(v));
    }
    let id = match obj.get("id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return Err(DecodeError::Malformed("missing or non-string `id`".into())),
    };
    let ts = match obj.get("ts").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return Err(DecodeError::Malformed("missing or non-string `ts`".into())),
    };
    let from = match obj.get("from").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return Err(DecodeError::Malformed("missing or non-string `from`".into())),
    };
    let raw_body = obj
        .get("body")
        .cloned()
        .ok_or_else(|| DecodeError::Malformed("missing `body`".into()))?;

    let body = match msg_type {
        MessageType::Join => Body::Join(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `join` body: {e}")))?,
        ),
        MessageType::JoinOk => Body::JoinOk(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `join_ok` body: {e}")))?,
        ),
        MessageType::Auth => Body::Auth(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `auth` body: {e}")))?,
        ),
        MessageType::Hello => Body::Hello(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `hello` body: {e}")))?,
        ),
        MessageType::Ping => Body::Ping(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `ping` body: {e}")))?,
        ),
        MessageType::Pong => Body::Pong(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `pong` body: {e}")))?,
        ),
        MessageType::Error => Body::Error(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `error` body: {e}")))?,
        ),
        MessageType::Query => Body::Query(
            serde_json::from_value(raw_body)
                .map_err(|e| DecodeError::Malformed(format!("bad `query` body: {e}")))?,
        ),
        MessageType::QueryOk => Body::QueryOk(raw_body),
        MessageType::Unknown => Body::Unknown(raw_body),
    };

    Ok(Envelope {
        v,
        msg_type,
        id,
        ts,
        from,
        body,
    })
}

/// A correlation id (spec §3: "ULID or UUID"). Not a real ULID (no
/// monotonic timestamp component, and not built on a CSPRNG) — this
/// client only needs *unique*, not sortable or unguessable: correlation
/// ids are never secrets and this crate has no other use for a CSPRNG
/// dependency. `RandomState`'s per-instance keys are seeded from OS
/// randomness (`std`'s `sys::hashmap_random_keys`), which is enough
/// entropy for "don't collide within one connection's lifetime".
pub fn new_id() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut s = String::with_capacity(32);
    for _ in 0..2 {
        let word = RandomState::new().build_hasher().finish();
        s.push_str(&format!("{word:016x}"));
    }
    s
}

fn now_ts() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// Builds this client's `join` envelope (spec §4.1). `from` is the join
/// token's public `token_id` — this connection has no `client_id` yet, so
/// unlike every other envelope this crate sends, `from` is not one.
pub fn join_envelope(token_id: &str, secret: &str, hostname: &str) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Join,
        id: new_id(),
        ts: now_ts(),
        from: token_id.to_string(),
        body: Body::Join(JoinBody {
            secret: secret.to_string(),
            hostname: hostname.to_string(),
        }),
    }
}

/// Builds this client's `auth` envelope (spec §4): the persisted client
/// credential, never the one-time join token.
pub fn auth_envelope(from: &str, credential: &str) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Auth,
        id: new_id(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::Auth(AuthBody {
            credential: credential.to_string(),
        }),
    }
}

/// Builds this client's `hello` (spec §6). `features`/`harnesses`/
/// `sessions` are the caller's job to compute honestly (issue #30;
/// ADR-0001 "advertise only what is real") — see
/// [`crate::connection::connect_and_auth`] for how this binary derives them
/// from [`crate::query::CLIENT_FEATURES`] and
/// [`crate::config::SessionRegistry::confirmed_harnesses`].
pub fn client_hello(
    from: &str,
    hostname: &str,
    client_id: &str,
    features: Vec<String>,
    harnesses: Vec<String>,
    sessions: Vec<HelloSession>,
) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Hello,
        id: new_id(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::Hello(HelloBody {
            protocol: PROTOCOL_VERSION,
            protocol_min: PROTOCOL_VERSION,
            protocol_max: PROTOCOL_VERSION,
            role: Role::Client,
            hostname: hostname.to_string(),
            token_id: None,
            client_id: Some(client_id.to_string()),
            harnesses,
            harnesses_known: Vec::new(),
            harnesses_confirmed: Vec::new(),
            features,
            sessions,
        }),
    }
}

/// This client's heartbeat `ping`, including hostname per issue #24.
pub fn heartbeat_ping(from: &str, hostname: &str) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Ping,
        id: new_id(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::Ping(PingBody {
            hostname: Some(hostname.to_string()),
        }),
    }
}

/// The `pong` this client sends back when the server `ping`s it,
/// including hostname per issue #24. Reuses the request's `id`
/// (spec §3: "Replies reuse the request id").
pub fn pong_reply(reply_id: &str, from: &str, hostname: &str) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Pong,
        id: reply_id.to_string(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::Pong(PongBody {
            hostname: Some(hostname.to_string()),
        }),
    }
}

/// This client's reply to an inbound `query` (spec §7). Reuses the
/// request's `id` for correlation; `body` is already shaped for the
/// specific `cmd` by [`crate::query::dispatch`].
pub fn query_ok_reply(reply_id: &str, from: &str, body: Value) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::QueryOk,
        id: reply_id.to_string(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::QueryOk(body),
    }
}

/// A fail-closed `error` reply to a malformed or unrecognized `query`
/// (spec §11: `unknown_cmd`, `unknown_feature`). Reuses the request's `id`.
pub fn error_reply(reply_id: &str, from: &str, code: &str, cmd: Option<&str>, message: &str) -> Envelope {
    Envelope {
        v: PROTOCOL_VERSION,
        msg_type: MessageType::Error,
        id: reply_id.to_string(),
        ts: now_ts(),
        from: from.to_string(),
        body: Body::Error(ErrorBody {
            code: code.to_string(),
            cmd: cmd.map(|s| s.to_string()),
            message: Some(message.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_round_trips_with_token_id_as_from() {
        let env = join_envelope("tok_7f3a", "hlr_join_sometoken", "kiwi");
        assert_eq!(env.from, "tok_7f3a");
        let raw = encode(&env).unwrap();
        assert!(raw.contains("hlr_join_sometoken"));
        let decoded = decode(&raw).unwrap();
        assert_eq!(decoded, env);
        match decoded.body {
            Body::Join(JoinBody { secret, hostname }) => {
                assert_eq!(secret, "hlr_join_sometoken");
                assert_eq!(hostname, "kiwi");
            }
            other => panic!("expected `join`, got {other:?}"),
        }
    }

    #[test]
    fn join_ok_round_trips() {
        let env = Envelope {
            v: PROTOCOL_VERSION,
            msg_type: MessageType::JoinOk,
            id: "req-join".to_string(),
            ts: now_ts(),
            from: "server".to_string(),
            body: Body::JoinOk(JoinOkBody {
                client_id: "cli_19".to_string(),
                credential: "hlr_live_abc".to_string(),
            }),
        };
        let raw = encode(&env).unwrap();
        let decoded = decode(&raw).unwrap();
        assert_eq!(decoded, env);
    }

    #[test]
    fn auth_round_trips() {
        let env = auth_envelope("cli_1", "hlr_live_secret");
        let raw = encode(&env).unwrap();
        assert!(raw.contains("hlr_live_secret"));
        let decoded = decode(&raw).unwrap();
        assert_eq!(decoded, env);
    }

    #[test]
    fn hello_round_trips() {
        let env = client_hello(
            "cli_1",
            "kiwi",
            "cli_1",
            vec!["ping".to_string(), "query".to_string()],
            vec!["opencode".to_string()],
            vec![HelloSession {
                name: "alpha".to_string(),
                harness: "opencode".to_string(),
            }],
        );
        let raw = encode(&env).unwrap();
        let decoded = decode(&raw).unwrap();
        assert_eq!(decoded, env);
    }

    #[test]
    fn query_round_trips() {
        let env = Envelope {
            v: PROTOCOL_VERSION,
            msg_type: MessageType::Query,
            id: new_id(),
            ts: now_ts(),
            from: "server".to_string(),
            body: Body::Query(QueryBody {
                cmd: "support".to_string(),
                args: vec!["opencode".to_string()],
            }),
        };
        let raw = encode(&env).unwrap();
        let decoded = decode(&raw).unwrap();
        assert_eq!(decoded, env);
    }

    #[test]
    fn query_ok_round_trips_arbitrary_body_shape() {
        let body = serde_json::json!({"cmd": "status", "role": "client"});
        let env = query_ok_reply("req-1", "cli_1", body.clone());
        assert_eq!(env.id, "req-1");
        let raw = encode(&env).unwrap();
        let decoded = decode(&raw).unwrap();
        match decoded.body {
            Body::QueryOk(v) => assert_eq!(v, body),
            other => panic!("expected QueryOk, got {other:?}"),
        }
    }

    #[test]
    fn error_reply_carries_code_and_cmd() {
        let env = error_reply("req-2", "cli_1", CODE_UNKNOWN_CMD, Some("bogus"), "unknown query cmd");
        assert_eq!(env.id, "req-2");
        match env.body {
            Body::Error(ErrorBody { code, cmd, .. }) => {
                assert_eq!(code, CODE_UNKNOWN_CMD);
                assert_eq!(cmd.as_deref(), Some("bogus"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn ping_pong_round_trip_with_hostname() {
        let ping = heartbeat_ping("cli_1", "kiwi");
        let raw = encode(&ping).unwrap();
        let decoded = decode(&raw).unwrap();
        match decoded.body {
            Body::Ping(PingBody { hostname }) => assert_eq!(hostname.as_deref(), Some("kiwi")),
            other => panic!("expected Ping, got {other:?}"),
        }

        let pong = pong_reply(&ping.id, "cli_1", "kiwi");
        assert_eq!(pong.id, ping.id);
    }

    #[test]
    fn wrong_version_is_rejected() {
        let raw = r#"{"v":2,"type":"hello","id":"x","ts":"t","from":"server","body":{}}"#;
        let err = decode(raw).unwrap_err();
        assert_eq!(err, DecodeError::UnsupportedVersion(2));
    }

    #[test]
    fn unknown_type_decodes_instead_of_erroring() {
        // `prompt` is real v1 vocabulary (spec §5) but not yet implemented
        // by this client (prompt routing is a later story) — `query` is
        // now a known type, so it can't stand in for "unrecognized" here.
        let raw = r#"{"v":1,"type":"prompt","id":"x","ts":"t","from":"server","body":{"session":"alpha","text":"hi"}}"#;
        let decoded = decode(raw).unwrap();
        assert!(matches!(decoded.body, Body::Unknown(_)));
    }

    #[test]
    fn malformed_json_is_rejected() {
        let err = decode("not json").unwrap_err();
        assert!(matches!(err, DecodeError::Malformed(_)));
    }

    #[test]
    fn error_body_decodes() {
        let raw = r#"{"v":1,"type":"error","id":"x","ts":"t","from":"server","body":{"code":"unauthenticated","message":"nope"}}"#;
        let decoded = decode(raw).unwrap();
        match decoded.body {
            Body::Error(ErrorBody { code, message, .. }) => {
                assert_eq!(code, CODE_UNAUTHENTICATED);
                assert_eq!(message.as_deref(), Some("nope"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn new_id_is_not_empty_and_varies() {
        let a = new_id();
        let b = new_id();
        assert!(!a.is_empty());
        assert_ne!(a, b);
    }
}
