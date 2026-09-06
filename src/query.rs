//! Answers `query` — `status` / `caps` / `support` / `protocol` — from
//! local probes only, never a model (issue #30; ADR-0001; holler-server
//! ADR-0001; `docs/protocol/v1.md` §7).
//!
//! One dispatcher, [`dispatch`], serves both faces of "answer query": the
//! wire path ([`crate::connection`]'s inbound `Body::Query` handling,
//! answering the server) and the local CLI path (`holler status`/
//! `support`/`caps`/`query`, answering the operator) build the exact same
//! `query_ok` body. The spec says this explicitly for `status` ("same
//! schema whether the local process is the server or a client") and
//! there's no reason `support`/`caps`/`protocol` should differ between the
//! two call sites either.
//!
//! # Scope: this client never issues a remote `query`
//!
//! The spec allows a client to `query` the server the same way (§7), and
//! `docs/protocol/v1.md` §8 sketches `holler query <id> <cmd>` as a general
//! remote form. That requires addressing another peer by id, which needs
//! roster/target routing this crate does not have yet (a later story).
//! This module and its CLI callers only ever answer queries *about this
//! process* — local commands with no `<id>` target, and inbound wire
//! `query` frames this client answers as the peer being asked.

use serde_json::{json, Value};

use crate::config::SessionRegistry;
use crate::connection::LiveState;
use crate::proto::{self, QueryBody};
use crate::status;

/// Protocol features this binary actually implements right now. Spec §9's
/// full vocabulary is [`KNOWN_PROTOCOL_FEATURES`] — most of it is not wired
/// to the wire yet. `interrupt` and `presence` exist as local Rust APIs
/// ([`crate::session_manager`]) but are not yet reachable from a live
/// socket ([`crate::connection`]'s inbound dispatch only understands
/// `ping`/`query`), so advertising them here would violate "advertise only
/// what is real" (ADR-0001).
pub const CLIENT_FEATURES: &[&str] = &["ping", "query"];

/// The v1 protocol feature vocabulary (spec §9).
pub const KNOWN_PROTOCOL_FEATURES: &[&str] =
    &["interrupt", "presence", "ping", "query", "roster", "token", "wait"];

/// The v1 harness id vocabulary (spec §9). "Unknown ids are legal" per that
/// section — this list is only used to build `caps`'s full map, not to
/// reject a `support` query about an id outside it.
pub const KNOWN_HARNESS_IDS: &[&str] = &[
    "opencode", "claude", "codex", "grok", "hermes", "pi", "cursor", "copilot", "droid", "kimi",
    "qwen", "kilo", "goose",
];

/// Why [`dispatch`] could not answer a `query`. Both map to the spec's
/// fail-closed `error` codes (§11) — see [`QueryError::code`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryError {
    /// `cmd` is not one of `status`/`caps`/`support`/`protocol`.
    UnknownCmd,
    /// `support` was asked with no argument, or `protocol`'s argument
    /// isn't a positive integer.
    UnknownFeature,
}

impl QueryError {
    /// The wire `error.code` this maps to (spec §11).
    pub fn code(self) -> &'static str {
        match self {
            QueryError::UnknownCmd => proto::CODE_UNKNOWN_CMD,
            QueryError::UnknownFeature => proto::CODE_UNKNOWN_FEATURE,
        }
    }
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::UnknownCmd => write!(f, "unknown query cmd"),
            QueryError::UnknownFeature => write!(f, "unknown or missing feature"),
        }
    }
}

impl std::error::Error for QueryError {}

/// One id's support answer (a `support` reply's `ok`/`kind`/`how`/`reason`,
/// or one entry of `caps`'s capability map).
struct SupportAnswer {
    ok: bool,
    kind: &'static str,
    how: Option<String>,
    reason: Option<String>,
}

/// Decides `ok` for one feature-or-harness id via a **local probe only**
/// (ADR-0001: "never by asking a model"). A protocol feature is `ok` iff
/// this binary implements it ([`CLIENT_FEATURES`]); a harness is `ok` iff
/// at least one configured session names it *and* that session's command
/// is confirmed runnable right now
/// ([`SessionRegistry::confirmed_command_for_harness`]). An id outside
/// both vocabularies is treated as an (unconfigured) harness id — spec §9:
/// "Unknown ids are legal."
fn answer_support(id: &str, registry: &SessionRegistry) -> SupportAnswer {
    if KNOWN_PROTOCOL_FEATURES.contains(&id) {
        let ok = CLIENT_FEATURES.contains(&id);
        return SupportAnswer {
            ok,
            kind: "feature",
            how: None,
            reason: if ok { None } else { Some("not implemented".to_string()) },
        };
    }
    match registry.confirmed_command_for_harness(id) {
        Some(how) => SupportAnswer {
            ok: true,
            kind: "harness",
            how: Some(how),
            reason: None,
        },
        None => SupportAnswer {
            ok: false,
            kind: "harness",
            how: None,
            reason: Some("no adapter".to_string()),
        },
    }
}

fn support_value(feature: &str, answer: &SupportAnswer) -> Value {
    let mut body = json!({
        "cmd": "support",
        "args": [feature],
        "ok": answer.ok,
        "feature": feature,
        "kind": answer.kind,
    });
    if let Some(how) = &answer.how {
        body["how"] = json!(how);
    }
    if let Some(reason) = &answer.reason {
        body["reason"] = json!(reason);
    }
    body
}

fn status_value(
    client_id: Option<&str>,
    registry: &SessionRegistry,
    hostname: &str,
    live: LiveState,
) -> Value {
    let confirmed = registry.confirmed_harnesses();
    let features: Vec<String> = CLIENT_FEATURES.iter().map(|s| s.to_string()).collect();
    let doc = status::build(client_id, registry, hostname.to_string(), live, &confirmed, features);
    serde_json::to_value(doc).expect("ClientStatus always serializes")
}

/// `caps`: `status` plus an explicit `capabilities` map of every known
/// protocol feature and harness id to `{ok, kind, how?, reason?}` — the
/// same per-id shape [`support_value`] answers with, minus `cmd`/`args`/
/// `feature`. The spec (§7) requires "status plus [a] map"; it does not
/// name the map's field, so `capabilities` is this story's own judgment
/// call for that name.
fn caps_value(
    client_id: Option<&str>,
    registry: &SessionRegistry,
    hostname: &str,
    live: LiveState,
) -> Value {
    let mut body = status_value(client_id, registry, hostname, live);
    body["cmd"] = json!("caps");

    let mut capabilities = serde_json::Map::new();
    for id in KNOWN_PROTOCOL_FEATURES.iter().chain(KNOWN_HARNESS_IDS.iter()) {
        let answer = answer_support(id, registry);
        let mut entry = json!({ "ok": answer.ok, "kind": answer.kind });
        if let Some(how) = &answer.how {
            entry["how"] = json!(how);
        }
        if let Some(reason) = &answer.reason {
            entry["reason"] = json!(reason);
        }
        capabilities.insert((*id).to_string(), entry);
    }
    body["capabilities"] = Value::Object(capabilities);
    body
}

/// `protocol`: no args reports this binary's `min`/`max` range (spec §7
/// "highest version this binary can handle"); one arg asks "can you speak
/// N?" (`ok = min <= asked <= max`). `session_v` is the *socket's* envelope
/// `v`, reported alongside but never confused with `min`/`max` (spec §2).
fn protocol_value(args: &[String], session_v: u32) -> Result<Value, QueryError> {
    if args.is_empty() {
        return Ok(json!({
            "cmd": "protocol",
            "session": session_v,
            "min": proto::PROTOCOL_VERSION,
            "max": proto::PROTOCOL_VERSION,
        }));
    }
    let asked: u32 = args[0].parse().map_err(|_| QueryError::UnknownFeature)?;
    if asked == 0 {
        // "args[0] must be a positive integer" (spec §7).
        return Err(QueryError::UnknownFeature);
    }
    // Written as a range (not `min <= asked && asked <= max`) so this
    // reads as "is `asked` within [min, max]" even though, today, this
    // binary's min and max are the same value.
    let ok = (proto::PROTOCOL_VERSION..=proto::PROTOCOL_VERSION).contains(&asked);
    Ok(json!({
        "cmd": "protocol",
        "args": args,
        "ok": ok,
        "asked": asked,
        "session": session_v,
        "min": proto::PROTOCOL_VERSION,
        "max": proto::PROTOCOL_VERSION,
    }))
}

/// Answers one `query` (from the wire or a local CLI invocation) with the
/// `query_ok` body it deserves, or the [`QueryError`] to fail closed with
/// (spec: "Unknown `cmd` → `error`/`unknown_cmd`. Do not invent an
/// answer."). `session_v` is the *socket's* envelope `v` — only meaningful
/// for `protocol`'s `session` field; local (non-wire) callers pass
/// [`proto::PROTOCOL_VERSION`], since there is no socket.
pub fn dispatch(
    query: &QueryBody,
    session_v: u32,
    client_id: Option<&str>,
    registry: &SessionRegistry,
    hostname: &str,
    live: LiveState,
) -> Result<Value, QueryError> {
    match query.cmd.as_str() {
        "status" => Ok(status_value(client_id, registry, hostname, live)),
        "caps" => Ok(caps_value(client_id, registry, hostname, live)),
        "support" => {
            let feature = query.args.first().ok_or(QueryError::UnknownFeature)?;
            Ok(support_value(feature, &answer_support(feature, registry)))
        }
        "protocol" => protocol_value(&query.args, session_v),
        _ => Err(QueryError::UnknownCmd),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SessionConfig;

    fn empty_registry() -> SessionRegistry {
        SessionRegistry::from_configs(vec![]).unwrap()
    }

    /// A registry with one harness whose command is confirmed runnable
    /// (`/bin/sh`, a real absolute path — deterministic regardless of the
    /// test host's `$PATH`).
    fn runnable_registry() -> SessionRegistry {
        SessionRegistry::from_configs(vec![SessionConfig {
            name: "alpha".to_string(),
            harness: "opencode".to_string(),
            command: vec!["/bin/sh".to_string()],
            interrupt: None,
        }])
        .unwrap()
    }

    #[test]
    fn support_known_implemented_feature_is_ok() {
        let query = QueryBody { cmd: "support".to_string(), args: vec!["ping".to_string()] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["kind"], "feature");
        assert_eq!(body["feature"], "ping");
    }

    #[test]
    fn support_known_unimplemented_feature_is_not_ok() {
        let query = QueryBody { cmd: "support".to_string(), args: vec!["roster".to_string()] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], false);
        assert_eq!(body["kind"], "feature");
        assert_eq!(body["reason"], "not implemented");
    }

    #[test]
    fn support_unconfigured_harness_is_not_ok() {
        let query = QueryBody { cmd: "support".to_string(), args: vec!["claude".to_string()] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], false);
        assert_eq!(body["kind"], "harness");
        assert_eq!(body["reason"], "no adapter");
    }

    #[test]
    fn support_confirmed_runnable_harness_is_ok_with_how() {
        let query = QueryBody { cmd: "support".to_string(), args: vec!["opencode".to_string()] };
        let body = dispatch(&query, 1, None, &runnable_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["kind"], "harness");
        assert_eq!(body["how"], "/bin/sh");
    }

    #[test]
    fn support_with_no_args_is_unknown_feature() {
        let query = QueryBody { cmd: "support".to_string(), args: vec![] };
        let err = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap_err();
        assert_eq!(err, QueryError::UnknownFeature);
        assert_eq!(err.code(), "unknown_feature");
    }

    #[test]
    fn status_reports_role_client_and_confirmed_harnesses() {
        let query = QueryBody { cmd: "status".to_string(), args: vec![] };
        let body = dispatch(&query, 1, Some("cli_1"), &runnable_registry(), "kiwi", LiveState::Connected).unwrap();
        assert_eq!(body["cmd"], "status");
        assert_eq!(body["role"], "client");
        assert_eq!(body["connected"], true);
        assert_eq!(body["client_id"], "cli_1");
        assert_eq!(body["harnesses"], json!(["opencode"]));
    }

    #[test]
    fn caps_includes_every_known_id_in_capabilities_map() {
        let query = QueryBody { cmd: "caps".to_string(), args: vec![] };
        let body = dispatch(&query, 1, None, &runnable_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["cmd"], "caps");
        for id in KNOWN_PROTOCOL_FEATURES.iter().chain(KNOWN_HARNESS_IDS.iter()) {
            assert!(body["capabilities"].get(id).is_some(), "missing capability entry for {id}");
        }
        assert_eq!(body["capabilities"]["ping"]["ok"], true);
        assert_eq!(body["capabilities"]["opencode"]["ok"], true);
        assert_eq!(body["capabilities"]["claude"]["ok"], false);
    }

    #[test]
    fn protocol_with_no_args_reports_min_max_and_session() {
        let query = QueryBody { cmd: "protocol".to_string(), args: vec![] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["cmd"], "protocol");
        assert_eq!(body["session"], 1);
        assert_eq!(body["min"], 1);
        assert_eq!(body["max"], 1);
        assert!(body.get("args").is_none());
        assert!(body.get("ok").is_none());
    }

    #[test]
    fn protocol_asking_supported_version_is_ok() {
        let query = QueryBody { cmd: "protocol".to_string(), args: vec!["1".to_string()] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["asked"], 1);
    }

    #[test]
    fn protocol_asking_unsupported_version_is_not_ok_but_not_an_error() {
        let query = QueryBody { cmd: "protocol".to_string(), args: vec!["2".to_string()] };
        let body = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap();
        assert_eq!(body["ok"], false);
        assert_eq!(body["asked"], 2);
    }

    #[test]
    fn protocol_non_integer_arg_is_unknown_feature() {
        let query = QueryBody { cmd: "protocol".to_string(), args: vec!["not-a-number".to_string()] };
        let err = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap_err();
        assert_eq!(err, QueryError::UnknownFeature);
    }

    #[test]
    fn protocol_zero_is_not_a_positive_integer() {
        let query = QueryBody { cmd: "protocol".to_string(), args: vec!["0".to_string()] };
        let err = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap_err();
        assert_eq!(err, QueryError::UnknownFeature);
    }

    #[test]
    fn unknown_cmd_fails_closed() {
        let query = QueryBody { cmd: "summarize".to_string(), args: vec![] };
        let err = dispatch(&query, 1, None, &empty_registry(), "kiwi", LiveState::Disconnected).unwrap_err();
        assert_eq!(err, QueryError::UnknownCmd);
        assert_eq!(err.code(), "unknown_cmd");
    }
}
