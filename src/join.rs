//! The join redeem seam (issue #23), with a real transport (ADR 0015,
//! issue #16's client-side follow-up).
//!
//! `holler join` exchanges a one-time join token for a durable client
//! identity: a `client_id` and a long-lived credential (mirroring, without
//! sharing code with, the server's join-secret/client-credential split —
//! see holler-server ADR 0010 and `docs/protocol/v1.md`). The join token
//! itself is single-use and is never sent, logged, or persisted again once
//! this exchange completes.
//!
//! The real exchange is a dedicated `join` / `join_ok` wire frame pair
//! (`docs/protocol/v1.md` §4.1, holler-server ADR 0015): connect, send
//! `join` as the first frame, await `join_ok` or `error`, then close — a
//! one-shot bootstrap, never continuing into `auth`/`hello` on the same
//! socket. [`WsJoinTransport`] is that real implementation. [`JoinTransport`]
//! remains the seam and [`StubJoinTransport`] remains as a test double for
//! callers (e.g. issue #23's own tests) that don't want a real socket.
//!
//! # `--token` is `<token_id>:<secret>`
//!
//! A join envelope's `from` is the join token's public `token_id` (spec
//! §4.1) — a value distinct from the secret and not derivable from it.
//! holler-server's `holler-server token mint` prints both separately (`token_id:
//! tok_...` / `secret: hlr_...`), and this crate's `holler join --token`
//! flag (issue #23) takes a single string. Rather than add a second CLI
//! flag, this crate treats that one pasted value as
//! `<token_id>:<secret>` — a single copy-pasted artifact, matching how a
//! join token is described throughout this crate's docs and
//! `docs/adr/ADR-0003.md`'s existing `--token <join>` CLI shape (which
//! doesn't get a new flag added to it). See [`split_token_id_and_secret`].

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use crate::debug::{self, DebugConfig};
use crate::proto::{self, Body, ErrorBody};
use crate::server_address::ServerAddress;

/// The durable identity returned by a successful join redeem.
///
/// Both fields are opaque strings from this crate's point of view: this
/// crate does not need to know the server's exact prefix scheme (`cli_`
/// for `client_id`, `hlr_live_` for the credential, per ADR 0010) to persist
/// and reuse them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedeemedIdentity {
    pub client_id: String,
    pub credential: String,
    /// The join token's public `token_id` (the part before the `:` in
    /// `--token <token_id>:<secret>`), needed by [`crate::connection`] to
    /// send the right `from` on the `auth` envelope (spec §4/§6: `from` is
    /// the client's `token_id`, not its `client_id`).
    pub token_id: String,
}

/// Failure redeeming a join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinError {
    /// The transport could not complete the redeem exchange. Carries a
    /// human-readable message; #24's real transport is expected to add
    /// typed variants (connection refused, server `error`/`unauthenticated`,
    /// version mismatch, ...) once there is an actual socket to fail on.
    Failed(String),
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinError::Failed(message) => write!(f, "join failed: {message}"),
        }
    }
}

impl std::error::Error for JoinError {}

/// Redeems a join token against a server for a durable client identity.
///
/// Implementations must never retain or re-send `token` beyond this single
/// call: it is single-use by protocol contract.
pub trait JoinTransport {
    fn redeem(
        &self,
        server: &ServerAddress,
        token: &str,
        hostname: &str,
        cfg: DebugConfig,
    ) -> Result<RedeemedIdentity, JoinError>;
}

/// Stand-in [`JoinTransport`] until issue #24 wires a real WebSocket-based
/// implementation.
///
/// **This never talks to a network.** It derives a `client_id` and
/// credential deterministically from its inputs (a non-cryptographic hash,
/// not a CSPRNG) purely so that higher-level flows — `join` persisting a
/// credential, `status` reading it back, `detach` deleting it — have
/// something real to exercise end to end in tests today. The values it
/// produces are not secrets in any meaningful sense and must never be
/// treated as such once #24 lands a real transport.
#[derive(Debug, Default, Clone, Copy)]
pub struct StubJoinTransport;

impl JoinTransport for StubJoinTransport {
    fn redeem(
        &self,
        server: &ServerAddress,
        token: &str,
        hostname: &str,
        _cfg: DebugConfig,
    ) -> Result<RedeemedIdentity, JoinError> {
        // A new join token is a new pairing (per protocol), so the derived
        // client_id depends on `token` too, not just server/hostname.
        let client_id = format!(
            "cli_{:016x}",
            stub_hash(&[&server.to_canonical_url(), "client_id", hostname, token])
        );
        let credential = format!(
            "hlr_live_{:016x}{:016x}",
            stub_hash(&[token, "credential", hostname]),
            stub_hash(&[hostname, token, &server.to_canonical_url()]),
        );
        let token_id = format!(
            "tok_{:016x}",
            stub_hash(&[&server.to_canonical_url(), "token_id", hostname, token])
        );
        Ok(RedeemedIdentity {
            client_id,
            credential,
            token_id,
        })
    }
}

/// Splits a `holler join --token` value into its `token_id` and `secret`
/// parts (see module docs). Splits on the *first* colon only — neither
/// part is expected to contain one, but the secret is the operator's
/// pasted data and the token_id is not, so if anything is ambiguous it
/// should be the tail, not the head.
fn split_token_id_and_secret(token: &str) -> Result<(&str, &str), JoinError> {
    token.split_once(':').ok_or_else(|| {
        JoinError::Failed(
            "expected `--token <token_id>:<secret>` (both printed by `holler-server token mint`)"
                .to_string(),
        )
    })
}

/// The real [`JoinTransport`]: a one-shot WebSocket round-trip against
/// holler-server's `join`/`join_ok` frame pair (`docs/protocol/v1.md`
/// §4.1; ADR 0015). This is the production default for `holler join` —
/// see `src/main.rs`.
#[derive(Debug, Default, Clone, Copy)]
pub struct WsJoinTransport;

impl JoinTransport for WsJoinTransport {
    fn redeem(
        &self,
        server: &ServerAddress,
        token: &str,
        hostname: &str,
        cfg: DebugConfig,
    ) -> Result<RedeemedIdentity, JoinError> {
        let (token_id, secret) = split_token_id_and_secret(token)?;

        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            JoinError::Failed(format!("failed to start async runtime for join: {e}"))
        })?;
        runtime.block_on(redeem_async(server, token_id, secret, hostname, cfg))
    }
}

/// The actual connect → `join` → `join_ok`/`error` → close exchange behind
/// [`WsJoinTransport::redeem`].
async fn redeem_async(
    server: &ServerAddress,
    token_id: &str,
    secret: &str,
    hostname: &str,
    cfg: DebugConfig,
) -> Result<RedeemedIdentity, JoinError> {
    if cfg.is_on() {
        debug::info(cfg, "logging_started")
            .field("format", cfg.format.to_string())
            .field("note", "frames to stderr, secrets redacted")
            .emit();
    }
    let url = server.to_canonical_url();
    let (mut ws, _response) = tokio_tungstenite::connect_async(&url)
        .await
        .map_err(|e| JoinError::Failed(format!("connect to {url} failed: {e}")))?;

    let envelope = proto::join_envelope(token_id, secret, hostname);
    let raw = proto::encode(&envelope).expect("v1 join envelope always serializes");
    debug::outgoing(cfg, "join")
        .id(&envelope.id)
        .peer(token_id)
        .frame(|| debug::redact_secret(&raw, secret))
        .emit();
    if let Err(e) = ws.send(Message::Text(raw.into())).await {
        return Err(JoinError::Failed(format!("failed to send join: {e}")));
    }

    let reply_raw = match ws.next().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        Some(Ok(_)) => {
            let _ = ws.close(None).await;
            return Err(JoinError::Failed(
                "unexpected non-text frame while awaiting join_ok".to_string(),
            ));
        }
        Some(Err(e)) => {
            let _ = ws.close(None).await;
            return Err(JoinError::Failed(format!(
                "socket error awaiting join_ok: {e}"
            )));
        }
        None => {
            return Err(JoinError::Failed(
                "connection closed awaiting join_ok".to_string(),
            ))
        }
    };

    // The server closes right after replying (spec §4.1); close our end
    // too rather than leaving a one-shot socket open.
    let _ = ws.close(None).await;

    let reply = proto::decode(&reply_raw)
        .map_err(|e| JoinError::Failed(format!("malformed frame awaiting join_ok: {e}")))?;
    match reply.body {
        Body::JoinOk(body) => {
            debug::incoming(cfg, "join_ok")
                .id(&reply.id)
                .peer(&body.client_id)
                .frame(|| debug::redact_secret(&reply_raw, &body.credential))
                .emit();
            Ok(RedeemedIdentity {
                client_id: body.client_id,
                credential: body.credential,
                token_id: token_id.to_string(),
            })
        }
        Body::Error(ErrorBody { code, message, .. }) => {
            debug::incoming(cfg, "error")
                .id(&reply.id)
                .field("code", code.as_str())
                .frame(|| reply_raw.clone())
                .emit();
            Err(JoinError::Failed(
                message.unwrap_or_else(|| format!("join failed ({code})")),
            ))
        }
        other => Err(JoinError::Failed(format!(
            "expected `join_ok` after `join`, got {:?}",
            std::mem::discriminant(&other)
        ))),
    }
}

/// A small deterministic (not cryptographic) hash used only to give
/// [`StubJoinTransport`] stable, input-dependent output.
fn stub_hash(parts: &[&str]) -> u64 {
    use std::hash::{Hash, Hasher};
    // `DefaultHasher::new()` uses fixed keys, unlike `RandomState`, so this
    // is reproducible across runs and processes.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for part in parts {
        part.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> ServerAddress {
        ServerAddress::parse("ws://example.com:41807").unwrap()
    }

    #[test]
    fn stub_redeem_succeeds() {
        let identity = StubJoinTransport.redeem(&server(), "hlr_join_sometoken", "kiwi", DebugConfig::default());
        assert!(identity.is_ok());
    }

    #[test]
    fn stub_redeem_is_deterministic_for_same_inputs() {
        let a = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi", DebugConfig::default())
            .unwrap();
        let b = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi", DebugConfig::default())
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn stub_redeem_varies_with_token() {
        let a = StubJoinTransport
            .redeem(&server(), "hlr_join_one", "kiwi", DebugConfig::default())
            .unwrap();
        let b = StubJoinTransport
            .redeem(&server(), "hlr_join_two", "kiwi", DebugConfig::default())
            .unwrap();
        assert_ne!(a.credential, b.credential);
    }

    #[test]
    fn stub_redeem_uses_expected_prefixes() {
        let identity = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi", DebugConfig::default())
            .unwrap();
        assert!(identity.client_id.starts_with("cli_"));
        assert!(identity.credential.starts_with("hlr_live_"));
    }

    #[test]
    fn stub_redeem_never_echoes_the_join_token() {
        let token = "hlr_join_veryunique";
        let identity = StubJoinTransport.redeem(&server(), token, "kiwi", DebugConfig::default()).unwrap();
        assert!(!identity.client_id.contains(token));
        assert!(!identity.credential.contains(token));
        assert!(!format!("{identity:?}").contains(token));
    }

    #[test]
    fn splits_token_id_and_secret_on_first_colon() {
        let (token_id, secret) =
            split_token_id_and_secret("tok_7f3a:hlr_join_sometoken").unwrap();
        assert_eq!(token_id, "tok_7f3a");
        assert_eq!(secret, "hlr_join_sometoken");
    }

    #[test]
    fn split_rejects_a_token_with_no_colon() {
        let err = split_token_id_and_secret("hlr_join_sometoken").unwrap_err();
        assert!(matches!(err, JoinError::Failed(_)));
    }

    #[test]
    fn ws_join_transport_surfaces_a_clear_error_for_a_malformed_token() {
        // No network I/O should even be attempted for a token this crate
        // can already tell is malformed.
        let err = WsJoinTransport
            .redeem(&server(), "not-a-valid-token", "kiwi", DebugConfig::default())
            .unwrap_err();
        let JoinError::Failed(message) = err;
        assert!(message.contains("token_id"), "message: {message}");
    }

    #[test]
    fn ws_join_transport_connection_refused_is_a_clear_error_not_a_panic() {
        // Nothing is listening on this loopback port (bound then
        // immediately dropped, so it's very unlikely to be reused for
        // something else within this test's lifetime): connect_async
        // should fail promptly and cleanly rather than hang or panic.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let server = ServerAddress::parse(&format!("ws://127.0.0.1:{port}")).unwrap();

        let err = WsJoinTransport
            .redeem(&server, "tok_x:hlr_join_y", "kiwi", DebugConfig::default())
            .unwrap_err();
        let JoinError::Failed(message) = err;
        assert!(!message.is_empty());
    }
}
