//! The join redeem seam (issue #23).
//!
//! `holler join` exchanges a one-time join token for a durable client
//! identity: a `client_id` and a long-lived credential (mirroring, without
//! sharing code with, the server's join-secret/client-credential split —
//! see holler-server ADR 0010 and `docs/protocol/v1.md`). The join token
//! itself is single-use and is never sent, logged, or persisted again once
//! this exchange completes.
//!
//! The real exchange is a WebSocket round-trip (`auth` / `hello`) against
//! holler-server, which does not exist in this crate yet — that is issue
//! #24 ("first talk"). [`JoinTransport`] is the seam: this story wires
//! everything around it (CLI, URL/port handling, hostname, credential
//! persistence) against the trait, and [`StubJoinTransport`] stands in
//! until #24 replaces it with a real implementation. This mirrors the
//! `ConnectionProbe` / `AlwaysDisconnected` seam in holler-server's
//! `src/token/mod.rs` (issue #29/#31).

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
        Ok(RedeemedIdentity {
            client_id,
            credential,
        })
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
        let identity = StubJoinTransport.redeem(&server(), "hlr_join_sometoken", "kiwi");
        assert!(identity.is_ok());
    }

    #[test]
    fn stub_redeem_is_deterministic_for_same_inputs() {
        let a = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi")
            .unwrap();
        let b = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi")
            .unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn stub_redeem_varies_with_token() {
        let a = StubJoinTransport
            .redeem(&server(), "hlr_join_one", "kiwi")
            .unwrap();
        let b = StubJoinTransport
            .redeem(&server(), "hlr_join_two", "kiwi")
            .unwrap();
        assert_ne!(a.credential, b.credential);
    }

    #[test]
    fn stub_redeem_uses_expected_prefixes() {
        let identity = StubJoinTransport
            .redeem(&server(), "hlr_join_sometoken", "kiwi")
            .unwrap();
        assert!(identity.client_id.starts_with("cli_"));
        assert!(identity.credential.starts_with("hlr_live_"));
    }

    #[test]
    fn stub_redeem_never_echoes_the_join_token() {
        let token = "hlr_join_veryunique";
        let identity = StubJoinTransport.redeem(&server(), token, "kiwi").unwrap();
        assert!(!identity.client_id.contains(token));
        assert!(!identity.credential.contains(token));
        assert!(!format!("{identity:?}").contains(token));
    }
}
