//! Builds the protocol `status` document for `role: "client"` (issue #23).
//!
//! Shape per `docs/protocol/v1.md` §7 ("status document"), holler-server
//! repo, read-only reference — there is no shared crate between the two
//! repos, so this is modeled independently, not imported.
//!
//! `connected`/`reconnecting` reflect the live socket state a separate
//! `holler run` process persists to [`crate::connection::ConnectionStateStore`]
//! (issue #24) — this process has no socket of its own, so it can only
//! ever report what that file says (or "disconnected" if there is none /
//! it's stale). `harnesses` and `sessions` come from the locally
//! configured [`SessionRegistry`] (issue #25), not a live PATH probe (ADR
//! 0001's "harnesses it can actually drive" probe is a later story) — this
//! is an honest "configured to use", analogous to the protocol's `known`
//! vs. `confirmed` distinction, not a claim that the adapter is confirmed
//! runnable right now.
//!
//! Never includes the credential or join token — only `client_id` is
//! surfaced, and only when a credential is actually persisted.

use serde::Serialize;

use crate::config::SessionRegistry;
use crate::connection::LiveState;
use crate::credential::PersistedCredential;

const PROTOCOL_VERSION: u32 = 1;
const PROTOCOL_MIN: u32 = 1;
const PROTOCOL_MAX: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionStatus {
    pub name: String,
    pub harness: String,
    /// Always `false` today: this story tracks no live driver state.
    /// Real busy/idle tracking arrives with the live session work (#24+).
    pub busy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClientStatus {
    pub cmd: &'static str,
    pub role: &'static str,
    pub protocol: u32,
    pub protocol_min: u32,
    pub protocol_max: u32,
    pub hostname: String,
    pub connected: bool,
    /// True while a connection attempt (initial or a retry after a
    /// drop) is in flight but not yet past `auth`+`hello`. Not part of
    /// the wire `status` schema (`docs/protocol/v1.md` §7 only defines
    /// `connected`) — an extra field local `holler status` output adds
    /// for a live human/CLI reader; a future query-dispatcher story
    /// (#30) should decide whether to carry it onto the wire too.
    pub reconnecting: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    pub harnesses: Vec<String>,
    pub sessions: Vec<SessionStatus>,
}

/// Builds the status document from currently-persisted state.
///
/// `credential` is `None` when nothing is joined ("not joined"): the
/// document is still well-formed, just without a `client_id`. `live` is
/// this invocation's read of [`crate::connection::ConnectionStateStore`]
/// — this process has no socket of its own to ask directly.
pub fn build(
    credential: Option<&PersistedCredential>,
    registry: &SessionRegistry,
    hostname: String,
    live: LiveState,
) -> ClientStatus {
    let mut harnesses: Vec<String> = registry
        .sessions()
        .iter()
        .map(|s| s.harness.clone())
        .collect();
    harnesses.sort();
    harnesses.dedup();

    let sessions = registry
        .sessions()
        .iter()
        .map(|s| SessionStatus {
            name: s.name.clone(),
            harness: s.harness.clone(),
            busy: false,
        })
        .collect();

    let (connected, reconnecting) = match live {
        LiveState::Connected => (true, false),
        LiveState::Connecting | LiveState::Reconnecting => (false, true),
        LiveState::Disconnected => (false, false),
    };

    ClientStatus {
        cmd: "status",
        role: "client",
        protocol: PROTOCOL_VERSION,
        protocol_min: PROTOCOL_MIN,
        protocol_max: PROTOCOL_MAX,
        hostname,
        connected,
        reconnecting,
        client_id: credential.map(|c| c.client_id.clone()),
        harnesses,
        sessions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_joined_omits_client_id() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
        );
        assert_eq!(status.client_id, None);
        assert_eq!(status.role, "client");
        assert!(!status.connected);
        assert!(!status.reconnecting);

        let json = serde_json::to_string(&status).unwrap();
        assert!(!json.contains("client_id"));
    }

    #[test]
    fn joined_includes_client_id() {
        let credential = PersistedCredential {
            client_id: "cli_abc123".to_string(),
            credential: "hlr_live_shouldnotappear".to_string(),
            server: "ws://example.com:41807".to_string(),
            hostname: "kiwi".to_string(),
        };
        let status = build(
            Some(&credential),
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
        );
        assert_eq!(status.client_id.as_deref(), Some("cli_abc123"));
    }

    #[test]
    fn never_leaks_the_credential_value() {
        let credential = PersistedCredential {
            client_id: "cli_abc123".to_string(),
            credential: "hlr_live_shouldnotappear".to_string(),
            server: "ws://example.com:41807".to_string(),
            hostname: "kiwi".to_string(),
        };
        let status = build(
            Some(&credential),
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
        );
        let json = serde_json::to_string(&status).unwrap();
        assert!(!json.contains("hlr_live_shouldnotappear"));
    }

    #[test]
    fn default_registry_sessions_and_harnesses_are_populated() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
        );
        assert_eq!(status.harnesses, vec!["opencode"]);
        assert_eq!(status.sessions.len(), 2);
        assert!(status.sessions.iter().all(|s| !s.busy));
    }

    #[test]
    fn live_connected_reports_connected_not_reconnecting() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Connected,
        );
        assert!(status.connected);
        assert!(!status.reconnecting);
    }

    #[test]
    fn live_connecting_reports_reconnecting_not_connected() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Connecting,
        );
        assert!(!status.connected);
        assert!(status.reconnecting);
    }

    #[test]
    fn live_reconnecting_reports_reconnecting_not_connected() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Reconnecting,
        );
        assert!(!status.connected);
        assert!(status.reconnecting);
    }
}
