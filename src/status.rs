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
//! it's stale). `harnesses` and `sessions` are filtered to
//! `confirmed_harnesses` (issue #30: `crate::config::SessionRegistry::confirmed_harnesses`,
//! a real PATH/executable probe) — ADR 0001's "harnesses it can actually
//! drive", not merely "configured to use". This module takes that list as
//! an input rather than probing itself, so it stays a pure function of its
//! arguments and fully unit-testable without touching the filesystem.
//!
//! Never includes the credential or join token — only `client_id` is
//! surfaced (as a plain `&str`, never the full [`PersistedCredential`], so
//! this module has no way to leak the credential value even by accident),
//! and only when one is actually persisted.
//!
//! [`PersistedCredential`]: crate::credential::PersistedCredential

use serde::Serialize;

use crate::config::SessionRegistry;
use crate::connection::LiveState;

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
    /// Protocol features this binary actually implements right now
    /// (`crate::query::CLIENT_FEATURES`), not the full v1 vocabulary.
    pub features: Vec<String>,
    pub harnesses: Vec<String>,
    pub sessions: Vec<SessionStatus>,
}

/// Builds the status document from currently-persisted state.
///
/// `client_id` is `None` when nothing is joined ("not joined"): the
/// document is still well-formed, just without a `client_id`. `live` is
/// this invocation's read of [`crate::connection::ConnectionStateStore`]
/// — this process has no socket of its own to ask directly. `harnesses`
/// and `sessions` are filtered to `confirmed_harnesses` — see module docs.
pub fn build(
    client_id: Option<&str>,
    registry: &SessionRegistry,
    hostname: String,
    live: LiveState,
    confirmed_harnesses: &[String],
    features: Vec<String>,
) -> ClientStatus {
    let sessions = registry
        .sessions()
        .iter()
        .filter(|s| confirmed_harnesses.iter().any(|h| h == &s.harness))
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
        client_id: client_id.map(|c| c.to_string()),
        features,
        harnesses: confirmed_harnesses.to_vec(),
        sessions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn confirmed_opencode() -> Vec<String> {
        vec!["opencode".to_string()]
    }

    fn features() -> Vec<String> {
        vec!["ping".to_string(), "query".to_string()]
    }

    #[test]
    fn not_joined_omits_client_id() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
            &confirmed_opencode(),
            features(),
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
        let status = build(
            Some("cli_abc123"),
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
            &confirmed_opencode(),
            features(),
        );
        assert_eq!(status.client_id.as_deref(), Some("cli_abc123"));
    }

    #[test]
    fn default_registry_sessions_and_harnesses_are_populated_when_confirmed() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
            &confirmed_opencode(),
            features(),
        );
        assert_eq!(status.harnesses, vec!["opencode"]);
        assert_eq!(status.sessions.len(), 2);
        assert!(status.sessions.iter().all(|s| !s.busy));
        assert_eq!(status.features, vec!["ping".to_string(), "query".to_string()]);
    }

    #[test]
    fn unconfirmed_harnesses_hide_their_sessions() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Disconnected,
            &[], // nothing confirmed runnable
            features(),
        );
        assert!(status.harnesses.is_empty());
        assert!(status.sessions.is_empty());
    }

    #[test]
    fn live_connected_reports_connected_not_reconnecting() {
        let status = build(
            None,
            &SessionRegistry::defaults(),
            "kiwi".to_string(),
            LiveState::Connected,
            &confirmed_opencode(),
            features(),
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
            &confirmed_opencode(),
            features(),
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
            &confirmed_opencode(),
            features(),
        );
        assert!(!status.connected);
        assert!(status.reconnecting);
    }
}
