//! Local credential persistence for `holler join` / `detach` / `status`
//! (issue #23).
//!
//! Stores exactly what is needed to reconnect: `client_id`, the client
//! credential, the server address joined, and the hostname sent at join.
//! **Never** the one-time join token — it is redeemed and discarded by
//! [`crate::join`] before this module ever sees it.
//!
//! Location: `$HOLLER_STATE_DIR/credential.json` if `HOLLER_STATE_DIR` is
//! set, else `~/.holler/credential.json`. This deliberately mirrors
//! holler-server's `HOLLER_STATE_DIR` variable name for a consistent
//! operator mental model, but not its default: the server defaults to a
//! cwd-relative directory because it's normally run from one fixed
//! operational directory, while this CLI is invoked interactively from
//! wherever the user happens to be, so a cwd-relative default would make
//! `join` in one shell invisible to `status` in another.

use std::fmt;
use std::fs;
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Shared with [`crate::connection`]'s state files (`connection_state.json`,
/// `detach_request`), which live in this same directory alongside
/// `credential.json`.
pub(crate) const STATE_DIR_ENV: &str = "HOLLER_STATE_DIR";
const DEFAULT_STATE_SUBDIR: &str = ".holler";
const CREDENTIAL_FILE: &str = "credential.json";

/// Resolves `$HOLLER_STATE_DIR`, or `$HOME/.holler` if unset. Shared by
/// [`CredentialStore::open`] and [`crate::connection::ConnectionStateStore::open`]
/// so both stores agree on one directory without duplicating this logic.
pub(crate) fn resolve_state_dir(
    state_dir_env: Option<String>,
    home_env: Option<String>,
) -> Result<PathBuf, CredentialError> {
    match state_dir_env {
        Some(dir) => Ok(PathBuf::from(dir)),
        None => {
            let home = home_env.ok_or(CredentialError::NoStateDir)?;
            Ok(PathBuf::from(home).join(DEFAULT_STATE_SUBDIR))
        }
    }
}

/// What's persisted after a successful join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedCredential {
    pub client_id: String,
    /// The join token's public `token_id` — what the client must send as
    /// `from` on `auth` (and every subsequent envelope on that
    /// connection), per `docs/protocol/v1.md` §4/§6. Distinct from
    /// `client_id`, which the server never authenticates against.
    pub token_id: String,
    pub credential: String,
    /// Canonical `scheme://host:port` of the server joined, so a future
    /// reconnect (issue #24) knows where to dial without re-parsing
    /// whatever the user originally typed.
    pub server: String,
    pub hostname: String,
}

/// Failures reading, writing, or locating the credential store.
#[derive(Debug)]
pub enum CredentialError {
    /// Neither `HOLLER_STATE_DIR` nor `HOME` is set, so no default
    /// location can be determined. Fails closed rather than guessing a
    /// path (e.g. the current directory).
    NoStateDir,
    Io(io::Error),
    Serde(serde_json::Error),
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CredentialError::NoStateDir => write!(
                f,
                "cannot determine credential state directory: set {STATE_DIR_ENV} or HOME"
            ),
            CredentialError::Io(e) => write!(f, "credential store I/O error: {e}"),
            CredentialError::Serde(e) => write!(f, "credential store is corrupt: {e}"),
        }
    }
}

impl std::error::Error for CredentialError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CredentialError::Io(e) => Some(e),
            CredentialError::Serde(e) => Some(e),
            CredentialError::NoStateDir => None,
        }
    }
}

impl From<io::Error> for CredentialError {
    fn from(e: io::Error) -> Self {
        CredentialError::Io(e)
    }
}

impl From<serde_json::Error> for CredentialError {
    fn from(e: serde_json::Error) -> Self {
        CredentialError::Serde(e)
    }
}

/// The credential store: a single JSON file at a resolved path.
#[derive(Debug)]
pub struct CredentialStore {
    path: PathBuf,
}

impl CredentialStore {
    /// Opens the store at `HOLLER_STATE_DIR/credential.json`, or
    /// `~/.holler/credential.json` if that variable is unset.
    pub fn open() -> Result<Self, CredentialError> {
        Self::open_from_env(std::env::var(STATE_DIR_ENV).ok(), std::env::var("HOME").ok())
    }

    /// Testable variant of [`Self::open`] that takes explicit env values
    /// instead of reading the process environment.
    fn open_from_env(
        state_dir_env: Option<String>,
        home_env: Option<String>,
    ) -> Result<Self, CredentialError> {
        let dir = resolve_state_dir(state_dir_env, home_env)?;
        Ok(CredentialStore {
            path: dir.join(CREDENTIAL_FILE),
        })
    }

    /// Opens a store at an explicit path, bypassing environment
    /// resolution. Used by tests to get an isolated, temp-dir-backed store.
    #[cfg(test)]
    pub fn at_path(path: PathBuf) -> Self {
        CredentialStore { path }
    }

    /// Loads the persisted credential, if any. `Ok(None)` means "not
    /// joined" (no file present yet, or it was deleted by `detach`).
    pub fn load(&self) -> Result<Option<PersistedCredential>, CredentialError> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => Ok(Some(serde_json::from_str(&contents)?)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persists a credential, overwriting any previous one (a re-join
    /// replaces the prior identity outright rather than merging with it).
    pub fn save(&self, credential: &PersistedCredential) -> Result<(), CredentialError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let contents = serde_json::to_string_pretty(credential)?;
        fs::write(&self.path, contents)?;
        Ok(())
    }

    /// Deletes the persisted credential. Idempotent: deleting when nothing
    /// is persisted is not an error, since `detach`'s job ("make sure no
    /// credential is left behind") is already satisfied in that case.
    pub fn delete(&self) -> Result<(), CredentialError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PersistedCredential {
        PersistedCredential {
            client_id: "cli_abc123".to_string(),
            token_id: "tok_7f3a".to_string(),
            credential: "hlr_live_secretvalue".to_string(),
            server: "ws://example.com:41807".to_string(),
            hostname: "kiwi".to_string(),
        }
    }

    #[test]
    fn load_on_empty_store_is_not_joined() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("credential.json"));
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("credential.json"));
        store.save(&sample()).unwrap();
        assert_eq!(store.load().unwrap(), Some(sample()));
    }

    #[test]
    fn save_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("nested").join("credential.json"));
        store.save(&sample()).unwrap();
        assert_eq!(store.load().unwrap(), Some(sample()));
    }

    #[test]
    fn save_overwrites_previous_credential() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("credential.json"));
        store.save(&sample()).unwrap();

        let mut second = sample();
        second.client_id = "cli_different".to_string();
        store.save(&second).unwrap();

        assert_eq!(store.load().unwrap(), Some(second));
    }

    #[test]
    fn delete_removes_persisted_credential() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("credential.json"));
        store.save(&sample()).unwrap();
        store.delete().unwrap();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn delete_on_empty_store_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::at_path(dir.path().join("credential.json"));
        assert!(store.delete().is_ok());
    }

    #[test]
    fn open_uses_state_dir_env_when_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open_from_env(
            Some(dir.path().to_string_lossy().to_string()),
            None,
        )
        .unwrap();
        store.save(&sample()).unwrap();
        assert!(dir.path().join("credential.json").exists());
    }

    #[test]
    fn open_falls_back_to_home_dot_holler_when_state_dir_unset() {
        let home = tempfile::tempdir().unwrap();
        let store =
            CredentialStore::open_from_env(None, Some(home.path().to_string_lossy().to_string()))
                .unwrap();
        store.save(&sample()).unwrap();
        assert!(home.path().join(".holler").join("credential.json").exists());
    }

    #[test]
    fn open_fails_closed_with_neither_env_set() {
        let err = CredentialStore::open_from_env(None, None).unwrap_err();
        assert!(matches!(err, CredentialError::NoStateDir));
    }

    #[test]
    fn persisted_credential_never_contains_the_one_time_join_secret() {
        // Structural guard: PersistedCredential has no field for the
        // one-time join secret (the `<secret>` half of `--token
        // <token_id>:<secret>`) — only the public `token_id`, which is
        // safe and necessary to persist (issue #47) for `auth`'s `from`.
        // Serializing `sample()` (whose `credential` deliberately looks
        // like a secret, and whose `token_id` deliberately looks like a
        // real one) and checking for the join-secret's `hlr_join_` prefix
        // and a `join_token`/`secret` key ages this test well even if
        // field values change later.
        let json = serde_json::to_string(&sample()).unwrap();
        assert!(!json.contains("join_token"));
        assert!(!json.contains("\"secret\""));
        assert!(!json.contains("hlr_join_"));
    }
}
