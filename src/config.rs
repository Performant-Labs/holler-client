//! Body configuration and the local session registry.
//!
//! A body process hosts zero or more named local sessions, entirely as
//! configured — there is no default session. Each session names a harness
//! and the argv used to launch it; sessions are configured via a TOML file
//! with a top-level `[[session]]` array of tables, e.g.:
//!
//! ```toml
//! [[session]]
//! name = "alpha"
//! harness = "opencode"
//! command = ["opencode", "acp"]
//!
//! [[session]]
//! name = "beta"
//! harness = "opencode"
//! command = ["opencode", "acp"]
//! interrupt = "SIGINT"
//! ```
//!
//! "Advertise them on presence" and the `holler status` CLI listing are out
//! of scope here (later stories #24 and #23); this module only provides the
//! data types, a loader, and [`SessionRegistry::session_names`] for those
//! stories to build on.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

/// Which transport a session uses to reach its harness (issue #99, ADR-0005
/// / holler-server ADR-0017).
///
/// `Spawn` is the v1 default: this process owns and execs `command`.
/// `Attach` is additive: an already-running OpenCode (typically inside a
/// Herdr pane on the same box) is driven over its own HTTP control surface
/// instead — this process is never its parent, never execs anything for
/// it, and never calls `session/new` against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionMode {
    #[default]
    Spawn,
    Attach,
}

/// Configuration for a single local session.
///
/// `command` is required when `mode` is `Spawn` (the default) and ignored
/// when `mode` is `Attach` (never used as a spawn fallback). `endpoint` /
/// `session_id` are required when `mode` is `Attach` and unused otherwise.
/// [`SessionRegistry::from_configs`] enforces this per-mode requirement;
/// this struct's own field types stay permissive (`Option`/default-empty)
/// so a mixed registry (one spawn session + one attach session) can
/// round-trip through TOML at all — the real validation is deliberately a
/// separate, explicit step (see `validate_mode_fields`) rather than baked
/// into `serde`'s required-field checking the way `command` alone used to
/// be, since which fields are required now depends on `mode`.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SessionConfig {
    pub name: String,
    pub harness: String,
    #[serde(default)]
    pub mode: SessionMode,
    #[serde(default)]
    pub command: Vec<String>,
    /// Optional interrupt signal/command for the session's harness process.
    /// Modeled as a single string (e.g. a signal name like `"SIGINT"`)
    /// rather than argv, since an interrupt is a single control action, not
    /// a program invocation. Spawn-mode only; meaningless for attach.
    #[serde(default)]
    pub interrupt: Option<String>,
    /// Attach mode only: the base URL of the already-running OpenCode's own
    /// HTTP control surface (e.g. `http://127.0.0.1:4096`).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Attach mode only: the OpenCode session id (`ses_...`) to attach to.
    /// Required — attach never mints a new session (`session/new` /
    /// `POST /session`) on the operator's behalf; a missing id is a
    /// fail-closed config error, not a cue to create one.
    #[serde(default)]
    pub session_id: Option<String>,
}

impl SessionConfig {
    /// Whether this is an attach-mode session.
    pub fn is_attach(&self) -> bool {
        self.mode == SessionMode::Attach
    }
}

/// Top-level shape of the TOML config file: a `[[session]]` array of tables.
#[derive(Debug, Default, serde::Deserialize)]
struct BodyConfig {
    #[serde(default)]
    session: Vec<SessionConfig>,
}

/// Errors from loading or validating body config.
#[derive(Debug)]
pub enum ConfigError {
    /// The config file could not be read.
    Io(std::io::Error),
    /// The config file's TOML could not be parsed.
    Parse(toml::de::Error),
    /// Two or more sessions in the config share the same name. Fail-closed
    /// rather than silently deduplicating, since a silent drop would hide
    /// a session the caller expected to exist.
    DuplicateSessionName(String),
    /// A `mode = "spawn"` (or omitted) session has an empty `command` —
    /// there is no program to spawn (issue #99: today's existing rule,
    /// now enforced explicitly rather than via `serde`'s required-field
    /// checking, since `command` had to become optional at the struct
    /// level to let attach sessions omit it).
    SpawnMissingCommand(String),
    /// A `mode = "attach"` session has an empty/missing `endpoint`.
    AttachMissingEndpoint(String),
    /// A `mode = "attach"` session has an empty/missing `session_id`. Fail
    /// closed here rather than minting a new OpenCode session on the
    /// operator's behalf — attach never calls `session/new`.
    AttachMissingSessionId(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "failed to read config file: {e}"),
            ConfigError::Parse(e) => write!(f, "failed to parse config TOML: {e}"),
            ConfigError::DuplicateSessionName(name) => {
                write!(f, "duplicate session name in config: {name}")
            }
            ConfigError::SpawnMissingCommand(name) => {
                write!(f, "session '{name}': mode=spawn requires a non-empty command")
            }
            ConfigError::AttachMissingEndpoint(name) => {
                write!(f, "session '{name}': mode=attach requires a non-empty endpoint")
            }
            ConfigError::AttachMissingSessionId(name) => {
                write!(
                    f,
                    "session '{name}': mode=attach requires a non-empty session_id \
                     (attach never mints a new OpenCode session on your behalf)"
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(e) => Some(e),
            ConfigError::Parse(e) => Some(e),
            ConfigError::DuplicateSessionName(_)
            | ConfigError::SpawnMissingCommand(_)
            | ConfigError::AttachMissingEndpoint(_)
            | ConfigError::AttachMissingSessionId(_) => None,
        }
    }
}

/// Enforces the per-`mode` required fields (issue #99). Split out from
/// [`SessionRegistry::from_configs`] so it's independently testable and so
/// the duplicate-name check and the per-mode field check each report their
/// own precise error rather than one being masked by the other.
fn validate_mode_fields(session: &SessionConfig) -> Result<(), ConfigError> {
    match session.mode {
        SessionMode::Spawn => {
            if session.command.is_empty() {
                return Err(ConfigError::SpawnMissingCommand(session.name.clone()));
            }
        }
        SessionMode::Attach => {
            let endpoint_ok = session.endpoint.as_deref().is_some_and(|s| !s.is_empty());
            if !endpoint_ok {
                return Err(ConfigError::AttachMissingEndpoint(session.name.clone()));
            }
            let session_id_ok = session
                .session_id
                .as_deref()
                .is_some_and(|s| !s.is_empty());
            if !session_id_ok {
                return Err(ConfigError::AttachMissingSessionId(session.name.clone()));
            }
        }
    }
    Ok(())
}

/// In-memory registry of a process's local sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRegistry {
    sessions: Vec<SessionConfig>,
}

impl SessionRegistry {
    /// Builds a registry from a list of session configs, rejecting
    /// duplicate names.
    pub fn from_configs(sessions: Vec<SessionConfig>) -> Result<Self, ConfigError> {
        let mut seen = HashSet::with_capacity(sessions.len());
        for session in &sessions {
            if !seen.insert(session.name.clone()) {
                return Err(ConfigError::DuplicateSessionName(session.name.clone()));
            }
            validate_mode_fields(session)?;
        }
        Ok(SessionRegistry { sessions })
    }

    /// The empty registry: no sessions. This is what a process gets when no
    /// config file is supplied (see [`load`]) — there is no default
    /// session. `holler run` with no config maintains a live connection but
    /// drives nothing locally until an operator lists sessions explicitly
    /// in a TOML config file.
    ///
    /// A "two sessions, `<hostname>-alpha`/`<hostname>-beta`" default used
    /// to live here (issue #25's "at least two named local sessions"). It
    /// was removed: that default forced `holler run` to eagerly spawn agent
    /// subprocesses whether or not the operator wanted them, purely so the
    /// wire-harness/acceptance-gate tests (which need two concurrent
    /// sessions to prove per-session routing and interrupt isolation) had
    /// something to point at with zero setup. That fixture belongs to those
    /// tests directly now, not to the shipped default.
    fn empty() -> Self {
        SessionRegistry {
            sessions: Vec::new(),
        }
    }

    /// The names of every configured session, in configured order. A future
    /// `holler status` CLI (issue #23) is expected to call this directly.
    pub fn session_names(&self) -> Vec<&str> {
        self.sessions.iter().map(|s| s.name.as_str()).collect()
    }

    /// Looks up a session config by name.
    pub fn get(&self, name: &str) -> Option<&SessionConfig> {
        self.sessions.iter().find(|s| s.name == name)
    }

    /// All session configs, in configured order.
    pub fn sessions(&self) -> &[SessionConfig] {
        &self.sessions
    }

    /// Harness ids that are both configured and, right now, actually
    /// spawnable on this box — the "confirmed" bar (ADR-0001, holler-server
    /// ADR-0001: "harnesses it can actually drive"), not merely "configured
    /// to use". Sorted and deduped.
    pub fn confirmed_harnesses(&self) -> Vec<String> {
        let mut confirmed: Vec<String> = self
            .sessions
            .iter()
            .filter(|s| command_is_runnable(&s.command))
            .map(|s| s.harness.clone())
            .collect();
        confirmed.sort();
        confirmed.dedup();
        confirmed
    }

    /// The configured command for `harness`, rendered as a display string
    /// (e.g. `"opencode acp"`), if at least one configured session naming
    /// that harness is confirmed runnable right now. Used for `holler
    /// support`'s `how` field (`crate::query`).
    pub fn confirmed_command_for_harness(&self, harness: &str) -> Option<String> {
        self.sessions
            .iter()
            .find(|s| s.harness == harness && command_is_runnable(&s.command))
            .map(|s| s.command.join(" "))
    }
}

/// Whether `path` is a file this process could actually execute right now.
fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Resolves `program` to an executable file: a direct check if it names a
/// path (contains a path separator), otherwise a scan of `dirs` (mirroring
/// shell `$PATH` lookup; this only needs to know whether *any* directory
/// has a match, not which one wins).
fn resolve_executable<'a>(program: &str, dirs: impl Iterator<Item = &'a Path>) -> bool {
    if program.contains(std::path::MAIN_SEPARATOR) {
        return is_executable_file(Path::new(program));
    }
    dirs.map(|dir| dir.join(program))
        .any(|candidate| is_executable_file(&candidate))
}

/// Whether `command`'s program (`command[0]`) is actually spawnable on this
/// box right now, via `$PATH`. An empty command is never runnable. This is
/// a real filesystem/PATH check — deliberately *not* whether the harness is
/// merely present in [`SessionRegistry`]'s config, per ADR-0001's "known vs
/// confirmed" distinction.
pub fn command_is_runnable(command: &[String]) -> bool {
    let Some(program) = command.first() else {
        return false;
    };
    let path_dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    resolve_executable(program, path_dirs.iter().map(PathBuf::as_path))
}

/// Parses body config from a TOML string and builds a [`SessionRegistry`].
pub fn load_from_str(toml_str: &str) -> Result<SessionRegistry, ConfigError> {
    let body: BodyConfig = toml::from_str(toml_str).map_err(ConfigError::Parse)?;
    SessionRegistry::from_configs(body.session)
}

/// Loads body config from an optional file path.
///
/// `None` means no config file was supplied by the caller (there is no
/// implicit conventional-location search); this yields
/// [`SessionRegistry::empty`] — no sessions. `Some(path)` reads and parses
/// that file, failing closed on I/O errors, parse errors, or duplicate
/// session names.
pub fn load(path: Option<&Path>) -> Result<SessionRegistry, ConfigError> {
    match path {
        Some(path) => {
            let contents = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
            load_from_str(&contents)
        }
        None => Ok(SessionRegistry::empty()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_custom_sessions_from_toml() {
        let toml_str = r#"
            [[session]]
            name = "work"
            harness = "opencode"
            command = ["opencode", "acp"]

            [[session]]
            name = "scratch"
            harness = "stub-acp"
            command = ["tests/stub-acp"]
            interrupt = "SIGINT"
        "#;

        let registry = load_from_str(toml_str).expect("valid config should parse");

        assert_eq!(registry.session_names(), vec!["work", "scratch"]);
        assert_eq!(
            registry.get("work"),
            Some(&SessionConfig {
                name: "work".to_string(),
                harness: "opencode".to_string(),
                command: vec!["opencode".to_string(), "acp".to_string()],
                interrupt: None,
                ..Default::default()
            })
        );
        assert_eq!(
            registry.get("scratch"),
            Some(&SessionConfig {
                name: "scratch".to_string(),
                harness: "stub-acp".to_string(),
                command: vec!["tests/stub-acp".to_string()],
                interrupt: Some("SIGINT".to_string()),
                ..Default::default()
            })
        );
    }

    #[test]
    fn no_config_supplied_yields_empty_registry() {
        let registry = load(None).expect("default load never fails");

        assert!(registry.session_names().is_empty());
    }

    #[test]
    fn duplicate_session_names_are_rejected() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            command = ["opencode", "acp"]

            [[session]]
            name = "alpha"
            harness = "opencode"
            command = ["opencode", "acp"]
        "#;

        let err = load_from_str(toml_str).expect_err("duplicate names must fail closed");
        match err {
            ConfigError::DuplicateSessionName(name) => assert_eq!(name, "alpha"),
            other => panic!("expected DuplicateSessionName, got {other:?}"),
        }
    }

    #[test]
    fn command_is_runnable_true_for_real_absolute_executable() {
        // A direct path bypasses PATH scanning entirely, so this is
        // deterministic regardless of the test host's $PATH.
        assert!(command_is_runnable(&["/bin/sh".to_string()]));
    }

    #[test]
    fn command_is_runnable_false_for_missing_absolute_path() {
        assert!(!command_is_runnable(&[
            "/no/such/executable/here".to_string()
        ]));
    }

    #[test]
    fn command_is_runnable_false_for_empty_command() {
        assert!(!command_is_runnable(&[]));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_executable_finds_executable_file_in_dirs() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("fake-harness");
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").unwrap();
        let mut perms = std::fs::metadata(&exe).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&exe, perms).unwrap();

        assert!(resolve_executable(
            "fake-harness",
            std::iter::once(dir.path())
        ));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_executable_false_for_non_executable_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("not-executable"), "no shebang, no bits").unwrap();

        assert!(!resolve_executable(
            "not-executable",
            std::iter::once(dir.path())
        ));
    }

    #[test]
    fn resolve_executable_false_when_absent_from_every_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!resolve_executable("nope", std::iter::once(dir.path())));
    }

    #[test]
    fn confirmed_harnesses_only_includes_runnable_ones() {
        let registry = SessionRegistry::from_configs(vec![
            SessionConfig {
                name: "a".to_string(),
                harness: "opencode".to_string(),
                command: vec!["/bin/sh".to_string()],
                interrupt: None,
                ..Default::default()
            },
            SessionConfig {
                name: "b".to_string(),
                harness: "claude".to_string(),
                command: vec!["/no/such/binary".to_string()],
                interrupt: None,
                ..Default::default()
            },
        ])
        .unwrap();
        assert_eq!(registry.confirmed_harnesses(), vec!["opencode".to_string()]);
    }

    #[test]
    fn confirmed_command_for_harness_returns_display_string_or_none() {
        let registry = SessionRegistry::from_configs(vec![SessionConfig {
            name: "a".to_string(),
            harness: "opencode".to_string(),
            command: vec!["/bin/sh".to_string(), "-c".to_string()],
            interrupt: None,
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(
            registry.confirmed_command_for_harness("opencode"),
            Some("/bin/sh -c".to_string())
        );
        assert_eq!(registry.confirmed_command_for_harness("claude"), None);
    }

    #[test]
    fn session_names_returns_exact_configured_names() {
        let sessions = vec![
            SessionConfig {
                name: "one".to_string(),
                harness: "opencode".to_string(),
                command: vec!["opencode".to_string(), "acp".to_string()],
                interrupt: None,
                ..Default::default()
            },
            SessionConfig {
                name: "two".to_string(),
                harness: "opencode".to_string(),
                command: vec!["opencode".to_string(), "acp".to_string()],
                interrupt: None,
                ..Default::default()
            },
        ];
        let registry = SessionRegistry::from_configs(sessions).unwrap();

        assert_eq!(registry.session_names(), vec!["one", "two"]);
    }

    // --- Attach mode (issue #99) ---------------------------------------

    #[test]
    fn attach_config_parses_from_toml() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            endpoint = "http://127.0.0.1:4096"
            session_id = "ses_abc123"
        "#;

        let registry = load_from_str(toml_str).expect("valid attach config should parse");
        let session = registry.get("alpha").expect("session present");
        assert!(session.is_attach());
        assert_eq!(session.endpoint.as_deref(), Some("http://127.0.0.1:4096"));
        assert_eq!(session.session_id.as_deref(), Some("ses_abc123"));
        assert!(session.command.is_empty());
    }

    #[test]
    fn attach_missing_endpoint_fails_closed() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            session_id = "ses_abc123"
        "#;

        let err = load_from_str(toml_str).expect_err("missing endpoint must fail closed");
        match err {
            ConfigError::AttachMissingEndpoint(name) => assert_eq!(name, "alpha"),
            other => panic!("expected AttachMissingEndpoint, got {other:?}"),
        }
    }

    #[test]
    fn attach_empty_endpoint_fails_closed() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            endpoint = ""
            session_id = "ses_abc123"
        "#;

        let err = load_from_str(toml_str).expect_err("empty endpoint must fail closed");
        assert!(matches!(err, ConfigError::AttachMissingEndpoint(_)));
    }

    #[test]
    fn attach_missing_session_id_fails_closed() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            endpoint = "http://127.0.0.1:4096"
        "#;

        let err = load_from_str(toml_str).expect_err("missing session_id must fail closed");
        match err {
            ConfigError::AttachMissingSessionId(name) => assert_eq!(name, "alpha"),
            other => panic!("expected AttachMissingSessionId, got {other:?}"),
        }
    }

    #[test]
    fn attach_with_command_present_is_fine_but_ignored() {
        // `command` must never be required for attach, but it also must not
        // be an error if present (e.g. a config edited from a spawn entry) —
        // it's simply ignored, never used as a spawn fallback.
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            command = ["opencode", "acp"]
            endpoint = "http://127.0.0.1:4096"
            session_id = "ses_abc123"
        "#;

        let registry = load_from_str(toml_str).expect("command present must not error");
        assert!(registry.get("alpha").unwrap().is_attach());
    }

    #[test]
    fn spawn_missing_command_fails_closed() {
        // Explicit regression check: `command` becoming `Option`-shaped at
        // the struct level (to let attach configs omit it) must not weaken
        // this existing rule for spawn mode.
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
        "#;

        let err = load_from_str(toml_str).expect_err("spawn with no command must fail closed");
        match err {
            ConfigError::SpawnMissingCommand(name) => assert_eq!(name, "alpha"),
            other => panic!("expected SpawnMissingCommand, got {other:?}"),
        }
    }

    #[test]
    fn explicit_mode_spawn_still_requires_command() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "spawn"
        "#;

        let err = load_from_str(toml_str).expect_err("explicit spawn with no command must fail");
        assert!(matches!(err, ConfigError::SpawnMissingCommand(_)));
    }

    #[test]
    fn mixed_spawn_and_attach_registry_is_allowed() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            command = ["opencode", "acp"]

            [[session]]
            name = "beta"
            harness = "opencode"
            mode = "attach"
            endpoint = "http://127.0.0.1:4096"
            session_id = "ses_xyz"
        "#;

        let registry = load_from_str(toml_str).expect("mixed registry should parse");
        assert!(!registry.get("alpha").unwrap().is_attach());
        assert!(registry.get("beta").unwrap().is_attach());
        assert_eq!(registry.session_names(), vec!["alpha", "beta"]);
    }

    #[test]
    fn duplicate_names_rejected_even_across_mode_types() {
        let toml_str = r#"
            [[session]]
            name = "alpha"
            harness = "opencode"
            command = ["opencode", "acp"]

            [[session]]
            name = "alpha"
            harness = "opencode"
            mode = "attach"
            endpoint = "http://127.0.0.1:4096"
            session_id = "ses_xyz"
        "#;

        let err = load_from_str(toml_str).expect_err("duplicate name must fail closed");
        assert!(matches!(err, ConfigError::DuplicateSessionName(_)));
    }

    #[test]
    fn attach_session_is_never_confirmed_by_path_lookup() {
        // Attach sessions have an empty `command`, so today's PATH-based
        // `command_is_runnable` correctly (if incidentally) already treats
        // them as unconfirmed -- confirmation-by-HTTP-probe is story
        // #100/#102's job, not this one's. This test pins that this story
        // does not accidentally "fix" that by lying that attach is
        // confirmed just because e.g. `/bin/sh` happens to be on PATH.
        let registry = SessionRegistry::from_configs(vec![SessionConfig {
            name: "alpha".to_string(),
            harness: "opencode".to_string(),
            mode: SessionMode::Attach,
            endpoint: Some("http://127.0.0.1:4096".to_string()),
            session_id: Some("ses_abc".to_string()),
            ..Default::default()
        }])
        .unwrap();

        assert!(registry.confirmed_harnesses().is_empty());
        assert_eq!(registry.confirmed_command_for_harness("opencode"), None);
    }
}
