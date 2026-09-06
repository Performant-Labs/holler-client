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

/// Configuration for a single local session.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SessionConfig {
    pub name: String,
    pub harness: String,
    pub command: Vec<String>,
    /// Optional interrupt signal/command for the session's harness process.
    /// Modeled as a single string (e.g. a signal name like `"SIGINT"`)
    /// rather than argv, since an interrupt is a single control action, not
    /// a program invocation.
    #[serde(default)]
    pub interrupt: Option<String>,
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
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "failed to read config file: {e}"),
            ConfigError::Parse(e) => write!(f, "failed to parse config TOML: {e}"),
            ConfigError::DuplicateSessionName(name) => {
                write!(f, "duplicate session name in config: {name}")
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(e) => Some(e),
            ConfigError::Parse(e) => Some(e),
            ConfigError::DuplicateSessionName(_) => None,
        }
    }
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
            })
        );
        assert_eq!(
            registry.get("scratch"),
            Some(&SessionConfig {
                name: "scratch".to_string(),
                harness: "stub-acp".to_string(),
                command: vec!["tests/stub-acp".to_string()],
                interrupt: Some("SIGINT".to_string()),
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
            },
            SessionConfig {
                name: "b".to_string(),
                harness: "claude".to_string(),
                command: vec!["/no/such/binary".to_string()],
                interrupt: None,
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
            },
            SessionConfig {
                name: "two".to_string(),
                harness: "opencode".to_string(),
                command: vec!["opencode".to_string(), "acp".to_string()],
                interrupt: None,
            },
        ];
        let registry = SessionRegistry::from_configs(sessions).unwrap();

        assert_eq!(registry.session_names(), vec!["one", "two"]);
    }
}
