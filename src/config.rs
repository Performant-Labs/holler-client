//! Body configuration and the local session registry.
//!
//! A body process hosts at least two named local sessions (issue #25). Each
//! session names a harness and the argv used to launch it; sessions are
//! configured via a TOML file with a top-level `[[session]]` array of
//! tables, e.g.:
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
use std::path::Path;

/// v1 default harness and command, per issue #25 ("v1 default `command` is
/// `opencode acp`").
const DEFAULT_HARNESS: &str = "opencode";
fn default_command() -> Vec<String> {
    vec!["opencode".to_string(), "acp".to_string()]
}

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

impl SessionConfig {
    fn default_named(name: &str) -> Self {
        SessionConfig {
            name: name.to_string(),
            harness: DEFAULT_HARNESS.to_string(),
            command: default_command(),
            interrupt: None,
        }
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

    /// The built-in default registry: two sessions, `alpha` and `beta`,
    /// both using the v1 default harness and command. Used when no config
    /// file is supplied, so "at least two named local sessions" holds with
    /// zero configuration.
    pub fn defaults() -> Self {
        SessionRegistry {
            sessions: vec![
                SessionConfig::default_named("alpha"),
                SessionConfig::default_named("beta"),
            ],
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
/// [`SessionRegistry::defaults`]. `Some(path)` reads and parses that file,
/// failing closed on I/O errors, parse errors, or duplicate session names.
pub fn load(path: Option<&Path>) -> Result<SessionRegistry, ConfigError> {
    match path {
        Some(path) => {
            let contents = std::fs::read_to_string(path).map_err(ConfigError::Io)?;
            load_from_str(&contents)
        }
        None => Ok(SessionRegistry::defaults()),
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
    fn no_config_supplied_falls_back_to_defaults() {
        let registry = load(None).expect("default load never fails");

        assert_eq!(registry.session_names(), vec!["alpha", "beta"]);
        for name in ["alpha", "beta"] {
            let session = registry.get(name).unwrap();
            assert_eq!(session.harness, "opencode");
            assert_eq!(session.command, vec!["opencode", "acp"]);
            assert_eq!(session.interrupt, None);
        }
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
