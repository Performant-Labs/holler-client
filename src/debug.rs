//! Debug levels: `none` / `quiet` / `noisy`, with secret redaction.
//!
//! Same names and redaction rules as the server side (see holler-server's
//! debug-levels story and ADR 0010): tokens are never printed in the clear.
//!
//! Precedence for choosing a level: an explicit `--debug=` flag wins over
//! `HOLLER_DEBUG`; if neither is set, the default is [`DebugLevel::None`].
//! An invalid value at whichever precedence level wins is an error — it
//! never silently falls back to [`DebugLevel::Noisy`] or the default.

use std::fmt;

/// The three supported debug verbosity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DebugLevel {
    /// No debug statements at all.
    #[default]
    None,
    /// High-level only: direction, `type`, session/query identifiers,
    /// server URL host, `client_id`, correlation id. No bodies.
    Quiet,
    /// Full handshakes and frames, one JSON object per line, with secrets
    /// redacted per [`redact`] / [`redact_secret`].
    Noisy,
}

impl fmt::Display for DebugLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DebugLevel::None => "none",
            DebugLevel::Quiet => "quiet",
            DebugLevel::Noisy => "noisy",
        })
    }
}

/// An invalid debug level string was supplied, either via the `--debug=`
/// flag or the `HOLLER_DEBUG` environment variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugLevelError {
    /// Which source supplied the invalid value.
    pub source: DebugLevelSource,
    /// The offending value, verbatim.
    pub value: String,
}

/// Where an invalid debug level value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugLevelSource {
    /// The `--debug=` command-line flag.
    Flag,
    /// The `HOLLER_DEBUG` environment variable.
    Env,
}

impl fmt::Display for DebugLevelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match self.source {
            DebugLevelSource::Flag => "--debug",
            DebugLevelSource::Env => "HOLLER_DEBUG",
        };
        write!(
            f,
            "invalid debug level {:?} from {} (expected one of: none, quiet, noisy)",
            self.value, source
        )
    }
}

impl std::error::Error for DebugLevelError {}

impl std::str::FromStr for DebugLevel {
    type Err = ();

    /// Parses exactly `none`/`quiet`/`noisy`, case-insensitively.
    ///
    /// Case-insensitivity is a convenience for shell/env use
    /// (`HOLLER_DEBUG=NOISY`) and doesn't weaken the fail-closed contract:
    /// anything that isn't one of the three names is still rejected.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(DebugLevel::None),
            "quiet" => Ok(DebugLevel::Quiet),
            "noisy" => Ok(DebugLevel::Noisy),
            _ => Err(()),
        }
    }
}

impl DebugLevel {
    /// Resolves the effective debug level from an optional flag value and
    /// an optional environment value.
    ///
    /// Precedence: `flag`, if present, wins outright — even if it's
    /// invalid, in which case this returns an error rather than
    /// consulting `env`. If `flag` is absent, `env` is used the same way.
    /// If neither is set, the default ([`DebugLevel::None`]) applies.
    pub fn resolve(
        flag: Option<&str>,
        env: Option<&str>,
    ) -> Result<DebugLevel, DebugLevelError> {
        if let Some(value) = flag {
            return value.parse().map_err(|_| DebugLevelError {
                source: DebugLevelSource::Flag,
                value: value.to_string(),
            });
        }
        if let Some(value) = env {
            return value.parse().map_err(|_| DebugLevelError {
                source: DebugLevelSource::Env,
                value: value.to_string(),
            });
        }
        Ok(DebugLevel::default())
    }
}

/// Field names that must never be printed in the clear, at any debug
/// level. Matched case-insensitively against the field/key name.
const NEVER_PRINT_FIELDS: &[&str] = &[
    "join_token",
    "jointoken",
    "join token",
    "client_credential",
    "clientcredential",
    "client credential",
    "connect_ticket",
    "connectticket",
    "connect ticket",
    "authorization",
];

/// The literal string substituted for a redacted value.
pub const REDACTED: &str = "[redacted]";

/// Redacts a named field's value for logging.
///
/// If `field_name` matches one of the never-print fields (join token,
/// client credential, connect ticket, `Authorization`) case-insensitively,
/// returns [`REDACTED`] regardless of `value`. Otherwise returns `value`
/// unchanged — this allows fields like `token_id`, `client_id`, hostnames,
/// and session names to pass through.
pub fn redact(field_name: &str, value: &str) -> String {
    let normalized = field_name.to_ascii_lowercase();
    if NEVER_PRINT_FIELDS.contains(&normalized.as_str()) {
        REDACTED.to_string()
    } else {
        value.to_string()
    }
}

/// Scrubs every occurrence of a known secret substring out of arbitrary
/// text before it is logged.
///
/// Use this for values that won't arrive as a named field — e.g. a
/// `--token` command-line argument or the contents of a persisted
/// credential file — where [`redact`]'s field-name matching doesn't apply.
/// Returns `text` unchanged if `secret` is empty, to avoid pathological
/// behavior on an empty needle.
pub fn redact_secret(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        return text.to_string();
    }
    text.replace(secret, REDACTED)
}

/// Emits one debug line for a wire event, gated by `level`.
///
/// `quiet_line()` is used for [`DebugLevel::Quiet`] — a high-level
/// summary only (direction, envelope `type`, session/query identifiers,
/// no bodies), per the issue's own example (`DEBUG quiet -> prompt
/// session=alpha id=01J… from=server`). `noisy_line()` is used for
/// [`DebugLevel::Noisy`] — the caller is responsible for having already
/// redacted any secret out of it (see [`redact`]/[`redact_secret`])
/// before this prints it verbatim. Nothing is printed, and neither
/// closure runs, at [`DebugLevel::None`] — so building the (potentially
/// expensive) noisy line never costs anything when debug is off.
pub fn log(level: DebugLevel, quiet_line: impl FnOnce() -> String, noisy_line: impl FnOnce() -> String) {
    match level {
        DebugLevel::None => {}
        DebugLevel::Quiet => eprintln!("DEBUG quiet {}", quiet_line()),
        DebugLevel::Noisy => eprintln!("DEBUG noisy {}", noisy_line()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_wins_over_env_when_both_valid() {
        let result = DebugLevel::resolve(Some("quiet"), Some("noisy"));
        assert_eq!(result, Ok(DebugLevel::Quiet));
    }

    #[test]
    fn env_used_when_flag_absent() {
        let result = DebugLevel::resolve(None, Some("noisy"));
        assert_eq!(result, Ok(DebugLevel::Noisy));
    }

    #[test]
    fn default_none_when_neither_set() {
        let result = DebugLevel::resolve(None, None);
        assert_eq!(result, Ok(DebugLevel::None));
    }

    #[test]
    fn invalid_flag_errors_and_does_not_fall_through() {
        let result = DebugLevel::resolve(Some("bogus"), Some("noisy"));
        assert_eq!(
            result,
            Err(DebugLevelError {
                source: DebugLevelSource::Flag,
                value: "bogus".to_string(),
            })
        );
    }

    #[test]
    fn invalid_env_errors_when_flag_absent() {
        let result = DebugLevel::resolve(None, Some("bogus"));
        assert_eq!(
            result,
            Err(DebugLevelError {
                source: DebugLevelSource::Env,
                value: "bogus".to_string(),
            })
        );
    }

    #[test]
    fn case_insensitive_parsing() {
        assert_eq!(DebugLevel::resolve(Some("NOISY"), None), Ok(DebugLevel::Noisy));
        assert_eq!(DebugLevel::resolve(Some("Quiet"), None), Ok(DebugLevel::Quiet));
    }

    #[test]
    fn redact_join_token() {
        assert_eq!(redact("join_token", "supersecret"), REDACTED);
        assert_eq!(redact("Join_Token", "supersecret"), REDACTED);
    }

    #[test]
    fn redact_client_credential() {
        assert_eq!(redact("client_credential", "supersecret"), REDACTED);
    }

    #[test]
    fn redact_connect_ticket() {
        assert_eq!(redact("connect_ticket", "supersecret"), REDACTED);
    }

    #[test]
    fn redact_authorization() {
        assert_eq!(redact("Authorization", "Bearer abc123"), REDACTED);
        assert_eq!(redact("authorization", "Bearer abc123"), REDACTED);
    }

    #[test]
    fn passes_through_allowed_fields() {
        assert_eq!(redact("token_id", "tok_123"), "tok_123");
        assert_eq!(redact("client_id", "cli_456"), "cli_456");
        assert_eq!(redact("hostname", "example.com"), "example.com");
        assert_eq!(redact("session_name", "my-session"), "my-session");
    }

    #[test]
    fn redact_secret_scrubs_known_substring() {
        let text = "connecting with token=abc123secret to server";
        let scrubbed = redact_secret(text, "abc123secret");
        assert_eq!(scrubbed, "connecting with token=[redacted] to server");
        assert!(!scrubbed.contains("abc123secret"));
    }

    #[test]
    fn redact_secret_handles_multiple_occurrences() {
        let text = "abc123secret and again abc123secret";
        let scrubbed = redact_secret(text, "abc123secret");
        assert_eq!(scrubbed, "[redacted] and again [redacted]");
    }

    #[test]
    fn redact_secret_no_op_on_empty_needle() {
        let text = "unchanged text";
        assert_eq!(redact_secret(text, ""), text);
    }
}
