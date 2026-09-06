//! Debug levels and output formats, with secret redaction.
//!
//! Two independent axes:
//!
//! - **Level** ([`DebugLevel`]): `none` / `quiet` / `noisy` — *how much* is
//!   logged. Same names and redaction rules as the server side (see
//!   holler-server's debug-levels story and ADR 0010): tokens are never
//!   printed in the clear.
//! - **Format** ([`LogFormat`]): `text` / `json` — *how it is shaped*.
//!   `text` is a fixed-width console line for a human tailing a session;
//!   `json` is JSON Lines for a log analyzer.
//!
//! Precedence for both: an explicit flag (`--debug=` / `--log-format=`)
//! wins over the environment (`HOLLER_DEBUG` / `HOLLER_LOG_FORMAT`); if
//! neither is set the default applies ([`DebugLevel::None`],
//! [`LogFormat::Text`]). An invalid value at whichever precedence level
//! wins is an error — it never silently falls back to a default.
//!
//! # Emission timestamp vs. frame `ts`
//!
//! Every line carries an **emission** timestamp: when *this* process
//! logged the line, from *this* host's clock. That is deliberately not
//! the frame's own `ts` field, which is the peer's claim from the peer's
//! clock. The two diverge in practice — a measured cross-machine session
//! showed ~180ms of skew, enough that sorting a handshake by frame `ts`
//! reordered it against causality — so only the emission timestamp is
//! safe to sort a log by. The frame `ts` is still present inside the
//! frame body at `noisy`, as protocol data.

use std::fmt;

use serde::Serialize;
use time::OffsetDateTime;

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

/// How each debug line is shaped on the wire to stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// Fixed-width console line, emission timestamp first. At
    /// [`DebugLevel::Noisy`] the redacted frame JSON is appended last, so
    /// a line's frame can still be copied out and replayed.
    #[default]
    Text,
    /// JSON Lines: the whole line is one JSON object, so `jq`/Vector/Loki
    /// can ingest the stream directly. The frame is nested under `frame`.
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        })
    }
}

/// Where an invalid configuration value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugLevelSource {
    /// A command-line flag (`--debug=` / `--log-format=`).
    Flag,
    /// An environment variable (`HOLLER_DEBUG` / `HOLLER_LOG_FORMAT`).
    Env,
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

/// An invalid log format string was supplied, either via the
/// `--log-format=` flag or the `HOLLER_LOG_FORMAT` environment variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFormatError {
    /// Which source supplied the invalid value.
    pub source: DebugLevelSource,
    /// The offending value, verbatim.
    pub value: String,
}

impl fmt::Display for LogFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match self.source {
            DebugLevelSource::Flag => "--log-format",
            DebugLevelSource::Env => "HOLLER_LOG_FORMAT",
        };
        write!(
            f,
            "invalid log format {:?} from {} (expected one of: text, json)",
            self.value, source
        )
    }
}

impl std::error::Error for LogFormatError {}

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

impl std::str::FromStr for LogFormat {
    type Err = ();

    /// Parses exactly `text`/`json`, case-insensitively — same fail-closed
    /// contract as [`DebugLevel`]'s.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "text" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
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
    pub fn resolve(flag: Option<&str>, env: Option<&str>) -> Result<DebugLevel, DebugLevelError> {
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

impl LogFormat {
    /// Resolves the effective log format. Same precedence and fail-closed
    /// contract as [`DebugLevel::resolve`].
    pub fn resolve(flag: Option<&str>, env: Option<&str>) -> Result<LogFormat, LogFormatError> {
        if let Some(value) = flag {
            return value.parse().map_err(|_| LogFormatError {
                source: DebugLevelSource::Flag,
                value: value.to_string(),
            });
        }
        if let Some(value) = env {
            return value.parse().map_err(|_| LogFormatError {
                source: DebugLevelSource::Env,
                value: value.to_string(),
            });
        }
        Ok(LogFormat::default())
    }
}

/// The resolved logging configuration: how much, and in what shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DebugConfig {
    pub level: DebugLevel,
    pub format: LogFormat,
}

impl DebugConfig {
    pub fn new(level: DebugLevel, format: LogFormat) -> Self {
        DebugConfig { level, format }
    }

    /// Whether anything at all is logged. Call sites can use this to skip
    /// work that even building an [`Event`] would cost.
    pub fn is_on(&self) -> bool {
        self.level != DebugLevel::None
    }
}

/// Which way a frame moved, relative to this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Sent by this process.
    Out,
    /// Received by this process.
    In,
    /// Not a frame at all — a local lifecycle event (connected, dropped,
    /// detached). Rendered with a blank direction column so the rest of
    /// the columns still line up.
    Local,
}

impl Direction {
    fn as_text(self) -> &'static str {
        match self {
            Direction::Out => "->",
            Direction::In => "<-",
            Direction::Local => "  ",
        }
    }

    fn as_json(self) -> Option<&'static str> {
        match self {
            Direction::Out => Some("out"),
            Direction::In => Some("in"),
            Direction::Local => None,
        }
    }
}

/// Width the `type` column is padded to in [`LogFormat::Text`]. The
/// longest v1 frame type is `interrupt` (9); one space of slack keeps a
/// gap before the first `k=v` pair.
const TYPE_COLUMN_WIDTH: usize = 10;

/// How many leading characters of an id/peer survive into a `text` line.
/// Enough to stay distinctive past a `cli_`/`tok_` prefix while keeping
/// the column narrow; `json` always carries the untruncated value.
const SHORT_ID_LEN: usize = 12;

fn short(value: &str) -> &str {
    match value.char_indices().nth(SHORT_ID_LEN) {
        Some((byte_idx, _)) => &value[..byte_idx],
        None => value,
    }
}

/// RFC 3339 UTC at fixed microsecond precision, so the timestamp column
/// is genuinely fixed-width (27 chars) and a parser never has to handle
/// variable fractional digits. Formatted by hand rather than via a
/// `time` format description so no extra crate feature is needed.
fn emission_ts() -> String {
    let now = OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.microsecond(),
    )
}

#[derive(Serialize)]
struct JsonLine<'a> {
    ts: String,
    level: &'static str,
    verbosity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dir: Option<&'static str>,
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer: Option<&'a str>,
    #[serde(flatten)]
    fields: std::collections::BTreeMap<&'static str, &'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frame: Option<serde_json::Value>,
}

/// One debug line, built up field by field and rendered by [`Event::emit`]
/// in whichever [`LogFormat`] is configured.
///
/// Construct with [`outgoing`], [`incoming`], or [`local`]. When the
/// configured level is [`DebugLevel::None`] every method is a no-op and
/// [`Event::frame`]'s closure is never called, so a disabled logger costs
/// nothing beyond the (stack-only) builder itself.
pub struct Event<'a> {
    cfg: DebugConfig,
    dir: Direction,
    kind: &'a str,
    id: Option<&'a str>,
    peer: Option<&'a str>,
    fields: Vec<(&'static str, String)>,
    frame: Option<String>,
}

/// A frame this process is sending.
pub fn outgoing(cfg: DebugConfig, kind: &str) -> Event<'_> {
    Event::new(cfg, Direction::Out, kind)
}

/// A frame this process just received.
pub fn incoming(cfg: DebugConfig, kind: &str) -> Event<'_> {
    Event::new(cfg, Direction::In, kind)
}

/// A local lifecycle event, not a frame.
pub fn local(cfg: DebugConfig, kind: &str) -> Event<'_> {
    Event::new(cfg, Direction::Local, kind)
}

impl<'a> Event<'a> {
    fn new(cfg: DebugConfig, dir: Direction, kind: &'a str) -> Self {
        Event {
            cfg,
            dir,
            kind,
            id: None,
            peer: None,
            fields: Vec::new(),
            frame: None,
        }
    }

    /// The frame's correlation id.
    pub fn id(mut self, id: &'a str) -> Self {
        self.id = Some(id);
        self
    }

    /// The other end: a `token_id`, `client_id`, or `server`. Never a
    /// secret — see [`redact`] for what must never appear.
    pub fn peer(mut self, peer: &'a str) -> Self {
        self.peer = Some(peer);
        self
    }

    /// An extra `k=v` detail (session name, query cmd, ...).
    pub fn field(mut self, key: &'static str, value: impl Into<String>) -> Self {
        if self.cfg.is_on() {
            self.fields.push((key, value.into()));
        }
        self
    }

    /// The frame body, materialized **only** at [`DebugLevel::Noisy`] —
    /// the closure is not called at any other level, so redaction and
    /// serialization cost nothing when they would be thrown away.
    ///
    /// The caller is responsible for having already redacted secrets out
    /// of what the closure returns (see [`redact_secret`]).
    pub fn frame(mut self, render: impl FnOnce() -> String) -> Self {
        if self.cfg.level == DebugLevel::Noisy {
            self.frame = Some(render());
        }
        self
    }

    /// Writes the line to stderr, or does nothing at
    /// [`DebugLevel::None`].
    pub fn emit(self) {
        if !self.cfg.is_on() {
            return;
        }
        match self.cfg.format {
            LogFormat::Text => eprintln!("{}", self.render_text()),
            LogFormat::Json => eprintln!("{}", self.render_json()),
        }
    }

    fn render_text(&self) -> String {
        let mut line = format!(
            "{} DEBUG {} {:<width$}",
            emission_ts(),
            self.dir.as_text(),
            self.kind,
            width = TYPE_COLUMN_WIDTH,
        );
        if let Some(id) = self.id {
            line.push_str(&format!(" id={}", short(id)));
        }
        if let Some(peer) = self.peer {
            line.push_str(&format!(" peer={}", short(peer)));
        }
        for (key, value) in &self.fields {
            line.push_str(&format!(" {key}={value}"));
        }
        if let Some(frame) = &self.frame {
            line.push(' ');
            line.push_str(frame);
        }
        line
    }

    fn render_json(&self) -> String {
        // A frame that somehow doesn't re-parse is still worth logging:
        // fall back to carrying it as a JSON string rather than dropping
        // the line entirely.
        let frame = self.frame.as_ref().map(|raw| {
            serde_json::from_str::<serde_json::Value>(raw)
                .unwrap_or_else(|_| serde_json::Value::String(raw.clone()))
        });
        let line = JsonLine {
            ts: emission_ts(),
            level: "debug",
            verbosity: match self.cfg.level {
                DebugLevel::Noisy => "noisy",
                _ => "quiet",
            },
            dir: self.dir.as_json(),
            kind: self.kind,
            id: self.id,
            peer: self.peer,
            fields: self
                .fields
                .iter()
                .map(|(k, v)| (*k, v.as_str()))
                .collect(),
            frame,
        };
        serde_json::to_string(&line)
            .unwrap_or_else(|_| format!(r#"{{"ts":"{}","level":"debug","error":"unserializable log line"}}"#, emission_ts()))
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
        assert_eq!(
            DebugLevel::resolve(Some("NOISY"), None),
            Ok(DebugLevel::Noisy)
        );
        assert_eq!(
            DebugLevel::resolve(Some("Quiet"), None),
            Ok(DebugLevel::Quiet)
        );
    }

    #[test]
    fn log_format_defaults_to_text() {
        assert_eq!(LogFormat::resolve(None, None), Ok(LogFormat::Text));
    }

    #[test]
    fn log_format_flag_wins_over_env() {
        assert_eq!(
            LogFormat::resolve(Some("text"), Some("json")),
            Ok(LogFormat::Text)
        );
    }

    #[test]
    fn log_format_env_used_when_flag_absent() {
        assert_eq!(LogFormat::resolve(None, Some("json")), Ok(LogFormat::Json));
    }

    #[test]
    fn log_format_invalid_value_errors_and_does_not_fall_through() {
        assert_eq!(
            LogFormat::resolve(Some("yaml"), Some("json")),
            Err(LogFormatError {
                source: DebugLevelSource::Flag,
                value: "yaml".to_string(),
            })
        );
    }

    #[test]
    fn log_format_is_case_insensitive() {
        assert_eq!(LogFormat::resolve(Some("JSON"), None), Ok(LogFormat::Json));
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

    fn noisy_json() -> DebugConfig {
        DebugConfig::new(DebugLevel::Noisy, LogFormat::Json)
    }

    fn noisy_text() -> DebugConfig {
        DebugConfig::new(DebugLevel::Noisy, LogFormat::Text)
    }

    #[test]
    fn emission_ts_is_fixed_width_rfc3339_micros() {
        let ts = emission_ts();
        // 2026-09-06T20:59:54.712345Z
        assert_eq!(ts.len(), 27, "ts was {ts:?}");
        assert!(ts.ends_with('Z'));
        assert_eq!(&ts[10..11], "T");
        assert_eq!(&ts[19..20], ".");
    }

    #[test]
    fn text_line_starts_with_timestamp_then_fixed_columns() {
        let line = outgoing(noisy_text(), "auth").id("abc123").render_text();
        assert_eq!(&line[26..27], "Z", "ts should occupy the first column");
        assert!(line.contains(" DEBUG -> auth       "), "line was {line:?}");
    }

    #[test]
    fn json_line_is_a_single_parseable_object_with_ts_first() {
        let line = incoming(noisy_json(), "hello")
            .id("id-1")
            .peer("server")
            .render_json();
        assert!(line.starts_with(r#"{"ts":"#), "line was {line:?}");
        let parsed: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");
        assert_eq!(parsed["type"], "hello");
        assert_eq!(parsed["dir"], "in");
        assert_eq!(parsed["id"], "id-1");
        assert_eq!(parsed["peer"], "server");
    }

    #[test]
    fn json_nests_the_frame_as_an_object_not_a_string() {
        let line = outgoing(noisy_json(), "auth")
            .frame(|| r#"{"v":1,"type":"auth"}"#.to_string())
            .render_json();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["frame"]["v"], 1);
        assert_eq!(parsed["frame"]["type"], "auth");
    }

    #[test]
    fn json_carries_the_full_untruncated_id() {
        let long_id = "c14fb1a960b3d14d690e652e53b8b33a";
        let line = outgoing(noisy_json(), "auth").id(long_id).render_json();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["id"], long_id);
    }

    #[test]
    fn text_truncates_long_ids_for_scannability() {
        let long_id = "c14fb1a960b3d14d690e652e53b8b33a";
        let line = outgoing(noisy_text(), "auth").id(long_id).render_text();
        assert!(line.contains("id=c14fb1a960b3"), "line was {line:?}");
        assert!(!line.contains(long_id));
    }

    #[test]
    fn quiet_never_materializes_the_frame() {
        let cfg = DebugConfig::new(DebugLevel::Quiet, LogFormat::Json);
        let mut called = false;
        let line = outgoing(cfg, "auth")
            .frame(|| {
                called = true;
                "{}".to_string()
            })
            .render_json();
        assert!(!called, "frame closure must not run below noisy");
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(parsed.get("frame").is_none());
        assert_eq!(parsed["verbosity"], "quiet");
    }

    #[test]
    fn none_level_never_materializes_the_frame_either() {
        let cfg = DebugConfig::new(DebugLevel::None, LogFormat::Text);
        let mut called = false;
        outgoing(cfg, "auth")
            .frame(|| {
                called = true;
                "{}".to_string()
            })
            .emit();
        assert!(!called);
        assert!(!cfg.is_on());
    }

    #[test]
    fn local_events_carry_no_direction() {
        let line = local(noisy_json(), "connected")
            .field("addr", "127.0.0.1:41807")
            .render_json();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert!(parsed.get("dir").is_none());
        assert_eq!(parsed["addr"], "127.0.0.1:41807");
    }

    #[test]
    fn extra_fields_are_flattened_not_nested() {
        let line = outgoing(noisy_json(), "reply")
            .field("session", "m1")
            .render_json();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["session"], "m1", "line was {line:?}");
    }
}
