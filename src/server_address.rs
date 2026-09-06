//! Parses `--server` URLs for `holler join` (issue #23).
//!
//! This does **not** use the `url` crate: that crate's WHATWG-compliant
//! `Url::port()` treats an omitted port as equal to the *scheme's* default
//! port (80 for `ws`, 443 for `wss`) and normalizes an explicit `:80` /
//! `:443` away to "no port" too — indistinguishable from "port omitted".
//! Holler's default port is 41807, unrelated to the URL standard's `ws`
//! default, so that normalization would silently mis-handle an explicit
//! `ws://host:80`. A small hand-rolled parser avoids the footgun.
//!
//! Per `docs/protocol/v1.md` (server repo): IPv6 literals use brackets
//! (`ws://[::1]:41807`); an omitted port defaults to 41807 regardless of
//! scheme; a literal address is dialed as written, and a hostname is not
//! assumed to resolve to IPv4 only. This module only parses and normalizes
//! the address — it does not resolve hostnames (`getaddrinfo`) or dial
//! anything; that is the connecting transport's job (issue #24).

use std::fmt;

/// Default Holler port when `--server` omits one.
pub const DEFAULT_PORT: u16 = 41807;

/// A parsed `--server` address: scheme, host (literal, unresolved), and
/// port (defaulted if omitted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAddress {
    pub scheme: Scheme,
    /// The host exactly as given, minus IPv6 brackets if present. Never
    /// resolved here.
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Ws,
    Wss,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scheme::Ws => "ws",
            Scheme::Wss => "wss",
        })
    }
}

/// Errors parsing a `--server` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAddressError {
    /// No `scheme://` prefix at all.
    MissingScheme,
    /// A scheme was present but isn't `ws` or `wss`.
    UnsupportedScheme(String),
    /// The authority (host[:port]) was empty.
    EmptyHost,
    /// An IPv6 literal must be `[...]`-bracketed; this looked like one
    /// (contains `:`) but wasn't bracketed.
    UnbracketedIpv6(String),
    /// A `[` was opened but never closed with `]`.
    UnterminatedIpv6Bracket,
    /// The port segment wasn't a valid `u16`.
    InvalidPort(String),
    /// Content after the authority (e.g. a path) that this parser doesn't
    /// support for a join URL.
    TrailingContent(String),
}

impl fmt::Display for ServerAddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerAddressError::MissingScheme => {
                write!(f, "--server must start with ws:// or wss://")
            }
            ServerAddressError::UnsupportedScheme(s) => {
                write!(f, "unsupported --server scheme {s:?} (expected ws or wss)")
            }
            ServerAddressError::EmptyHost => write!(f, "--server is missing a host"),
            ServerAddressError::UnbracketedIpv6(s) => write!(
                f,
                "IPv6 literal {s:?} must be bracketed, e.g. [{s}]"
            ),
            ServerAddressError::UnterminatedIpv6Bracket => {
                write!(f, "--server has an unterminated [ in the host")
            }
            ServerAddressError::InvalidPort(s) => write!(f, "invalid port {s:?} in --server"),
            ServerAddressError::TrailingContent(s) => {
                write!(f, "unexpected content after --server host[:port]: {s:?}")
            }
        }
    }
}

impl std::error::Error for ServerAddressError {}

impl ServerAddress {
    /// Parses a `--server` value such as `ws://host:41807`,
    /// `wss://example.com` (port defaulted), or `ws://[::1]:41807`.
    pub fn parse(input: &str) -> Result<Self, ServerAddressError> {
        let (scheme_str, authority) = input
            .split_once("://")
            .ok_or(ServerAddressError::MissingScheme)?;
        let scheme = match scheme_str {
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            other => return Err(ServerAddressError::UnsupportedScheme(other.to_string())),
        };

        if authority.is_empty() {
            return Err(ServerAddressError::EmptyHost);
        }

        let (host, port) = if let Some(after_bracket) = authority.strip_prefix('[') {
            let (host, rest) = after_bracket
                .split_once(']')
                .ok_or(ServerAddressError::UnterminatedIpv6Bracket)?;
            if host.is_empty() {
                return Err(ServerAddressError::EmptyHost);
            }
            let port = match rest.strip_prefix(':') {
                Some(port_str) => parse_port(port_str)?,
                None if rest.is_empty() => DEFAULT_PORT,
                None => return Err(ServerAddressError::TrailingContent(rest.to_string())),
            };
            (host.to_string(), port)
        } else {
            // Exactly one `:` is an unambiguous `host:port` separator. Zero
            // means no port was given. More than one can only be an
            // unbracketed IPv6 literal, which the protocol requires to be
            // bracketed (`[::1]`) precisely to avoid this ambiguity.
            match authority.matches(':').count() {
                0 => (authority.to_string(), DEFAULT_PORT),
                1 => {
                    let (host, port_str) = authority.split_once(':').unwrap();
                    if host.is_empty() {
                        return Err(ServerAddressError::EmptyHost);
                    }
                    (host.to_string(), parse_port(port_str)?)
                }
                _ => return Err(ServerAddressError::UnbracketedIpv6(authority.to_string())),
            }
        };

        Ok(ServerAddress { scheme, host, port })
    }

    /// Renders back to a canonical `scheme://host:port` string, bracketing
    /// the host if it contains `:` (an IPv6 literal).
    pub fn to_canonical_url(&self) -> String {
        if self.host.contains(':') {
            format!("{}://[{}]:{}", self.scheme, self.host, self.port)
        } else {
            format!("{}://{}:{}", self.scheme, self.host, self.port)
        }
    }
}

fn parse_port(s: &str) -> Result<u16, ServerAddressError> {
    s.parse::<u16>()
        .map_err(|_| ServerAddressError::InvalidPort(s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_port_is_used() {
        let addr = ServerAddress::parse("ws://example.com:9000").unwrap();
        assert_eq!(addr.scheme, Scheme::Ws);
        assert_eq!(addr.host, "example.com");
        assert_eq!(addr.port, 9000);
    }

    #[test]
    fn omitted_port_defaults_to_41807() {
        let addr = ServerAddress::parse("wss://example.com").unwrap();
        assert_eq!(addr.port, DEFAULT_PORT);
    }

    #[test]
    fn explicit_port_80_is_not_confused_with_omitted() {
        // Regression: the `url` crate normalizes `ws://host:80` to "no
        // port" because 80 is ws's WHATWG default. We must keep it as 80.
        let addr = ServerAddress::parse("ws://example.com:80").unwrap();
        assert_eq!(addr.port, 80);
    }

    #[test]
    fn ipv6_bracket_with_port() {
        let addr = ServerAddress::parse("ws://[::1]:41807").unwrap();
        assert_eq!(addr.host, "::1");
        assert_eq!(addr.port, 41807);
    }

    #[test]
    fn ipv6_bracket_without_port_defaults() {
        let addr = ServerAddress::parse("ws://[::1]").unwrap();
        assert_eq!(addr.host, "::1");
        assert_eq!(addr.port, DEFAULT_PORT);
    }

    #[test]
    fn ipv6_full_literal_bracketed() {
        let addr = ServerAddress::parse("wss://[2001:db8::1]:1234").unwrap();
        assert_eq!(addr.host, "2001:db8::1");
        assert_eq!(addr.port, 1234);
    }

    #[test]
    fn unbracketed_ipv6_is_rejected() {
        let err = ServerAddress::parse("ws://::1:41807").unwrap_err();
        assert!(matches!(err, ServerAddressError::UnbracketedIpv6(_)));
    }

    #[test]
    fn localhost_hostname_is_kept_literal_not_resolved() {
        let addr = ServerAddress::parse("ws://localhost:41807").unwrap();
        assert_eq!(addr.host, "localhost");
    }

    #[test]
    fn localhost_without_port_still_defaults() {
        let addr = ServerAddress::parse("ws://localhost").unwrap();
        assert_eq!(addr.host, "localhost");
        assert_eq!(addr.port, DEFAULT_PORT);
    }

    #[test]
    fn missing_scheme_errors() {
        let err = ServerAddress::parse("example.com:41807").unwrap_err();
        assert_eq!(err, ServerAddressError::MissingScheme);
    }

    #[test]
    fn unsupported_scheme_errors() {
        let err = ServerAddress::parse("http://example.com").unwrap_err();
        assert!(matches!(err, ServerAddressError::UnsupportedScheme(s) if s == "http"));
    }

    #[test]
    fn empty_host_errors() {
        let err = ServerAddress::parse("ws://").unwrap_err();
        assert_eq!(err, ServerAddressError::EmptyHost);
    }

    #[test]
    fn invalid_port_errors() {
        let err = ServerAddress::parse("ws://example.com:notaport").unwrap_err();
        assert!(matches!(err, ServerAddressError::InvalidPort(_)));
    }

    #[test]
    fn unterminated_bracket_errors() {
        let err = ServerAddress::parse("ws://[::1:41807").unwrap_err();
        assert_eq!(err, ServerAddressError::UnterminatedIpv6Bracket);
    }

    #[test]
    fn canonical_url_bracketed_for_ipv6() {
        let addr = ServerAddress::parse("ws://[::1]").unwrap();
        assert_eq!(addr.to_canonical_url(), "ws://[::1]:41807");
    }

    #[test]
    fn canonical_url_plain_for_hostname() {
        let addr = ServerAddress::parse("wss://example.com:9000").unwrap();
        assert_eq!(addr.to_canonical_url(), "wss://example.com:9000");
    }
}
