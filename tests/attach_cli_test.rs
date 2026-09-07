//! Regression tests for issue #109 (`holler attach sessions` / `holler
//! attach init`): a real subprocess invocation of the built `holler`
//! binary against a fake in-process OpenCode HTTP double serving `GET
//! /session`, plus a library-level round-trip test for
//! `config::render_attach_toml`. Same fake-server shape as
//! `http_attach_driver_test.rs`'s `FakeOpenCode` (a minimal `std`-only TCP
//! listener), scoped down to just what `/session` listing needs.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use assert_cmd::Command;
use holler_client::config::{self, SessionConfig, SessionMode};
use holler_client::http_attach_driver::list_sessions;

fn holler() -> Command {
    Command::cargo_bin("holler").expect("holler binary not built")
}

/// A fake OpenCode HTTP endpoint that only answers `GET /session` with a
/// fixed JSON array -- everything issue #109's two subcommands need.
struct FakeSessionList {
    addr: SocketAddr,
}

impl FakeSessionList {
    fn start(body: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                std::thread::spawn(move || handle(stream, body));
            }
        });
        FakeSessionList { addr }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

fn handle(mut stream: TcpStream, body: &str) {
    let mut buf = [0u8; 4096];
    let mut received = Vec::new();
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                received.extend_from_slice(&buf[..n]);
                if received.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
}

const TWO_SESSIONS: &str = r#"[
    {"id": "ses_older", "title": "Older session", "time": {"created": 1000, "updated": 1000}},
    {"id": "ses_newer", "title": "Newer session", "time": {"created": 2000, "updated": 2000}}
]"#;

/// `list_sessions` sorts newest-updated first regardless of the endpoint's
/// own array order -- `GET /session`'s contract makes no ordering promise,
/// so `holler attach init`'s no-`--session` default must not trust it.
#[tokio::test]
async fn list_sessions_sorts_newest_updated_first() {
    let server = FakeSessionList::start(TWO_SESSIONS);
    let sessions = list_sessions(&server.base_url())
        .await
        .expect("fake server answers 200 with a valid array");
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0].id, "ses_newer");
    assert_eq!(sessions[1].id, "ses_older");
}

/// An unreachable endpoint fails closed with a clear error, not a panic or
/// a hang -- `holler attach sessions`/`init` are pure best-effort local
/// tools, never worth retrying against a dead endpoint.
#[tokio::test]
async fn list_sessions_against_unreachable_endpoint_fails_closed() {
    let result = list_sessions("http://127.0.0.1:1").await;
    assert!(result.is_err(), "an unreachable endpoint must be a clean Err, not a panic");
}

/// `holler attach sessions` against a real (fake) endpoint: both session
/// ids appear in the printed listing.
#[test]
fn attach_sessions_lists_real_session_ids() {
    let server = FakeSessionList::start(TWO_SESSIONS);
    let assert = holler()
        .args(["attach", "sessions", "--endpoint", &server.base_url()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();
    assert!(stdout.contains("ses_newer"), "got:\n{stdout}");
    assert!(stdout.contains("ses_older"), "got:\n{stdout}");
}

/// `holler attach sessions` against an endpoint with zero sessions prints
/// a clear "no sessions" message rather than an empty table or an error.
#[test]
fn attach_sessions_against_empty_endpoint_says_so() {
    let server = FakeSessionList::start("[]");
    let assert = holler()
        .args(["attach", "sessions", "--endpoint", &server.base_url()])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).into_owned();
    assert!(stdout.contains("no sessions"), "got:\n{stdout}");
}

/// `holler attach init` with no `--session` auto-picks the most recently
/// updated session and writes a config that round-trips through the real
/// loader with the expected attach fields.
#[test]
fn attach_init_auto_picks_newest_session_and_writes_valid_config() {
    let server = FakeSessionList::start(TWO_SESSIONS);
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("attach.toml");

    holler()
        .args([
            "attach",
            "init",
            "--endpoint",
            &server.base_url(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .assert()
        .success();

    let registry = config::load(Some(&out_path)).expect("written config must load cleanly");
    let sessions = registry.sessions();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].name, "alpha");
    assert_eq!(sessions[0].mode, SessionMode::Attach);
    assert_eq!(sessions[0].endpoint.as_deref(), Some(server.base_url().as_str()));
    assert_eq!(sessions[0].session_id.as_deref(), Some("ses_newer"));
}

/// `holler attach init` fails closed on an existing `--out` without
/// `--force` -- never silently clobbers a config the operator already has.
#[test]
fn attach_init_fails_closed_on_existing_out_without_force() {
    let server = FakeSessionList::start(TWO_SESSIONS);
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("attach.toml");
    std::fs::write(&out_path, "# pre-existing operator config\n").unwrap();

    let assert = holler()
        .args([
            "attach",
            "init",
            "--endpoint",
            &server.base_url(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).into_owned();
    assert!(stderr.contains("--force"), "got:\n{stderr}");

    let contents = std::fs::read_to_string(&out_path).unwrap();
    assert_eq!(
        contents, "# pre-existing operator config\n",
        "a rejected init must never touch the existing file"
    );
}

/// `--force` overwrites, and `--session`/`--name` override the auto-pick
/// and default name.
#[test]
fn attach_init_force_overwrites_with_explicit_session_and_name() {
    let server = FakeSessionList::start(TWO_SESSIONS);
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("attach.toml");
    std::fs::write(&out_path, "# stale\n").unwrap();

    holler()
        .args([
            "attach",
            "init",
            "--endpoint",
            &server.base_url(),
            "--session",
            "ses_older",
            "--name",
            "gamma",
            "--out",
            out_path.to_str().unwrap(),
            "--force",
        ])
        .assert()
        .success();

    let registry = config::load(Some(&out_path)).unwrap();
    let sessions = registry.sessions();
    assert_eq!(sessions[0].name, "gamma");
    assert_eq!(sessions[0].session_id.as_deref(), Some("ses_older"));
}

/// `holler attach init` against an endpoint with zero sessions and no
/// explicit `--session` fails closed rather than writing an unattachable
/// config.
#[test]
fn attach_init_against_empty_endpoint_without_explicit_session_fails_closed() {
    let server = FakeSessionList::start("[]");
    let out_dir = tempfile::tempdir().unwrap();
    let out_path = out_dir.path().join("attach.toml");

    holler()
        .args([
            "attach",
            "init",
            "--endpoint",
            &server.base_url(),
            "--out",
            out_path.to_str().unwrap(),
        ])
        .assert()
        .failure();

    assert!(!out_path.exists(), "a failed init must not leave a partial file behind");
}

/// `config::render_attach_toml`'s output round-trips through the real
/// loader with every field intact -- the same guarantee the CLI tests
/// above exercise end-to-end, pinned here at the library level too.
#[test]
fn render_attach_toml_round_trips_through_load() {
    let session = SessionConfig {
        name: "alpha".to_string(),
        harness: "opencode".to_string(),
        mode: SessionMode::Attach,
        command: Vec::new(),
        interrupt: None,
        endpoint: Some("http://127.0.0.1:4096".to_string()),
        session_id: Some("ses_abc".to_string()),
    };
    let rendered = config::render_attach_toml(&session).expect("rendering must succeed");
    assert!(
        !rendered.contains("command"),
        "attach sessions must render without a stray `command` field, got:\n{rendered}"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attach.toml");
    std::fs::write(&path, &rendered).unwrap();

    let registry = config::load(Some(&path)).expect("rendered TOML must be loadable");
    assert_eq!(registry.sessions(), &[session]);
}
