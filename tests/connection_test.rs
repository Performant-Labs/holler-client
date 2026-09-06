//! Integration tests for the live WebSocket session (issue #24), driven
//! through the actual built `holler` binary (`holler run`) against a
//! minimal local WS test server built in this file with `tokio-tungstenite`
//! directly — not the holler-server repo's own `tests/wire/` harness,
//! which assumes a fixed port (`ws://127.0.0.1:41807`) this file must not
//! collide with. Every server here binds `127.0.0.1:0` (an OS-assigned
//! free port) instead.
//!
//! `holler join`'s redeem step is still a stub (issue #23; see
//! `src/join.rs`), so these tests never run `holler join` against this
//! fake server — they write `credential.json` directly, exactly the
//! shape a real join would have persisted, which is exactly what "resume
//! with the credential" (this story's actual scope) means to exercise.

use std::io::Read as _;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use holler_client::proto::{self, Body, ErrorBody, HelloBody, PingBody, QueryBody, Role};

fn holler() -> Command {
    Command::new(env!("CARGO_BIN_EXE_holler"))
}

/// A fresh, isolated `HOLLER_STATE_DIR` per test, with `$PATH`
/// deterministically controlled so harness-confirmation checks (issue
/// #30) never depend on what happens to be installed on the machine
/// running these tests.
struct Env {
    dir: tempfile::TempDir,
    /// Becomes the spawned `holler` process's entire `$PATH`. Empty by
    /// default, so `opencode` (or any other harness) reads as unconfirmed
    /// unless a test opts in via [`Env::with_fake_executable`].
    path_dir: tempfile::TempDir,
    /// A body config (`--config`) declaring two `opencode` sessions,
    /// `test-alpha`/`test-beta`. `SessionRegistry` has no built-in default
    /// (every session is explicit) — this fixture is this test file's own
    /// stand-in for "a body process with sessions configured", the same
    /// role a shipped default used to play.
    config_path: std::path::PathBuf,
    /// See [`Env::with_heartbeat_interval_ms`].
    heartbeat_interval_ms: Option<u64>,
}

impl Env {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path_dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("holler.toml");
        std::fs::write(
            &config_path,
            r#"
[[session]]
name = "test-alpha"
harness = "opencode"
command = ["opencode", "acp"]

[[session]]
name = "test-beta"
harness = "opencode"
command = ["opencode", "acp"]
"#,
        )
        .unwrap();
        Env {
            dir,
            path_dir,
            config_path,
            heartbeat_interval_ms: None,
        }
    }

    /// Places a fake, executable file named `name` on the `$PATH` this
    /// env's spawned processes see, so a `SessionConfig` naming it as its
    /// command resolves as "confirmed runnable" — without depending on any
    /// real harness binary being installed on the test host.
    fn with_fake_executable(self, name: &str) -> Self {
        let exe = self.path_dir.path().join(name);
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&exe).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&exe, perms).unwrap();
        }
        self
    }

    /// Like [`Env::with_fake_executable`], but the process stays alive
    /// (never reads stdin, never writes stdout, never exits) instead of
    /// exiting immediately — so `SessionManager::spawn`'s ACP
    /// `initialize` handshake is talking to something that is still
    /// running and simply never answers, not something that already
    /// closed. In practice the ACP driver still surfaces a `spawn_failed`
    /// error quickly rather than waiting out the full
    /// `SESSION_MANAGER_SPAWN_BUDGET` — the point of this helper isn't a
    /// specific delay, it's that the subprocess genuinely cannot have
    /// completed a valid handshake, so a test using it proves an
    /// ordering, not a race against how fast a script happens to exit.
    fn with_hanging_executable(self, name: &str) -> Self {
        let exe = self.path_dir.path().join(name);
        std::fs::write(&exe, "#!/bin/sh\nsleep 30\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&exe).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&exe, perms).unwrap();
        }
        self
    }

    /// Rewrites this env's config so both configured sessions
    /// (`test-alpha`/`test-beta`) use the real `stub-acp` binary
    /// (issue #32) instead of the fake `opencode` script — needed for
    /// any test that actually dispatches a `prompt`/`interrupt`, since
    /// `SessionManager::spawn` performs a real ACP `initialize` +
    /// `session/new` handshake the fake script (which just exits) can't
    /// answer. `stub-acp`'s path is absolute, so it's confirmed runnable
    /// with no `$PATH` setup needed (unlike `with_fake_executable`).
    /// One session whose stub emits `chunks` streamed updates per turn,
    /// for exercising reply coalescing (issue #83).
    fn with_stub_acp_chunks(self, chunks: usize) -> Self {
        let stub_acp = env!("CARGO_BIN_EXE_stub-acp");
        std::fs::write(
            &self.config_path,
            format!(
                r#"
[[session]]
name = "test-alpha"
harness = "stub-acp"
command = ["{stub_acp}", "--chunks", "{chunks}"]
"#
            ),
        )
        .unwrap();
        self
    }

    fn with_stub_acp_sessions(self) -> Self {
        let stub_acp = env!("CARGO_BIN_EXE_stub-acp");
        std::fs::write(
            &self.config_path,
            format!(
                r#"
[[session]]
name = "test-alpha"
harness = "stub-acp"
command = ["{stub_acp}"]

[[session]]
name = "test-beta"
harness = "stub-acp"
command = ["{stub_acp}"]
"#
            ),
        )
        .unwrap();
        self
    }

    fn cmd(&self) -> Command {
        let mut cmd = holler();
        cmd.env("HOLLER_STATE_DIR", self.dir.path());
        cmd.env("PATH", self.path_dir.path());
        if let Some(ms) = self.heartbeat_interval_ms {
            cmd.env("HOLLER_HEARTBEAT_INTERVAL_MS", ms.to_string());
        }
        cmd.arg("--config").arg(&self.config_path);
        cmd
    }

    /// Overrides `HOLLER_HEARTBEAT_INTERVAL_MS` on every command this env
    /// spawns (`holler run` *and* `holler detach`/`status`, so both
    /// processes agree on the same `stale_after()` window) — see issue
    /// #50's regression test, which needs a staleness window far shorter
    /// than the real 45s default to run in well under a second.
    fn with_heartbeat_interval_ms(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = Some(ms);
        self
    }

    /// Writes `credential.json` directly — standing in for a completed
    /// `holler join` (whose real network redeem doesn't exist yet; see
    /// module docs), since this story's scope is resuming with an
    /// already-persisted credential.
    fn write_credential(
        &self,
        server_url: &str,
        credential: &str,
        token_id: &str,
        client_id: &str,
        hostname: &str,
    ) {
        let contents = serde_json::json!({
            "client_id": client_id,
            "token_id": token_id,
            "credential": credential,
            "server": server_url,
            "hostname": hostname,
        });
        std::fs::write(
            self.dir.path().join("credential.json"),
            serde_json::to_string_pretty(&contents).unwrap(),
        )
        .unwrap();
    }

    fn status_json(&self) -> Value {
        let out = self.cmd().arg("status").output().unwrap();
        assert!(out.status.success(), "{out:?}");
        serde_json::from_slice(&out.stdout).unwrap()
    }

    /// Polls `holler status` until `predicate` accepts the parsed
    /// document, or panics after `budget`. A hang here is a test
    /// failure, not a silent pass.
    fn wait_for_status(&self, budget: Duration, predicate: impl Fn(&Value) -> bool) -> Value {
        let deadline = std::time::Instant::now() + budget;
        loop {
            let doc = self.status_json();
            if predicate(&doc) {
                return doc;
            }
            if std::time::Instant::now() >= deadline {
                panic!("status never matched predicate within {budget:?}; last seen: {doc}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn spawn_run(env: &Env) -> Child {
    env.cmd()
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn `holler run`")
}

fn spawn_run_capturing_stderr(env: &Env) -> Child {
    env.cmd()
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn `holler run`")
}

fn kill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Waits for `child` to exit on its own within `budget`; kills it and
/// returns `None` on timeout (a timeout is the test's failure to report,
/// never a silent hang).
fn wait_for_exit(child: &mut Child, budget: Duration) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn bind_local() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, format!("ws://127.0.0.1:{port}"))
}

async fn accept_ws(listener: &TcpListener) -> WebSocketStream<TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    tokio_tungstenite::accept_async(stream).await.unwrap()
}

/// Reads the next `Message::Text` frame, skipping WS-level ping/pong
/// control frames, treating close/EOF/error as "no frame".
async fn next_text(ws: &mut WebSocketStream<TcpStream>) -> Option<String> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => return Some(t.to_string()),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => return None,
        }
    }
}

async fn next_envelope(ws: &mut WebSocketStream<TcpStream>) -> Option<proto::Envelope> {
    let raw = next_text(ws).await?;
    proto::decode(&raw).ok()
}

async fn send_envelope(ws: &mut WebSocketStream<TcpStream>, env: &proto::Envelope) {
    let raw = proto::encode(env).unwrap();
    ws.send(Message::Text(raw.into())).await.unwrap();
}

/// Reads the client's `auth` frame and validates `from` the way a real
/// server would (`docs/protocol/v1.md` §3/§4: `from` is the client's
/// public `token_id`) — issue #47 shipped silently because this used to
/// accept any `from` value, including `client_id`, without checking it
/// looked like the `token_id` a real server binds against.
async fn expect_auth(
    ws: &mut WebSocketStream<TcpStream>,
    expected_token_id: &str,
) -> proto::AuthBody {
    assert!(
        expected_token_id.starts_with("tok_"),
        "test fixture bug: expected_token_id should look like a real token_id, got {expected_token_id:?}"
    );
    let envelope = next_envelope(ws).await.expect("expected an `auth` frame");
    assert_eq!(
        envelope.from, expected_token_id,
        "`auth`'s `from` must be the client's token_id, not client_id or anything else"
    );
    match envelope.body {
        Body::Auth(a) => a,
        other => panic!("expected `auth`, got {other:?}"),
    }
}

fn server_hello_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Hello,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Hello(HelloBody {
            protocol: 1,
            protocol_min: 1,
            protocol_max: 1,
            role: Role::Server,
            hostname: "test-server".to_string(),
            token_id: None,
            client_id: None,
            harnesses: Vec::new(),
            harnesses_known: Vec::new(),
            harnesses_confirmed: Vec::new(),
            features: vec!["ping".to_string()],
            sessions: Vec::new(),
        }),
    }
}

fn ping_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Ping,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Ping(PingBody {
            hostname: Some("test-server".to_string()),
        }),
    }
}

fn query_envelope(cmd: &str, args: Vec<String>) -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Query,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Query(QueryBody {
            cmd: cmd.to_string(),
            args,
        }),
    }
}

fn prompt_envelope(session: &str, text: &str) -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Prompt,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Prompt(proto::PromptBody {
            session: session.to_string(),
            text: text.to_string(),
            meta: None,
        }),
    }
}

fn interrupt_envelope(session: &str) -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Interrupt,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Interrupt(proto::InterruptBody {
            session: session.to_string(),
        }),
    }
}

fn unauthenticated_error_envelope() -> proto::Envelope {
    proto::Envelope {
        v: 1,
        msg_type: proto::MessageType::Error,
        id: proto::new_id(),
        ts: "2026-01-01T00:00:00Z".to_string(),
        from: "server".to_string(),
        body: Body::Error(ErrorBody {
            code: proto::CODE_UNAUTHENTICATED.to_string(),
            cmd: None,
            message: Some("credential revoked".to_string()),
        }),
    }
}

const STATUS_BUDGET: Duration = Duration::from_secs(5);

// --- Startup ordering: the debug banner must precede any I/O that could
// stall (subprocess spawn, network connect) --------------------------------

#[tokio::test]
async fn logging_started_banner_prints_before_a_hanging_session_spawn_can_block_it() {
    // The bug this guards against: the banner used to be emitted inside
    // connection::run, *after* `spawn_session_manager` had already run
    // to completion (up to SESSION_MANAGER_SPAWN_BUDGET, ~10s, in the
    // worst case). A user watching stderr saw nothing — not even
    // confirmation the binary had started — until that finished.
    //
    // `with_hanging_executable` (not `with_fake_executable`, which exits
    // immediately) points the harness at a subprocess that is still
    // alive and has not spoken ACP, so this proves the banner precedes
    // session-manager spawn's outcome (whatever shape that outcome
    // takes — a real hang, or the driver failing fast against a
    // non-conformant process) rather than merely preceding whatever
    // happened to run quickly.
    let env = Env::new().with_hanging_executable("opencode");
    // A credential must exist or `run_run` fails at "not joined" before
    // ever reaching `spawn_session_manager` — which would make this test
    // pass for the wrong reason (any first line beats an error that
    // never mentions the banner). The address is never dialed: the
    // session-manager spawn happens *before* the network connect in
    // `run_run`, and the process is killed well before it gets there.
    env.write_credential(
        "ws://127.0.0.1:1",
        "hlr_live_unused",
        "tok_unused",
        "cli_unused",
        "unused-host",
    );
    let mut cmd = env.cmd();
    cmd.arg("--debug").arg("quiet").arg("run");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("failed to spawn `holler run`");

    let mut stderr = std::io::BufReader::new(child.stderr.take().expect("stderr was piped"));
    let read = tokio::task::spawn_blocking(move || {
        let mut line = String::new();
        std::io::BufRead::read_line(&mut stderr, &mut line).ok();
        line
    });

    let line = tokio::time::timeout(Duration::from_secs(3), read)
        .await
        .expect(
            "the banner should print almost instantly; 3s is still well \
             under the ~10s session-manager spawn budget, so a timeout \
             here means the banner is still stuck behind the spawn",
        )
        .expect("reading stderr panicked");

    assert!(
        line.contains("logging_started"),
        "expected the startup banner as the first stderr line, got: {line:?}"
    );

    kill(child);
}

#[tokio::test]
async fn auth_then_hello_round_trip_and_status_reports_connected() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_test1", "cli_test1", "test-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    let auth = expect_auth(&mut ws, "tok_test1").await;
    assert_eq!(auth.credential, "hlr_live_good");
    send_envelope(&mut ws, &server_hello_envelope()).await;

    // The client's own `hello`, per spec §4 ("each side sends hello").
    let client_hello = next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    match client_hello.body {
        Body::Hello(hello) => {
            assert_eq!(hello.role, Role::Client);
            assert_eq!(hello.hostname, "test-host");
            assert_eq!(hello.client_id.as_deref(), Some("cli_test1"));
        }
        other => panic!("expected `hello`, got {other:?}"),
    }

    let status = env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    assert_eq!(status["reconnecting"], false);
    assert_eq!(status["client_id"], "cli_test1");

    kill(child);
}

#[tokio::test]
async fn ping_from_server_is_answered_with_pong_including_hostname() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_test2", "cli_test2", "pong-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_test2").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`"); // drain it
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`"); // drain it

    send_envelope(&mut ws, &ping_envelope()).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `pong` reply");
    match reply.body {
        Body::Pong(pong) => assert_eq!(pong.hostname.as_deref(), Some("pong-host")),
        other => panic!("expected `pong`, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn reconnect_with_backoff_triggers_and_eventually_succeeds() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_test3",
        "cli_test3",
        "reconnect-host",
    );

    let child = spawn_run(&env);

    // First connection: complete the handshake, then drop it without
    // warning (a real dropped-connection simulation).
    {
        let mut ws = accept_ws(&listener).await;
        expect_auth(&mut ws, "tok_test3").await;
        send_envelope(&mut ws, &server_hello_envelope()).await;
        let _ = ws.close(None).await;
    }

    env.wait_for_status(STATUS_BUDGET, |doc| doc["reconnecting"] == true);

    // Second connection: the client's backoff loop retries against the
    // same listener; complete the handshake again.
    let mut ws2 = accept_ws(&listener).await;
    expect_auth(&mut ws2, "tok_test3").await;
    send_envelope(&mut ws2, &server_hello_envelope()).await;

    let status = env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    assert_eq!(status["reconnecting"], false);

    kill(child);
}

#[tokio::test]
async fn wrong_credential_surfaces_as_a_clear_failure_not_a_retry_loop() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_bad", "tok_test4", "cli_test4", "bad-host");

    let mut child = spawn_run_capturing_stderr(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_test4").await;
    send_envelope(&mut ws, &unauthenticated_error_envelope()).await;

    let status = wait_for_exit(&mut child, Duration::from_secs(5))
        .expect("`holler run` should exit promptly on an unauthenticated error, not retry forever");
    assert!(
        !status.success(),
        "expected a non-zero exit for a rejected credential"
    );

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr was piped")
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.to_lowercase().contains("authentication failed"),
        "stderr should clearly report the auth failure, got: {stderr:?}"
    );
    assert!(
        !stderr.contains("hlr_live_bad"),
        "must never log the credential"
    );

    // The credential is left in place (an operator decision, not this
    // story's to make); the connection is not.
    let status_doc = env.status_json();
    assert_eq!(status_doc["connected"], false);
    assert_eq!(status_doc["reconnecting"], false);
    assert_eq!(status_doc["client_id"], "cli_test4");
}

#[tokio::test]
async fn detach_closes_a_live_connection_and_the_run_process_exits() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_test5",
        "cli_test5",
        "detach-host",
    );

    let mut child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_test5").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    let detach_out = env.cmd().arg("detach").output().unwrap();
    assert!(detach_out.status.success(), "{detach_out:?}");
    assert!(String::from_utf8(detach_out.stdout)
        .unwrap()
        .contains("detached"));

    let status = wait_for_exit(&mut child, Duration::from_secs(5))
        .expect("`holler run` should exit once detach is requested");
    assert!(status.success(), "expected a clean exit on detach");
    assert!(!env.dir.path().join("credential.json").exists());
}

// Needs `multi_thread`: the auto-pong responder task below must keep
// making progress concurrently with this test's own blocking
// `std::thread::sleep` polling loops (`wait_for_status`'s style), which a
// single-threaded runtime would starve it behind.
#[tokio::test(flavor = "multi_thread")]
async fn detach_still_works_after_the_connection_has_outlived_one_stale_window() {
    // Issue #50: `session_loop` used to call `state.mark_connected()`
    // exactly once, at connect time, never refreshing `updated_at` again.
    // A connection alive longer than `stale_after()` (with no reconnect
    // to re-stamp it) then read as `Disconnected` from
    // `ConnectionStateStore::current_state`, even though it was very much
    // alive — which made `holler detach`'s "is there anything live to
    // detach" guard silently skip `request_detach()` entirely, leaving
    // the `run` process running forever. `current_state`'s staleness
    // check has whole-second granularity (`OffsetDateTime::unix_timestamp`/
    // `Duration::as_secs`), so this uses a 2s heartbeat (`stale_after()` =
    // 6s) rather than a sub-second one — still a small fraction of the
    // real 45s default, without also fighting that granularity.
    let env = Env::new().with_heartbeat_interval_ms(2000);
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_test6",
        "cli_test6",
        "stale-host",
    );

    let mut child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_test6").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    // With a 2s heartbeat, this client will send its own `ping` every 2s
    // and consider the connection dead if a `pong` doesn't come back
    // within one more interval — so, unlike the other tests in this file
    // (which finish well inside the real 15s default), this one must
    // actually answer every heartbeat for the whole test, not just drain
    // frames at the end.
    let auto_pong = tokio::spawn(async move {
        loop {
            match next_envelope(&mut ws).await {
                Some(env) if matches!(env.body, Body::Ping(_)) => {
                    let pong = proto::pong_reply(&env.id, "server", "test-server");
                    send_envelope(&mut ws, &pong).await;
                }
                Some(_) => continue,
                None => return,
            }
        }
    });

    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    // Outlive the stale window (6s) while the connection stays healthy
    // (auto-answered heartbeats keep it that way) — this is exactly the
    // case the old, never-refreshed timestamp got wrong.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        env.status_json()["connected"] == true,
        "connection should still read as connected after outliving a stale window"
    );

    let detach_out = env.cmd().arg("detach").output().unwrap();
    assert!(detach_out.status.success(), "{detach_out:?}");
    assert!(String::from_utf8(detach_out.stdout)
        .unwrap()
        .contains("detached"));

    let status = wait_for_exit(&mut child, Duration::from_secs(5)).expect(
        "`holler run` should exit once detach is requested, even after outliving a stale window",
    );
    assert!(status.success(), "expected a clean exit on detach");
    assert!(!env.dir.path().join("credential.json").exists());

    auto_pong.abort();
}

// --- Answering inbound `query` (issue #30) ---------------------------------

#[tokio::test]
async fn query_status_from_server_is_answered_with_the_real_status_document() {
    let env = Env::new().with_fake_executable("opencode");
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query1",
        "cli_query1",
        "query-host",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query1").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = query_envelope("status", vec![]);
    send_envelope(&mut ws, &request).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    assert_eq!(reply.id, request.id, "query_ok must reuse the request id");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["cmd"], "status");
            assert_eq!(body["role"], "client");
            assert_eq!(body["connected"], true);
            assert_eq!(body["client_id"], "cli_query1");
            assert_eq!(body["harnesses"], serde_json::json!(["opencode"]));
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_support_reports_true_for_an_implemented_protocol_feature() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query2",
        "cli_query2",
        "support-host",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query2").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(
        &mut ws,
        &query_envelope("support", vec!["ping".to_string()]),
    )
    .await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["ok"], true);
            assert_eq!(body["kind"], "feature");
            assert_eq!(body["feature"], "ping");
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_support_reports_true_for_confirmed_runnable_harness() {
    let env = Env::new().with_fake_executable("opencode");
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query3",
        "cli_query3",
        "support-host2",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query3").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(
        &mut ws,
        &query_envelope("support", vec!["opencode".to_string()]),
    )
    .await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["ok"], true);
            assert_eq!(body["kind"], "harness");
            assert!(body["how"].as_str().unwrap().contains("opencode"));
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_support_reports_false_for_a_harness_not_on_path() {
    // No `with_fake_executable`: the default env's `$PATH` is an empty
    // tempdir, so `opencode` — configured by `Env`'s fixture `--config`
    // — is not confirmed runnable, even though it *is* configured.
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query4",
        "cli_query4",
        "support-host3",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query4").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(
        &mut ws,
        &query_envelope("support", vec!["opencode".to_string()]),
    )
    .await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["ok"], false);
            assert_eq!(body["kind"], "harness");
            assert_eq!(body["reason"], "no adapter");
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_caps_reports_a_capability_entry_for_every_known_id() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query5",
        "cli_query5",
        "caps-host",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query5").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(&mut ws, &query_envelope("caps", vec![])).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["cmd"], "caps");
            assert_eq!(body["capabilities"]["ping"]["ok"], true);
            assert_eq!(body["capabilities"]["claude"]["ok"], false);
            assert!(body["capabilities"]["opencode"].is_object());
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_protocol_with_no_args_reports_this_binarys_min_max() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query6",
        "cli_query6",
        "protocol-host",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query6").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(&mut ws, &query_envelope("protocol", vec![])).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["cmd"], "protocol");
            assert_eq!(body["min"], 1);
            assert_eq!(body["max"], 1);
            assert_eq!(body["session"], 1);
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_protocol_with_arg_answers_can_you_speak_n() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query7",
        "cli_query7",
        "protocol-host2",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query7").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    send_envelope(&mut ws, &query_envelope("protocol", vec!["2".to_string()])).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected a `query_ok` reply");
    match reply.body {
        Body::QueryOk(body) => {
            assert_eq!(body["ok"], false);
            assert_eq!(body["asked"], 2);
            assert_eq!(body["max"], 1);
        }
        other => panic!("expected QueryOk, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn query_unknown_cmd_fails_closed_with_error_reply() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_query8",
        "cli_query8",
        "unknown-cmd-host",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_query8").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = query_envelope("summarize", vec![]);
    send_envelope(&mut ws, &request).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected an `error` reply");
    assert_eq!(reply.id, request.id, "error must reuse the request id");
    match reply.body {
        Body::Error(ErrorBody { code, cmd, .. }) => {
            assert_eq!(code, proto::CODE_UNKNOWN_CMD);
            assert_eq!(cmd.as_deref(), Some("summarize"));
        }
        other => panic!("expected Error, got {other:?}"),
    }

    kill(child);
}

// --- Honest `hello` advertisement (issue #30) ------------------------------

#[tokio::test]
async fn hello_advertises_harness_only_when_confirmed_runnable() {
    let env = Env::new().with_fake_executable("opencode");
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_hello1",
        "cli_hello1",
        "hello-host1",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_hello1").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    let client_hello = next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    match client_hello.body {
        Body::Hello(hello) => {
            assert_eq!(hello.harnesses, vec!["opencode".to_string()]);
            assert_eq!(
                hello.sessions.len(),
                2,
                "both configured sessions use the confirmed harness"
            );
            assert!(hello.features.contains(&"query".to_string()));
        }
        other => panic!("expected `hello`, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn hello_advertises_no_harness_when_not_confirmed_runnable() {
    // Default env: empty `$PATH`, so `opencode` (configured but not
    // installed) must not be advertised — "advertise only what is real"
    // (ADR-0001), not "configured to use".
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_hello2",
        "cli_hello2",
        "hello-host2",
    );

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_hello2").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    let client_hello = next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    match client_hello.body {
        Body::Hello(hello) => {
            assert!(hello.harnesses.is_empty());
            assert!(hello.sessions.is_empty());
        }
        other => panic!("expected `hello`, got {other:?}"),
    }

    kill(child);
}

// --- Presence, prompt dispatch, interrupt dispatch (issue #49) ------------

#[tokio::test]
async fn presence_advertises_confirmed_sessions_with_busy_state() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p1", "cli_p1", "presence-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p1").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    let presence = next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");
    match presence.body {
        Body::Presence(proto::PresenceBody { sessions }) => {
            assert_eq!(sessions.len(), 2);
            for row in &sessions {
                assert_eq!(row["busy"], false);
                assert!(row["name"].as_str().unwrap().starts_with("test-"));
                assert_eq!(row["harness"], "stub-acp");
            }
        }
        other => panic!("expected Presence, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn prompt_dispatches_to_session_and_streams_reply_then_done() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p2", "cli_p2", "prompt-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p2").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = prompt_envelope("test-alpha", "hello");
    send_envelope(&mut ws, &request).await;

    // stub-acp answers with one "PONG" update chunk, then ends the turn.
    //
    // Asserted as a contract rather than a fixed frame count: since issue
    // #83 the client coalesces streamed updates, so how many frames carry
    // the text is a scheduling detail (a turn that ends inside the
    // debounce window ships its text on the terminal frame). What must
    // hold is that every frame reuses the prompt's id, the concatenation
    // of `text` + `chunks` across frames is exactly the reply, and
    // exactly one frame closes the turn — which is what holler-server
    // reassembles (`registry.rs`, spec §10).
    let mut assembled = String::new();
    let mut terminal_frames = 0;
    loop {
        let frame = next_envelope(&mut ws)
            .await
            .expect("expected a `reply` frame before the turn ended");
        assert_eq!(frame.id, request.id, "reply must reuse the prompt's id");
        match frame.body {
            Body::Reply(proto::ReplyBody {
                session,
                text,
                chunks,
                done,
                ..
            }) => {
                assert_eq!(session, "test-alpha");
                if let Some(t) = text {
                    assembled.push_str(&t);
                }
                for chunk in chunks {
                    assembled.push_str(&chunk);
                }
                if done {
                    terminal_frames += 1;
                    break;
                }
            }
            other => panic!("expected Reply, got {other:?}"),
        }
    }
    assert_eq!(assembled, "PONG", "no streamed text may be lost");
    assert_eq!(terminal_frames, 1, "exactly one frame closes the turn");

    kill(child);
}

#[tokio::test]
async fn many_streamed_updates_coalesce_into_fewer_frames() {
    // Issue #83: the client used to emit one wire frame per ACP update,
    // re-sending ~130 bytes of invariant preamble every time. Here the
    // stub streams 8 updates in a tight loop — well inside the debounce
    // window — so they must arrive as *fewer* frames than updates, with
    // every byte of text intact.
    const UPDATES: usize = 8;

    let env = Env::new().with_stub_acp_chunks(UPDATES);
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_c1", "cli_c1", "coalesce-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_c1").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws).await.expect("client `hello`");
    next_envelope(&mut ws).await.expect("client `presence`");

    let request = prompt_envelope("test-alpha", "hello");
    send_envelope(&mut ws, &request).await;

    let mut assembled = String::new();
    let mut frames = 0;
    loop {
        let frame = next_envelope(&mut ws).await.expect("expected a `reply`");
        assert_eq!(frame.id, request.id, "every chunk reuses the prompt id");
        match frame.body {
            Body::Reply(proto::ReplyBody {
                text, chunks, done, ..
            }) => {
                frames += 1;
                if let Some(t) = text {
                    assembled.push_str(&t);
                }
                for chunk in chunks {
                    assembled.push_str(&chunk);
                }
                if done {
                    break;
                }
            }
            other => panic!("expected Reply, got {other:?}"),
        }
    }

    // The invariant that must never break, whatever the batching:
    assert_eq!(
        assembled,
        "PONG".repeat(UPDATES),
        "no streamed text may be lost or reordered by coalescing"
    );
    // The point of the change: strictly fewer frames than updates.
    assert!(
        frames < UPDATES,
        "expected {UPDATES} updates to coalesce into fewer than {UPDATES} frames, got {frames}"
    );

    kill(child);
}

#[tokio::test]
async fn prompt_to_unknown_session_errors_not_silently_dropped() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p3", "cli_p3", "prompt-host2");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p3").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = prompt_envelope("no-such-session", "hi");
    send_envelope(&mut ws, &request).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected an `error` reply");
    assert_eq!(reply.id, request.id);
    match reply.body {
        Body::Error(ErrorBody { code, .. }) => assert_eq!(code, proto::CODE_UNKNOWN_SESSION),
        other => panic!("expected Error, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn interrupt_with_no_turn_in_flight_is_still_acked() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p4", "cli_p4", "interrupt-host");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p4").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = interrupt_envelope("test-alpha");
    send_envelope(&mut ws, &request).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected an `ack` reply");
    assert_eq!(reply.id, request.id);
    match reply.body {
        Body::Ack(proto::AckBody { of }) => assert_eq!(of.as_deref(), Some(request.id.as_str())),
        other => panic!("expected Ack, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn interrupt_to_unknown_session_errors_not_silently_dropped() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p5", "cli_p5", "interrupt-host2");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p5").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    let request = interrupt_envelope("no-such-session");
    send_envelope(&mut ws, &request).await;
    let reply = next_envelope(&mut ws)
        .await
        .expect("expected an `error` reply");
    assert_eq!(reply.id, request.id);
    match reply.body {
        Body::Error(ErrorBody { code, .. }) => assert_eq!(code, proto::CODE_UNKNOWN_SESSION),
        other => panic!("expected Error, got {other:?}"),
    }

    kill(child);
}

#[tokio::test]
async fn interrupting_one_session_over_the_wire_does_not_affect_its_sibling() {
    let env = Env::new().with_stub_acp_sessions();
    let (listener, url) = bind_local().await;
    env.write_credential(&url, "hlr_live_good", "tok_p6", "cli_p6", "interrupt-host3");

    let child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_p6").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    // Start a turn on beta, interrupt alpha (idle — a clean no-op that
    // still acks), then confirm beta's own turn completes untouched.
    send_envelope(&mut ws, &prompt_envelope("test-beta", "hello")).await;
    let interrupt_req = interrupt_envelope("test-alpha");
    send_envelope(&mut ws, &interrupt_req).await;

    let mut saw_ack = false;
    let mut saw_beta_done = false;
    for _ in 0..8 {
        if saw_ack && saw_beta_done {
            break;
        }
        let envelope = next_envelope(&mut ws).await.expect("expected a frame");
        match envelope.body {
            Body::Ack(proto::AckBody { of })
                if of.as_deref() == Some(interrupt_req.id.as_str()) =>
            {
                saw_ack = true;
            }
            Body::Reply(proto::ReplyBody { session, done, .. })
                if session == "test-beta" && done =>
            {
                saw_beta_done = true;
            }
            // A heartbeat `ping` genuinely can interleave here if this
            // test happens to straddle the (real, default) heartbeat
            // interval; answer it like a real server would so the
            // connection doesn't time itself out mid-test.
            Body::Ping(_) => {
                let pong = proto::pong_reply(&envelope.id, "server", "test-server");
                send_envelope(&mut ws, &pong).await;
            }
            Body::Reply(_) => continue,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert!(saw_ack, "expected an ack for the idle interrupt on alpha");
    assert!(saw_beta_done, "expected beta's turn to complete normally");

    kill(child);
}

// --- Single-instance guard (issue #52) -------------------------------------

#[tokio::test]
async fn second_run_refuses_to_start_while_first_is_live() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_lock1",
        "cli_lock1",
        "lock-host1",
    );

    let mut first = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_lock1").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    // A second `run` against the same state dir must refuse immediately,
    // not hang, retry, or start a second racing connection.
    let mut second = spawn_run_capturing_stderr(&env);
    let status = wait_for_exit(&mut second, Duration::from_secs(5))
        .expect("second `holler run` should refuse to start promptly, not hang");
    assert!(
        !status.success(),
        "expected a non-zero exit for a second instance against the same state dir"
    );
    let mut stderr = String::new();
    second
        .stderr
        .take()
        .expect("stderr was piped")
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.to_lowercase().contains("already"),
        "stderr should clearly explain the refusal, got: {stderr:?}"
    );

    // The first process must be completely unaffected by the second
    // instance's failed attempt: still alive, still observable.
    assert!(
        matches!(first.try_wait(), Ok(None)),
        "first `run` process should still be alive after a second instance was refused"
    );
    assert_eq!(
        env.status_json()["connected"],
        true,
        "first run's status should still report connected"
    );

    kill(first);
}

#[tokio::test]
async fn fresh_run_starts_normally_after_a_prior_run_was_killed_uncleanly() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_lock2",
        "cli_lock2",
        "lock-host2",
    );

    let mut first = spawn_run(&env);

    {
        let mut ws = accept_ws(&listener).await;
        expect_auth(&mut ws, "tok_lock2").await;
        send_envelope(&mut ws, &server_hello_envelope()).await;
        next_envelope(&mut ws)
            .await
            .expect("expected client `hello`");
        next_envelope(&mut ws)
            .await
            .expect("expected client `presence`");
        // Check `connected` while `ws` is still open: dropping it first
        // would race the client's own drop-detection, which can flip the
        // persisted state to `reconnecting` (and then get stuck there,
        // since nothing accepts its retry) before this ever gets to look.
        env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    }

    // Simulate a crash (SIGKILL), not a graceful `holler detach` — the
    // process gets no chance to release anything explicitly. The kernel
    // releasing the advisory `flock` the instant this process's file
    // descriptors close (even on SIGKILL) is exactly the property under
    // test: a stale lock from an unclean exit must not block a fresh run.
    first.kill().expect("failed to SIGKILL the first `run`");
    first.wait().expect("failed to reap the killed `run`");

    let second = spawn_run(&env);
    let mut ws2 = accept_ws(&listener).await;
    expect_auth(&mut ws2, "tok_lock2").await;
    send_envelope(&mut ws2, &server_hello_envelope()).await;
    next_envelope(&mut ws2)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws2)
        .await
        .expect("expected client `presence`");

    let status = env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);
    assert_eq!(status["client_id"], "cli_lock2");

    kill(second);
}

#[tokio::test]
async fn detach_works_with_the_instance_lock_held_and_releases_it_on_clean_exit() {
    let env = Env::new();
    let (listener, url) = bind_local().await;
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_lock3",
        "cli_lock3",
        "lock-host3",
    );

    let mut child = spawn_run(&env);

    let mut ws = accept_ws(&listener).await;
    expect_auth(&mut ws, "tok_lock3").await;
    send_envelope(&mut ws, &server_hello_envelope()).await;
    next_envelope(&mut ws)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws)
        .await
        .expect("expected client `presence`");

    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    // Regression: `holler detach` must still find and signal the live
    // `run` process with the new lock in place.
    let detach_out = env.cmd().arg("detach").output().unwrap();
    assert!(detach_out.status.success(), "{detach_out:?}");
    assert!(String::from_utf8(detach_out.stdout)
        .unwrap()
        .contains("detached"));

    let status = wait_for_exit(&mut child, Duration::from_secs(5)).expect(
        "`holler run` should exit once detach is requested, even with the instance lock held",
    );
    assert!(status.success(), "expected a clean exit on detach");

    // Detach deletes the credential; re-persist it (same as this file's
    // other fixtures stand in for a real `join`) so a fresh `run` has
    // something to resume with, and confirm the lock the just-exited
    // process held is fully released by its own clean exit — not only on
    // a crash (see the SIGKILL-based test above).
    env.write_credential(
        &url,
        "hlr_live_good",
        "tok_lock3",
        "cli_lock3",
        "lock-host3",
    );
    let second = spawn_run(&env);
    let mut ws2 = accept_ws(&listener).await;
    expect_auth(&mut ws2, "tok_lock3").await;
    send_envelope(&mut ws2, &server_hello_envelope()).await;
    next_envelope(&mut ws2)
        .await
        .expect("expected client `hello`");
    next_envelope(&mut ws2)
        .await
        .expect("expected client `presence`");
    env.wait_for_status(STATUS_BUDGET, |doc| doc["connected"] == true);

    kill(second);
}
