# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and version
numbers follow the policy in [holler-server's ADR 0014](https://github.com/Performant-Labs/holler-server/blob/main/docs/adr/ADR-0014.md)
(standard SemVer per crate, starting at 0.1.0; this repo's [ADR 0003](docs/adr/ADR-0003.md)
points at it). This file starts from that decision forward — it is not backfilled with
pre-decision history.

## [Unreleased]

## [0.1.1] - 2026-09-07

### Enhancements

- Attach mode detects a real OpenCode `question`/permission gate blocking a session's
  turn (polling `GET /question`/`GET /permission`) and answers it via the new
  `holler-server answer <session> <choice>` wire path (issue #133) — surfaced as
  `DriverStatus::Blocked`. Verified live end-to-end against a real `opencode serve`
  instance: a real `question` tool call was detected, answered, and the turn resumed
  correctly.

### Known Issues

- Spawn-mode (ACP) sessions cannot answer a question/permission yet — only attach-mode
  sessions can (`DriverError::AnswerUnsupported`). ACP's `session/request_permission`
  needs inbound-RPC plumbing the driver doesn't have.
- Multi-question requests (more than one question per single request) aren't answerable
  via a single `choice` argument.
- The `Blocked` status isn't yet surfaced in `presence`/`roster`/`status` documents — the
  detect-and-reply mechanism works, but an operator watching `roster` alone can't yet see
  that a session is blocked on a question versus just busy.

## [0.1.0] - 2026-09-07

First tagged release. Covers everything since this repo's inception — this file's own
stated starting point, not a delta from a prior tag (there isn't one yet).

### Enhancements

- Real join flow: `holler join`/`holler detach`/`holler status` redeem a one-time join
  token over the wire and persist session identity locally.
- WebSocket session transport with heartbeat and automatic reconnect (`wss://` reachable
  via TLS on tokio-tungstenite).
- ACP driver: spawns a configured coding agent and drives `session/new` + `prompt` over
  the Agent Client Protocol (v1's default harness is OpenCode).
- Session manager: interrupt mapping and busy-turn queueing, so `holler interrupt` and
  back-to-back `say`s behave predictably.
- Attach mode: a session can attach to an already-running harness process (e.g. inside a
  Herdr pane) over HTTP instead of being spawned; `status`/`support`/`caps` all advertise
  attach sessions correctly (ADR 0005).
- Answer query support for `status`/`support`/`caps` without invoking the LLM.
- `--version`/`-V`.
- Debug logging: `none`/`quiet`/`noisy` levels with secret redaction, emission
  timestamps, `text`/`json` output formats, and per-line `component` tags attributing a
  line to the layer that emitted it, without prior codebase knowledge.
- `holler run` refuses to start a second instance against the same state rather than
  racing it.
- Streamed reply chunks are coalesced into single frames before being shown.
- The `hlrclnt-*` test-case catalog and its shared harness (`scripts/test-run.rb`, in
  holler-server) plus `docs/releasing.md`'s release process — project-facing rather than
  binary-facing, but part of what this release actually ships as a maintained project.

### Breaking Changes

- The crate is `holler-client`, but the compiled binary is named `holler`, not
  `holler-client` — deliberate, to avoid an install collision with `holler-server`'s own
  `holler-server` binary. There is no prior tagged release this could break, but noted
  here since it's exactly the kind of change ADR 0014 classifies as breaking.

### Bug Fixes

- Auth was sending `client_id` where the server expects `token_id`.
- Heartbeat interval corrected from 20s to 15s per research memo.
- Attach-mode sessions were never getting a real `SessionManager` driver wired up.

### Known Issues

- No Windows support — `holler-client` does not compile on Windows (`instance_lock.rs`
  uses Unix-only APIs): [#60](https://github.com/Performant-Labs/holler-client/issues/60)
- Interrupting one session can break an unrelated sibling session's in-flight `say` (WS
  reset, cross-contamination), root cause partly in this repo's `connection.rs`:
  [holler-server#202](https://github.com/Performant-Labs/holler-server/issues/202)
- Roster can stay stuck reporting "reconnecting" with a growing `LAST_SEEN` despite
  active, successful `say` traffic:
  [holler-server#203](https://github.com/Performant-Labs/holler-server/issues/203)
- After an interrupt, the next `say` can return a reply continuing the cancelled turn
  instead of answering the new prompt:
  [holler-server#204](https://github.com/Performant-Labs/holler-server/issues/204)
