# holler-client

Thin client for the Holler talk circuit (**Rust**). Joins holler-server with a minted token and drives local coding-agent bodies (v1: OpenCode). This machine does **not** run Herdr. Speaks **[Holler protocol v1](https://github.com/Performant-Labs/holler-server/blob/main/docs/protocol/v1.md)**; answers `query` / `support` from local probes, not a model.

## Quickstart

This repo is the far-machine half of the circuit — it needs a [holler-server](https://github.com/Performant-Labs/holler-server) somewhere to join. On that server, run `holler-server token mint` first; it prints a ready-to-paste `holler join` command.

```
# 1. Build
cargo build --release

# 2. Redeem the token the server minted, and persist this machine's identity
holler join --server ws://myhost.example.com:41807 --token <token_id>:<secret>
#   joined ws://myhost.example.com:41807 as client_id=cli_...

# 3. Declare the local sessions this machine offers — there is no default,
#    every session is explicit. Each one names a harness and the command to
#    spawn for it; v1's default harness is OpenCode via ACP:
cat > sessions.toml <<'EOF'
[[session]]
name = "alpha"
harness = "opencode"
command = ["opencode", "acp"]

[[session]]
name = "beta"
harness = "opencode"
command = ["opencode", "acp"]
EOF

# 4. Connect and stay connected — this spawns "alpha" and "beta" as real
#    opencode subprocesses, then holds the WebSocket open (auth, hello,
#    heartbeat, reconnect with backoff) until you Ctrl-C or `holler detach`
holler run --config sessions.toml
```

From here, the server side drives the actual conversation (`holler-server roster`, `holler-server say <session> "..."`, `holler-server interrupt <session>`) — see [holler-server](https://github.com/Performant-Labs/holler-server)'s README for that half.

Other commands, run locally on this machine (no live `run` process required for these — they answer from local probes, not a model):

| Command | Job |
| --- | --- |
| `holler status` | This process's own status document — connected/reconnecting, confirmed harnesses, session list |
| `holler support <feature>` | Does this binary support a protocol feature (`ping`, `query`, ...) or harness (`opencode`, `claude`, ...) right now |
| `holler caps` | Full capability document: status plus every known feature/harness's support answer |
| `holler query <cmd> [args...]` | General form of the above, e.g. `holler query protocol` |
| `holler detach` | Close the live connection (if `run` is holding one) and delete the persisted credential |

`--config <path>` works with any of these, not just `run` — pass the same session file so `status`/`caps` report the sessions you actually intend to run, not an empty list.

### Dev scripts

Wraps the connect side of a manual cross-machine test (tunnel + run) per the org's `object:sub-object:verb` script-naming convention ([`Performant-Labs/playbook`](https://github.com/Performant-Labs/playbook/blob/main/frameworks/node/npm-scripts.md)). This crate has no `package.json`, so `./scripts/run <name>` is the `npm run <name>` equivalent — the actual command you type, not just a documented mapping:

```
HOLLER_SERVER_HOST=io ./scripts/run app:local:tunnel
./scripts/run app:local:run
```

| Command name | Does |
|---|---|
| `app:local:tunnel` | Open the SSH tunnel to `$HOLLER_SERVER_HOST`'s loopback `holler-server serve` port (required until `wss` lands — see holler-server ADR 0004/0010); no-ops if something already owns the port |
| `app:local:run` | Build, then `holler run --debug=noisy` in the foreground against `session.toml`/`sessions.toml` (or `$HOLLER_CONFIG`). Requires an existing `holler join` — its own error tells you the command if not |
| `app:local:stop` | `holler detach`, then close the tunnel from `app:local:tunnel` |

Each maps to a same-named `scripts/local-*.sh` file if you'd rather call the script directly. `--debug=noisy` is the default for `app:local:run` deliberately — this is the dev/test entry point, and full frame visibility (secrets already redacted) is exactly what you want here.

### Debug output

Two global flags control the debug log, each overriding its environment variable when both are set:

| Flag | Env | Values | Default |
|---|---|---|---|
| `--debug` | `HOLLER_DEBUG` | `none`, `quiet`, `noisy` | `none` |
| `--log-format` | `HOLLER_LOG_FORMAT` | `text`, `json` | `text` |

An invalid value at whichever level wins is an error — it never silently falls back to the default. Redaction applies identically in both formats: the join token, client credential, connect ticket, and `Authorization` header are never printed in the clear.

`text` is a fixed-width console line for a human tailing a session — emission timestamp first, then direction, frame type, and `k=v` pairs. At `noisy` the redacted frame JSON is appended last, so a line's frame can still be copied out and replayed:

```
2026-09-06T20:59:54.712345Z DEBUG -> auth       id=c14fb1a960b3 peer=tok_fb2d7e54 {"v":1,"type":"auth",...}
2026-09-06T20:59:54.883012Z DEBUG <- hello      id=12cfd6a8e401 peer=server       {"v":1,"type":"hello",...}
```

`json` is JSON Lines — the whole line is one object, so `jq`/Vector/Loki/Datadog can ingest the stream directly, and the frame is nested under `frame` (`jq .frame`):

```
holler run --debug=noisy --log-format=json 2>debug.log
jq -e . debug.log > /dev/null   # every line parses
jq -r .ts debug.log | sort -c   # emission timestamps are ordered
```

#### Severity is independent of `--debug`

`--debug` controls how much of the *frame trace* you get. It does not gate operational facts. Every line carries a `level`:

| `level` | Gated by | Examples |
|---|---|---|
| `debug` | `--debug` (nothing at `none`) | frames in/out, connect/detach lifecycle |
| `info` | never — only shaped by `--log-format` | `logging_started` |
| `warn` | never — only shaped by `--log-format` | connection dropped, connect failed, local sessions failed to start |

So a dropped connection is reported **whether or not `--debug` is set** — it was an unconditional message before this existed, and it is the line you would build an alert on:

```
$ holler run --log-format=json          # note: no --debug at all
{"ts":"...","level":"warn","verbosity":"none","type":"conn","event":"dropped","reason":"socket read error: ..."}
```

```
$ holler run                            # no flags at all, default text
2026-09-06T21:43:22.975623Z WARN     conn       event=dropped reason=socket read error: ...
```

#### stderr also carries CLI errors

One caveat for anyone pointing a log shipper at this: a fatal CLI error prints as plain text on stderr immediately before a non-zero exit, because it is user-facing command output rather than a log line:

```
error: join failed: connect to ws://... failed: IO error: Connection refused (os error 111)
```

Everything the logger emits is JSON in `json` mode, but stderr as a whole is therefore not guaranteed pure JSONL. Redirect the log to its own file (`2>debug.log`) and ship that, or filter the single `error: ` line, rather than assuming the raw stream parses end to end.

The leading `ts` is an **emission** timestamp — when this process logged the line, from this host's clock — at fixed RFC 3339 microsecond precision, so the column is genuinely fixed-width. That is deliberately not the frame's own `ts`, which is the peer's claim from the peer's clock; the two diverge enough in practice (~180ms measured cross-machine) that sorting a handshake by frame `ts` reorders it against causality. Sort a log by `ts`, not by `.frame.ts`.

## Architecture

`holler join` and `holler run` are two separate steps, not one. `join` is a one-shot redeem: it exchanges the server's one-time token for a persisted `client_id` + long-lived credential, then exits — it does not touch any local agent. `run` is the long-lived process: it reads the session config, spawns one ACP subprocess per configured session (via [`agent-client-protocol`](https://github.com/agentclientprotocol/rust-sdk) v1, JSON-RPC over stdio), and only then opens the live WebSocket to the server. A hung or non-conformant harness can't block the connection itself from coming up — `run` still answers `ping`/`query`/`hello` either way, it just can't route `prompt`/`interrupt` to whichever session failed to spawn.

A new harness is a new `command` row in the session config, not a new client release — see [ADR 0002](docs/adr/ADR-0002.md). Out-of-tree adapters (a harness that doesn't speak ACP natively) are just another binary on `command`.

See [issue #1](https://github.com/Performant-Labs/holler-client/issues/1) (orientation), [dev environment](https://github.com/Performant-Labs/holler-server/blob/main/docs/dev-env.md) (both repos), [how they talk](https://github.com/Performant-Labs/holler-server/blob/main/docs/protocol/talk.md), and [docs/adr](docs/adr/README.md).

## Status and license

The [v1 epic](https://github.com/Performant-Labs/holler-client/issues/22) is complete: all 10 builder-order stories are closed, and the shared acceptance gate — join, roster, independent prompts to two live sessions, cooperative interrupt with sibling-session isolation, and clean detach — has passed end-to-end against real OpenCode sessions.

License is `AGPL-3.0-or-later` (see [`LICENSE`](LICENSE) and [ADR-0004](https://github.com/Performant-Labs/holler-client/issues/5)) — outside PRs are welcome, see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Companion repo

[holler-server](https://github.com/Performant-Labs/holler-server) — the hub half of the circuit: mints join tokens, hosts the roster, and routes `say`/`interrupt` to whichever client currently owns a session.
