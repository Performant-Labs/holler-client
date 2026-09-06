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

## Architecture

`holler join` and `holler run` are two separate steps, not one. `join` is a one-shot redeem: it exchanges the server's one-time token for a persisted `client_id` + long-lived credential, then exits — it does not touch any local agent. `run` is the long-lived process: it reads the session config, spawns one ACP subprocess per configured session (via [`agent-client-protocol`](https://github.com/agentclientprotocol/rust-sdk) v1, JSON-RPC over stdio), and only then opens the live WebSocket to the server. A hung or non-conformant harness can't block the connection itself from coming up — `run` still answers `ping`/`query`/`hello` either way, it just can't route `prompt`/`interrupt` to whichever session failed to spawn.

A new harness is a new `command` row in the session config, not a new client release — see [ADR 0002](docs/adr/ADR-0002.md). Out-of-tree adapters (a harness that doesn't speak ACP natively) are just another binary on `command`.

See [issue #1](https://github.com/Performant-Labs/holler-client/issues/1) (orientation), [dev environment](https://github.com/Performant-Labs/holler-server/blob/main/docs/dev-env.md) (both repos), [how they talk](https://github.com/Performant-Labs/holler-server/blob/main/docs/protocol/talk.md), and [docs/adr](docs/adr/README.md).

## Status and license

The [v1 epic](https://github.com/Performant-Labs/holler-client/issues/22) is complete: all 10 builder-order stories are closed, and the shared acceptance gate — join, roster, independent prompts to two live sessions, cooperative interrupt with sibling-session isolation, and clean detach — has passed end-to-end against real OpenCode sessions.

License is `AGPL-3.0-or-later` (see [`LICENSE`](LICENSE) and [ADR-0004](https://github.com/Performant-Labs/holler-client/issues/5)) — outside PRs are welcome, see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Companion repo

[holler-server](https://github.com/Performant-Labs/holler-server) — the hub half of the circuit: mints join tokens, hosts the roster, and routes `say`/`interrupt` to whichever client currently owns a session.
