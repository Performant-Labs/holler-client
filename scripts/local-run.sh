#!/usr/bin/env bash
#
# app:local:run — build and run the joined holler-client session.
#
# Usage: ./scripts/local-run.sh
#        HOLLER_CONFIG=custom.toml ./scripts/local-run.sh
#        HOLLER_DEBUG=quiet ./scripts/local-run.sh
#
# Assumes `holler join` has already been run (if not, `holler run`'s own
# error tells you the exact command to run first). Runs in the foreground.
# No --debug is passed here — the wrapper doesn't impose a verbosity
# opinion; holler run's own default/HOLLER_DEBUG/--debug precedence
# applies exactly as it would running the binary directly.
#
# Config file: $HOLLER_CONFIG if set, else session.toml or sessions.toml
# (whichever exists — both names appear in this repo's own docs/examples).

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [ -n "${HOLLER_CONFIG:-}" ]; then
  config="$HOLLER_CONFIG"
elif [ -f session.toml ]; then
  config="session.toml"
elif [ -f sessions.toml ]; then
  config="sessions.toml"
else
  echo "error: no session config found — create session.toml (or sessions.toml), or set HOLLER_CONFIG=<path>" >&2
  exit 1
fi

cargo build --release
exec ./target/release/holler run --config "$config"
