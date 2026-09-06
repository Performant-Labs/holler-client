#!/usr/bin/env bash
#
# app:local:stop — detach the live session and close the SSH tunnel.
#
# Usage: ./scripts/local-stop.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

./target/release/holler detach

pid="$(pgrep -f "ssh -N -L 127.0.0.1:41807:127.0.0.1:41807" || true)"
if [ -n "$pid" ]; then
  kill "$pid"
  echo "app:local:stop: tunnel closed (pid $pid)"
else
  echo "app:local:stop: no tunnel found"
fi
