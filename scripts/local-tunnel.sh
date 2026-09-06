#!/usr/bin/env bash
#
# app:local:tunnel — open the SSH tunnel to a remote holler-server's
# loopback port (required until wss lands — see holler-server ADR 0004/0010).
#
# Usage: HOLLER_SERVER_HOST=io ./scripts/local-tunnel.sh
#
# Backgrounds `ssh -N -L 127.0.0.1:41807:127.0.0.1:41807 <host>`. Skips
# (rather than double-tunneling) if something already owns the local port.

set -euo pipefail

if [ -z "${HOLLER_SERVER_HOST:-}" ]; then
  echo "error: HOLLER_SERVER_HOST is not set — export it to the remote host running holler-server serve (e.g. HOLLER_SERVER_HOST=io)" >&2
  exit 1
fi

if lsof -nP -iTCP:41807 -sTCP:LISTEN > /dev/null 2>&1; then
  echo "app:local:tunnel: something is already listening on 127.0.0.1:41807 — assuming a tunnel is already up (lsof -nP -i:41807 to check)"
  exit 0
fi

ssh -N -L 127.0.0.1:41807:127.0.0.1:41807 "$HOLLER_SERVER_HOST" &
disown
echo "app:local:tunnel: tunnel to $HOLLER_SERVER_HOST backgrounded (pid $!)"
