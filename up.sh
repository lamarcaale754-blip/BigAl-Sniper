#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "Cleaning environment..."
pkill -f bigalbot_customizable.py || true
pkill -f bigal-executor || true
pkill -f sidecar_wss.js || true
rm -f /tmp/bigal_*.ipc /tmp/bigal_grpc_sidecar.ipc || true

echo "Setting CPU affinity for m7i-flex.large..."
if ! command -v taskset >/dev/null 2>&1; then
  echo "taskset not found (install util-linux). Running without pinning."
  node sidecar/sidecar_wss.js > sidecar.log 2>&1 &
  python3 bigalbot_customizable.py > bot.log 2>&1 &
else
  # Core 0: discovery sidecar, Core 1: strategy + execution.
  taskset -c 0 node sidecar/sidecar_wss.js > sidecar.log 2>&1 &
  taskset -c 1 python3 bigalbot_customizable.py > bot.log 2>&1 &
fi

echo "Sniper online. Core 0: Discovery | Core 1: Execution."
