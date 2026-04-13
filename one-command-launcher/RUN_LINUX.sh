#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
REQ="$(cd "$(dirname "$0")" && pwd)/requirements.txt"

if [[ ! -x "venv/bin/python" ]]; then
  echo "[BigAl] Creating Python venv..."
  python3 -m venv venv
fi
# shellcheck disable=SC1091
source "venv/bin/activate"
echo "[BigAl] Installing / updating dependencies..."
pip install -q -r "$REQ"

BIN="rust/bigal-executor/target/release/bigal-executor"
if [[ ! -x "$BIN" ]] && [[ -f "rust/bigal-executor/Cargo.toml" ]]; then
  echo "[BigAl] Building Rust executor (first run may take several minutes)..."
  (cd rust/bigal-executor && cargo build --release)
fi

echo "[BigAl] Starting dashboard + bot..."
exec python "$(dirname "$0")/run_bot.py" "$@"
