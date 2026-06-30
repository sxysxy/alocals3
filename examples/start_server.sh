#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
DB_URL="${DB_URL:-sqlite:///$ROOT_DIR/alocals3.db}"
STORAGE_ROOT="${STORAGE_ROOT:-$ROOT_DIR/data}"
SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/target/release/alocals3-server}"

if [[ ! -x "$SERVER_BIN" ]]; then
  echo "Missing server binary. Building: $SERVER_BIN" >&2
  PYO3_NO_PYTHON=1 cargo build \
    --release \
    --no-default-features \
    --features server,server-binary \
    --bin alocals3-server
fi

"$SERVER_BIN" \
  --host "$HOST" \
  --port "$PORT" \
  --database-url "$DB_URL" \
  --storage-root "$STORAGE_ROOT"
