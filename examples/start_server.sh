#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
DB_URL="${DB_URL:-sqlite:///$ROOT_DIR/alocals3.db}"
STORAGE_ROOT="${STORAGE_ROOT:-$ROOT_DIR/data}"

if ! python - <<'PY' >/dev/null 2>&1
import importlib
for mod in ("fastapi", "uvicorn", "sqlalchemy"):
    importlib.import_module(mod)
PY
then
  echo "Missing runtime dependencies. Run: pip install -r requirements.txt" >&2
  exit 1
fi

python -m alocals3.server \
  --host "$HOST" \
  --port "$PORT" \
  --database-url "$DB_URL" \
  --storage-root "$STORAGE_ROOT"
