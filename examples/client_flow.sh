#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINT="${ENDPOINT:-http://127.0.0.1:8000}"
BUCKET="${BUCKET:-demo-bucket}"
KEY="${KEY:-docs/hello.txt}"
WORK_DIR="${WORK_DIR:-./examples/.tmp}"

mkdir -p "$WORK_DIR"
SRC_FILE="$WORK_DIR/hello.txt"
DST_FILE="$WORK_DIR/downloaded.txt"

echo "hello from alocals3 at $(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$SRC_FILE"

echo "[1/7] health"
python -m alocals3.client --endpoint "$ENDPOINT" HEALTH

echo "[2/7] create bucket: $BUCKET"
python -m alocals3.client --endpoint "$ENDPOINT" CREATE_BUCKET "$BUCKET" >/dev/null 2>&1 || true

echo "[3/7] put object: $BUCKET/$KEY"
python -m alocals3.client --endpoint "$ENDPOINT" PUT "$BUCKET" "$KEY" "$SRC_FILE" --content-type text/plain

echo "[4/7] list buckets"
python -m alocals3.client --endpoint "$ENDPOINT" LIST_BUCKETS

echo "[5/7] list objects"
python -m alocals3.client --endpoint "$ENDPOINT" LIST_OBJECTS "$BUCKET"

echo "[6/7] download object"
python -m alocals3.client --endpoint "$ENDPOINT" GET "$BUCKET" "$KEY" "$DST_FILE"
cat "$DST_FILE"

echo "[7/7] cleanup"
python -m alocals3.client --endpoint "$ENDPOINT" DELETE "$BUCKET" "$KEY"
python -m alocals3.client --endpoint "$ENDPOINT" DELETE_BUCKET "$BUCKET"

echo "done"
