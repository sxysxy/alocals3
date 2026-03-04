#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENDPOINT="${ENDPOINT:-http://127.0.0.1:8000}"
BUCKET="${BUCKET:-cache-bucket}"
KEY="${KEY:-cache/item.txt}"
WORK_DIR="${WORK_DIR:-./examples/.tmp}"

mkdir -p "$WORK_DIR"
SRC_FILE="$WORK_DIR/cache-item.txt"

echo "cache demo $(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$SRC_FILE"

python -m alocals3.client --endpoint "$ENDPOINT" CREATE_BUCKET "$BUCKET" >/dev/null 2>&1 || true
python -m alocals3.client --endpoint "$ENDPOINT" PUT "$BUCKET" "$KEY" "$SRC_FILE" --content-type text/plain >/dev/null

HEADERS_FILE="$WORK_DIR/headers.txt"
curl -sS -D "$HEADERS_FILE" -o /dev/null "$ENDPOINT/s3/$BUCKET/$KEY"

ETAG="$(grep -i '^etag:' "$HEADERS_FILE" | head -n1 | cut -d' ' -f2- | tr -d '\r')"
LAST_MODIFIED="$(grep -i '^last-modified:' "$HEADERS_FILE" | head -n1 | cut -d' ' -f2- | tr -d '\r')"

if [[ -z "${ETAG}" || -z "${LAST_MODIFIED}" ]]; then
  echo "failed to parse ETag/Last-Modified from response headers:"
  cat "$HEADERS_FILE"
  exit 1
fi

echo "ETag: $ETAG"
echo "Last-Modified: $LAST_MODIFIED"
echo "If-None-Match: $ETAG"
echo "If-Modified-Since: $LAST_MODIFIED"

echo "check 304 by If-None-Match"
STATUS1="$(curl -sS -o /dev/null -w '%{http_code}' -H "If-None-Match: $ETAG" "$ENDPOINT/s3/$BUCKET/$KEY")"
echo "status=$STATUS1"

echo "check 304 by If-Modified-Since"
STATUS2="$(curl -sS -o /dev/null -w '%{http_code}' -H "If-Modified-Since: $LAST_MODIFIED" "$ENDPOINT/s3/$BUCKET/$KEY")"
echo "status=$STATUS2"

# Comment the following two lines to keep the test data
python -m alocals3.client --endpoint "$ENDPOINT" DELETE "$BUCKET" "$KEY" >/dev/null
python -m alocals3.client --endpoint "$ENDPOINT" DELETE_BUCKET "$BUCKET" >/dev/null
