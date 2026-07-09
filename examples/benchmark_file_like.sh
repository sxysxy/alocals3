#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python examples/bench_file_like.py \
  --endpoint "${ENDPOINT:-http://127.0.0.1:8000}" \
  --bucket "${BUCKET:-bench-file-like}" \
  --key "${KEY:-large/file-like.bin}" \
  --size-mib "${SIZE_MIB:-1024}" \
  --chunk-mib "${CHUNK_MIB:-8}" \
  --repeats "${REPEATS:-1}" \
  --timeout "${TIMEOUT:-300}" \
  ${DISABLE_PROXY:+--disable-proxy} \
  ${KEEP_OBJECT:+--keep-object}
