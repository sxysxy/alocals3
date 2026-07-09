#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python examples/bench_stream_multiplex.py \
  --endpoint "${ENDPOINT:-http://127.0.0.1:8000}" \
  --bucket "${BUCKET:-bench-stream-multiplex}" \
  --key "${KEY:-large/stream-multiplex.bin}" \
  --writer-key "${WRITER_KEY:-large/stream-multiplex-writer.bin}" \
  --size-mib "${SIZE_MIB:-256}" \
  --chunk-mib "${CHUNK_MIB:-8}" \
  --resume-after-mib "${RESUME_AFTER_MIB:-64}" \
  --idle-secs "${IDLE_SECS:-12}" \
  --probe-interval "${PROBE_INTERVAL:-0.25}" \
  --timeout "${TIMEOUT:-2}" \
  ${DISABLE_PROXY:+--disable-proxy} \
  ${KEEP_OBJECTS:+--keep-objects}
