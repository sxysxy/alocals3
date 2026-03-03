#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python examples/bench_stress.py \
  --endpoint "${ENDPOINT:-http://127.0.0.1:8000}" \
  --duration "${DURATION:-30}" \
  --concurrency "${CONCURRENCY:-50}" \
  --key-space "${KEY_SPACE:-1000}" \
  --object-size "${OBJECT_SIZE:-4096}" \
  --write-ratio "${WRITE_RATIO:-0.5}"
