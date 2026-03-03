#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

python examples/bench_consistency.py \
  --endpoint "${ENDPOINT:-http://127.0.0.1:8000}" \
  --duration "${DURATION:-20}" \
  --writers "${WRITERS:-4}" \
  --readers "${READERS:-4}" \
  --deleters "${DELETERS:-2}" \
  --payload-size "${PAYLOAD_SIZE:-2048}"
