#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGET="${TARGET:-x86_64-unknown-linux-musl}"
OUT_DIR="${OUT_DIR:-dist}"
PYTHON="${PYTHON:-python3.12}"
USE_DOCKER="${USE_DOCKER:-1}"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-}"
MATURIN_IMAGE="${MATURIN_IMAGE:-ghcr.io/pyo3/maturin:v1.7}"
WHEEL_COMPATIBILITY="${WHEEL_COMPATIBILITY:-manylinux_2_28}"
USE_ZIG="${USE_ZIG:-0}"

mkdir -p "$OUT_DIR"

echo "==> Building static Linux server for $TARGET"
if ! rustup target list --installed | grep -qx "$TARGET"; then
  rustup target add "$TARGET"
fi

PYO3_NO_PYTHON=1 cargo build \
  --release \
  --locked \
  --no-default-features \
  --features server,server-binary \
  --bin alocals3-server \
  --target "$TARGET"

SERVER_SRC="target/$TARGET/release/alocals3-server"
SERVER_DST="$OUT_DIR/alocals3-server-linux-${TARGET}"
cp "$SERVER_SRC" "$SERVER_DST"
chmod +x "$SERVER_DST"

if command -v ldd >/dev/null 2>&1; then
  echo "==> Linkage check"
  ldd "$SERVER_DST" || true
fi

echo "==> Building Linux cp312 abi3 wheel"
MATURIN_ARGS=(
  build
  --release
  --locked
  --features extension-module
  --compatibility "$WHEEL_COMPATIBILITY"
)
if [[ "$USE_ZIG" == "1" ]]; then
  MATURIN_ARGS+=(--zig)
fi

if [[ "$USE_DOCKER" == "1" ]]; then
  if [[ -z "$CONTAINER_RUNTIME" ]]; then
    if command -v docker >/dev/null 2>&1; then
      CONTAINER_RUNTIME="docker"
    elif command -v podman >/dev/null 2>&1; then
      CONTAINER_RUNTIME="podman"
    else
      echo "docker or podman is required when USE_DOCKER=1" >&2
      exit 1
    fi
  fi
  if ! command -v "$CONTAINER_RUNTIME" >/dev/null 2>&1; then
    echo "container runtime not found: $CONTAINER_RUNTIME" >&2
    exit 1
  fi
  DOCKER_OUT_DIR="$OUT_DIR"
  if [[ "$OUT_DIR" != /* ]]; then
    DOCKER_OUT_DIR="/io/$OUT_DIR"
  fi
  "$CONTAINER_RUNTIME" run --rm \
    -v "$ROOT_DIR":/io \
    -w /io \
    "$MATURIN_IMAGE" \
    maturin "${MATURIN_ARGS[@]}" \
      --interpreter python3.12 \
      --out "$DOCKER_OUT_DIR"
else
  "$PYTHON" -m pip install --upgrade "maturin>=1.7,<2"
  "$PYTHON" -m maturin "${MATURIN_ARGS[@]}" \
    --interpreter "$PYTHON" \
    --out "$OUT_DIR"
fi

echo "==> Artifacts"
ls -lh "$OUT_DIR"/alocals3-server-linux-"$TARGET" "$OUT_DIR"/*.whl
