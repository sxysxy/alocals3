#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${OUT_DIR:-dist}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/arm64}"
CONTAINER_NETWORK="${CONTAINER_NETWORK:-host}"
MATURIN_IMAGE="${MATURIN_IMAGE:-ghcr.io/pyo3/maturin:latest}"
WHEEL_COMPATIBILITY="${WHEEL_COMPATIBILITY:-manylinux_2_28}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required; no host cross-compiler is needed." >&2
  exit 1
fi

case "$OUT_DIR" in
  /* | ../* | */../* | */..)
    echo "OUT_DIR must be a path inside the repository: $OUT_DIR" >&2
    exit 1
    ;;
esac

CONTAINER_ARGS=(run --rm --platform "$DOCKER_PLATFORM")
if [[ -n "$CONTAINER_NETWORK" ]]; then
  CONTAINER_ARGS+=(--network "$CONTAINER_NETWORK")
fi

echo "==> Building Linux aarch64 artifacts in a $DOCKER_PLATFORM container"
docker "${CONTAINER_ARGS[@]}" \
  -v "$ROOT_DIR":/io \
  -w /io \
  -e "OUT_DIR=$OUT_DIR" \
  -e "WHEEL_COMPATIBILITY=$WHEEL_COMPATIBILITY" \
  -e CARGO_TARGET_DIR=/io/target/docker-linux-aarch64 \
  --entrypoint /bin/bash \
  "$MATURIN_IMAGE" \
  -euo pipefail -c '
    machine="$(uname -m)"
    if [[ "$machine" != "aarch64" && "$machine" != "arm64" ]]; then
      echo "expected an aarch64 container, got: $machine" >&2
      exit 1
    fi

    mkdir -p "$OUT_DIR"

    echo "==> Building Linux aarch64 server"
    PYO3_NO_PYTHON=1 cargo build \
      --release \
      --locked \
      --no-default-features \
      --features server,server-binary \
      --bin alocals3-server

    server_src="$CARGO_TARGET_DIR/release/alocals3-server"
    install -m 755 "$server_src" "$OUT_DIR/alocals3-server"
    rm -rf /io/alocals3/bin
    mkdir -p /io/alocals3/bin
    install -m 755 "$server_src" /io/alocals3/bin/alocals3-server

    echo "==> Building Linux aarch64 cp312 abi3 wheel"
    maturin build \
      --release \
      --locked \
      --features extension-module \
      --compatibility "$WHEEL_COMPATIBILITY" \
      --interpreter python3.12 \
      --out "$OUT_DIR"
  '

echo "==> Artifact metadata"
file "$OUT_DIR/alocals3-server"
ls -lh "$OUT_DIR/alocals3-server" "$OUT_DIR"/*aarch64.whl
