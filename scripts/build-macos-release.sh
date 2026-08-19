#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGET="${TARGET:-aarch64-apple-darwin}"
OUT_DIR="${OUT_DIR:-dist}"
PYTHON="${PYTHON:-python3.12}"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"

mkdir -p "$OUT_DIR"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script must run on macOS with Xcode command line tools installed." >&2
  exit 1
fi

echo "==> Building macOS server for $TARGET with MACOSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET"
if ! rustup target list --installed | grep -qx "$TARGET"; then
  rustup target add "$TARGET"
fi

PYO3_NO_PYTHON=1 cargo build \
  --release \
  --locked \
  --no-default-features \
  --features server,server-binary \
  --bin alocals3-server \
  --bin alocals3-migrate2pg \
  --target "$TARGET"

SERVER_SRC="target/$TARGET/release/alocals3-server"
SERVER_DST="$OUT_DIR/alocals3-server"
MIGRATE_SRC="target/$TARGET/release/alocals3-migrate2pg"
MIGRATE_DST="$OUT_DIR/alocals3-migrate2pg"
cp "$SERVER_SRC" "$SERVER_DST"
cp "$MIGRATE_SRC" "$MIGRATE_DST"
chmod +x "$SERVER_DST"
chmod +x "$MIGRATE_DST"
rm -rf "$ROOT_DIR/alocals3/bin"
mkdir -p "$ROOT_DIR/alocals3/bin"
cp "$SERVER_SRC" "$ROOT_DIR/alocals3/bin/alocals3-server"
cp "$MIGRATE_SRC" "$ROOT_DIR/alocals3/bin/alocals3-migrate2pg"
chmod +x "$ROOT_DIR/alocals3/bin/alocals3-server"
chmod +x "$ROOT_DIR/alocals3/bin/alocals3-migrate2pg"

echo "==> Building macOS cp312 abi3 wheel for $TARGET"
"$PYTHON" -m pip install --upgrade "maturin>=1.7,<2"
"$PYTHON" -m maturin build \
  --release \
  --locked \
  --features extension-module \
  --interpreter "$PYTHON" \
  --target "$TARGET" \
  --out "$OUT_DIR"

echo "==> Artifact metadata"
file "$SERVER_DST" "$MIGRATE_DST"
otool -l "$SERVER_DST" | awk '/LC_BUILD_VERSION/{show=1} show{print} /sdk/{if(show){show=0}}'
ls -lh "$SERVER_DST" "$MIGRATE_DST" "$OUT_DIR"/*.whl
