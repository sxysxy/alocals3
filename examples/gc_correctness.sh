#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SERVER_BIN="${SERVER_BIN:-$ROOT_DIR/target/debug/alocals3-server}"
BASE_DIR="${BASE_DIR:-$(mktemp -d /tmp/alocals3-gc-correctness.XXXXXX)}"
KEEP_BASE_DIR="${KEEP_BASE_DIR:-0}"
START_PORT="${START_PORT:-18111}"

PIDS=""

cleanup() {
  for pid in $PIDS; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  if [[ "$KEEP_BASE_DIR" != "1" ]]; then
    rm -rf "$BASE_DIR"
  fi
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_eq() {
  local got="$1"
  local expected="$2"
  local label="$3"
  if [[ "$got" != "$expected" ]]; then
    fail "$label: got '$got', expected '$expected'"
  fi
}

assert_file_exists() {
  [[ -f "$1" ]] || fail "expected file to exist: $1"
}

wait_for_health() {
  local endpoint="$1"
  for _ in $(seq 1 50); do
    if curl -fsS "$endpoint/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  fail "server did not become healthy: $endpoint"
}

wait_until() {
  local label="$1"
  local expr="$2"
  for _ in $(seq 1 100); do
    if eval "$expr"; then
      return 0
    fi
    sleep 0.1
  done
  fail "timed out waiting for $label"
}

start_server() {
  local port="$1"
  local dir="$2"
  local log="$3"
  shift 3
  "$SERVER_BIN" \
    --host 127.0.0.1 \
    --port "$port" \
    --database-url "sqlite:///$dir/alocals3.db" \
    --storage-root "$dir/data" \
    --log-level info \
    "$@" >"$log" 2>&1 &
  local pid=$!
  PIDS="$PIDS $pid"
  wait_for_health "http://127.0.0.1:$port"
}

put_body() {
  local endpoint="$1"
  local bucket="$2"
  local key="$3"
  local body="$4"
  printf '%s' "$body" | curl -fsS -X PUT --data-binary @- "$endpoint/s3/$bucket/$key" >/dev/null
}

get_body() {
  curl -fsS "$1/s3/$2/$3"
}

if [[ ! -x "$SERVER_BIN" ]]; then
  echo "Missing server binary. Building: $SERVER_BIN" >&2
  PYO3_NO_PYTHON=1 cargo build \
    --no-default-features \
    --features server,server-binary \
    --bin alocals3-server
fi

command -v curl >/dev/null || fail "curl is required"
command -v sqlite3 >/dev/null || fail "sqlite3 is required"
command -v shasum >/dev/null || fail "shasum is required"

mkdir -p "$BASE_DIR"
echo "GC correctness base dir: $BASE_DIR"

PORT_ENABLED=$START_PORT
PORT_GRACE=$((START_PORT + 1))
PORT_DISABLE=$((START_PORT + 2))
PORT_INTERVAL_ZERO=$((START_PORT + 3))
PORT_REUSE_RACE=$((START_PORT + 4))

# Scenario 1: GC enabled. It must clean orphan files and missing records while
# keeping live objects and still-referenced shared content files.
S1="$BASE_DIR/enabled"
mkdir -p "$S1"
start_server "$PORT_ENABLED" "$S1" "$S1/server.log" \
  --gc-interval-secs 1 \
  --gc-grace-secs 0 \
  --gc-start-delay-secs 0
EP1="http://127.0.0.1:$PORT_ENABLED"
DB1="$S1/alocals3.db"
OBJ1="$S1/data/objects"
curl -fsS -X PUT "$EP1/s3/gc" >/dev/null
put_body "$EP1" gc missing.txt missing-body
put_body "$EP1" gc keep.txt keep-body
put_body "$EP1" gc shared1.txt shared-body
put_body "$EP1" gc shared2.txt shared-body
missing_path=$(sqlite3 "$DB1" "select file_path from objects where key='missing.txt';")
keep_path=$(sqlite3 "$DB1" "select file_path from objects where key='keep.txt';")
shared1_path=$(sqlite3 "$DB1" "select file_path from objects where key='shared1.txt';")
shared2_path=$(sqlite3 "$DB1" "select file_path from objects where key='shared2.txt';")
assert_eq "$shared1_path" "$shared2_path" "same content should share file_path"
rm "$OBJ1/$missing_path"
mkdir -p "$OBJ1/aa/bb" "$OBJ1/tmp"
printf orphan > "$OBJ1/aa/bb/orphan-file"
printf tmp > "$OBJ1/tmp/.leftover.tmp"
curl -fsS -X DELETE "$EP1/s3/gc/shared1.txt" >/dev/null
wait_until "enabled GC cleanup" \
  "[[ ! -e '$OBJ1/aa/bb/orphan-file' && ! -e '$OBJ1/tmp/.leftover.tmp' && \$(sqlite3 '$DB1' \"select count(*) from objects where key='missing.txt';\") == 0 ]]"
assert_file_exists "$OBJ1/$keep_path"
assert_file_exists "$OBJ1/$shared2_path"
assert_eq "$(sqlite3 "$DB1" "select count(*) from objects;")" "2" \
  "enabled GC should leave only keep.txt and shared2.txt"
assert_eq "$(get_body "$EP1" gc keep.txt)" "keep-body" "keep object GET"
assert_eq "$(get_body "$EP1" gc shared2.txt)" "shared-body" "shared object GET"
grep -q 'orphan_files_deleted=1' "$S1/server.log" || fail "enabled GC log missing orphan deletion count"
grep -q 'missing_records_deleted=1' "$S1/server.log" || fail "enabled GC log missing missing-record deletion count"

# Scenario 2: Grace period must keep young orphan files and young missing-file
# DB records.
S2="$BASE_DIR/grace"
mkdir -p "$S2"
start_server "$PORT_GRACE" "$S2" "$S2/server.log" \
  --gc-interval-secs 1 \
  --gc-grace-secs 3600 \
  --gc-start-delay-secs 0
EP2="http://127.0.0.1:$PORT_GRACE"
DB2="$S2/alocals3.db"
OBJ2="$S2/data/objects"
curl -fsS -X PUT "$EP2/s3/gc" >/dev/null
put_body "$EP2" gc young-missing.txt young-body
young_path=$(sqlite3 "$DB2" "select file_path from objects where key='young-missing.txt';")
rm "$OBJ2/$young_path"
mkdir -p "$OBJ2/cc/dd"
printf young-orphan > "$OBJ2/cc/dd/young-orphan"
sleep 3
assert_file_exists "$OBJ2/cc/dd/young-orphan"
assert_eq "$(sqlite3 "$DB2" "select count(*) from objects where key='young-missing.txt';")" "1" \
  "grace should keep young missing record"

# Scenario 3: --disable-gc must prevent runtime automatic cleanup.
S3="$BASE_DIR/disable-flag"
mkdir -p "$S3"
start_server "$PORT_DISABLE" "$S3" "$S3/server.log" \
  --disable-gc \
  --gc-interval-secs 1 \
  --gc-grace-secs 0 \
  --gc-start-delay-secs 0
EP3="http://127.0.0.1:$PORT_DISABLE"
DB3="$S3/alocals3.db"
OBJ3="$S3/data/objects"
curl -fsS -X PUT "$EP3/s3/gc" >/dev/null
put_body "$EP3" gc disabled-missing.txt disabled-body
disabled_path=$(sqlite3 "$DB3" "select file_path from objects where key='disabled-missing.txt';")
rm "$OBJ3/$disabled_path"
mkdir -p "$OBJ3/ee/ff"
printf disabled-orphan > "$OBJ3/ee/ff/disabled-orphan"
sleep 3
assert_file_exists "$OBJ3/ee/ff/disabled-orphan"
assert_eq "$(sqlite3 "$DB3" "select count(*) from objects where key='disabled-missing.txt';")" "1" \
  "--disable-gc should keep missing record"
grep -q 'background gc disabled' "$S3/server.log" || fail "--disable-gc log missing"

# Scenario 4: ALOCALS3_GC_INTERVAL_SECS=0 must be equivalent to disabling
# automatic GC.
S4="$BASE_DIR/interval-zero"
mkdir -p "$S4"
ALOCALS3_GC_INTERVAL_SECS=0 "$SERVER_BIN" \
  --host 127.0.0.1 \
  --port "$PORT_INTERVAL_ZERO" \
  --database-url "sqlite:///$S4/alocals3.db" \
  --storage-root "$S4/data" \
  --log-level info >"$S4/server.log" 2>&1 &
pid=$!
PIDS="$PIDS $pid"
wait_for_health "http://127.0.0.1:$PORT_INTERVAL_ZERO"
EP4="http://127.0.0.1:$PORT_INTERVAL_ZERO"
DB4="$S4/alocals3.db"
OBJ4="$S4/data/objects"
curl -fsS -X PUT "$EP4/s3/gc" >/dev/null
put_body "$EP4" gc interval-missing.txt interval-body
interval_path=$(sqlite3 "$DB4" "select file_path from objects where key='interval-missing.txt';")
rm "$OBJ4/$interval_path"
mkdir -p "$OBJ4/11/22"
printf interval-orphan > "$OBJ4/11/22/interval-orphan"
sleep 3
assert_file_exists "$OBJ4/11/22/interval-orphan"
assert_eq "$(sqlite3 "$DB4" "select count(*) from objects where key='interval-missing.txt';")" "1" \
  "GC interval zero should keep missing record"
grep -q 'background gc disabled' "$S4/server.log" || fail "interval-zero disabled log missing"

# Scenario 5: Regression for orphan path reuse. Create an old unreferenced file
# at the exact content-addressed path, then PUT the same content repeatedly while
# GC runs with grace=0. The reused path must not be removed.
S5="$BASE_DIR/reuse-race"
mkdir -p "$S5"
start_server "$PORT_REUSE_RACE" "$S5" "$S5/server.log" \
  --gc-interval-secs 1 \
  --gc-grace-secs 0 \
  --gc-start-delay-secs 0
EP5="http://127.0.0.1:$PORT_REUSE_RACE"
DB5="$S5/alocals3.db"
OBJ5="$S5/data/objects"
curl -fsS -X PUT "$EP5/s3/gc" >/dev/null
RACE_BODY="race-body-for-content-addressed-path"
RACE_DIGEST=$(printf '%s' "$RACE_BODY" | shasum -a 256 | awk '{print $1}')
RACE_PATH="${RACE_DIGEST:0:2}/${RACE_DIGEST:2:2}/$RACE_DIGEST"
mkdir -p "$OBJ5/${RACE_DIGEST:0:2}/${RACE_DIGEST:2:2}"
printf '%s' "$RACE_BODY" > "$OBJ5/$RACE_PATH"
touch -t 200001010000 "$OBJ5/$RACE_PATH"
for i in $(seq 1 30); do
  put_body "$EP5" gc "race-$i.txt" "$RACE_BODY"
done
sleep 2
for i in $(seq 1 30); do
  assert_eq "$(get_body "$EP5" gc "race-$i.txt")" "$RACE_BODY" "race object $i GET"
done
assert_file_exists "$OBJ5/$RACE_PATH"
assert_eq "$(sqlite3 "$DB5" "select count(*) from objects where file_path='$RACE_PATH';")" "30" \
  "race objects should all reference reused path"

echo "GC correctness and race scenarios passed"
echo "enabled_log_counts: $(grep 'orphan_files_deleted=1' "$S1/server.log" | tail -1)"
echo "remaining_objects: grace=$(sqlite3 "$DB2" "select count(*) from objects;") disable_flag=$(sqlite3 "$DB3" "select count(*) from objects;") interval_zero=$(sqlite3 "$DB4" "select count(*) from objects;") race=$(sqlite3 "$DB5" "select count(*) from objects;")"
if [[ "$KEEP_BASE_DIR" == "1" ]]; then
  echo "Kept GC correctness base dir: $BASE_DIR"
fi
