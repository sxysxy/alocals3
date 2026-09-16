[简体中文](README.zh-CN.md)

# alocals3

`alocals3` is a local S3-like object store focused on fast local development and internal workloads.

The current `main` branch is Rust-first:

- Server: pure Rust binary, no Python runtime required.
- Metadata backend: SQLite or PostgreSQL.
- Object payloads: local filesystem with SHA-256 based sharding.
- Python client: wheel package backed by Rust networking through `reqwest`.
- Python target: Python 3.12+ with PyO3 `abi3-py312` limited API.

The project implements an S3-compatible subset, not the full AWS S3 API surface.

## Quick Start

Build and run the Rust server:

```bash
PYO3_NO_PYTHON=1 cargo build --release --no-default-features --features server,server-binary --bin alocals3-server

target/release/alocals3-server \
  --host 127.0.0.1 \
  --port 8000 \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

Background garbage collection is enabled by default. It periodically removes object files that are no longer referenced by the database and removes database records whose files are missing. Candidates must be older than the GC grace period before they are deleted.

```bash
target/release/alocals3-server \
  --gc-interval-secs 300 \
  --gc-grace-secs 300 \
  --gc-start-delay-secs 30
```

Set `--disable-gc` to disable background GC. `--gc-interval-secs 0` and `ALOCALS3_GC_INTERVAL_SECS=0` are equivalent.

Run the reproducible GC correctness suite:

```bash
./examples/gc_correctness.sh
```

It covers orphan files, missing-file records, stale temp files, live/shared object retention, grace-period behavior, GC disable modes, and orphan-path reuse races. Latest local result:

```text
GC correctness and race scenarios passed
orphan_files_deleted=1 missing_records_deleted=1 stale_tmp_files_deleted=1
remaining_objects: grace=1 disable_flag=1 interval_zero=1 race=30
```

Use PostgreSQL instead of SQLite:

```bash
target/release/alocals3-server \
  --host 127.0.0.1 \
  --port 8000 \
  --database-url "postgresql://user:password@127.0.0.1:5432/alocals3" \
  --storage-root ./data
```

Install the Python client from source:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip maturin
python -m pip install -e .
```

## Build Artifacts

Release helper scripts live in [scripts](scripts/README.md):

```bash
# Linux static server plus Python 3.12+ ABI3 wheel
scripts/build-linux-release.sh

# macOS arm64, macOS 11 deployment baseline
scripts/build-macos-release.sh

# Windows 10+ PowerShell
.\scripts\build-windows-release.ps1
```

Linux server builds default to `x86_64-unknown-linux-musl`; Linux wheels default to `manylinux_2_28`. macOS builds default to `aarch64-apple-darwin` with `MACOSX_DEPLOYMENT_TARGET=11.0`.

The wheel is configured as Python 3.12+ ABI3 via PyO3 `abi3-py312`. It is not a `cp312-cp312` wheel unless the ABI3 feature is removed.

Platform wheels include the Rust server executable and install an `alocals3-server` command. Standalone server artifacts are also named `alocals3-server` on Unix-like platforms and `alocals3-server.exe` on Windows.

## Configuration

Server CLI flags:

- `--host`: bind host, default `127.0.0.1`
- `--port`: bind port, default `8000`
- `--database-url`: SQLite or PostgreSQL URL
- `--storage-root`: object payload root directory
- `--log-level`: log level or `tracing_subscriber` filter directive, default `info`
- `--max-upload-size`: maximum upload body size in bytes, enforced while streaming; default `0` means unlimited
- `--max-concurrent-uploads`: maximum number of request bodies concurrently streaming to disk, default `16`
- `--auth-enabled <true|false>`: explicitly enable or disable authentication, default `false`
- `--service-token`: bearer token for the internal `/s3` API; empty by default
- `--user-auth-secret`: HMAC secret for the read-only `/api/object` API; empty by default
- `--object-auth-cookie`: browser access-token cookie name, default `alocals3-auth-cookie`
- `--version`: print server version and author information

Environment variables:

- `ALOCALS3_DATABASE_URL`: default `sqlite:///./alocals3.db`
- `ALOCALS3_STORAGE_ROOT`: default `./data`
- `ALOCALS3_LOG_LEVEL`: default `info`
- `ALOCALS3_MAX_UPLOAD_SIZE`: default `0`
- `ALOCALS3_MAX_CONCURRENT_UPLOADS`: default `16`
- `ALOCALS3_AUTH_ENABLED`: authentication switch, default `false`; the CLI flag takes precedence
- `ALOCALS3_SERVICE_TOKEN`: bearer token required by `/s3` when authentication is enabled
- `ALOCALS3_USER_AUTH_SECRET`: HMAC secret used to validate Lauraycs access tokens on `/api/object`
- `ALOCALS3_OBJECT_AUTH_COOKIE`: access-token cookie name, default `alocals3-auth-cookie`

### Authentication

Authentication is controlled explicitly by `--auth-enabled <true|false>` (or
`ALOCALS3_AUTH_ENABLED`). Credentials no longer implicitly enable it:

- With `--auth-enabled true`, `/s3` requires
  `Authorization: Bearer <service-token>`, while
  `/api/object/{bucket}/{key}` requires a valid HMAC user token. Both
  `--service-token` and `--user-auth-secret` are required; the server refuses to
  start if either is empty.
- With `--auth-enabled false` (the default), both API families are accessible
  without credentials. Token and secret values may remain configured, but are
  ignored until authentication is enabled.
- `/healthz` does not require authentication.

The switch and credentials can all be supplied directly on the command line:

```bash
SERVICE_TOKEN="$(openssl rand -hex 32)"
USER_AUTH_SECRET="$(openssl rand -hex 32)"

target/release/alocals3-server \
  --auth-enabled true \
  --service-token "$SERVICE_TOKEN" \
  --user-auth-secret "$USER_AUTH_SECRET" \
  --object-auth-cookie "alocals3-auth-cookie" \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

Environment variables are also supported. For the remaining examples, export
the same values before starting the server and clients:

```bash
export ALOCALS3_AUTH_ENABLED=true
export ALOCALS3_SERVICE_TOKEN="$SERVICE_TOKEN"
export ALOCALS3_USER_AUTH_SECRET="$USER_AUTH_SECRET"
export ALOCALS3_OBJECT_AUTH_COOKIE="alocals3-auth-cookie" # optional
```

Use the protected `/s3` API with curl or the Python client:

```bash
curl -H "Authorization: Bearer ${ALOCALS3_SERVICE_TOKEN}" \
  http://127.0.0.1:8000/s3

# The CLI and Python client also read ALOCALS3_SERVICE_TOKEN automatically.
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_BUCKETS
```

```python
from alocals3.client import ALocalS3Client

with ALocalS3Client(
    "http://127.0.0.1:8000",
    service_token="the-same-service-token-as-the-server",
) as client:
    print(client.list_buckets())
```

A user access token has the form `<payload>.<signature>`. `payload` is the
unpadded URL-safe Base64 encoding of
`<username>|<access>|<expires-unix-seconds>|<nonce>`, and `signature` is the
lowercase hex HMAC-SHA256 of the encoded payload using
`ALOCALS3_USER_AUTH_SECRET`. The server requires a non-empty username and nonce,
checks the expiry and signature, and currently carries the `access` field
without interpreting it as an authorization rule. Lauraycs normally issues
this token; the following standalone example shows the exact wire format:

```bash
USER_TOKEN="$(python - <<'PY'
import base64, hashlib, hmac, os, secrets, time

raw = f"demo-user|read|{int(time.time()) + 3600}|{secrets.token_hex(16)}"
payload = base64.urlsafe_b64encode(raw.encode()).rstrip(b"=").decode()
signature = hmac.new(
    os.environ["ALOCALS3_USER_AUTH_SECRET"].encode(),
    payload.encode(),
    hashlib.sha256,
).hexdigest()
print(f"{payload}.{signature}")
PY
)"

curl -H "Authorization: Bearer ${USER_TOKEN}" \
  http://127.0.0.1:8000/api/object/demo/file.bin
```

Browsers may send the same token in the cookie configured by
`ALOCALS3_OBJECT_AUTH_COOKIE`; for example,
`Cookie: alocals3-auth-cookie=<user-token>`. A Bearer header takes precedence
when both are present. Amazon S3 does not define an authentication cookie name;
its SigV4 authentication uses the `Authorization` header, query parameters, or
browser POST fields. `alocals3-auth-cookie` is therefore an alocals3-specific
name, not an `x-amz-*` protocol field. CloudFront signed-cookie names are not
used because CloudFront's three-cookie signature format is a different protocol.

To disable authentication explicitly, restart with the CLI switch set to
`false` (this overrides `ALOCALS3_AUTH_ENABLED=true`):

```bash
target/release/alocals3-server \
  --auth-enabled false \
  --database-url "sqlite:///./alocals3.db" \
  --storage-root ./data
```

Run an unauthenticated deployment only on a trusted development machine.

Database URL examples:

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql://user:password@127.0.0.1:5432/alocals3`

Use an absolute SQLite path in scripts and services to avoid accidentally writing to different database files from different working directories. PostgreSQL is recommended for sustained concurrent workloads.

Logging is written to stderr. Common levels are `debug`, `info`, `warn`, and `error`; full `EnvFilter` directives such as `warn,alocals3_server=debug` are also accepted.

PUT request bodies are streamed to temporary files while MD5 and SHA-256 are calculated incrementally, so memory use does not grow with object size. Completed uploads are synced and atomically moved into the content-addressed blob layout before metadata is committed. `--max-concurrent-uploads` bounds active upload streams, and `--max-upload-size` is enforced for both fixed-length and chunked requests.

## Storage Layout

- Bucket and object metadata is stored in SQLite or PostgreSQL.
- Object bytes are stored on local disk.
- Blob paths are content-addressed and sharded:
  - `sha256(<object bytes>) = <digest>`
  - `{storage_root}/objects/{digest[:2]}/{digest[2:4]}/{digest}`

Object keys, bucket names, prefixes, delimiters, and continuation tokens are UTF-8 text. Client path parameters are UTF-8 percent-encoded automatically; pass raw strings such as `logs/data.txt` or `logs/数据.txt`, not pre-encoded URL fragments.

## HTTP API

- `GET /healthz`: health check
- `GET /s3`: list buckets
- `PUT /s3/{bucket}`: create bucket
- `DELETE /s3/{bucket}`: delete empty bucket
- `GET /s3/{bucket}/objects`: list objects
- `GET /s3/{bucket}?list-type=2`: S3-style ListObjectsV2
- `PUT /s3/{bucket}/{key}`: upload object
- `GET /s3/{bucket}/{key}`: download object
- `HEAD /s3/{bucket}/{key}`: object metadata
- `DELETE /s3/{bucket}/{key}`: delete object
- `GET|HEAD /api/object/{bucket}/{key}`: authenticated, read-only browser object access

Supported object features:

- `ETag` is the MD5 hex digest of the object body.
- `GET` streams object data from disk in 1 MiB chunks. The server does not
  buffer the complete object, and dropping a disconnected client's response
  also drops its file stream.
- Range requests seek directly to the requested offset and stream only the
  selected byte count.
- `Range` requests return `206` or `416`.
- `If-None-Match` and `If-Match` are supported for `PUT`.
- `Content-MD5` is validated on `PUT`.
- `If-None-Match` is supported for `GET` and `HEAD`.
- Object responses use `private, no-cache` with ETag revalidation and return
  `304` when unchanged. This permits private browser caching without treating a
  mutable bucket/key path as a permanent object identity.
- `/api/object` redirects an unversioned path to
  `/api/object/{bucket}/{key}?version={etag}` with `private, no-store`. A request
  whose version matches the current object receives
  `private, max-age=31536000, immutable`; a missing or stale version redirects
  to the current version. The query string is part of the browser cache key, so
  overwriting an object produces a new cache entry instead of reusing stale
  bytes. `private` prevents shared-proxy caching of authenticated objects, and
  `Vary: Authorization, Cookie` separates private cache entries by credential.

Clients upgrading from a release that cached the path-only URL as `immutable`
should change the requested URL once (for example, append `?cache-version=2`)
or clear the old browser cache. An already-cached immutable response cannot be
revoked by new server headers; the changed URL reaches the server and is then
redirected to the canonical `?version={etag}` URL.

The browser-facing endpoint normally uses the same streaming path. Only
`*/3dtiles/tileset.json` and `*/terrain/layer.json` are buffered because their
legacy compatibility fields may be rewritten; this buffer is capped at 16 MiB.

`PUT /s3/{bucket}/{key}` returns:

- `201`: new object created
- `200`: existing object overwritten
- `400`: invalid `Content-MD5`
- `412`: conditional request failed

## Client Usage

The Python runtime dependency list is intentionally empty. HTTP networking is implemented in Rust, not `httpx`.

```python
import asyncio
from pathlib import Path

from alocals3.client import ALocalS3Client, ALocalS3ClientAsync

with ALocalS3Client(
    "http://127.0.0.1:8000",
    disable_proxy=True,
    service_token="the-internal-service-token",
) as client:
    client.create_bucket("demo")
    info = client.put_object("demo", "logs/数据.txt", Path("data.txt"))
    print(info["etag"])
    copied = client.copy_object(
        "demo", "logs/数据.txt", "demo", "logs/copied.txt",
        metadata={"foo": "bar"},
    )

    data, headers = client.get_object_range("demo", "logs/数据.txt", "bytes=0-99")
    print(len(data), headers.get("content-range"))

    with client.open("s3://demo/logs/数据.txt", "r") as f:
        print(f.read())

    with client.open("s3://demo/logs/from-open.txt", "wb") as f:
        f.write(b"hello from file-like API\n")

    client.get_object_to_file("demo", "logs/数据.txt", Path("copy.txt"))


async def main() -> None:
    async with ALocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as client:
        print(await client.list_buckets())
        async with client.open("s3://demo/logs/from-async-open.txt", "wb") as f:
            await f.write(b"hello from async file-like API\n")
        async with client.open("s3://demo/logs/from-async-open.txt", "rb") as f:
            print(await f.read())


asyncio.run(main())
```

CLI:

```bash
python -m alocals3.client --endpoint http://127.0.0.1:8000 CREATE_BUCKET demo
python -m alocals3.client --endpoint http://127.0.0.1:8000 PUT demo file.bin ./file.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 COPY demo file.bin demo copy.bin --metadata foo=bar
python -m alocals3.client --endpoint http://127.0.0.1:8000 GET demo file.bin ./copy.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_OBJECTS_V2 demo --prefix logs/ --delimiter /
```

`CopyObject` is also available through the S3-compatible `PUT` form using
`x-amz-copy-source`. It reuses the source content-addressed blob, so it performs
no object-byte copy and remains O(1) with respect to object size. User metadata
headers (`x-amz-meta-*`) are persisted; use `x-amz-metadata-directive: REPLACE`
to replace metadata during a copy (the default is `COPY`).

## Migrate SQLite to PostgreSQL

Stop writers or take a SQLite snapshot first, then run:

```bash
alocals3-migrate2pg \
  --source sqlite:///./alocals3.db \
  --target postgresql://user:password@127.0.0.1:5432/alocals3
```

The command migrates metadata only. Keep the same `--storage-root` (or move the
storage directory separately), because object blobs are referenced by relative
content-addressed paths. The migration is idempotent and commits all migrated
bucket/object rows in one PostgreSQL transaction.

Set `disable_proxy=True` or pass `--disable-proxy` to ignore proxy environment variables such as `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and `NO_PROXY`.

`client.open()` is file-like but not an in-memory object wrapper:

- Read modes (`"rb"` / `"r"`) create a Rust-backed streaming HTTP reader. `open()` sends the request and reads response headers, but object bytes are pulled from the network when the returned file object's `read()` path runs.
- Write modes (`"wb"` / `"w"`) spool writes to a Rust-owned temporary file. The HTTP `PUT` is sent when the file is closed or the `with` block exits successfully. If the `with` block exits with an exception, the upload is discarded.
- `cache_path=` is best-effort and is populated as bytes pass through the file-like object; cache write failures do not fail the network operation.
- `ALocalS3ClientAsync` is backed by native Tokio/reqwest futures exposed through PyO3; it does not dispatch the synchronous client through `asyncio.to_thread()`. Its `open()` method returns an async file-like object for `async with` with awaitable `read()`, `readline()`, `readinto()`, `write()`, `flush()`, `close()`, and `discard()` methods.

Benchmark the Python file-like API with a large object:

```bash
./examples/benchmark_file_like.sh
```

Validate stream multiplexing and Range-based resume reads:

```bash
./examples/benchmark_stream_multiplex.sh
```

Stress the native async client itself:

```bash
python examples/bench_async_client.py --duration 30 --concurrency 50 --disable-proxy
```

The current file-like API supports resume reads through `range_header="bytes=N-"`. Upload resume is not implemented yet; write modes spool to a Rust-owned temporary file and use one HTTP `PUT` on close.

## Curl Examples

```bash
curl -i -X PUT http://127.0.0.1:8000/s3/demo
curl -i -X PUT --data-binary @file.bin http://127.0.0.1:8000/s3/demo/file.bin
curl -i http://127.0.0.1:8000/s3/demo/file.bin
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/file.bin
curl -sS "http://127.0.0.1:8000/s3/demo?list-type=2&prefix=logs/&delimiter=/&max-keys=100"
```

Conditional PUT:

```bash
curl -i -X PUT -H "If-None-Match: *" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

curl -i -X PUT -H 'If-Match: "d41d8cd98f00b204e9800998ecf8427e"' --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

MD5_B64=$(openssl md5 -binary file.bin | openssl base64)
curl -i -X PUT -H "Content-MD5: ${MD5_B64}" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin
```

## Consistency Notes

- Object bytes are written through a temporary file and atomic rename.
- Metadata updates are committed through the selected database backend.
- This is not a single distributed transaction across database and filesystem.
- Under process or machine failure, orphan blob files may exist. The Python package no longer ships an alternate storage backend or Python server path; operational cleanup should be handled outside the request path.

## Updates

[updates.md](updates.md)

## License

[The MIT License](LICENSE)

### Async object metadata (HEAD)

```python
headers = await client.head_object("my-bucket", "folder/object.arrow")
size = int(headers["content-length"])
etag = headers["etag"]
```

`ALocalS3ClientAsync.head_object` returns lowercase HTTP response headers,
including object size, ETag, last-modified, content-type and custom
`x-amz-meta-*` fields. It uses the native async connection pool, authentication,
proxy and timeout settings, without downloading the body or listing the bucket.
Empty objects return `content-length: 0`. HTTP failures raise `RuntimeError`
with the existing `ALOCALS3_HTTP_STATUS:<code>` marker, including 404.

### Multipart uploads

Both `ALocalS3Client` and the native `ALocalS3ClientAsync` support
`create_multipart_upload`, `upload_part`, `list_parts`,
`complete_multipart_upload`, and `abort_multipart_upload`.

```python
upload = await client.create_multipart_upload("bucket", "result.arrow")
upload_id = upload["upload_id"]  # persist this to resume after restart
first = await client.upload_part(upload_id, 1, b"first batch")
second = await client.upload_part(upload_id, 2, b"second batch")
result = await client.complete_multipart_upload(upload_id, [first, second])
await client.abort_multipart_upload(upload_id)  # release receipt; object stays
```

Unpublished sessions/parts use separate `pending_uploads` and
`pending_upload_parts` tables. No upload-state columns are added to `objects`.
Completion validates every part, assembles with bounded memory, then atomically
publishes the object, deletes pending records, and writes a retry receipt in
`completed_uploads`. Failed transactions preserve the old object and pending
parts. Identical completion retries return the original receipt without
replacing newer object versions. Database connections are not held during
upload reception or file assembly.

Part numbers must be consecutive from 1 and the completion manifest must match
all stored parts and their ETags. Re-uploading a number replaces that part;
different parts may upload concurrently. Sessions survive server restarts.
There is no minimum part size, 16 MiB limit, or policy limit on part count;
an explicitly configured server `max_upload_size` also applies to aggregate
size (default 0: unlimited). Pending sessions are retained until explicitly
aborted; persist upload IDs and abort abandoned uploads. An empty manifest can
publish an empty object. Both SQLite and PostgreSQL are supported; new tables
are created on server startup. Filesystem locks coordinate servers sharing
the same storage directory; building requires Rust 1.89+.

This is an alocals3 JSON protocol, not the AWS XML multipart API:
`POST /s3-multipart/<bucket>/<key>`, `PUT /s3-uploads/<id>/<part_number>`,
and GET/POST/DELETE on `/s3-uploads/<id>` for list/complete/abort respectively.
All endpoints require the usual service credentials. Update **both** server
and client builds before using this API.

Run `python tests/test_multipart.py` after building/installing. Tests create
an isolated SQLite database by default. Set `ALOCALS3_TEST_POSTGRES_URL` to use
an isolated temporary PostgreSQL schema instead (requires psycopg for tests).
