[简体中文](README.zh-CN.md)

# alocals3

A local-storage-based S3-like service (server + client) powered by FastAPI.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python -m alocals3.server --reload
# override DB URL from CLI
python -m alocals3.server --database-url "sqlite:////absolute/path/alocals3.db" --reload
# set application log level
python -m alocals3.server --log-level WARNING
```

## Rust Storage Backend

The `main` branch builds a PyO3 extension module, `alocals3._rust`, through `maturin`. For SQLite database URLs, the FastAPI server uses the Rust storage backend by default and falls back to the original Python/SQLAlchemy backend when the extension is unavailable or when a non-SQLite database URL is configured.

```bash
pip install -e .
python -m alocals3.server --database-url "sqlite:///./alocals3.db"
```

The HTTP S3-compatible API is unchanged. The Python client remains asyncio-friendly through `LocalS3ClientAsync`.

Local write-only stress benchmark, 10s, 50 concurrency, 4KiB objects, SQLite WAL, uvicorn access log disabled:

- `pre-rust`: 213.45 ops/s, p50 161.57 ms, p95 725.38 ms, p99 1218.26 ms
- `main` Rust/PyO3 storage: 104.59 ops/s, p50 480.92 ms, p95 607.50 ms, p99 683.51 ms

The current PyO3 storage swap is functionally compatible but not yet a throughput win for write-heavy workloads. The next performance step should move the HTTP server path itself to Rust, avoiding the Python ASGI/PyO3 boundary for hot requests.

## Configuration

- `ALOCALS3_APP_NAME`: app name, default `alocals3`
- `ALOCALS3_STORAGE_ROOT`: object storage root, default `./data`
- `ALOCALS3_DATABASE_URL`: database URL, default `sqlite:///./alocals3.db`
- `ALOCALS3_LOG_LEVEL`: log level, default `INFO` (`DEBUG/INFO/WARNING/ERROR/CRITICAL`)

Examples:

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql+psycopg://user:password@127.0.0.1:5432/alocals3`

Note: for SQLite, prefer an absolute path to avoid writing to different DB files due to different working directories.
For concurrent writes, SQLite is configured with `WAL` + `busy_timeout`, and write transactions apply short retry on `database is locked`.
For production workloads, PostgreSQL is strongly recommended.

## Storage Strategy

- Key/object metadata is indexed in DB (Rust/PyO3 for SQLite; Python/SQLAlchemy fallback for PostgreSQL or missing extension)
- Object payload is stored on local disk
- Blob file path uses hash sharding:
  - `sha256(<object bytes>) = <digest>`
  - `{storage_root}/objects/{digest[:2]}/{digest[2:4]}/{digest}`

## API Overview

- `GET /healthz`: health check
- `GET /s3`: list buckets
- `PUT /s3/{bucket}`: create bucket
- `DELETE /s3/{bucket}`: delete empty bucket
- `GET /s3/{bucket}/objects`: list objects
- `GET /s3/{bucket}?list-type=2`: S3-style ListObjectsV2 (`prefix`, `delimiter`, `max-keys`, `continuation-token`)
- `PUT /s3/{bucket}/{key}`: upload object
- `GET /s3/{bucket}/{key}`: download object (supports `304`, `Range` => `206/416`)
- `HEAD /s3/{bucket}/{key}`: metadata only (supports `304`, `Range` => `206/416`)
- `DELETE /s3/{bucket}/{key}`: delete object

Bucket names, object keys, prefixes, delimiters, and continuation tokens are treated as UTF-8 text. Client path parameters are UTF-8 percent-encoded automatically; pass raw strings such as `logs/数据.txt`, not pre-encoded URL fragments.

## Conditional PUT and Integrity

`PUT /s3/{bucket}/{key}` supports:

- `If-None-Match`: prevent overwrite when ETag matches (`*` supported)
- `If-Match`: only overwrite when ETag matches
- `Content-MD5`: server validates payload checksum and returns `400 BadDigest` on mismatch

Status codes:

- `201`: new object created
- `200`: existing object overwritten
- `412`: precondition failed

```bash
# only create if key does not exist
curl -i -X PUT -H "If-None-Match: *" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

# only overwrite when ETag matches
curl -i -X PUT -H 'If-Match: "d41d8cd98f00b204e9800998ecf8427e"' --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin

# with Content-MD5
MD5_B64=$(openssl md5 -binary file.bin | openssl base64)
curl -i -X PUT -H "Content-MD5: ${MD5_B64}" --data-binary @file.bin \
  http://127.0.0.1:8000/s3/demo/file.bin
```

## ListObjectsV2 Example

```bash
curl -sS "http://127.0.0.1:8000/s3/demo?list-type=2&prefix=logs/&delimiter=/&max-keys=100"

python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  LIST_OBJECTS_V2 demo --prefix logs/ --delimiter / --max-keys 100
```

## Partial Content Examples

```bash
# first 100 bytes
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/video.bin

# last 512 bytes
curl -i -H "Range: bytes=-512" http://127.0.0.1:8000/s3/demo/video.bin

# client CLI
python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  GET demo video.bin ./part.bin --range "bytes=0-99"
```

```python
import asyncio
from pathlib import Path
from alocals3.client import LocalS3Client, LocalS3ClientAsync

client = LocalS3Client("http://127.0.0.1:8000")
data, headers = client.get_object_range("demo", "video.bin", "bytes=0-99")
print(len(data), headers.get("content-range"))

headers = client.get_object_to_file(
    "demo",
    "video.bin",
    Path("./part.bin"),
    range_header="bytes=100-199",
)
print(headers.get("content-range"))
client.close()

# Ignore HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY from the environment.
client = LocalS3Client("http://127.0.0.1:8000", disable_proxy=True)
client.close()

async def main():
    async with LocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as async_client:
        print(await async_client.list_buckets())

asyncio.run(main())
```

## Consistency and Atomicity

- `PUT`: atomic blob replacement via temp file + `os.replace`; metadata mapping committed in DB transaction.
- `DELETE`: metadata mapping deletion is transactional in DB (physical blob deletion is intentionally out of request path to minimize critical section and avoid races).
- This is not a single global transaction across DB + filesystem. Under extreme failures, orphan blobs may exist and can be reclaimed by offline GC.

## Offline GC

```bash
# scan only
python -m alocals3.gc

# delete orphan blobs
python -m alocals3.gc --apply

# after installation
alocals3-gc --apply
```

## Updates

[updates.md](updates.md)

## License

[The MIT License](LICENSE)
