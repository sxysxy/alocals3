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

Linux server builds default to `x86_64-unknown-linux-musl`. macOS builds default to `aarch64-apple-darwin` with `MACOSX_DEPLOYMENT_TARGET=11.0`.

The wheel is configured as Python 3.12+ ABI3 via PyO3 `abi3-py312`. It is not a `cp312-cp312` wheel unless the ABI3 feature is removed.

## Configuration

Server CLI flags:

- `--host`: bind host, default `127.0.0.1`
- `--port`: bind port, default `8000`
- `--database-url`: SQLite or PostgreSQL URL
- `--storage-root`: object payload root directory

Environment variables:

- `ALOCALS3_DATABASE_URL`: default `sqlite:///./alocals3.db`
- `ALOCALS3_STORAGE_ROOT`: default `./data`

Database URL examples:

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql://user:password@127.0.0.1:5432/alocals3`

Use an absolute SQLite path in scripts and services to avoid accidentally writing to different database files from different working directories. PostgreSQL is recommended for sustained concurrent workloads.

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

Supported object features:

- `ETag` is the MD5 hex digest of the object body.
- `Range` requests return `206` or `416`.
- `If-None-Match` and `If-Match` are supported for `PUT`.
- `Content-MD5` is validated on `PUT`.
- `If-None-Match` is supported for `GET` and `HEAD`.

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

from alocals3.client import LocalS3Client, LocalS3ClientAsync

with LocalS3Client("http://127.0.0.1:8000", disable_proxy=True) as client:
    client.create_bucket("demo")
    info = client.put_object("demo", "logs/数据.txt", Path("data.txt"))
    print(info["etag"])

    data, headers = client.get_object_range("demo", "logs/数据.txt", "bytes=0-99")
    print(len(data), headers.get("content-range"))

    client.get_object_to_file("demo", "logs/数据.txt", Path("copy.txt"))


async def main() -> None:
    async with LocalS3ClientAsync("http://127.0.0.1:8000", disable_proxy=True) as client:
        print(await client.list_buckets())


asyncio.run(main())
```

CLI:

```bash
python -m alocals3.client --endpoint http://127.0.0.1:8000 CREATE_BUCKET demo
python -m alocals3.client --endpoint http://127.0.0.1:8000 PUT demo file.bin ./file.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 GET demo file.bin ./copy.bin
python -m alocals3.client --endpoint http://127.0.0.1:8000 LIST_OBJECTS_V2 demo --prefix logs/ --delimiter /
```

Set `disable_proxy=True` or pass `--disable-proxy` to ignore proxy environment variables such as `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and `NO_PROXY`.

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
- Under process or machine failure, orphan blob files may exist and can be removed by offline GC.

## Legacy Python Code

The repository still contains the previous Python server/storage modules for compatibility and migration work. The Rust server is the intended runtime on `main`.

Legacy Python paths may require optional dependencies:

```bash
python -m pip install ".[legacy-python-server]"
```

Offline GC is currently a Python utility:

```bash
python -m alocals3.gc
python -m alocals3.gc --apply
```

## Updates

[updates.md](updates.md)

## License

[The MIT License](LICENSE)
