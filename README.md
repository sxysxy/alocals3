[简体中文](README.zh-CN.md)

# alocals3

A local-storage-based S3-like service (server + client) powered by FastAPI.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m alocals3.server --reload
# override DB URL from CLI
python -m alocals3.server --database-url "sqlite:////absolute/path/alocals3.db" --reload
```

## Configuration

- `ALOCALS3_APP_NAME`: app name, default `alocals3`
- `ALOCALS3_STORAGE_ROOT`: object storage root, default `./data`
- `ALOCALS3_DATABASE_URL`: database URL, default `sqlite:///./alocals3.db`

Examples:

- SQLite: `sqlite:///./alocals3.db`
- PostgreSQL: `postgresql+psycopg://user:password@127.0.0.1:5432/alocals3`

Note: for SQLite, prefer an absolute path to avoid writing to different DB files due to different working directories.

## Storage Strategy

- Key/object metadata is indexed in DB (SQLAlchemy, supports SQLite/PostgreSQL)
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
from pathlib import Path
from alocals3.client import LocalS3Client

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
