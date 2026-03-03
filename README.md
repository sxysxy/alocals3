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
- `PUT /s3/{bucket}/{key}`: upload object
- `GET /s3/{bucket}/{key}`: download object (supports `304`(Not Modified”), `Range` => `206/416`(Partial Content/Request Range Not Satisfiable))
- `HEAD /s3/{bucket}/{key}`: metadata only (supports `304`(Not Modified”))
- `DELETE /s3/{bucket}/{key}`: delete object

## Partial Content Examples

```bash
# first 100 bytes
curl -i -H "Range: bytes=0-99" http://127.0.0.1:8000/s3/demo/video.bin

# last 512 bytes
curl -i -H "Range: bytes=-512" http://127.0.0.1:8000/s3/demo/video.bin

# client CLI
python -m alocals3.client --endpoint http://127.0.0.1:8000 \
  get demo video.bin ./part.bin --range "bytes=0-99"
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

## License

[The MIT License](LICENSE)
