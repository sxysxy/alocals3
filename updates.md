# Version 0.9.3

- Cache-Contorl with version

# Version 0.9.2

- Add a native Tokio/PyO3 async Python client without `asyncio.to_thread`
- Stream PUT request bodies directly to temporary files with incremental MD5 and SHA-256 hashing
- Bound concurrent uploads with `--max-concurrent-uploads` / `ALOCALS3_MAX_CONCURRENT_UPLOADS`
- Enforce upload size limits while streaming, including chunked requests

# Version 0.9.1

- Stream object downloads from disk in bounded 1 MiB chunks instead of buffering the full object
- Serve byte ranges with direct file seek and length-limited reads
- Release the response-owned file stream when a client disconnects
- Bound buffering for the two public JSON compatibility transformations

# Version 0.9.0

- Add optional bearer-token authentication for the internal `/s3` API
- Add HMAC access-token authentication for the read-only `/api/object` API

# Version 0.8.0

- Add O(1) S3-compatible CopyObject with `x-amz-copy-source`
- Add persistent `x-amz-meta-*` user metadata
- Add the `alocals3-migrate2pg` SQLite-to-PostgreSQL migration CLI
- Bundle both server and migration binaries in release wheels

# Version 0.3.0

- Logging
- Optimize for sqlite backend


# Version 0.2.0

- Add garbage collect
- PUT returns 200 on overwriting, 201 on creating
- Support Content-MD5 header for verification
- Add ListObjectsV2

# Version 0.1.0

- Initial implementation
