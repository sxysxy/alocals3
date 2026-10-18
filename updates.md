# Version 0.11.1

- Add ThirdpartyLicense.txt with third-party Rust and Python license texts and notices; include it in release packages.

# Version 0.11.0

- Add object bulk deletion with one bucket transaction and set-based SQL per 1000-key batch; clients automatically batch iterables and support quiet responses and per-object ETag conditions.
- Persist bucket/prefix expiration rules with enabled flags, paginated previews (including disabled-rule previews), and bounded background expiration sweeps coordinated with concurrent writers.
- Maintain object counts and logical byte usage transactionally with SQLite/PostgreSQL triggers; enforce persistent bucket quotas for PUT, overwrite, copy and multipart completion.
- Protect disk free-space reserves while receiving uploads/parts and assembling multipart objects; coordinate writes across server processes and return 507 on insufficient capacity.
- Add authenticated storage/bucket usage APIs and Prometheus metrics, public readiness probes, bounded route labels, streaming byte counters and request IDs.
- Add SQLite/PostgreSQL maintenance regressions; retain GC grace periods for bulk/lifecycle reclamation.

# Version 0.10.0

- Fix ListObjectsV2 continuation boundaries and delimiter paging; use bounded SQL range queries with deterministic UTF-8 ordering and context-bound opaque cursors.
- Honor max-keys=0 and push simple object listing limits/prefixes into SQL.
- Make conditional PUT and multipart completion atomic across server processes; coordinate copy, delete and missing-record GC with the same bucket write transactions.
- Expose write preconditions on synchronous and async Python clients, and If-Match on object reads.
- Add paginated incomplete-upload listings with stored byte/part counts and batch cancellation (up to 100 IDs).
- Add opt-in idle multipart cleanup with cross-process activity locks; preserve active receivers and completed receipts.
- Add read-only --verify / --verify-full inspection with JSON diagnostics and corruption exit status.
- Stream client downloads into version-bound partial files, resume interrupted downloads with ETag/Range checks, validate checksums, and atomically replace destinations.
- Add SQLite/PostgreSQL multi-process regressions and interrupted-download wire tests.
- Keep HTTP requests working when a detached launcher closes both stdout/stderr log pipes; failed-log diagnostics must not panic in request tasks.
- Add an actual-server regression for data requests and sampled health logs after both output pipes lose their reader.

# Version 0.9.5

- Include `status`, `system` and `module` in the public `/healthz` response
- Add `--health-system` / `ALOCALS3_HEALTH_SYSTEM` to identify the embedding platform; standalone servers default to `alocals3`
- Preserve unauthenticated health probes; public identity fields do not replace authentication or process ownership checks

# Version 0.9.4

- Add native async `head_object` for metadata queries without downloading objects or listing buckets
- Add multipart upload APIs to synchronous and native async clients, with resumable sessions and per-part retries
- Store pending uploads and parts in separate tables; publish complete objects and remove pending records atomically
- Validate complete manifests and part checksums, preserve completion receipts for safe retries, and support abort cleanup
- Support SQLite and PostgreSQL, cross-process upload coordination, and bounded-memory file assembly
- Require Rust 1.89 or newer for nonblocking filesystem locks

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
