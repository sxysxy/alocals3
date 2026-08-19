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
