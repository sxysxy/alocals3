#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alocals3.client import ALocalS3Client


def _mib(value: int) -> int:
    return value * 1024 * 1024


def _object_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key.lstrip('/')}"


def _make_chunk(size: int, seed: str) -> bytes:
    out = bytearray()
    block = seed.encode("utf-8")
    while len(out) < size:
        block = hashlib.sha256(block).digest()
        out.extend(block)
    return bytes(out[:size])


def _ensure_bucket(client: ALocalS3Client, bucket: str) -> None:
    try:
        client.create_bucket(bucket)
    except RuntimeError as exc:
        if "ALOCALS3_HTTP_STATUS:409" not in str(exc):
            raise


def _write_object(
    client: ALocalS3Client,
    url: str,
    *,
    total_size: int,
    chunk: bytes,
    content_type: str,
) -> tuple[float, str, dict | None]:
    digest = hashlib.sha256()
    remaining = total_size
    started = time.perf_counter()
    with client.open(url, "wb", content_type=content_type) as f:
        while remaining > 0:
            part = chunk if remaining >= len(chunk) else chunk[:remaining]
            f.write(part)
            digest.update(part)
            remaining -= len(part)
        result = f
    elapsed = time.perf_counter() - started
    return elapsed, digest.hexdigest(), result.result


def _read_object(
    client: ALocalS3Client,
    url: str,
    *,
    chunk_size: int,
) -> tuple[float, int, str, dict]:
    digest = hashlib.sha256()
    total = 0
    started = time.perf_counter()
    with client.open(url, "rb") as f:
        headers = dict(getattr(f, "headers", {}))
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            total += len(data)
            digest.update(data)
    elapsed = time.perf_counter() - started
    return elapsed, total, digest.hexdigest(), headers


def _format_speed(bytes_count: int, elapsed: float) -> str:
    if elapsed <= 0:
        return "inf MiB/s"
    return f"{bytes_count / elapsed / 1024 / 1024:.2f} MiB/s"


def _print_result(record: dict[str, Any]) -> None:
    print(json.dumps(record, ensure_ascii=False, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark ALocalS3Client file-like open() read/write throughput")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--bucket", default="bench-file-like")
    parser.add_argument("--key", default="large/file-like.bin")
    parser.add_argument("--size-mib", type=int, default=1024)
    parser.add_argument("--chunk-mib", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--disable-proxy", action="store_true")
    parser.add_argument("--content-type", default="application/octet-stream")
    parser.add_argument("--seed", default="alocals3-file-like-benchmark")
    parser.add_argument("--keep-object", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.size_mib < 1:
        raise SystemExit("--size-mib must be >= 1")
    if args.chunk_mib < 1:
        raise SystemExit("--chunk-mib must be >= 1")
    if args.repeats < 1:
        raise SystemExit("--repeats must be >= 1")

    total_size = _mib(args.size_mib)
    chunk_size = min(_mib(args.chunk_mib), total_size)
    chunk = _make_chunk(chunk_size, args.seed)
    url = _object_url(args.bucket, args.key)

    with ALocalS3Client(args.endpoint, timeout=args.timeout, disable_proxy=args.disable_proxy) as client:
        _ensure_bucket(client, args.bucket)
        _print_result(
            {
                "event": "start",
                "endpoint": args.endpoint,
                "url": url,
                "size_bytes": total_size,
                "chunk_bytes": chunk_size,
                "repeats": args.repeats,
            }
        )

        last_write_result: dict | None = None
        for idx in range(1, args.repeats + 1):
            write_elapsed, write_sha256, write_result = _write_object(
                client,
                url,
                total_size=total_size,
                chunk=chunk,
                content_type=args.content_type,
            )
            last_write_result = write_result
            _print_result(
                {
                    "event": "write",
                    "iteration": idx,
                    "seconds": round(write_elapsed, 6),
                    "speed": _format_speed(total_size, write_elapsed),
                    "sha256": write_sha256,
                    "result": write_result,
                }
            )

            read_elapsed, read_size, read_sha256, headers = _read_object(
                client,
                url,
                chunk_size=chunk_size,
            )
            ok = read_size == total_size and read_sha256 == write_sha256
            _print_result(
                {
                    "event": "read",
                    "iteration": idx,
                    "seconds": round(read_elapsed, 6),
                    "speed": _format_speed(read_size, read_elapsed),
                    "size_bytes": read_size,
                    "sha256": read_sha256,
                    "etag": headers.get("etag"),
                    "ok": ok,
                }
            )
            if not ok:
                raise SystemExit("read verification failed")

        if not args.keep_object:
            client.delete_object(args.bucket, args.key)

        _print_result(
            {
                "event": "done",
                "size_bytes": total_size,
                "repeats": args.repeats,
                "last_result": last_write_result,
                "kept_object": args.keep_object,
            }
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
