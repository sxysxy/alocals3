#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from alocals3.client import LocalS3ClientAsync


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


async def _ensure_bucket(client: LocalS3ClientAsync, bucket: str) -> None:
    try:
        await client.create_bucket(bucket)
    except RuntimeError as exc:
        if "ALOCALS3_HTTP_STATUS:409" not in str(exc):
            raise


async def _health_probe(
    client: LocalS3ClientAsync,
    *,
    duration: float,
    interval: float,
    label: str,
) -> dict[str, Any]:
    latencies: list[float] = []
    failures = 0
    deadline = time.perf_counter() + duration
    while time.perf_counter() < deadline:
        started = time.perf_counter()
        try:
            await client.health()
            latencies.append(time.perf_counter() - started)
        except Exception:
            failures += 1
        await asyncio.sleep(interval)
    latencies.sort()
    p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))] if latencies else None
    return {
        "event": "health_probe",
        "label": label,
        "count": len(latencies),
        "failures": failures,
        "p95_ms": round(p95 * 1000, 3) if p95 is not None else None,
    }


async def _write_object(
    client: LocalS3ClientAsync,
    url: str,
    *,
    total_size: int,
    chunk: bytes,
    content_type: str,
) -> tuple[float, str, dict | None]:
    digest = hashlib.sha256()
    remaining = total_size
    writer = await client.open(url, "wb", content_type=content_type)
    started = time.perf_counter()
    try:
        while remaining > 0:
            part = chunk if remaining >= len(chunk) else chunk[:remaining]
            await asyncio.to_thread(writer.write, part)
            digest.update(part)
            remaining -= len(part)
        await asyncio.to_thread(writer.close)
    except Exception:
        await asyncio.to_thread(writer.discard)
        raise
    elapsed = time.perf_counter() - started
    return elapsed, digest.hexdigest(), writer.result


async def _idle_reader_check(
    client: LocalS3ClientAsync,
    url: str,
    *,
    chunk_size: int,
    idle_secs: float,
    probe_interval: float,
) -> dict[str, Any]:
    reader = await client.open(url, "rb")
    try:
        first = await asyncio.to_thread(reader.read, chunk_size)
        probe_task = asyncio.create_task(
            _health_probe(
                client,
                duration=idle_secs,
                interval=probe_interval,
                label="idle_reader",
            )
        )
        await asyncio.sleep(idle_secs)
        second = await asyncio.to_thread(reader.read, chunk_size)
        probe = await probe_task
    finally:
        await asyncio.to_thread(reader.close)
    if len(first) != chunk_size or len(second) == 0:
        raise RuntimeError("idle reader failed to continue after pause")
    return {
        "event": "idle_reader_ok",
        "first_bytes": len(first),
        "second_bytes": len(second),
        "idle_secs": idle_secs,
        "probe": probe,
    }


async def _idle_writer_and_close_check(
    client: LocalS3ClientAsync,
    url: str,
    *,
    total_size: int,
    chunk: bytes,
    idle_secs: float,
    probe_interval: float,
    content_type: str,
) -> dict[str, Any]:
    writer = await client.open(url, "wb", content_type=content_type)
    try:
        await asyncio.to_thread(writer.write, chunk)
        idle_probe_task = asyncio.create_task(
            _health_probe(
                client,
                duration=idle_secs,
                interval=probe_interval,
                label="idle_writer",
            )
        )
        await asyncio.sleep(idle_secs)
        idle_probe = await idle_probe_task

        remaining = total_size - len(chunk)
        while remaining > 0:
            part = chunk if remaining >= len(chunk) else chunk[:remaining]
            await asyncio.to_thread(writer.write, part)
            remaining -= len(part)

        close_task = asyncio.create_task(asyncio.to_thread(writer.close))
        close_probe_task = asyncio.create_task(
            _health_probe(
                client,
                duration=max(0.5, idle_secs),
                interval=probe_interval,
                label="writer_close_upload",
            )
        )
        await close_task
        close_probe = await close_probe_task
    except Exception:
        await asyncio.to_thread(writer.discard)
        raise
    return {
        "event": "idle_writer_ok",
        "idle_secs": idle_secs,
        "idle_probe": idle_probe,
        "close_probe": close_probe,
        "result": writer.result,
    }


async def _range_resume_check(
    client: LocalS3ClientAsync,
    url: str,
    *,
    total_size: int,
    chunk_size: int,
    expected_sha256: str,
    resume_after: int,
) -> dict[str, Any]:
    digest = hashlib.sha256()
    offset = 0
    reader = await client.open(url, "rb")
    try:
        while offset < resume_after:
            data = await asyncio.to_thread(reader.read, min(chunk_size, resume_after - offset))
            if not data:
                raise RuntimeError("reader ended before resume offset")
            digest.update(data)
            offset += len(data)
    finally:
        await asyncio.to_thread(reader.close)

    reader = await client.open(url, "rb", range_header=f"bytes={offset}-")
    try:
        content_range = getattr(reader, "headers", {}).get("content-range")
        while True:
            data = await asyncio.to_thread(reader.read, chunk_size)
            if not data:
                break
            digest.update(data)
            offset += len(data)
    finally:
        await asyncio.to_thread(reader.close)
    actual = digest.hexdigest()
    if offset != total_size or actual != expected_sha256:
        raise RuntimeError("range resume verification failed")
    return {
        "event": "range_resume_ok",
        "resume_after": resume_after,
        "size_bytes": offset,
        "sha256": actual,
        "content_range": content_range,
    }


def _print(record: dict[str, Any]) -> None:
    print(json.dumps(record, ensure_ascii=False, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate file-like stream multiplexing and Range resume behavior")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--bucket", default="bench-stream-multiplex")
    parser.add_argument("--key", default="large/stream-multiplex.bin")
    parser.add_argument("--writer-key", default="large/stream-multiplex-writer.bin")
    parser.add_argument("--size-mib", type=int, default=256)
    parser.add_argument("--chunk-mib", type=int, default=8)
    parser.add_argument("--resume-after-mib", type=int, default=64)
    parser.add_argument("--idle-secs", type=float, default=12.0)
    parser.add_argument("--probe-interval", type=float, default=0.25)
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--disable-proxy", action="store_true")
    parser.add_argument("--content-type", default="application/octet-stream")
    parser.add_argument("--keep-objects", action="store_true")
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    total_size = _mib(args.size_mib)
    chunk_size = min(_mib(args.chunk_mib), total_size)
    resume_after = min(_mib(args.resume_after_mib), total_size)
    if total_size < 1 or chunk_size < 1:
        raise SystemExit("size and chunk must be positive")
    chunk = _make_chunk(chunk_size, "alocals3-stream-multiplex")
    url = _object_url(args.bucket, args.key)
    writer_url = _object_url(args.bucket, args.writer_key)

    async with LocalS3ClientAsync(args.endpoint, timeout=args.timeout, disable_proxy=args.disable_proxy) as client:
        await _ensure_bucket(client, args.bucket)
        _print(
            {
                "event": "start",
                "endpoint": args.endpoint,
                "url": url,
                "writer_url": writer_url,
                "size_bytes": total_size,
                "chunk_bytes": chunk_size,
                "idle_secs": args.idle_secs,
                "timeout_secs": args.timeout,
            }
        )
        write_elapsed, write_sha256, write_result = await _write_object(
            client,
            url,
            total_size=total_size,
            chunk=chunk,
            content_type=args.content_type,
        )
        _print(
            {
                "event": "prepared_object",
                "seconds": round(write_elapsed, 6),
                "sha256": write_sha256,
                "result": write_result,
            }
        )
        _print(
            await _idle_reader_check(
                client,
                url,
                chunk_size=chunk_size,
                idle_secs=args.idle_secs,
                probe_interval=args.probe_interval,
            )
        )
        _print(
            await _range_resume_check(
                client,
                url,
                total_size=total_size,
                chunk_size=chunk_size,
                expected_sha256=write_sha256,
                resume_after=resume_after,
            )
        )
        _print(
            await _idle_writer_and_close_check(
                client,
                writer_url,
                total_size=total_size,
                chunk=chunk,
                idle_secs=args.idle_secs,
                probe_interval=args.probe_interval,
                content_type=args.content_type,
            )
        )
        if not args.keep_objects:
            await client.delete_object(args.bucket, args.key)
            await client.delete_object(args.bucket, args.writer_key)
        _print({"event": "done", "kept_objects": args.keep_objects})
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
