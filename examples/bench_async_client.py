#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import random
import time
from collections import Counter

from alocals3.client import ALocalS3ClientAsync


async def run(args: argparse.Namespace) -> int:
    client = ALocalS3ClientAsync(
        args.endpoint, timeout=args.timeout, disable_proxy=args.disable_proxy
    )
    try:
        try:
            await client.create_bucket(args.bucket)
        except RuntimeError as exc:
            if "ALOCALS3_HTTP_STATUS:409" not in str(exc):
                raise

        stop_at = time.monotonic() + args.duration
        counters: Counter[str] = Counter()
        latencies_ms: list[float] = []
        heartbeat_gaps_ms: list[float] = []
        stop_heartbeat = asyncio.Event()

        async def heartbeat() -> None:
            previous = time.perf_counter()
            while not stop_heartbeat.is_set():
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                heartbeat_gaps_ms.append((now - previous) * 1000)
                previous = now

        async def worker(worker_id: int) -> tuple[Counter[str], list[float]]:
            rng = random.Random(worker_id)
            expected: dict[int, bytes] = {}
            local = Counter()
            local_latencies: list[float] = []
            while time.monotonic() < stop_at:
                slot = rng.randrange(args.keys_per_worker)
                key = f"workers/{worker_id:03d}/{slot:03d}.bin"
                started = time.perf_counter()
                if slot not in expected or rng.random() < args.write_ratio:
                    nonce = local["put_ok"].to_bytes(8, "little")
                    body = hashlib.sha256(nonce + key.encode()).digest()
                    body = (body * ((args.object_size + 31) // 32))[: args.object_size]
                    await client.put_object_bytes(
                        args.bucket, key, body, "application/octet-stream"
                    )
                    expected[slot] = body
                    local["put_ok"] += 1
                else:
                    body, _ = await client.get_object_bytes(args.bucket, key)
                    if body != expected[slot]:
                        local["corrupt_get"] += 1
                    else:
                        local["get_ok"] += 1
                local_latencies.append((time.perf_counter() - started) * 1000)
            return local, local_latencies

        heartbeat_task = asyncio.create_task(heartbeat())
        started = time.monotonic()
        try:
            results = await asyncio.gather(
                *(worker(worker_id) for worker_id in range(args.concurrency))
            )
        finally:
            stop_heartbeat.set()
            await heartbeat_task
        elapsed = time.monotonic() - started
        for local, local_latencies in results:
            counters.update(local)
            latencies_ms.extend(local_latencies)

        latencies_ms.sort()

        def percentile(fraction: float) -> float:
            if not latencies_ms:
                return 0.0
            index = min(len(latencies_ms) - 1, int(len(latencies_ms) * fraction))
            return latencies_ms[index]

        total = counters["put_ok"] + counters["get_ok"] + counters["corrupt_get"]
        print("=== Native Async Client Benchmark ===")
        print(
            f"endpoint={args.endpoint} concurrency={args.concurrency} "
            f"duration={elapsed:.2f}s object_size={args.object_size}"
        )
        print(f"total_ops={total} throughput={total / elapsed:.2f} ops/s")
        print(
            f"latency_ms p50={percentile(0.50):.2f} "
            f"p95={percentile(0.95):.2f} p99={percentile(0.99):.2f}"
        )
        print(
            f"event_loop_heartbeat count={len(heartbeat_gaps_ms)} "
            f"max_gap_ms={max(heartbeat_gaps_ms, default=0.0):.2f}"
        )
        for name in sorted(counters):
            print(f"{name}={counters[name]}")

        if not args.keep_objects:
            objects = await client.list_objects(args.bucket)
            await asyncio.gather(
                *(client.delete_object(args.bucket, item["key"]) for item in objects)
            )
            await client.delete_bucket(args.bucket)
        return 1 if counters["corrupt_get"] else 0
    finally:
        await client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stress the native PyO3 async client")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--bucket", default="bench-native-async-client")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--keys-per-worker", type=int, default=8)
    parser.add_argument("--object-size", type=int, default=64 * 1024)
    parser.add_argument("--write-ratio", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--disable-proxy", action="store_true")
    parser.add_argument("--keep-objects", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(parse_args())))
