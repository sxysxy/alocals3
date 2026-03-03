#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import random
import string
import time
from collections import Counter

import httpx


def _rand_key(key_space: int) -> str:
    i = random.randint(0, key_space - 1)
    return f"k/{i:06d}.bin"


def _rand_body(size: int) -> bytes:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(size)).encode("utf-8")


async def run(args: argparse.Namespace) -> int:
    endpoint = args.endpoint.rstrip("/")
    stop_at = time.monotonic() + args.duration
    counters: Counter[str] = Counter()
    latencies_ms: list[float] = []
    lat_lock = asyncio.Lock()

    async with httpx.AsyncClient(base_url=endpoint, timeout=args.timeout) as client:
        r = await client.put(f"/s3/{args.bucket}")
        if r.status_code not in (201, 409):
            print(f"failed to ensure bucket, status={r.status_code}, body={r.text}")
            return 2

        async def worker() -> None:
            local_counts: Counter[str] = Counter()
            local_lats: list[float] = []
            while time.monotonic() < stop_at:
                key = _rand_key(args.key_space)
                op_put = random.random() < args.write_ratio
                t0 = time.perf_counter()
                if op_put:
                    body = _rand_body(args.object_size)
                    resp = await client.put(
                        f"/s3/{args.bucket}/{key}",
                        content=body,
                        headers={"content-type": "application/octet-stream"},
                    )
                    dt = (time.perf_counter() - t0) * 1000
                    local_lats.append(dt)
                    if resp.status_code in (200, 201):
                        local_counts["put_ok"] += 1
                    else:
                        local_counts[f"put_{resp.status_code}"] += 1
                else:
                    resp = await client.get(f"/s3/{args.bucket}/{key}")
                    dt = (time.perf_counter() - t0) * 1000
                    local_lats.append(dt)
                    if resp.status_code == 200:
                        etag = resp.headers.get("etag", "").strip('"')
                        md5 = hashlib.md5(resp.content, usedforsecurity=False).hexdigest()
                        if etag and etag != md5:
                            local_counts["corrupt_get"] += 1
                        else:
                            local_counts["get_ok"] += 1
                    elif resp.status_code == 404:
                        local_counts["get_404"] += 1
                    else:
                        local_counts[f"get_{resp.status_code}"] += 1

            async with lat_lock:
                counters.update(local_counts)
                latencies_ms.extend(local_lats)

        tasks = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
        started = time.monotonic()
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - started

    latencies_ms.sort()
    total = sum(counters.values())
    throughput = total / elapsed if elapsed > 0 else 0.0

    def pct(p: float) -> float:
        if not latencies_ms:
            return 0.0
        idx = min(len(latencies_ms) - 1, int(len(latencies_ms) * p))
        return latencies_ms[idx]

    print("=== Stress Benchmark ===")
    print(f"endpoint={endpoint} bucket={args.bucket}")
    print(f"duration={elapsed:.2f}s total_ops={total} throughput={throughput:.2f} ops/s")
    print(f"latency_ms p50={pct(0.50):.2f} p95={pct(0.95):.2f} p99={pct(0.99):.2f}")
    for k in sorted(counters):
        print(f"{k}={counters[k]}")

    return 1 if counters.get("corrupt_get", 0) > 0 else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stress benchmark for alocals3")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--bucket", default="bench-stress")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--key-space", type=int, default=1000)
    parser.add_argument("--object-size", type=int, default=4096)
    parser.add_argument("--write-ratio", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(parse_args())))
