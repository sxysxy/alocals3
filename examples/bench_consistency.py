#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import random
import string
import threading
import time
from collections import Counter

import httpx


def _rand_text(n: int) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(n))


def build_payload(seq: int, blob_size: int) -> bytes:
    blob = _rand_text(blob_size).encode("utf-8")
    blob_sha = hashlib.sha256(blob).hexdigest()
    header = f"seq:{seq}\nsha256:{blob_sha}\n".encode("utf-8")
    return header + blob


def validate_payload(data: bytes) -> bool:
    parts = data.split(b"\n", 2)
    if len(parts) != 3:
        return False
    seq_line, sha_line, blob = parts
    if not seq_line.startswith(b"seq:"):
        return False
    if not sha_line.startswith(b"sha256:"):
        return False
    expected = sha_line[len(b"sha256:") :].decode("utf-8", errors="ignore")
    actual = hashlib.sha256(blob).hexdigest()
    return expected == actual


def run(args: argparse.Namespace) -> int:
    endpoint = args.endpoint.rstrip("/")
    bucket = args.bucket
    key = args.key

    counts: Counter[str] = Counter()
    lock = threading.Lock()
    stop_at = time.time() + args.duration

    client = httpx.Client(base_url=endpoint, timeout=args.timeout)
    try:
        r = client.put(f"/s3/{bucket}")
        if r.status_code not in (201, 409):
            print(f"failed to ensure bucket, status={r.status_code}, body={r.text}")
            return 2

        def writer(worker_id: int) -> None:
            seq = 0
            local = Counter()
            while time.time() < stop_at:
                seq += 1
                payload = build_payload(seq=(worker_id * 10_000_000 + seq), blob_size=args.payload_size)
                try:
                    resp = client.put(
                        f"/s3/{bucket}/{key}",
                        content=payload,
                        headers={"content-type": "application/octet-stream"},
                    )
                    if resp.status_code in (200, 201):
                        local["put_ok"] += 1
                    elif resp.status_code == 409:
                        # Expected under high contention on the same key.
                        local["put_conflict_409"] += 1
                    else:
                        local[f"put_{resp.status_code}"] += 1
                        local["fail"] += 1
                except httpx.HTTPError:
                    local["put_http_error"] += 1
                    local["fail"] += 1
            with lock:
                counts.update(local)

        def reader() -> None:
            local = Counter()
            while time.time() < stop_at:
                try:
                    resp = client.get(f"/s3/{bucket}/{key}")
                    if resp.status_code == 200:
                        etag = resp.headers.get("etag", "").strip('"')
                        md5 = hashlib.md5(resp.content, usedforsecurity=False).hexdigest()
                        if not validate_payload(resp.content) or (etag and etag != md5):
                            local["corrupt_read"] += 1
                            local["fail"] += 1
                        else:
                            local["get_ok"] += 1
                    elif resp.status_code == 404:
                        local["get_404"] += 1
                    else:
                        local[f"get_{resp.status_code}"] += 1
                        local["fail"] += 1
                except httpx.HTTPError:
                    local["get_http_error"] += 1
                    local["fail"] += 1
            with lock:
                counts.update(local)

        def deleter() -> None:
            local = Counter()
            while time.time() < stop_at:
                try:
                    resp = client.delete(f"/s3/{bucket}/{key}")
                    if resp.status_code == 204:
                        local["del_ok"] += 1
                    elif resp.status_code == 404:
                        local["del_404"] += 1
                    else:
                        local[f"del_{resp.status_code}"] += 1
                        local["fail"] += 1
                except httpx.HTTPError:
                    local["del_http_error"] += 1
                    local["fail"] += 1
            with lock:
                counts.update(local)

        threads: list[threading.Thread] = []
        for i in range(args.writers):
            threads.append(threading.Thread(target=writer, args=(i,), daemon=True))
        for _ in range(args.readers):
            threads.append(threading.Thread(target=reader, daemon=True))
        for _ in range(args.deleters):
            threads.append(threading.Thread(target=deleter, daemon=True))

        started = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - started

        total_ops = sum(v for k, v in counts.items() if k.endswith("_ok") or k.endswith("_404"))
        print("=== Consistency Benchmark ===")
        print(f"endpoint={endpoint} bucket={bucket} key={key}")
        print(f"duration={elapsed:.2f}s ops={total_ops} fail={counts.get('fail', 0)}")
        for key_name in sorted(counts):
            print(f"{key_name}={counts[key_name]}")

        strict_fail = counts.get("fail", 0)
        if not args.strict_409:
            strict_fail -= counts.get("put_conflict_409", 0)
            if strict_fail < 0:
                strict_fail = 0
        return 1 if strict_fail > 0 else 0
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Concurrent consistency benchmark for alocals3")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000")
    parser.add_argument("--bucket", default="bench-consistency")
    parser.add_argument("--key", default="shared/object.bin")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--readers", type=int, default=4)
    parser.add_argument("--deleters", type=int, default=2)
    parser.add_argument("--payload-size", type=int, default=2048)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--strict-409",
        action="store_true",
        help="Treat PUT 409 conflict as test failure (default: conflict is informational)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
