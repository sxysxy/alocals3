from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - runtime environment dependent
    httpx = None  # type: ignore[assignment]


class LocalS3Client:
    def __init__(self, base_url: str = "http://127.0.0.1:8000", timeout: float = 10.0) -> None:
        if httpx is None:
            raise RuntimeError("httpx is required, run: pip install -r requirements.txt")
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def health(self) -> dict:
        response = self._client.get("/healthz")
        response.raise_for_status()
        return response.json()

    def list_buckets(self) -> list[dict]:
        response = self._client.get("/s3")
        response.raise_for_status()
        return response.json()

    def create_bucket(self, bucket: str) -> dict:
        response = self._client.put(f"/s3/{bucket}")
        response.raise_for_status()
        return response.json()

    def delete_bucket(self, bucket: str) -> None:
        response = self._client.delete(f"/s3/{bucket}")
        response.raise_for_status()

    def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[dict]:
        params = {"limit": limit}
        if prefix:
            params["prefix"] = prefix
        response = self._client.get(f"/s3/{bucket}/objects", params=params)
        response.raise_for_status()
        return response.json()

    def put_object(self, bucket: str, key: str, file_path: Path, content_type: str | None = None) -> dict:
        body = file_path.read_bytes()
        headers: dict[str, str] = {}
        if content_type:
            headers["content-type"] = content_type
        response = self._client.put(f"/s3/{bucket}/{key}", content=body, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_object(self, bucket: str, key: str, output_path: Path) -> None:
        response = self._client.get(f"/s3/{bucket}/{key}")
        response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)

    def delete_object(self, bucket: str, key: str) -> None:
        response = self._client.delete(f"/s3/{bucket}/{key}")
        response.raise_for_status()

    def close(self) -> None:
        self._client.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simple CLI client for alocals3 server")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000", help="Server endpoint")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="Check server health")
    subparsers.add_parser("lsb", help="List buckets")

    mb = subparsers.add_parser("mb", help="Create bucket")
    mb.add_argument("bucket")

    rb = subparsers.add_parser("rb", help="Delete empty bucket")
    rb.add_argument("bucket")

    lso = subparsers.add_parser("lso", help="List objects in bucket")
    lso.add_argument("bucket")
    lso.add_argument("--prefix", default=None)
    lso.add_argument("--limit", type=int, default=1000)

    put = subparsers.add_parser("put", help="Upload object")
    put.add_argument("bucket")
    put.add_argument("key")
    put.add_argument("file", help="Local file path")
    put.add_argument("--content-type", default=None)

    get = subparsers.add_parser("get", help="Download object")
    get.add_argument("bucket")
    get.add_argument("key")
    get.add_argument("output", help="Output file path")

    rm = subparsers.add_parser("rm", help="Delete object")
    rm.add_argument("bucket")
    rm.add_argument("key")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        client = LocalS3Client(base_url=args.endpoint, timeout=args.timeout)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        if args.command == "health":
            print(json.dumps(client.health(), ensure_ascii=False, indent=2))
        elif args.command == "lsb":
            print(json.dumps(client.list_buckets(), ensure_ascii=False, indent=2))
        elif args.command == "mb":
            print(json.dumps(client.create_bucket(args.bucket), ensure_ascii=False, indent=2))
        elif args.command == "rb":
            client.delete_bucket(args.bucket)
            print(f"deleted bucket: {args.bucket}")
        elif args.command == "lso":
            objects = client.list_objects(args.bucket, prefix=args.prefix, limit=args.limit)
            print(json.dumps(objects, ensure_ascii=False, indent=2))
        elif args.command == "put":
            file_path = Path(args.file)
            result = client.put_object(
                bucket=args.bucket,
                key=args.key,
                file_path=file_path,
                content_type=args.content_type,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.command == "get":
            output_path = Path(args.output)
            client.get_object(args.bucket, args.key, output_path)
            print(f"downloaded to: {output_path}")
        elif args.command == "rm":
            client.delete_object(args.bucket, args.key)
            print(f"deleted object: {args.bucket}/{args.key}")
        else:
            parser.print_help()
            return 2
    except FileNotFoundError as exc:
        print(f"file not found: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        if httpx is not None and isinstance(exc, httpx.HTTPError):
            print(f"http error: {exc}", file=sys.stderr)
            return 1
        raise
    finally:
        client.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
