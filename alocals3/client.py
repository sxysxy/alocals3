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

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        params: dict[str, str | int] = {
            "list-type": 2,
            "prefix": prefix,
            "delimiter": delimiter,
            "max-keys": max_keys,
            "output": "json",
        }
        if continuation_token:
            params["continuation-token"] = continuation_token
        response = self._client.get(f"/s3/{bucket}", params=params)
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

    def get_object_range(self, bucket: str, key: str, range_header: str) -> tuple[bytes, dict]:
        response = self._client.get(f"/s3/{bucket}/{key}", headers={"range": range_header})
        response.raise_for_status()
        return response.content, dict(response.headers)

    def get_object_to_file(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        range_header: str | None = None,
    ) -> dict:
        headers: dict[str, str] = {}
        if range_header:
            headers["range"] = range_header
        response = self._client.get(f"/s3/{bucket}/{key}", headers=headers)
        response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        return dict(response.headers)

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

    subparsers.add_parser("HEALTH", aliases=["health"], help="Check server health")
    subparsers.add_parser("LIST_BUCKETS", aliases=["lsb"], help="List buckets")

    mb = subparsers.add_parser("CREATE_BUCKET", aliases=["mb"], help="Create bucket")
    mb.add_argument("bucket")

    rb = subparsers.add_parser("DELETE_BUCKET", aliases=["rb"], help="Delete empty bucket")
    rb.add_argument("bucket")

    lso = subparsers.add_parser("LIST_OBJECTS", aliases=["lso"], help="List objects in bucket")
    lso.add_argument("bucket")
    lso.add_argument("--prefix", default=None)
    lso.add_argument("--limit", type=int, default=1000)

    lso2 = subparsers.add_parser("LIST_OBJECTS_V2", help="List objects using S3 ListObjectsV2 semantics")
    lso2.add_argument("bucket")
    lso2.add_argument("--prefix", default="")
    lso2.add_argument("--delimiter", default="")
    lso2.add_argument("--max-keys", type=int, default=1000)
    lso2.add_argument("--continuation-token", default=None)

    put = subparsers.add_parser("PUT", aliases=["put"], help="Upload object")
    put.add_argument("bucket")
    put.add_argument("key")
    put.add_argument("file", help="Local file path")
    put.add_argument("--content-type", default=None)

    get = subparsers.add_parser("GET", aliases=["get"], help="Download object")
    get.add_argument("bucket")
    get.add_argument("key")
    get.add_argument("output", help="Output file path")
    get.add_argument("--range", dest="range_header", default=None, help='HTTP Range header value, e.g. "bytes=0-99"')

    rm = subparsers.add_parser("DELETE", aliases=["rm"], help="Delete object")
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

    cmd_map = {
        "health": "HEALTH",
        "lsb": "LIST_BUCKETS",
        "mb": "CREATE_BUCKET",
        "rb": "DELETE_BUCKET",
        "lso": "LIST_OBJECTS",
        "LIST_OBJECTS_V2": "LIST_OBJECTS_V2",
        "put": "PUT",
        "get": "GET",
        "rm": "DELETE",
    }
    command = cmd_map.get(args.command, args.command)

    try:
        if command == "HEALTH":
            print(json.dumps(client.health(), ensure_ascii=False, indent=2))
        elif command == "LIST_BUCKETS":
            print(json.dumps(client.list_buckets(), ensure_ascii=False, indent=2))
        elif command == "CREATE_BUCKET":
            print(json.dumps(client.create_bucket(args.bucket), ensure_ascii=False, indent=2))
        elif command == "DELETE_BUCKET":
            client.delete_bucket(args.bucket)
            print(f"deleted bucket: {args.bucket}")
        elif command == "LIST_OBJECTS":
            objects = client.list_objects(args.bucket, prefix=args.prefix, limit=args.limit)
            print(json.dumps(objects, ensure_ascii=False, indent=2))
        elif command == "LIST_OBJECTS_V2":
            result = client.list_objects_v2(
                bucket=args.bucket,
                prefix=args.prefix,
                delimiter=args.delimiter,
                max_keys=args.max_keys,
                continuation_token=args.continuation_token,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif command == "PUT":
            file_path = Path(args.file)
            result = client.put_object(
                bucket=args.bucket,
                key=args.key,
                file_path=file_path,
                content_type=args.content_type,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif command == "GET":
            output_path = Path(args.output)
            headers = client.get_object_to_file(args.bucket, args.key, output_path, range_header=args.range_header)
            content_range = headers.get("content-range")
            if content_range:
                print(f"downloaded range to: {output_path} ({content_range})")
            else:
                print(f"downloaded to: {output_path}")
        elif command == "DELETE":
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
