from __future__ import annotations

import argparse
import asyncio
import io
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from alocals3._alocals3_native import RustHttpClient

class ALocalS3Client:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8000",
        timeout: float = 10.0,
        disable_proxy: bool = False,
    ) -> None:
        if RustHttpClient is None:
            raise RuntimeError("alocals3._alocals3_native is required; install with: pip install -e .")
        self._client = RustHttpClient(base_url, timeout, disable_proxy)

    def __enter__(self) -> "ALocalS3Client":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def health(self) -> dict:
        return _loads(self._client.health_json())

    def list_buckets(self) -> list[dict]:
        return _loads(self._client.list_buckets_json())

    def create_bucket(self, bucket: str) -> dict:
        return _loads(self._client.create_bucket_json(bucket))

    def delete_bucket(self, bucket: str) -> None:
        self._client.delete_bucket(bucket)

    def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[dict]:
        return _loads(self._client.list_objects_json(bucket, prefix, limit))

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        return _loads(self._client.list_objects_v2_json(bucket, prefix, delimiter, max_keys, continuation_token))

    def put_object(
        self,
        bucket: str,
        key: str,
        file_path: Path,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict:
        return _loads(self._client.put_object_json(bucket, key, str(file_path), content_type, metadata))

    def put_object_bytes(
        self,
        bucket: str,
        key: str,
        body: bytes | bytearray | memoryview,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict:
        return _loads(self._client.put_object_bytes_json(bucket, key, bytes(body), content_type, metadata))

    def get_object(self, bucket: str, key: str, output_path: Path) -> None:
        self._client.get_object_to_file(bucket, key, str(output_path), None)

    def get_object_bytes(self, bucket: str, key: str, range_header: str | None = None) -> tuple[bytes, dict]:
        result = self._client.get_object_bytes(bucket, key, range_header)
        return result["body"], result["headers"]

    def get_object_range(self, bucket: str, key: str, range_header: str) -> tuple[bytes, dict]:
        result = self._client.get_object_range(bucket, key, range_header)
        return result["body"], result["headers"]

    def get_object_to_file(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        range_header: str | None = None,
    ) -> dict:
        return self._client.get_object_to_file(bucket, key, str(output_path), range_header)

    def delete_object(self, bucket: str, key: str) -> None:
        self._client.delete_object(bucket, key)

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str,
        *,
        metadata: dict[str, str] | None = None,
        metadata_directive: str | None = None,
        content_type: str | None = None,
    ) -> dict:
        """Copy an object in O(1) by sharing its content-addressed blob."""
        return _loads(
            self._client.copy_object_json(
                source_bucket,
                source_key,
                destination_bucket,
                destination_key,
                metadata,
                metadata_directive,
                content_type,
            )
        )

    def open(
        self,
        url: str,
        mode: str = "rb",
        *,
        encoding: str = "utf-8",
        errors: str | None = None,
        newline: str | None = None,
        content_type: str | None = None,
        cache_path: str | Path | None = None,
        range_header: str | None = None,
    ):
        bucket, key = _parse_object_url(url)
        mode_info = _parse_mode(mode)
        if mode_info["read"]:
            native_reader = self._client.open_object_reader(bucket, key, range_header)
            headers = native_reader.headers()
            raw_reader = _S3StreamingRawReader(native_reader, cache_path=cache_path)
            if mode_info["binary"]:
                reader = io.BufferedReader(raw_reader)
                _set_object_attrs(reader, headers=headers, bucket=bucket, key=key)
                return reader
            reader = io.TextIOWrapper(
                io.BufferedReader(raw_reader),
                encoding=encoding,
                errors=errors,
                newline=newline,
            )
            _set_object_attrs(reader, headers=headers, bucket=bucket, key=key)
            return reader
        if range_header is not None:
            raise ValueError("range_header is only supported for read modes")
        if mode_info["binary"]:
            native_writer = self._client.open_object_writer(bucket, key, content_type)
            return _S3StreamingBytesWriter(
                native_writer,
                bucket=bucket,
                key=key,
                cache_path=cache_path,
            )
        native_writer = self._client.open_object_writer(bucket, key, content_type)
        return _S3StreamingTextWriter(
            native_writer,
            bucket=bucket,
            key=key,
            encoding=encoding,
            errors=errors,
            newline=newline,
            cache_path=cache_path,
        )

    def close(self) -> None:
        return None


class ALocalS3ClientAsync:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8000",
        timeout: float = 10.0,
        disable_proxy: bool = False,
    ) -> None:
        self._sync = ALocalS3Client(base_url=base_url, timeout=timeout, disable_proxy=disable_proxy)

    async def __aenter__(self) -> "ALocalS3ClientAsync":
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        await self.close()

    async def health(self) -> dict:
        return await asyncio.to_thread(self._sync.health)

    async def list_buckets(self) -> list[dict]:
        return await asyncio.to_thread(self._sync.list_buckets)

    async def create_bucket(self, bucket: str) -> dict:
        return await asyncio.to_thread(self._sync.create_bucket, bucket)

    async def delete_bucket(self, bucket: str) -> None:
        await asyncio.to_thread(self._sync.delete_bucket, bucket)

    async def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[dict]:
        return await asyncio.to_thread(self._sync.list_objects, bucket, prefix, limit)

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        return await asyncio.to_thread(
            self._sync.list_objects_v2,
            bucket,
            prefix,
            delimiter,
            max_keys,
            continuation_token,
        )

    async def put_object(
        self,
        bucket: str,
        key: str,
        file_path: Path,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict:
        return await asyncio.to_thread(self._sync.put_object, bucket, key, file_path, content_type, metadata)

    async def put_object_bytes(
        self,
        bucket: str,
        key: str,
        body: bytes | bytearray | memoryview,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> dict:
        return await asyncio.to_thread(self._sync.put_object_bytes, bucket, key, body, content_type, metadata)

    async def get_object(self, bucket: str, key: str, output_path: Path) -> None:
        await asyncio.to_thread(self._sync.get_object, bucket, key, output_path)

    async def get_object_bytes(self, bucket: str, key: str, range_header: str | None = None) -> tuple[bytes, dict]:
        return await asyncio.to_thread(self._sync.get_object_bytes, bucket, key, range_header)

    async def get_object_range(self, bucket: str, key: str, range_header: str) -> tuple[bytes, dict]:
        return await asyncio.to_thread(self._sync.get_object_range, bucket, key, range_header)

    async def get_object_to_file(
        self,
        bucket: str,
        key: str,
        output_path: Path,
        range_header: str | None = None,
    ) -> dict:
        return await asyncio.to_thread(self._sync.get_object_to_file, bucket, key, output_path, range_header)

    async def delete_object(self, bucket: str, key: str) -> None:
        await asyncio.to_thread(self._sync.delete_object, bucket, key)

    async def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: str,
        *,
        metadata: dict[str, str] | None = None,
        metadata_directive: str | None = None,
        content_type: str | None = None,
    ) -> dict:
        return await asyncio.to_thread(
            self._sync.copy_object,
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            metadata=metadata,
            metadata_directive=metadata_directive,
            content_type=content_type,
        )

    def open(
        self,
        url: str,
        mode: str = "rb",
        *,
        encoding: str = "utf-8",
        errors: str | None = None,
        newline: str | None = None,
        content_type: str | None = None,
        cache_path: str | Path | None = None,
        range_header: str | None = None,
    ):
        return _AsyncS3OpenContext(
            self._sync,
            url,
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
            content_type=content_type,
            cache_path=cache_path,
            range_header=range_header,
        )

    async def close(self) -> None:
        self._sync.close()

    async def aclose(self) -> None:
        await self.close()


def _loads(value: str) -> Any:
    return json.loads(value)


class _S3ObjectMixin:
    headers: dict
    bucket: str
    key: str


class _S3BytesReader(_S3ObjectMixin, io.BytesIO):
    def __init__(self, body: bytes, *, headers: dict, bucket: str, key: str) -> None:
        super().__init__(body)
        self.headers = headers
        self.bucket = bucket
        self.key = key


class _S3TextReader(_S3ObjectMixin, io.StringIO):
    def __init__(
        self,
        body: str,
        *,
        headers: dict,
        bucket: str,
        key: str,
        newline: str | None,
    ) -> None:
        super().__init__(body, newline=newline)
        self.headers = headers
        self.bucket = bucket
        self.key = key


class _S3StreamingRawReader(io.RawIOBase):
    def __init__(self, native_reader, *, cache_path: str | Path | None) -> None:
        super().__init__()
        self._native_reader = native_reader
        self._cache = _open_cache_writer_best_effort(cache_path)

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed S3 reader")
        native_readinto = getattr(self._native_reader, "readinto", None)
        if native_readinto is not None:
            try:
                n = native_readinto(buffer)
            except (BufferError, TypeError, ValueError):
                n = None
            else:
                if n and self._cache is not None:
                    try:
                        self._cache.write(memoryview(buffer)[:n])
                    except OSError:
                        self._close_cache()
                return n
        data = self._native_reader.read(len(buffer))
        if not data:
            return 0
        buffer[: len(data)] = data
        if self._cache is not None:
            try:
                self._cache.write(data)
            except OSError:
                self._close_cache()
        return len(data)

    def close(self) -> None:
        if not self.closed:
            try:
                self._native_reader.close()
            finally:
                self._close_cache()
        super().close()

    def _close_cache(self) -> None:
        if self._cache is not None:
            try:
                self._cache.close()
            except OSError:
                pass
            self._cache = None


class _S3StreamingBytesWriter:
    def __init__(
        self,
        native_writer,
        *,
        bucket: str,
        key: str,
        cache_path: str | Path | None,
    ) -> None:
        self._native_writer = native_writer
        self.bucket = bucket
        self.key = key
        self.cache_path = cache_path
        self.result: dict | None = None
        self.closed = False
        self._uploaded = False
        self._cache = _open_cache_writer_best_effort(cache_path)

    def writable(self) -> bool:
        return not self.closed

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed S3 writer")
        body = bytes(data)
        written = self._native_writer.write(body)
        if self._cache is not None:
            try:
                self._cache.write(body)
            except OSError:
                self._close_cache()
        return written

    def flush(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed S3 writer")
        self._native_writer.flush()
        if self._cache is not None:
            try:
                self._cache.flush()
            except OSError:
                self._close_cache()

    def close(self) -> None:
        if self.closed:
            return
        try:
            result_json = self._native_writer.close()
            self.result = _loads(result_json) if result_json else None
            self._uploaded = True
        finally:
            self.closed = True
            self._close_cache()

    def discard(self) -> None:
        if not self.closed:
            self._native_writer.discard()
            self.closed = True
            self._close_cache()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if exc_type is None:
            self.close()
        else:
            self.discard()

    def _close_cache(self) -> None:
        if self._cache is not None:
            try:
                self._cache.close()
            except OSError:
                pass
            self._cache = None


class _S3StreamingTextWriter:
    def __init__(
        self,
        native_writer,
        *,
        bucket: str,
        key: str,
        encoding: str,
        errors: str | None,
        newline: str | None,
        cache_path: str | Path | None,
    ) -> None:
        self._binary = _S3StreamingBytesWriter(
            native_writer,
            bucket=bucket,
            key=key,
            cache_path=cache_path,
        )
        self.bucket = bucket
        self.key = key
        self.encoding = encoding
        self.errors = errors or "strict"
        self.newline = newline
        self.result: dict | None = None

    @property
    def closed(self) -> bool:
        return self._binary.closed

    def writable(self) -> bool:
        return self._binary.writable()

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("write() argument must be str, not bytes")
        if self.newline not in (None, ""):
            text = text.replace("\n", self.newline)
        data = text.encode(self.encoding, self.errors)
        self._binary.write(data)
        return len(text)

    def flush(self) -> None:
        self._binary.flush()

    def close(self) -> None:
        self._binary.close()
        self.result = self._binary.result

    def discard(self) -> None:
        self._binary.discard()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self._binary.__exit__(exc_type, exc_value, traceback)
        self.result = self._binary.result


class _AsyncS3OpenContext:
    def __init__(
        self,
        client: ALocalS3Client,
        url: str,
        mode: str,
        *,
        encoding: str,
        errors: str | None,
        newline: str | None,
        content_type: str | None,
        cache_path: str | Path | None,
        range_header: str | None,
    ) -> None:
        self._client = client
        self._url = url
        self._mode = mode
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._content_type = content_type
        self._cache_path = cache_path
        self._range_header = range_header
        self._file: _AsyncS3File | None = None

    def __await__(self):
        return self._open().__await__()

    async def __aenter__(self):
        self._file = await self._open()
        return self._file

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if self._file is not None:
            await self._file.__aexit__(exc_type, exc_value, traceback)
            self._file = None

    async def _open(self):
        file_obj = await asyncio.to_thread(
            self._client.open,
            self._url,
            self._mode,
            encoding=self._encoding,
            errors=self._errors,
            newline=self._newline,
            content_type=self._content_type,
            cache_path=self._cache_path,
            range_header=self._range_header,
        )
        return _AsyncS3File(file_obj)


class _AsyncS3File:
    def __init__(self, file_obj) -> None:
        self._file = file_obj
        self._lock = asyncio.Lock()

    @property
    def headers(self):
        return getattr(self._file, "headers", None)

    @property
    def bucket(self):
        return getattr(self._file, "bucket", None)

    @property
    def key(self):
        return getattr(self._file, "key", None)

    @property
    def result(self):
        return getattr(self._file, "result", None)

    @property
    def closed(self) -> bool:
        return bool(getattr(self._file, "closed", False))

    def __getattr__(self, name: str):
        return getattr(self._file, name)

    def readable(self) -> bool:
        readable = getattr(self._file, "readable", None)
        return bool(readable()) if readable is not None else False

    def writable(self) -> bool:
        writable = getattr(self._file, "writable", None)
        return bool(writable()) if writable is not None else False

    async def read(self, size: int = -1):
        return await self._call("read", size)

    async def read1(self, size: int = -1):
        if not hasattr(self._file, "read1"):
            return await self.read(size)
        return await self._call("read1", size)

    async def readline(self, size: int = -1):
        return await self._call("readline", size)

    async def readlines(self, hint: int = -1):
        return await self._call("readlines", hint)

    async def readinto(self, buffer) -> int:
        return await self._call("readinto", buffer)

    def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def write(self, data) -> int:
        return await self._call("write", data)

    async def flush(self) -> None:
        await self._call("flush")

    async def close(self) -> None:
        await self._call("close")

    async def discard(self) -> None:
        discard = getattr(self._file, "discard", None)
        if discard is None:
            await self.close()
            return
        async with self._lock:
            await asyncio.to_thread(discard)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        exit_func = getattr(self._file, "__exit__", None)
        if exit_func is None:
            await self.close()
            return
        async with self._lock:
            await asyncio.to_thread(exit_func, exc_type, exc_value, traceback)

    async def _call(self, method_name: str, *args):
        method = getattr(self._file, method_name)
        async with self._lock:
            return await asyncio.to_thread(method, *args)


class _S3BytesWriter(io.BytesIO):
    def __init__(
        self,
        client: ALocalS3Client,
        bucket: str,
        key: str,
        *,
        content_type: str | None,
        cache_path: str | Path | None,
    ) -> None:
        super().__init__()
        self._client = client
        self.bucket = bucket
        self.key = key
        self.content_type = content_type
        self.cache_path = cache_path
        self.result: dict | None = None
        self._uploaded = False

    def close(self) -> None:
        if not self.closed:
            try:
                if not self._uploaded:
                    self._upload()
            finally:
                super().close()

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            if exc_type is None and not self._uploaded:
                self._upload()
        finally:
            super().close()

    def _upload(self) -> None:
        body = self.getvalue()
        self.result = self._client.put_object_bytes(self.bucket, self.key, body, self.content_type)
        if self.cache_path is not None:
            _write_cache_best_effort(self.cache_path, body)
        self._uploaded = True


class _S3TextWriter(io.StringIO):
    def __init__(
        self,
        client: ALocalS3Client,
        bucket: str,
        key: str,
        *,
        encoding: str,
        errors: str | None,
        newline: str | None,
        content_type: str | None,
        cache_path: str | Path | None,
    ) -> None:
        super().__init__(newline=newline)
        self._client = client
        self.bucket = bucket
        self.key = key
        self._encoding = encoding
        self._errors = errors or "strict"
        self.content_type = content_type
        self.cache_path = cache_path
        self.result: dict | None = None
        self._uploaded = False

    def close(self) -> None:
        if not self.closed:
            try:
                if not self._uploaded:
                    self._upload()
            finally:
                super().close()

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            if exc_type is None and not self._uploaded:
                self._upload()
        finally:
            super().close()

    def _upload(self) -> None:
        body = self.getvalue().encode(self._encoding, self._errors)
        self.result = self._client.put_object_bytes(self.bucket, self.key, body, self.content_type)
        if self.cache_path is not None:
            _write_cache_best_effort(self.cache_path, body)
        self._uploaded = True


def _parse_mode(mode: str) -> dict:
    if not mode:
        raise ValueError("mode must not be empty")
    if "+" in mode or "a" in mode or "x" in mode:
        raise ValueError("S3 object open() supports read-only or overwrite-only modes, not update/append/exclusive modes")
    if "b" in mode and "t" in mode:
        raise ValueError("mode cannot contain both 'b' and 't'")
    read = "r" in mode
    write = "w" in mode
    if read == write:
        raise ValueError("mode must contain exactly one of 'r' or 'w'")
    allowed = {"r", "w", "b", "t"}
    extra = set(mode) - allowed
    if extra:
        raise ValueError(f"unsupported mode character(s): {''.join(sorted(extra))}")
    return {"read": read, "write": write, "binary": "b" in mode}


def _parse_object_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = unquote(parsed.path.lstrip("/"))
    elif parsed.scheme in {"http", "https"}:
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        if len(parts) < 3 or parts[0] != "s3":
            raise ValueError("HTTP object URL must have path /s3/{bucket}/{key}")
        bucket = parts[1]
        key = "/".join(parts[2:])
    elif parsed.scheme:
        raise ValueError(f"unsupported object URL scheme: {parsed.scheme}")
    else:
        bucket, sep, key = url.lstrip("/").partition("/")
        if not sep:
            raise ValueError("object URL must be s3://bucket/key, /bucket/key, or an HTTP /s3/{bucket}/{key} URL")
    if not bucket or not key:
        raise ValueError("object URL must include both bucket and key")
    return bucket, key


def _write_cache_best_effort(path: str | Path, body: bytes) -> None:
    try:
        cache_path = Path(path)
        if cache_path.parent != Path(""):
            cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(body)
    except OSError:
        pass


def _open_cache_writer_best_effort(path: str | Path | None):
    if path is None:
        return None
    try:
        cache_path = Path(path)
        if cache_path.parent != Path(""):
            cache_path.parent.mkdir(parents=True, exist_ok=True)
        return cache_path.open("wb")
    except OSError:
        return None


def _set_object_attrs(file_obj, *, headers: dict, bucket: str, key: str) -> None:
    file_obj.headers = headers
    file_obj.bucket = bucket
    file_obj.key = key


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simple CLI client for alocals3 server")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000", help="Server endpoint")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds")
    parser.add_argument("--disable-proxy", action="store_true", help="Ignore proxy environment variables")

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

    put_parser = subparsers.add_parser("PUT", aliases=["put"], help="Upload object")
    put_parser.add_argument("bucket")
    put_parser.add_argument("key")
    put_parser.add_argument("file", help="Local file path")
    put_parser.add_argument("--content-type", default=None)
    put_parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="User metadata; may be repeated",
    )

    copy_parser = subparsers.add_parser("COPY", aliases=["cp"], help="Copy object in O(1)")
    copy_parser.add_argument("source_bucket")
    copy_parser.add_argument("source_key")
    copy_parser.add_argument("destination_bucket")
    copy_parser.add_argument("destination_key")
    copy_parser.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Replacement user metadata; may be repeated",
    )
    copy_parser.add_argument("--content-type", default=None)

    get_parser = subparsers.add_parser("GET", aliases=["get"], help="Download object")
    get_parser.add_argument("bucket")
    get_parser.add_argument("key")
    get_parser.add_argument("output", help="Output file path")
    get_parser.add_argument("--range", dest="range_header", default=None)

    rm = subparsers.add_parser("DELETE", aliases=["rm"], help="Delete object")
    rm.add_argument("bucket")
    rm.add_argument("key")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    client = ALocalS3Client(base_url=args.endpoint, timeout=args.timeout, disable_proxy=args.disable_proxy)
    command = {
        "health": "HEALTH",
        "lsb": "LIST_BUCKETS",
        "mb": "CREATE_BUCKET",
        "rb": "DELETE_BUCKET",
        "lso": "LIST_OBJECTS",
        "put": "PUT",
        "cp": "COPY",
        "get": "GET",
        "rm": "DELETE",
    }.get(args.command, args.command)

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
            print(json.dumps(client.list_objects(args.bucket, prefix=args.prefix, limit=args.limit), ensure_ascii=False, indent=2))
        elif command == "LIST_OBJECTS_V2":
            result = client.list_objects_v2(args.bucket, args.prefix, args.delimiter, args.max_keys, args.continuation_token)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif command == "PUT":
            metadata = dict(item.split("=", 1) for item in args.metadata)
            print(json.dumps(client.put_object(args.bucket, args.key, Path(args.file), args.content_type, metadata or None), ensure_ascii=False, indent=2))
        elif command == "COPY":
            metadata = dict(item.split("=", 1) for item in args.metadata)
            result = client.copy_object(
                args.source_bucket,
                args.source_key,
                args.destination_bucket,
                args.destination_key,
                metadata=metadata or None,
                content_type=args.content_type,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif command == "GET":
            headers = client.get_object_to_file(args.bucket, args.key, Path(args.output), args.range_header)
            content_range = headers.get("content-range")
            if content_range:
                print(f"downloaded range to: {args.output} ({content_range})")
            else:
                print(f"downloaded to: {args.output}")
        elif command == "DELETE":
            client.delete_object(args.bucket, args.key)
            print(f"deleted object: {args.bucket}/{args.key}")
        else:
            parser.print_help()
            return 2
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
