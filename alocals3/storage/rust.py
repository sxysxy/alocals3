from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, status

from alocals3.schemas.storage import BucketInfo, ObjectInfo, StoredObject

try:
    from alocals3._rust import RustStorageBackend as _RustStorageBackend
except ModuleNotFoundError:  # pragma: no cover - depends on extension build
    _RustStorageBackend = None  # type: ignore[assignment]


_ERR_PREFIX = "ALOCALS3_STORAGE_ERROR:"


class RustLocalStorageBackend:
    def __init__(self, root: Path, database_url: str) -> None:
        if _RustStorageBackend is None:
            raise RuntimeError("alocals3._rust is not installed")
        self._backend = _RustStorageBackend(str(root), database_url)

    def list_buckets(self) -> list[BucketInfo]:
        rows = self._call(self._backend.list_buckets)
        return [BucketInfo(name=row["name"], created_at=_parse_datetime(row["created_at"])) for row in rows]

    def create_bucket(self, bucket: str) -> BucketInfo:
        row = self._call(self._backend.create_bucket, bucket)
        return BucketInfo(name=row["name"], created_at=_parse_datetime(row["created_at"]))

    def delete_bucket(self, bucket: str) -> None:
        self._call(self._backend.delete_bucket, bucket)

    def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[ObjectInfo]:
        rows = self._call(self._backend.list_objects, bucket, prefix, limit)
        return [_object_info(row) for row in rows]

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        result = self._call(
            self._backend.list_objects_v2,
            bucket,
            prefix,
            delimiter,
            max_keys,
            continuation_token,
        )
        return {
            "bucket": result["bucket"],
            "prefix": result["prefix"],
            "delimiter": result["delimiter"],
            "max_keys": result["max_keys"],
            "key_count": result["key_count"],
            "is_truncated": result["is_truncated"],
            "next_continuation_token": result["next_continuation_token"],
            "contents": [_object_info(row) for row in result["contents"]],
            "common_prefixes": list(result["common_prefixes"]),
        }

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str | None = None) -> ObjectInfo:
        info, _ = self.put_object_with_state(bucket=bucket, key=key, body=body, content_type=content_type)
        return info

    def put_object_with_state(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str | None = None,
    ) -> tuple[ObjectInfo, bool]:
        result = self._call(self._backend.put_object_with_state, bucket, key, body, content_type)
        return _object_info(result["info"]), bool(result["created"])

    def get_object(self, bucket: str, key: str) -> StoredObject:
        row = self._call(self._backend.get_object, bucket, key)
        info = _object_info(row)
        return StoredObject(**info.model_dump(), body=row["body"])

    def get_object_info(self, bucket: str, key: str) -> ObjectInfo | None:
        row = self._call(self._backend.get_object_info, bucket, key)
        if row is None:
            return None
        return _object_info(row)

    def delete_object(self, bucket: str, key: str) -> None:
        self._call(self._backend.delete_object, bucket, key)

    def _call(self, func: Any, *args: Any) -> Any:
        try:
            return func(*args)
        except RuntimeError as exc:
            message = str(exc)
            if not message.startswith(_ERR_PREFIX):
                raise
            status_text, _, detail = message[len(_ERR_PREFIX) :].partition(":")
            try:
                status_code = int(status_text)
            except ValueError:
                status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(status_code=status_code, detail=detail or "Rust storage error") from exc


def _object_info(row: dict) -> ObjectInfo:
    return ObjectInfo(
        bucket=row["bucket"],
        key=row["key"],
        size=row["size"],
        content_type=row["content_type"],
        etag=row["etag"],
        updated_at=_parse_datetime(row["updated_at"]),
    )


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
