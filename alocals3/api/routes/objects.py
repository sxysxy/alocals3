from __future__ import annotations

import base64
import binascii
import hashlib
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from alocals3.api.deps import get_storage
from alocals3.schemas.storage import ObjectInfo, StoredObject
from alocals3.storage.local import LocalStorageBackend

router = APIRouter(prefix="/s3", tags=["objects"])


@router.get("/{bucket}/objects", response_model=list[ObjectInfo])
def list_objects(
    bucket: str,
    prefix: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=10000),
    storage: LocalStorageBackend = Depends(get_storage),
) -> list[ObjectInfo]:
    return storage.list_objects(bucket=bucket, prefix=prefix, limit=limit)


@router.put("/{bucket}/{key:path}", response_model=ObjectInfo)
async def put_object(bucket: str, key: str, request: Request, storage: LocalStorageBackend = Depends(get_storage)) -> Response:
    body = await request.body()
    content_type = request.headers.get("content-type")

    content_md5 = request.headers.get("content-md5")
    if content_md5:
        _validate_content_md5(content_md5, body)

    existing = storage.get_object_info(bucket=bucket, key=key)
    if_none_match = request.headers.get("if-none-match")
    if if_none_match and existing is not None and _etag_in_list(if_none_match, existing.etag):
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail="If-None-Match precondition failed")

    if_match = request.headers.get("if-match")
    if if_match:
        if existing is None or not _etag_in_list(if_match, existing.etag):
            raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail="If-Match precondition failed")

    obj, created = storage.put_object_with_state(bucket=bucket, key=key, body=body, content_type=content_type)
    status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return JSONResponse(
        status_code=status_code,
        content=obj.model_dump(mode="json"),
        headers={"ETag": f"\"{obj.etag}\""},
    )


@router.get("/{bucket}/{key:path}")
def get_object(bucket: str, key: str, request: Request, storage: LocalStorageBackend = Depends(get_storage)) -> Response:
    obj = storage.get_object(bucket=bucket, key=key)
    return _conditional_object_response(obj=obj, request=request, include_body=True)


@router.head("/{bucket}/{key:path}")
def head_object(bucket: str, key: str, request: Request, storage: LocalStorageBackend = Depends(get_storage)) -> Response:
    obj = storage.get_object(bucket=bucket, key=key)
    return _conditional_object_response(obj=obj, request=request, include_body=False)


@router.delete("/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_object(bucket: str, key: str, storage: LocalStorageBackend = Depends(get_storage)) -> None:
    storage.delete_object(bucket=bucket, key=key)


def _conditional_object_response(obj: StoredObject, request: Request, include_body: bool) -> Response:
    body_len = len(obj.body)
    headers = {
        "ETag": f'"{obj.etag}"',
        "Last-Modified": _http_date(obj.updated_at),
        "Accept-Ranges": "bytes",
    }

    if_none_match = request.headers.get("if-none-match")
    if if_none_match and _etag_matches(if_none_match, obj.etag):
        return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    if_modified_since = request.headers.get("if-modified-since")
    if if_modified_since:
        modified_since = _parse_http_date(if_modified_since)
        if modified_since is not None:
            last_updated = _to_utc(obj.updated_at).replace(microsecond=0)
            if last_updated <= modified_since.replace(microsecond=0):
                return Response(status_code=status.HTTP_304_NOT_MODIFIED, headers=headers)

    range_header = request.headers.get("range")
    if range_header:
        start, end = _parse_single_range(range_header, body_len)
        partial = obj.body[start : end + 1]
        partial_len = end - start + 1
        headers["Content-Range"] = f"bytes {start}-{end}/{body_len}"
        headers["Content-Length"] = str(partial_len)
        if include_body:
            return Response(
                status_code=status.HTTP_206_PARTIAL_CONTENT,
                content=partial,
                media_type=obj.content_type,
                headers=headers,
            )
        return Response(status_code=status.HTTP_206_PARTIAL_CONTENT, media_type=obj.content_type, headers=headers)

    headers["Content-Length"] = str(body_len)
    if include_body:
        return Response(content=obj.body, media_type=obj.content_type, headers=headers)

    return Response(status_code=status.HTTP_200_OK, media_type=obj.content_type, headers=headers)


def _etag_matches(if_none_match: str, current_etag: str) -> bool:
    for raw_token in if_none_match.split(","):
        token = raw_token.strip()
        if token == "*":
            return True
        if token.startswith("W/"):
            token = token[2:].strip()
        if token.startswith('"') and token.endswith('"'):
            token = token[1:-1]
        if token == current_etag:
            return True
    return False


def _etag_in_list(header_value: str, current_etag: str) -> bool:
    return _etag_matches(header_value, current_etag)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _http_date(dt: datetime) -> str:
    return format_datetime(_to_utc(dt), usegmt=True)


def _parse_http_date(value: str) -> datetime | None:
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if parsed is None:
        return None
    return _to_utc(parsed)


def _validate_content_md5(content_md5: str, body: bytes) -> None:
    try:
        provided = base64.b64decode(content_md5, validate=True)
    except (binascii.Error, ValueError):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-MD5 header")
    actual = hashlib.md5(body, usedforsecurity=False).digest()
    if provided != actual:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="BadDigest")


def _parse_single_range(range_header: str, total_size: int) -> tuple[int, int]:
    if total_size <= 0:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Range not satisfiable",
            headers={"Content-Range": "bytes */0"},
        )

    if not range_header.startswith("bytes="):
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Invalid range unit",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    # Only single-range requests are supported.
    raw = range_header[len("bytes=") :].strip()
    if "," in raw:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Multiple ranges are not supported",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    if "-" not in raw:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Invalid range format",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    start_raw, end_raw = raw.split("-", 1)

    # suffix-byte-range-spec: bytes=-N
    if not start_raw:
        try:
            suffix_len = int(end_raw)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                detail="Invalid range value",
                headers={"Content-Range": f"bytes */{total_size}"},
            )
        if suffix_len <= 0:
            raise HTTPException(
                status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                detail="Invalid range value",
                headers={"Content-Range": f"bytes */{total_size}"},
            )
        if suffix_len >= total_size:
            return 0, total_size - 1
        return total_size - suffix_len, total_size - 1

    try:
        start = int(start_raw)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Invalid range value",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    if start < 0 or start >= total_size:
        raise HTTPException(
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail="Range start out of bounds",
            headers={"Content-Range": f"bytes */{total_size}"},
        )

    if end_raw:
        try:
            end = int(end_raw)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                detail="Invalid range value",
                headers={"Content-Range": f"bytes */{total_size}"},
            )
        if end < start:
            raise HTTPException(
                status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                detail="Invalid range value",
                headers={"Content-Range": f"bytes */{total_size}"},
            )
        end = min(end, total_size - 1)
        return start, end

    return start, total_size - 1
