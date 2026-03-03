from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime

from fastapi import APIRouter, Depends, Query, Request, Response, status

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


@router.put("/{bucket}/{key:path}", response_model=ObjectInfo, status_code=status.HTTP_201_CREATED)
async def put_object(bucket: str, key: str, request: Request, storage: LocalStorageBackend = Depends(get_storage)) -> ObjectInfo:
    body = await request.body()
    content_type = request.headers.get("content-type")
    return storage.put_object(bucket=bucket, key=key, body=body, content_type=content_type)


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
    headers = {
        "ETag": f'"{obj.etag}"',
        "Last-Modified": _http_date(obj.updated_at),
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
