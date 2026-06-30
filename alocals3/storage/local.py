from __future__ import annotations

import hashlib
import mimetypes
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from alocals3.db import BucketModel, ObjectModel
from alocals3.schemas.storage import BucketInfo, ObjectInfo, StoredObject


class LocalStorageBackend:
    SQLITE_LOCK_RETRIES = 6
    SQLITE_LOCK_BACKOFF_BASE_SEC = 0.02

    def __init__(self, root: Path, session_factory: sessionmaker) -> None:
        self.root = root.resolve()
        self.objects_root = self.root / "objects"
        self.objects_root.mkdir(parents=True, exist_ok=True)
        self.session_factory = session_factory

    def list_buckets(self) -> list[BucketInfo]:
        with self.session_factory() as session:
            rows = session.scalars(select(BucketModel).order_by(BucketModel.name.asc())).all()
            return [BucketInfo(name=row.name, created_at=_to_utc(row.created_at)) for row in rows]

    def create_bucket(self, bucket: str) -> BucketInfo:
        self._validate_bucket(bucket)
        now = datetime.now(tz=timezone.utc)

        with self.session_factory() as session:
            existing = session.scalar(select(BucketModel).where(BucketModel.name == bucket))
            if existing is not None:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Bucket already exists")

            row = BucketModel(name=bucket, created_at=now)
            session.add(row)
            self._commit_with_retry(session, "create_bucket")
            return BucketInfo(name=row.name, created_at=_to_utc(row.created_at))

    def delete_bucket(self, bucket: str) -> None:
        self._validate_bucket(bucket)

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            object_count = session.scalar(
                select(func.count(ObjectModel.id)).where(ObjectModel.bucket_id == bucket_row.id)
            )
            if object_count and object_count > 0:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Bucket is not empty")

            session.delete(bucket_row)
            self._commit_with_retry(session, "delete_bucket")

    def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[ObjectInfo]:
        self._validate_bucket(bucket)
        normalized_prefix = _ensure_utf8_text(prefix, "prefix") if prefix is not None else None

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)

            stmt = select(ObjectModel).where(ObjectModel.bucket_id == bucket_row.id)
            if normalized_prefix:
                stmt = stmt.where(ObjectModel.key.startswith(normalized_prefix))
            stmt = stmt.order_by(ObjectModel.key.asc()).limit(limit)

            rows = session.scalars(stmt).all()
            return [self._to_object_info(bucket_row.name, row) for row in rows]

    def list_objects_v2(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str | None = None,
    ) -> dict:
        self._validate_bucket(bucket)
        prefix = _ensure_utf8_text(prefix, "prefix")
        delimiter = _ensure_utf8_text(delimiter, "delimiter")
        if continuation_token is not None:
            continuation_token = _ensure_utf8_text(continuation_token, "continuation_token")
        if max_keys < 1:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="max_keys must be >= 1")

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            stmt = select(ObjectModel).where(ObjectModel.bucket_id == bucket_row.id)
            if prefix:
                stmt = stmt.where(ObjectModel.key.startswith(prefix))
            if continuation_token:
                stmt = stmt.where(ObjectModel.key > continuation_token)
            stmt = stmt.order_by(ObjectModel.key.asc())

            rows = session.scalars(stmt).all()
            contents: list[ObjectInfo] = []
            common_prefixes: list[str] = []
            common_prefix_set: set[str] = set()
            key_count = 0
            next_token: str | None = None

            for row in rows:
                key = row.key
                if delimiter:
                    tail = key[len(prefix) :] if prefix and key.startswith(prefix) else key
                    if delimiter in tail:
                        cp = key[: len(prefix) + tail.find(delimiter) + 1]
                        if cp not in common_prefix_set:
                            if key_count >= max_keys:
                                next_token = key
                                break
                            common_prefix_set.add(cp)
                            common_prefixes.append(cp)
                            key_count += 1
                        continue

                if key_count >= max_keys:
                    next_token = key
                    break
                contents.append(self._to_object_info(bucket_row.name, row))
                key_count += 1

            return {
                "bucket": bucket_row.name,
                "prefix": prefix,
                "delimiter": delimiter,
                "max_keys": max_keys,
                "key_count": key_count,
                "is_truncated": next_token is not None,
                "next_continuation_token": next_token,
                "contents": contents,
                "common_prefixes": common_prefixes,
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
        self._validate_bucket(bucket)
        normalized_key = self._normalize_key(key)
        now = datetime.now(tz=timezone.utc)
        final_content_type = (
            _ensure_utf8_text(content_type, "content_type")
            if content_type
            else mimetypes.guess_type(normalized_key)[0] or "application/octet-stream"
        )

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            etag = hashlib.md5(body, usedforsecurity=False).hexdigest()
            digest = self._content_hash(body)
            relative_path = Path(digest[:2]) / digest[2:4] / digest
            absolute_path = self.objects_root / relative_path

            # Atomic file replace: readers never observe partial blob writes.
            self._atomic_write_blob(absolute_path, body)

            new_path = relative_path.as_posix()
            row: ObjectModel | None = None
            created = False

            for attempt in range(2):
                try:
                    existing = session.scalar(
                        select(ObjectModel).where(
                            ObjectModel.bucket_id == bucket_row.id,
                            ObjectModel.key == normalized_key,
                        )
                    )

                    if existing is None:
                        created = True
                        row = ObjectModel(
                            bucket_id=bucket_row.id,
                            key=normalized_key,
                            file_path=new_path,
                            size=len(body),
                            content_type=final_content_type,
                            etag=etag,
                            updated_at=now,
                            created_at=now,
                        )
                        session.add(row)
                    else:
                        row = existing
                        row.file_path = new_path
                        row.size = len(body)
                        row.content_type = final_content_type
                        row.etag = etag
                        row.updated_at = now

                    self._commit_with_retry(session, "put_object")
                    break
                except IntegrityError:
                    session.rollback()
                    if attempt == 1:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Concurrent write conflict",
                        )

            if row is None:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="PUT failed")
            session.refresh(row)
            result = self._to_object_info(bucket_row.name, row)

        return result, created

    def get_object(self, bucket: str, key: str) -> StoredObject:
        self._validate_bucket(bucket)
        normalized_key = self._normalize_key(key)

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            row = session.scalar(
                select(ObjectModel).where(
                    ObjectModel.bucket_id == bucket_row.id,
                    ObjectModel.key == normalized_key,
                )
            )
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

            file_path = self.objects_root / row.file_path
            if not file_path.exists() or not file_path.is_file():
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object data missing")

            info = self._to_object_info(bucket_row.name, row)
            return StoredObject(**info.model_dump(), body=file_path.read_bytes())

    def delete_object(self, bucket: str, key: str) -> None:
        self._validate_bucket(bucket)
        normalized_key = self._normalize_key(key)

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            row = session.scalar(
                select(ObjectModel).where(
                    ObjectModel.bucket_id == bucket_row.id,
                    ObjectModel.key == normalized_key,
                )
            )
            if row is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object not found")

            # Atomic delete on metadata mapping (critical section kept in DB transaction).
            session.delete(row)
            self._commit_with_retry(session, "delete_object")

    def get_object_info(self, bucket: str, key: str) -> ObjectInfo | None:
        self._validate_bucket(bucket)
        normalized_key = self._normalize_key(key)

        with self.session_factory() as session:
            bucket_row = self._get_bucket_or_404(session, bucket)
            row = session.scalar(
                select(ObjectModel).where(
                    ObjectModel.bucket_id == bucket_row.id,
                    ObjectModel.key == normalized_key,
                )
            )
            if row is None:
                return None
            return self._to_object_info(bucket_row.name, row)

    def _get_bucket_or_404(self, session: Session, bucket: str) -> BucketModel:
        row = session.scalar(select(BucketModel).where(BucketModel.name == bucket))
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bucket not found")
        return row

    def _to_object_info(self, bucket: str, row: ObjectModel) -> ObjectInfo:
        return ObjectInfo(
            bucket=bucket,
            key=row.key,
            size=row.size,
            content_type=row.content_type,
            etag=row.etag,
            updated_at=_to_utc(row.updated_at),
        )

    def _validate_bucket(self, bucket: str) -> None:
        bucket = _ensure_utf8_text(bucket, "bucket")
        if not bucket or "/" in bucket or "\\" in bucket or bucket in {".", ".."}:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid bucket name",
            )

    def _normalize_key(self, key: str) -> str:
        normalized = _ensure_utf8_text(key, "key").strip("/")
        if not normalized:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid object key")
        if normalized.startswith("../") or "/../" in normalized or normalized == "..":
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid object key")
        return normalized

    def _content_hash(self, body: bytes) -> str:
        return hashlib.sha256(body).hexdigest()

    def _atomic_write_blob(self, target_path: Path, body: bytes) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_name = f".{target_path.name}.{uuid.uuid4().hex}.tmp"
        tmp_path = target_path.parent / tmp_name
        try:
            with tmp_path.open("wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def _commit_with_retry(self, session: Session, operation: str) -> None:
        for attempt in range(self.SQLITE_LOCK_RETRIES):
            try:
                session.commit()
                return
            except OperationalError as exc:
                if not _is_locked_error(exc):
                    raise
                session.rollback()
                if attempt == self.SQLITE_LOCK_RETRIES - 1:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Database busy while {operation}, please retry",
                    ) from exc
                # Jitter avoids herd retries under contention.
                sleep_s = self.SQLITE_LOCK_BACKOFF_BASE_SEC * (2**attempt)
                time.sleep(sleep_s * (0.8 + random.random() * 0.4))


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_locked_error(exc: OperationalError) -> bool:
    text = str(exc).lower()
    return "database is locked" in text or "database table is locked" in text


def _ensure_utf8_text(value: str, field_name: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{field_name} must be valid UTF-8 text",
        ) from exc
    return value
