from __future__ import annotations

from typing import Protocol

from alocals3.schemas.storage import BucketInfo, ObjectInfo, StoredObject


class StorageBackend(Protocol):
    def list_buckets(self) -> list[BucketInfo]:
        ...

    def create_bucket(self, bucket: str) -> BucketInfo:
        ...

    def delete_bucket(self, bucket: str) -> None:
        ...

    def list_objects(self, bucket: str, prefix: str | None = None, limit: int = 1000) -> list[ObjectInfo]:
        ...

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str | None = None) -> ObjectInfo:
        ...

    def get_object(self, bucket: str, key: str) -> StoredObject:
        ...

    def delete_object(self, bucket: str, key: str) -> None:
        ...
