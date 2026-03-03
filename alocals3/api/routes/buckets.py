from __future__ import annotations

from fastapi import APIRouter, Depends, status

from alocals3.api.deps import get_storage
from alocals3.schemas.storage import BucketInfo
from alocals3.storage.local import LocalStorageBackend

router = APIRouter(prefix="/s3", tags=["buckets"])


@router.get("", response_model=list[BucketInfo])
def list_buckets(storage: LocalStorageBackend = Depends(get_storage)) -> list[BucketInfo]:
    return storage.list_buckets()


@router.put("/{bucket}", response_model=BucketInfo, status_code=status.HTTP_201_CREATED)
def create_bucket(bucket: str, storage: LocalStorageBackend = Depends(get_storage)) -> BucketInfo:
    return storage.create_bucket(bucket)


@router.delete("/{bucket}", status_code=status.HTTP_204_NO_CONTENT)
def delete_bucket(bucket: str, storage: LocalStorageBackend = Depends(get_storage)) -> None:
    storage.delete_bucket(bucket)
