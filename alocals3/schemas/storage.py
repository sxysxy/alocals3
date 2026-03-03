from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class BucketInfo(BaseModel):
    name: str = Field(..., description="Bucket name")
    created_at: datetime = Field(..., description="UTC timestamp")


class ObjectInfo(BaseModel):
    bucket: str
    key: str
    size: int
    content_type: str = "application/octet-stream"
    etag: str
    updated_at: datetime


class StoredObject(ObjectInfo):
    body: bytes
