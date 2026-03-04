from __future__ import annotations

from datetime import timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from fastapi import APIRouter, Depends, Query, Response, status

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


@router.get("/{bucket}", response_model=None)
def list_objects_v2(
    bucket: str,
    list_type: int = Query(alias="list-type", default=2),
    prefix: str = Query(default=""),
    delimiter: str = Query(default=""),
    max_keys: int = Query(alias="max-keys", default=1000, ge=1, le=10000),
    continuation_token: str | None = Query(alias="continuation-token", default=None),
    output: str = Query(default="xml"),
    storage: LocalStorageBackend = Depends(get_storage),
) -> Response | dict:
    if list_type != 2:
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content="Only list-type=2 is supported")

    result = storage.list_objects_v2(
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        continuation_token=continuation_token,
    )

    if output.lower() == "json":
        payload = {
            "Name": result["bucket"],
            "Prefix": result["prefix"],
            "Delimiter": result["delimiter"],
            "MaxKeys": result["max_keys"],
            "KeyCount": result["key_count"],
            "IsTruncated": result["is_truncated"],
            "NextContinuationToken": result["next_continuation_token"],
            "Contents": [item.model_dump(mode="json") for item in result["contents"]],
            "CommonPrefixes": result["common_prefixes"],
        }
        return payload

    root = Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
    SubElement(root, "Name").text = result["bucket"]
    SubElement(root, "Prefix").text = result["prefix"]
    SubElement(root, "KeyCount").text = str(result["key_count"])
    SubElement(root, "MaxKeys").text = str(result["max_keys"])
    SubElement(root, "Delimiter").text = result["delimiter"]
    SubElement(root, "IsTruncated").text = "true" if result["is_truncated"] else "false"
    if continuation_token:
        SubElement(root, "ContinuationToken").text = continuation_token
    if result["next_continuation_token"]:
        SubElement(root, "NextContinuationToken").text = result["next_continuation_token"]

    for item in result["contents"]:
        content = SubElement(root, "Contents")
        SubElement(content, "Key").text = item.key
        SubElement(content, "LastModified").text = item.updated_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        SubElement(content, "ETag").text = f"\"{item.etag}\""
        SubElement(content, "Size").text = str(item.size)
        SubElement(content, "StorageClass").text = "STANDARD"

    for cp in result["common_prefixes"]:
        cp_elem = SubElement(root, "CommonPrefixes")
        SubElement(cp_elem, "Prefix").text = cp

    xml_body = tostring(root, encoding="utf-8", xml_declaration=True)
    return Response(content=xml_body, media_type="application/xml")
