from __future__ import annotations

import asyncio
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from alocals3.client import ALocalS3ClientAsync


ENDPOINT = os.environ.get("ALOCALS3_TEST_ENDPOINT", "http://127.0.0.1:8000")


class AsyncClientIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = ALocalS3ClientAsync(ENDPOINT, timeout=10.0, disable_proxy=True)
        self.bucket = f"async-test-{uuid.uuid4().hex[:12]}"
        await self.client.create_bucket(self.bucket)

    async def asyncTearDown(self) -> None:
        try:
            objects = await self.client.list_objects(self.bucket)
            await asyncio.gather(
                *(self.client.delete_object(self.bucket, item["key"]) for item in objects)
            )
            await self.client.delete_bucket(self.bucket)
        finally:
            await self.client.close()

    async def test_crud_metadata_range_copy_and_file_download(self) -> None:
        self.assertEqual((await self.client.health())["status"], "ok")
        body = b"0123456789" * 1024
        created = await self.client.put_object_bytes(
            self.bucket,
            "folder/source.bin",
            body,
            "application/octet-stream",
            {"origin": "native-async"},
        )
        self.assertEqual(created["size"], len(body))

        downloaded, headers = await self.client.get_object_bytes(
            self.bucket, "folder/source.bin"
        )
        self.assertEqual(downloaded, body)
        self.assertEqual(headers["x-amz-meta-origin"], "native-async")

        ranged, range_headers = await self.client.get_object_range(
            self.bucket, "folder/source.bin", "bytes=7-31"
        )
        self.assertEqual(ranged, body[7:32])
        self.assertEqual(range_headers["content-range"], f"bytes 7-31/{len(body)}")

        copied = await self.client.copy_object(
            self.bucket,
            "folder/source.bin",
            self.bucket,
            "folder/copied.bin",
            metadata={"copy": "yes"},
            metadata_directive="REPLACE",
            content_type="application/x-copy",
        )
        self.assertEqual(copied["size"], len(body))
        copied_body, copied_headers = await self.client.get_object_bytes(
            self.bucket, "folder/copied.bin"
        )
        self.assertEqual(copied_body, body)
        self.assertEqual(copied_headers["content-type"], "application/x-copy")
        self.assertEqual(copied_headers["x-amz-meta-copy"], "yes")

        listing = await self.client.list_objects_v2(
            self.bucket, prefix="folder/", delimiter="/", max_keys=10
        )
        self.assertEqual(listing["KeyCount"], 2)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "download.bin"
            file_headers = await self.client.get_object_to_file(
                self.bucket, "folder/source.bin", output
            )
            self.assertEqual(output.read_bytes(), body)
            self.assertEqual(file_headers["content-length"], str(len(body)))

    async def test_binary_and_text_file_like_streams_with_cache(self) -> None:
        binary = b"first line\nsecond line\n" + bytes(range(256)) * 128
        with tempfile.TemporaryDirectory() as directory:
            write_cache = Path(directory) / "write.cache"
            writer = await self.client.open(
                f"s3://{self.bucket}/stream.bin", "wb", cache_path=write_cache
            )
            self.assertEqual(await writer.write(binary[:113]), 113)
            self.assertEqual(await writer.write(memoryview(binary)[113:]), len(binary) - 113)
            await writer.flush()
            await writer.close()
            self.assertEqual(write_cache.read_bytes(), binary)
            self.assertEqual(writer.result["size"], len(binary))

            read_cache = Path(directory) / "read.cache"
            async with self.client.open(
                f"s3://{self.bucket}/stream.bin", "rb", cache_path=read_cache
            ) as reader:
                self.assertEqual(await reader.readline(), b"first line\n")
                target = bytearray(11)
                self.assertEqual(await reader.readinto(target), 11)
                rest = await reader.read()
            self.assertEqual(b"first line\n" + bytes(target) + rest, binary)
            self.assertEqual(read_cache.read_bytes(), binary)

            text = "第一行\r\n第二行🙂\n最后一行"
            async with self.client.open(
                f"s3://{self.bucket}/unicode.txt", "w", encoding="utf-8"
            ) as text_writer:
                self.assertEqual(await text_writer.write(text[:5]), 5)
                self.assertEqual(await text_writer.write(text[5:]), len(text) - 5)
            async with self.client.open(
                f"s3://{self.bucket}/unicode.txt", "r", encoding="utf-8"
            ) as text_reader:
                self.assertEqual(await text_reader.read(3), "第一行")
                self.assertEqual(await text_reader.readline(), "\n")
                self.assertEqual(await text_reader.readline(), "第二行🙂\n")
                self.assertEqual(await text_reader.read(), "最后一行")

            with self.assertRaisesRegex(ValueError, "abort upload"):
                async with self.client.open(f"s3://{self.bucket}/discarded.bin", "wb") as discarded:
                    await discarded.write(b"must not be uploaded")
                    raise ValueError("abort upload")
            with self.assertRaisesRegex(RuntimeError, "ALOCALS3_HTTP_STATUS:404"):
                await self.client.get_object_bytes(self.bucket, "discarded.bin")

    async def test_concurrent_requests_keep_event_loop_responsive(self) -> None:
        payload = os.urandom(512 * 1024)
        stop = asyncio.Event()
        heartbeat_gaps: list[float] = []

        async def heartbeat() -> None:
            previous = time.perf_counter()
            while not stop.is_set():
                await asyncio.sleep(0.001)
                now = time.perf_counter()
                heartbeat_gaps.append(now - previous)
                previous = now

        async def round_trip(index: int) -> None:
            key = f"concurrent/{index:03d}.bin"
            await self.client.put_object_bytes(self.bucket, key, payload)
            body, _ = await self.client.get_object_bytes(self.bucket, key)
            self.assertEqual(body, payload)

        ticker = asyncio.create_task(heartbeat())
        try:
            await asyncio.wait_for(
                asyncio.gather(*(round_trip(index) for index in range(32))),
                timeout=30.0,
            )
        finally:
            stop.set()
            await ticker

        self.assertGreater(len(heartbeat_gaps), 2)
        self.assertLess(max(heartbeat_gaps), 0.25)


if __name__ == "__main__":
    unittest.main()
