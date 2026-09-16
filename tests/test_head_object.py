"""Native client wire tests; no running S3 service or object data required."""
import asyncio
import unittest
from urllib.parse import unquote

from alocals3.client import ALocalS3ClientAsync


class HeadObjectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.status = 200
        self.size = 0
        self.delay = 0
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.client = ALocalS3ClientAsync(
            f"http://127.0.0.1:{port}", timeout=1, disable_proxy=True,
            service_token="head-test-token",
        )

    async def asyncTearDown(self):
        await self.client.close()
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        try:
            request = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
            self.requests.append(request)
            await asyncio.sleep(self.delay)
            writer.write((
                f"HTTP/1.1 {self.status} Test\r\n"
                f"Content-Length: {self.size}\r\n"
                'ETag: "test-digest"\r\n'
                "Last-Modified: Wed, 16 Sep 2026 00:00:00 GMT\r\n"
                "Content-Type: application/octet-stream\r\n"
                "X-Amz-Meta-Origin: test\r\nConnection: close\r\n\r\n"
            ).encode("ascii"))
            await writer.drain()
            # HEAD has no body, even when content-length describes a huge object.
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_headers_without_body_for_empty_and_large_objects(self):
        for size in (0, 5 * 1024**3):
            self.size = size
            headers = await self.client.head_object("bucket", "中文/a #?.arrow")
            self.assertEqual(headers["content-length"], str(size))
            self.assertEqual(headers["etag"], '"test-digest"')
            self.assertEqual(headers["x-amz-meta-origin"], "test")
            self.assertEqual(headers["last-modified"], "Wed, 16 Sep 2026 00:00:00 GMT")
            request = self.requests[-1]
            self.assertEqual(unquote(request.split("\r\n")[0]),
                             "HEAD /s3/bucket/中文/a #?.arrow HTTP/1.1")
            self.assertIn("authorization: bearer head-test-token", request.lower())
        self.assertEqual(len(self.requests), 2)

    async def test_http_errors_preserve_status(self):
        for status in (404, 403, 500):
            self.status = status
            with self.assertRaisesRegex(RuntimeError, f"ALOCALS3_HTTP_STATUS:{status}"):
                await self.client.head_object("bucket", "missing")

    async def test_concurrent_heads_do_not_block_event_loop(self):
        self.delay = 0.05
        ticks = 0
        async def heartbeat():
            nonlocal ticks
            for _ in range(5):
                await asyncio.sleep(0.005)
                ticks += 1
        await asyncio.gather(heartbeat(), *(
            self.client.head_object("bucket", str(i)) for i in range(10)
        ))
        self.assertEqual(ticks, 5)
        self.assertEqual(len(self.requests), 10)


if __name__ == "__main__":
    unittest.main()
