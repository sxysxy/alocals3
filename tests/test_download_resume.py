"""Wire-level download faults: interruption, version changes and bad ranges."""
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from alocals3.client import ALocalS3Client, ALocalS3ClientAsync


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.output = Path(self.tmp.name)/'output.bin'
        self.output.write_bytes(b'previous valid output')
        self.body = b'abcdefghij'*10000
        self.etag = '"'+hashlib.md5(self.body).hexdigest()+'"'
        self.interrupt = 0
        self.bad_range = False
        self.corrupt = False
        self.pause = None
        self.requests = []
        self.server = await asyncio.start_server(self.handle, '127.0.0.1', 0)
        self.url = f'http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}'
        self.client = ALocalS3ClientAsync(self.url, disable_proxy=True, service_token='secret')
        self.sync = ALocalS3Client(self.url, disable_proxy=True, service_token='secret')

    async def asyncTearDown(self):
        await self.client.close()
        self.server.close()
        await self.server.wait_closed()
        self.tmp.cleanup()

    async def handle(self, reader, writer):
        try:
            raw = (await reader.readuntil(b'\r\n\r\n')).decode('ascii')
            lines = raw.split('\r\n')
            method = lines[0].split()[0]
            headers = dict(line.lower().split(': ', 1) for line in lines[1:] if ': ' in line)
            self.requests.append((method, headers))
            if method == 'HEAD':
                writer.write(f'HTTP/1.1 200 OK\r\nContent-Length: {len(self.body)}\r\nETag: {self.etag}\r\nConnection: close\r\n\r\n'.encode())
            else:
                start = int(headers.get('range', 'bytes=0-')[6:-1])
                body = self.body[start:]
                if self.corrupt:
                    body = b'!'*len(body)
                status = '206 Partial Content' if 'range' in headers else '200 OK'
                content_range = ''
                if 'range' in headers:
                    content_range = f'Content-Range: bytes {start + int(self.bad_range)}-{len(self.body)-1}/{len(self.body)}\r\n'
                writer.write(f'HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\nETag: {self.etag}\r\n{content_range}Connection: close\r\n\r\n'.encode())
                if self.pause is not None:
                    writer.write(body[:1000])
                    await writer.drain()
                    await self.pause.wait()
                    writer.write(body[1000:])
                elif self.interrupt:
                    self.interrupt -= 1
                    writer.write(body[:1000])
                    await writer.drain()
                    await asyncio.sleep(.025)
                else:
                    writer.write(body)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def download(self, sync=False, **kwargs):
        if sync:
            return await asyncio.to_thread(self.sync.get_object_to_file, 'bucket', 'object', self.output, **kwargs)
        return await self.client.get_object_to_file('bucket', 'object', self.output, **kwargs)

    async def test_interrupted_download_retries_with_range_and_pinned_etag(self):
        for sync in [False, True]:
            self.requests.clear()
            self.interrupt = 1
            await self.download(sync)
            self.assertEqual(self.output.read_bytes(), self.body)
            gets = [h for method, h in self.requests if method == 'GET']
            self.assertEqual(len(gets), 2)
            self.assertEqual(gets[1]['range'], 'bytes=1000-')
            for headers in gets:
                self.assertEqual(headers['if-match'], self.etag.lower())
                self.assertEqual(headers['authorization'], 'bearer secret')
            self.assertFalse(Path(str(self.output)+'.alocals3.part').exists())

    async def test_failed_download_keeps_target_and_resumes_next_call(self):
        for sync in [False, True]:
            self.output.write_bytes(b'keep')
            self.interrupt = 1
            with self.assertRaises(RuntimeError):
                await self.download(sync, max_retries=0)
            self.assertEqual(self.output.read_bytes(), b'keep')
            self.assertEqual(Path(str(self.output)+'.alocals3.part').stat().st_size, 1000)
            await self.download(sync)
            self.assertEqual(self.requests[-1][1]['range'], 'bytes=1000-')
            self.assertEqual(self.output.read_bytes(), self.body)

    async def test_new_etag_discards_partial_from_older_version(self):
        self.interrupt = 1
        with self.assertRaises(RuntimeError):
            await self.download(max_retries=0)
        self.body = b'new version'*20000
        self.etag = '"'+hashlib.md5(self.body).hexdigest()+'"'
        await self.download()
        self.assertNotIn('range', self.requests[-1][1])
        self.assertEqual(self.output.read_bytes(), self.body)

    async def test_bad_content_range_never_replaces_output(self):
        for sync in [False, True]:
            self.interrupt = 1
            self.bad_range = True
            with self.assertRaisesRegex(RuntimeError, 'Content-Range'):
                await self.download(sync)
            self.assertEqual(self.output.read_bytes(), b'previous valid output')
            self.bad_range = False
            await self.download(sync)
            self.assertEqual(self.output.read_bytes(), self.body)
            self.output.write_bytes(b'previous valid output')

    async def test_checksum_failure_and_explicit_restart(self):
        for sync in [False, True]:
            self.corrupt = True
            with self.assertRaisesRegex(RuntimeError, 'checksum'):
                await self.download(sync)
            self.assertEqual(self.output.read_bytes(), b'previous valid output')
            self.corrupt = False
            await self.download(sync, resume=False)
            self.assertEqual(self.output.read_bytes(), self.body)
            self.output.write_bytes(b'previous valid output')

    async def test_concurrent_destination_lock_and_cancel_resume(self):
        self.pause = asyncio.Event()
        task = asyncio.create_task(self.download())
        for _ in range(100):
            if any(method == 'GET' for method, _ in self.requests):
                break
            await asyncio.sleep(.01)
        await asyncio.sleep(.05)
        with self.assertRaisesRegex(RuntimeError, 'already being downloaded'):
            await self.download()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(.05)
        self.assertEqual(self.output.read_bytes(), b'previous valid output')
        self.pause.set()
        self.pause = None
        await self.download()
        self.assertEqual(self.output.read_bytes(), self.body)

    async def test_other_url_and_missing_partial_do_not_reuse_state(self):
        partial = Path(str(self.output)+'.alocals3.part')
        state = Path(str(self.output)+'.alocals3.json')
        state.write_text(json.dumps({'url': self.url+'/s3/bucket/object', 'etag': self.etag, 'size': len(self.body)}))
        await self.download()
        self.assertEqual(self.output.read_bytes(), self.body)
        partial.write_bytes(b'wrong')
        state.write_text(json.dumps({'url': self.url+'/s3/other/object', 'etag': self.etag, 'size': len(self.body)}))
        await self.download()
        self.assertNotIn('range', self.requests[-1][1])
        self.assertEqual(self.output.read_bytes(), self.body)


if __name__ == '__main__':
    unittest.main()
