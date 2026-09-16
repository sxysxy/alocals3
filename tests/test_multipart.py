"""End-to-end tests against an isolated real Rust server and SQLite database.

Build first: cargo build --features server --bin alocals3-server
Run with the rebuilt native Python client installed.
"""
import asyncio
import hashlib
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
import uuid
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from alocals3.client import ALocalS3ClientAsync, ALocalS3Client

SERVER = Path(os.environ.get("ALOCALS3_TEST_SERVER", Path(__file__).resolve().parents[1] / "target/debug/alocals3-server"))


class MultipartTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / 'metadata.db'
        self.pg_dsn = os.environ.get('ALOCALS3_TEST_POSTGRES_URL')
        self.schema = None
        self.database_url = f'sqlite:///{self.db}'
        if self.pg_dsn:
            import psycopg
            self.schema = 'multipart_test_' + uuid.uuid4().hex
            with psycopg.connect(self.pg_dsn, autocommit=True) as connection:
                connection.execute(f'CREATE SCHEMA {self.schema}')
            parsed = urlsplit(self.pg_dsn)
            query = dict(parse_qsl(parsed.query))
            query['options'] = '-csearch_path=' + self.schema
            self.database_url = urlunsplit(parsed._replace(query=urlencode(query)))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.url = f'http://127.0.0.1:{self.port}'
        self.client = ALocalS3ClientAsync(self.url, disable_proxy=True, service_token='multipart-test')
        await self.start()
        await self.client.create_bucket('test')

    async def start(self):
        self.log = open(self.root / 'server.log', 'ab')
        self.process = await asyncio.create_subprocess_exec(
            str(SERVER), '--port', str(self.port), '--database-url', self.database_url,
            '--storage-root', str(self.root / 'data'), '--disable-gc',
            '--max-upload-size', str(getattr(self, 'max_size', 0)),
            '--auth-enabled', 'true', '--service-token', 'multipart-test', '--user-auth-secret', 'test-secret',
            stdout=self.log, stderr=self.log,
        )
        for _ in range(200):
            if self.process.returncode is not None:
                self.fail((self.root / 'server.log').read_text())
            try:
                await self.client.health()
                return
            except RuntimeError:
                await asyncio.sleep(.025)
        self.fail('server did not start')

    async def stop(self):
        if self.process.returncode is None:
            self.process.terminate()
        await self.process.wait()
        self.log.close()

    async def asyncTearDown(self):
        await self.client.close()
        await self.stop()
        self.tmp.cleanup()
        if self.schema:
            import psycopg
            with psycopg.connect(self.pg_dsn, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA {self.schema} CASCADE')

    def connect_db(self):
        if self.pg_dsn:
            import psycopg
            return psycopg.connect(self.database_url)
        return sqlite3.connect(self.db)

    def count(self, table):
        with self.connect_db() as connection:
            return connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]

    async def session(self, key='result.bin'):
        return (await self.client.create_multipart_upload('test', key, 'application/x-parts', {'source': 'test'}))['upload_id']

    async def test_unpublished_parts_atomic_replace_and_idempotent_complete(self):
        await self.client.put_object_bytes('test', 'result.bin', b'old')
        upload = await self.session()
        parts = await asyncio.gather(*[
            self.client.upload_part(upload, i + 1, body)
            for i, body in enumerate([b'first', b'next', b'last'])
        ])
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'old')
        self.assertEqual(self.count('objects'), 1)
        self.assertEqual(self.count('pending_uploads'), 1)
        self.assertEqual(self.count('pending_upload_parts'), 3)
        # Replace just one failed/stale part; old manifest must not be accepted.
        replacement = await self.client.upload_part(upload, 2, b'retried')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:400'):
            await self.client.complete_multipart_upload(upload, parts)
        parts[1] = replacement
        info = await self.client.complete_multipart_upload(upload, parts)
        self.assertEqual(info['etag'], hashlib.md5(b'firstretriedlast').hexdigest())
        body, headers = await self.client.get_object_bytes('test', 'result.bin')
        self.assertEqual(body, b'firstretriedlast')
        self.assertEqual(headers['content-type'], 'application/x-parts')
        self.assertEqual(headers['x-amz-meta-source'], 'test')
        self.assertEqual(self.count('pending_uploads'), 0)
        self.assertEqual(self.count('pending_upload_parts'), 0)
        self.assertEqual(self.count('completed_uploads'), 1)
        # A retry must not overwrite a subsequently published version.
        await self.client.put_object_bytes('test', 'result.bin', b'newer')
        self.assertEqual(await self.client.complete_multipart_upload(upload, parts), info)
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'newer')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:409'):
            await self.client.upload_part(upload, 4, b'too late')
        await self.client.abort_multipart_upload(upload)
        self.assertEqual(self.count('completed_uploads'), 0)
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'newer')

    async def test_restart_resume_and_missing_duplicate_unordered_parts(self):
        upload = await self.session('中文/with #?.arrow')
        first = await self.client.upload_part(upload, 1, b'one')
        third = await self.client.upload_part(upload, 3, b'three')
        self.assertEqual(self.count('objects'), 0)
        with self.assertRaisesRegex(RuntimeError, 'STATUS:404'):
            await self.client.head_object('test', '中文/with #?.arrow')
        await self.stop()
        await self.start()
        self.assertEqual([p['part_number'] for p in (await self.client.list_parts(upload))['parts']], [1, 3])
        for manifest in ([first, third], [first], [first, first], [third, first]):
            with self.assertRaisesRegex(RuntimeError, 'STATUS:400'):
                await self.client.complete_multipart_upload(upload, manifest)
        second = await self.client.upload_part(upload, 2, b'two')
        await self.client.complete_multipart_upload(upload, [first, second, third])
        self.assertEqual((await self.client.get_object_bytes('test', '中文/with #?.arrow'))[0], b'onetwothree')

    async def test_abort_and_empty_object(self):
        upload = await self.session()
        await self.client.upload_part(upload, 1, b'discard')
        interrupted = self.root / 'data/multipart-staging' / upload
        interrupted.mkdir(parents=True, exist_ok=True)
        (interrupted / '.upload-interrupted.tmp').write_bytes(b'partial from crashed process')
        await self.client.abort_multipart_upload(upload)
        await self.client.abort_multipart_upload(upload)
        self.assertEqual(self.count('pending_uploads'), 0)
        self.assertFalse(interrupted.exists())
        self.assertFalse((self.root / 'data/multipart' / upload).exists())
        with self.assertRaisesRegex(RuntimeError, 'STATUS:404'):
            await self.client.upload_part(upload, 2, b'late')
        empty = await self.session()
        await self.client.complete_multipart_upload(empty, [])
        self.assertEqual((await self.client.head_object('test', 'result.bin'))['content-length'], '0')

    async def test_explicit_total_size_limit_preserves_previous_parts(self):
        await self.stop()
        self.max_size = 9
        await self.start()
        upload = await self.session()
        first = await self.client.upload_part(upload, 1, b'12345')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:413'):
            await self.client.upload_part(upload, 2, b'67890')
        self.assertEqual(len((await self.client.list_parts(upload))['parts']), 1)
        second = await self.client.upload_part(upload, 2, b'6789')
        await self.client.complete_multipart_upload(upload, [first, second])
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'123456789')

    async def test_two_server_processes_share_upload_and_commit_receipt(self):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        process = await asyncio.create_subprocess_exec(
            str(SERVER), '--port', str(port), '--database-url', self.database_url,
            '--storage-root', str(self.root / 'data'), '--disable-gc',
            '--auth-enabled', 'true', '--service-token', 'multipart-test', '--user-auth-secret', 'test-secret',
            stdout=self.log, stderr=self.log,
        )
        try:
            async with ALocalS3ClientAsync(f'http://127.0.0.1:{port}', disable_proxy=True, service_token='multipart-test') as other:
                for _ in range(200):
                    try:
                        await other.health()
                        break
                    except RuntimeError:
                        await asyncio.sleep(.025)
                upload = await self.session()
                parts = await asyncio.gather(
                    self.client.upload_part(upload, 1, b'first'),
                    other.upload_part(upload, 2, b'second'),
                )
                results = await asyncio.gather(
                    self.client.complete_multipart_upload(upload, parts),
                    other.complete_multipart_upload(upload, parts),
                )
                self.assertEqual(results[0], results[1])
                self.assertEqual((await other.get_object_bytes('test', 'result.bin'))[0], b'firstsecond')
                self.assertEqual(self.count('pending_uploads'), 0)
                self.assertEqual(self.count('completed_uploads'), 1)
        finally:
            if process.returncode is None:
                process.terminate()
            await process.wait()

    async def test_part_larger_than_16_mib_is_not_truncated(self):
        upload = await self.session()
        body = b'0123456789abcdef' * (1024 * 1024 + 1)
        part = await self.client.upload_part(upload, 1, body)
        self.assertEqual(part['size'], len(body))
        result = await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual(result['size'], len(body))
        self.assertEqual(result['etag'], hashlib.md5(body).hexdigest())
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], body)

    async def test_bucket_removal_cleans_pending_files(self):
        upload = await self.session()
        await self.client.upload_part(upload, 1, b'unpublished')
        await self.client.delete_bucket('test')
        self.assertEqual(self.count('pending_uploads'), 0)
        self.assertEqual(self.count('pending_upload_parts'), 0)
        self.assertFalse((self.root / 'data/multipart' / upload).exists())

    async def test_corrupt_part_refused_without_publishing(self):
        upload = await self.session()
        part = await self.client.upload_part(upload, 1, b'good')
        with self.connect_db() as connection:
            name = connection.execute('SELECT file FROM pending_upload_parts WHERE upload_id = ' + ('%s' if self.pg_dsn else '?'), (upload,)).fetchone()[0]
        (self.root / 'data/multipart' / upload / name).write_bytes(b'evil')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:409'):
            await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual(self.count('objects'), 0)
        part = await self.client.upload_part(upload, 1, b'good')
        await self.client.complete_multipart_upload(upload, [part])

    async def test_native_sync_client_and_service_auth(self):
        # The synchronous client is intentionally exercised outside the async test loop.
        def sync_roundtrip():
            with ALocalS3Client(self.url, disable_proxy=True, service_token='multipart-test') as client:
                upload = client.create_multipart_upload('test', 'sync')['upload_id']
                part = client.upload_part(upload, 1, b'sync-data')
                self.assertEqual(client.list_parts(upload)['parts'][0]['etag'], part['etag'])
                client.complete_multipart_upload(upload, [part])
                self.assertEqual(client.get_object_bytes('test', 'sync')[0], b'sync-data')
                client.abort_multipart_upload(upload)
        await asyncio.to_thread(sync_roundtrip)
        async with ALocalS3ClientAsync(self.url, disable_proxy=True, service_token='wrong') as unauthorized:
            with self.assertRaisesRegex(RuntimeError, 'STATUS:401'):
                await unauthorized.create_multipart_upload('test', 'forbidden')

    async def test_database_failure_rolls_back_publication_and_pending_removal(self):
        await self.client.put_object_bytes('test', 'result.bin', b'old')
        upload = await self.session()
        part = await self.client.upload_part(upload, 1, b'new')
        with self.connect_db() as connection:
            if self.pg_dsn:
                connection.execute("ALTER TABLE completed_uploads ADD CONSTRAINT fail_receipt CHECK (session = 'injected failure')")
            else:
                connection.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON completed_uploads BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
        with self.assertRaisesRegex(RuntimeError, 'STATUS:500'):
            await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'old')
        self.assertEqual(self.count('pending_uploads'), 1)
        self.assertEqual(self.count('pending_upload_parts'), 1)
        with self.connect_db() as connection:
            connection.execute('ALTER TABLE completed_uploads DROP CONSTRAINT fail_receipt' if self.pg_dsn else 'DROP TRIGGER fail_receipt')
        await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual((await self.client.get_object_bytes('test', 'result.bin'))[0], b'new')


if __name__ == '__main__':
    unittest.main()
