"""0.10 server regressions, isolated SQLite or ALOCALS3_TEST_POSTGRES_URL.

Build native client and server first; run: python -m unittest discover -s tests -p test_release_010.py
"""
import asyncio
import json
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from contextlib import closing
import uuid

from alocals3.client import ALocalS3Client, ALocalS3ClientAsync

SERVER = Path(os.environ.get('ALOCALS3_TEST_SERVER', Path(__file__).resolve().parents[1] / 'target/debug/alocals3-server'))


class ReleaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / 'metadata.db'
        self.dsn = f'sqlite:///{self.db}'
        self.pg_dsn = os.environ.get('ALOCALS3_TEST_POSTGRES_URL')
        self.schema = None
        if self.pg_dsn:
            import psycopg
            self.schema = 'release_test_' + uuid.uuid4().hex
            with psycopg.connect(self.pg_dsn, autocommit=True) as connection:
                connection.execute(f'CREATE SCHEMA {self.schema}')
            parsed = urlsplit(self.pg_dsn)
            query = dict(parse_qsl(parsed.query))
            query['options'] = '-csearch_path=' + self.schema
            self.dsn = urlunsplit(parsed._replace(query=urlencode(query)))
        self.processes = []
        self.clients = []
        self.client, self.port = await self.start()
        await self.client.create_bucket('test')

    async def start(self, *extra):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        log = open(self.root / 'server.log', 'ab')
        process = await asyncio.create_subprocess_exec(str(SERVER), '--port', str(port),
            '--database-url', self.dsn, '--storage-root', str(self.root / 'data'),
            *(() if getattr(self, 'enable_gc', False) else ('--disable-gc',)),
            *extra, stdout=log, stderr=log)
        self.processes.append((process, log))
        client = ALocalS3ClientAsync(f'http://127.0.0.1:{port}', disable_proxy=True)
        self.clients.append(client)
        for _ in range(200):
            if process.returncode is not None:
                self.fail((self.root / 'server.log').read_text())
            try:
                await client.health()
                return client, port
            except RuntimeError:
                await asyncio.sleep(.025)
        self.fail('server did not start')

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()
        for process, log in self.processes:
            if process.returncode is None:
                process.terminate()
            await process.wait()
            log.close()
        self.tmp.cleanup()
        if self.schema:
            import psycopg
            with psycopg.connect(self.pg_dsn, autocommit=True) as connection:
                connection.execute(f'DROP SCHEMA {self.schema} CASCADE')

    def connect_db(self):
        if self.pg_dsn:
            import psycopg
            return psycopg.connect(self.dsn)
        return sqlite3.connect(self.db)

    async def test_paging_no_missing_objects_and_literal_unicode_prefix(self):
        keys = ['a', 'b', 'c', 'd', '目录/%_a', '目录/%_b', '目录/其他']
        for key in keys:
            await self.client.put_object_bytes('test', key, key.encode())
        found = []
        token = None
        while True:
            page = await self.client.list_objects_v2('test', max_keys=2, continuation_token=token)
            found.extend(o['key'] for o in page['Contents'])
            if not page['IsTruncated']:
                self.assertIsNone(page['NextContinuationToken'])
                break
            token = page['NextContinuationToken']
        self.assertEqual(found, sorted(keys))
        page = await self.client.list_objects_v2('test', prefix='目录/%_', max_keys=1)
        self.assertEqual([o['key'] for o in page['Contents']], ['目录/%_a'])
        token = page['NextContinuationToken']
        next_page = await self.client.list_objects_v2('test', prefix='目录/%_', max_keys=1, continuation_token=token)
        self.assertEqual([o['key'] for o in next_page['Contents']], ['目录/%_b'])
        with self.assertRaisesRegex(RuntimeError, 'STATUS:400'):
            await self.client.list_objects_v2('test', prefix='other', continuation_token=token)
        empty = await self.client.list_objects_v2('test', max_keys=0)
        self.assertEqual(empty['KeyCount'], 0)
        self.assertFalse(empty['IsTruncated'])

    async def test_delimiter_paging_skips_large_directory_without_duplicates(self):
        # More than the server's internal query batch, plus a neighboring prefix.
        for i in range(270):
            await self.client.put_object_bytes('test', f'a/{i:04}', b'x')
        for key in ['a0', 'b/nested/x', 'c', '目录/x']:
            await self.client.put_object_bytes('test', key, b'x')
        found = []
        token = None
        while True:
            page = await self.client.list_objects_v2('test', delimiter='/', max_keys=1, continuation_token=token)
            found.extend(o['key'] for o in page['Contents'])
            found.extend(page['CommonPrefixes'])
            if not page['IsTruncated']:
                break
            token = page['NextContinuationToken']
        self.assertEqual(found, ['a/', 'a0', 'b/', 'c', '目录/'])

    async def test_conditional_writers_across_server_processes(self):
        other, _ = await self.start()
        async def create(i):
            try:
                return await (self.client if i % 2 else other).put_object_bytes('test', 'shared', str(i).encode(), if_none_match='*')
            except RuntimeError as error:
                self.assertIn('STATUS:412', str(error))
                return None
        results = await asyncio.gather(*(create(i) for i in range(16)))
        self.assertEqual(sum(r is not None for r in results), 1)
        etag = next(r['etag'] for r in results if r)
        async def replace(i):
            try:
                return await (self.client if i % 2 else other).put_object_bytes('test', 'shared', f'updated-{i}'.encode(), if_match=f'"{etag}"')
            except RuntimeError as error:
                self.assertIn('STATUS:412', str(error))
                return None
        results = await asyncio.gather(*(replace(i) for i in range(16)))
        self.assertEqual(sum(r is not None for r in results), 1)
        with self.assertRaisesRegex(RuntimeError, 'STATUS:412'):
            await self.client.put_object_bytes('test', 'missing', b'x', if_match='*')
        sync = ALocalS3Client(f'http://127.0.0.1:{self.port}', disable_proxy=True)
        await asyncio.to_thread(sync.put_object_bytes, 'test', 'sync', b'x', if_none_match='*')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:412'):
            await asyncio.to_thread(sync.put_object_bytes, 'test', 'sync', b'x', if_none_match='*')
        source = self.root / 'upload.bin'
        source.write_bytes(b'file')
        await self.client.put_object('test', 'file', source, if_none_match='*')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:412'):
            await self.client.put_object('test', 'file', source, if_none_match='*')

    async def test_slow_conditional_upload_rechecks_after_body_receive(self):
        for existing in (False, True):
            key = 'delayed-' + str(existing)
            if existing:
                original = await self.client.put_object_bytes('test', key, b'original')
                condition = f'If-Match: "{original["etag"]}"'
            else:
                condition = 'If-None-Match: *'
            reader, writer = await asyncio.open_connection('127.0.0.1', self.port)
            writer.write(f'PUT /s3/test/{key} HTTP/1.1\r\nHost: localhost\r\n{condition}\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx'.encode())
            await writer.drain()
            await asyncio.sleep(.025)
            await self.client.put_object_bytes('test', key, b'winner')
            writer.write(b'y')
            await writer.drain()
            self.assertIn(b'412 Precondition Failed', await reader.read())
            writer.close()
            await writer.wait_closed()
            self.assertEqual((await self.client.get_object_bytes('test', key))[0], b'winner')

    async def test_multipart_conditions_list_and_batch_abort(self):
        upload = (await self.client.create_multipart_upload('test', 'result'))['upload_id']
        part = await self.client.upload_part(upload, 1, b'new')
        old = await self.client.put_object_bytes('test', 'result', b'old')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:412'):
            await self.client.complete_multipart_upload(upload, [part], if_none_match='*')
        self.assertEqual((await self.client.get_object_bytes('test', 'result'))[0], b'old')
        pending = await self.client.list_multipart_uploads('test')
        self.assertEqual(pending['uploads'][0]['size'], 3)
        self.assertEqual(pending['uploads'][0]['part_count'], 1)
        info = await self.client.complete_multipart_upload(upload, [part], if_match=old['etag'])
        # Successful retry still returns its receipt after a later overwrite.
        await self.client.put_object_bytes('test', 'result', b'later')
        self.assertEqual(await self.client.complete_multipart_upload(upload, [part], if_match=old['etag']), info)
        ids = [(await self.client.create_multipart_upload('test', f'pending-{i}'))['upload_id'] for i in range(3)]
        first = await self.client.list_multipart_uploads('test', limit=2)
        second = await self.client.list_multipart_uploads('test', limit=2, continuation_token=first['next_continuation_token'])
        self.assertEqual({u['upload_id'] for u in first['uploads'] + second['uploads']}, set(ids))
        result = await self.client.abort_multipart_uploads(ids + [upload, 'bad-id'])
        self.assertEqual([r['status'] for r in result['results']], [204]*4 + [400])
        self.assertEqual((await self.client.list_multipart_uploads('test'))['uploads'], [])
        self.assertEqual((await self.client.get_object_bytes('test', 'result'))[0], b'later')

    async def test_cleanup_keeps_active_receivers_and_expires_idle_sessions(self):
        cleaner, port = await self.start('--multipart-idle-secs', '1', '--multipart-cleanup-interval-secs', '1')
        idle = (await cleaner.create_multipart_upload('test', 'idle'))['upload_id']
        active = (await cleaner.create_multipart_upload('test', 'active'))['upload_id']
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        writer.write(f'PUT /s3-uploads/{active}/1 HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\nx\r\n'.encode())
        await writer.drain()
        try:
            await asyncio.sleep(2.3)
            pending = await cleaner.list_multipart_uploads('test')
            self.assertEqual([u['upload_id'] for u in pending['uploads']], [active])
            writer.write(b'1\r\ny\r\n0\r\n\r\n')
            await writer.drain()
            response = await reader.read()
            self.assertIn(b'200 OK', response)
            parts = (await cleaner.list_parts(active))['parts']
            await cleaner.complete_multipart_upload(active, parts)
            self.assertEqual((await cleaner.get_object_bytes('test', 'active'))[0], b'xy')
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_verify_reports_corruption_without_changing_records(self):
        await self.client.put_object_bytes('test', 'damaged', b'original')
        with closing(self.connect_db()) as db:
            relative = db.execute('SELECT file_path FROM objects').fetchone()[0]
        path = self.root / 'data/objects' / relative
        path.write_bytes(b'corrupt!')
        async def verify(*flags):
            process = await asyncio.create_subprocess_exec(str(SERVER), '--database-url', self.dsn,
                '--storage-root', str(self.root/'data'), *flags, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            return process.returncode, json.loads(stdout)
        code, report = await verify('--verify')
        self.assertEqual((code, report['issues']), (0, []))
        code, report = await verify('--verify-full')
        self.assertEqual(code, 1)
        self.assertEqual(report['issues'][0]['problem'], 'sha256_mismatch')
        path.unlink()
        code, report = await verify('--verify')
        self.assertEqual((code, report['issues'][0]['problem']), (1, 'missing_file'))
        with closing(self.connect_db()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM objects').fetchone()[0], 1)

    async def test_verify_does_not_create_missing_database_or_storage(self):
        missing_db = self.root/'missing'/'db.sqlite'
        missing_root = self.root/'missing-data'
        process = await asyncio.create_subprocess_exec(str(SERVER), '--database-url', f'sqlite:///{missing_db}',
            '--storage-root', str(missing_root), '--verify', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse(missing_db.exists())
        self.assertFalse(missing_root.exists())

    async def test_atomic_file_download_sync_async_and_ranges(self):
        body = b'abcdefgh' * 131072
        await self.client.put_object_bytes('test', 'data', body)
        output = self.root/'nested'/'output'
        output.parent.mkdir()
        output.write_bytes(b'previous')
        await self.client.get_object_to_file('test', 'data', output)
        self.assertEqual(output.read_bytes(), body)
        sync = ALocalS3Client(f'http://127.0.0.1:{self.port}', disable_proxy=True)
        await asyncio.to_thread(sync.get_object_to_file, 'test', 'data', output, 'bytes=2-5')
        self.assertEqual(output.read_bytes(), body[2:6])
        await self.client.get_object_to_file('test', 'data', output, 'bytes=1-3')
        self.assertEqual(output.read_bytes(), body[1:4])
        await asyncio.to_thread(sync.get_object_to_file, 'test', 'data', output)
        self.assertEqual(output.read_bytes(), body)
        await self.client.put_object_bytes('test', 'empty', b'')
        await self.client.get_object_to_file('test', 'empty', output)
        self.assertEqual(output.read_bytes(), b'')
        output.write_bytes(b'keep')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:404'):
            await self.client.get_object_to_file('test', 'missing', output)
        self.assertEqual(output.read_bytes(), b'keep')


if __name__ == '__main__':
    unittest.main()
