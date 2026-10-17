"""Bulk deletion, lifecycle, quotas, disk admission and metrics on real servers."""
import asyncio
from contextlib import closing
from datetime import datetime, timedelta, timezone
import unittest
from urllib.request import Request, urlopen

import test_release_010 as release
from alocals3.client import ALocalS3Client, ALocalS3ClientAsync


class MaintenanceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = release.ReleaseTests.asyncSetUp
    asyncTearDown = release.ReleaseTests.asyncTearDown
    start = release.ReleaseTests.start
    connect_db = release.ReleaseTests.connect_db

    def age(self, keys, seconds=7200):
        stamp = (datetime.now(timezone.utc)-timedelta(seconds=seconds)).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        with closing(self.connect_db()) as db:
            marker = '%s' if self.pg_dsn else '?'
            for key in keys:
                db.execute(f'UPDATE objects SET updated_at={marker} WHERE key={marker}', (stamp, key))
            db.commit()

    def seed(self, prefix, count):
        with closing(self.connect_db()) as db:
            template = tuple(db.execute("SELECT bucket_id, file_path, size, content_type, etag, updated_at, created_at, metadata FROM objects WHERE key='seed'").fetchone())
            markers = ','.join(['%s' if self.pg_dsn else '?']*9)
            cursor = db.cursor()
            cursor.executemany(f'INSERT INTO objects(bucket_id,key,file_path,size,content_type,etag,updated_at,created_at,metadata) VALUES ({markers})',
                [(template[0], f'{prefix}{i}', *template[1:]) for i in range(count)])
            db.commit()

    async def wait_missing(self, client, key):
        for _ in range(100):
            try:
                await client.head_object('test', key)
            except RuntimeError as error:
                if 'STATUS:404' in str(error):
                    return
                raise
            await asyncio.sleep(.05)
        self.fail(f'{key} did not expire')

    async def test_bulk_2501_objects_uses_three_requests_and_preserves_shared_blob(self):
        await self.client.put_object_bytes('test', 'seed', b'shared')
        self.seed('bulk-', 2501)
        result = await self.client.delete_objects('test', (f'bulk-{i}' for i in range(2501)), quiet=True)
        self.assertEqual(result, {'deleted': [], 'errors': [], 'deleted_count': 2501, 'success_count': 2501})
        metrics = await self.client.metrics()
        self.assertIn('alocals3_http_requests_total{method="POST",route="/s3-admin/:bucket/delete",status="200"} 3', metrics)
        self.assertEqual((await self.client.get_object_bytes('test', 'seed'))[0], b'shared')
        usage = await self.client.bucket_usage('test')
        self.assertEqual((usage['object_count'], usage['logical_bytes']), (1, 6))

    async def test_bulk_errors_conditions_missing_duplicates_and_sync_client(self):
        obj = await self.client.put_object_bytes('test', 'keep', b'keep')
        await self.client.put_object_bytes('test', 'delete', b'x')
        result = await self.client.delete_objects('test', ['delete', 'missing', 'delete', {'key': 'keep', 'if_match': 'wrong'}, '../invalid'])
        self.assertEqual(result['deleted_count'], 1)
        self.assertEqual(result['success_count'], 3)
        self.assertEqual([e['status'] for e in result['errors']], [422, 412])
        self.assertEqual((await self.client.get_object_bytes('test', 'keep'))[0], b'keep')
        sync = ALocalS3Client(f'http://127.0.0.1:{self.port}', disable_proxy=True)
        result = await asyncio.to_thread(sync.delete_objects, 'test', [{'key':'keep','if_match':obj['etag']}])
        self.assertEqual(result['deleted_count'], 1)
        self.assertEqual((await self.client.bucket_usage('test'))['object_count'], 0)
        self.assertEqual((await self.client.delete_objects('test', []))['success_count'], 0)
        with self.assertRaises(TypeError):
            await self.client.delete_objects('test', 'keep')

    async def test_bulk_database_failure_rolls_back_all_objects_and_usage(self):
        for key in ['before', 'locked', 'after']:
            await self.client.put_object_bytes('test',key,b'x')
        with closing(self.connect_db()) as db:
            if self.pg_dsn:
                db.execute("CREATE FUNCTION block_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.key = 'locked' THEN RAISE EXCEPTION 'injected failure'; END IF; RETURN OLD; END; $$")
                db.execute('CREATE TRIGGER block_delete BEFORE DELETE ON objects FOR EACH ROW EXECUTE FUNCTION block_delete()')
            else:
                db.execute("CREATE TRIGGER block_delete BEFORE DELETE ON objects WHEN OLD.key='locked' BEGIN SELECT RAISE(ABORT, 'injected failure'); END")
            db.commit()
        with self.assertRaisesRegex(RuntimeError,'STATUS:500'):
            await self.client.delete_objects('test',['before','locked','after'])
        self.assertEqual((await self.client.bucket_usage('test'))['object_count'],3)
        for key in ['before','locked','after']:
            self.assertEqual((await self.client.get_object_bytes('test',key))[0],b'x')

    async def test_upgrade_initializes_usage_from_existing_objects_once(self):
        await self.client.put_object_bytes('test','seed',b'abcd')
        await self.client.copy_object('test','seed','test','copy')
        with closing(self.connect_db()) as db:
            if self.pg_dsn:
                db.execute('DROP TRIGGER alocals3_usage ON objects')
            else:
                for trigger in ['usage_insert','usage_update','usage_delete']:
                    db.execute(f'DROP TRIGGER {trigger}')
            db.execute('DROP TABLE bucket_usage')
            db.commit()
        upgraded, _ = await self.start()
        usage = await upgraded.bucket_usage('test')
        self.assertEqual((usage['object_count'],usage['logical_bytes']),(2,8))
        again, _ = await self.start()
        usage = await again.bucket_usage('test')
        self.assertEqual((usage['object_count'],usage['logical_bytes']),(2,8))

    async def test_bulk_and_lifecycle_gc_preserve_shared_references_and_grace(self):
        self.enable_gc = True
        gc, _ = await self.start('--gc-start-delay-secs','0','--gc-interval-secs','1','--gc-grace-secs','1','--lifecycle-interval-secs','1')
        await gc.put_object_bytes('test','source',b'shared')
        await gc.copy_object('test','source','test','keep')
        with closing(self.connect_db()) as db:
            relative = db.execute("SELECT file_path FROM objects WHERE key='source'").fetchone()[0]
        blob = self.root/'data/objects'/relative
        await gc.delete_objects('test',['source'])
        await asyncio.sleep(.1)
        self.assertTrue(blob.exists())
        self.assertEqual((await gc.get_object_bytes('test','keep'))[0],b'shared')
        # Expiration retires the last reference, still respecting grace.
        self.age(['keep'])
        await gc.set_lifecycle('test',[{'id':'expire','prefix':'keep','expire_after_secs':3600}])
        await self.wait_missing(gc,'keep')
        for _ in range(100):
            if not blob.exists():
                break
            await asyncio.sleep(.05)
        self.assertFalse(blob.exists())
        self.assertEqual((await gc.bucket_usage('test'))['object_count'],0)

    async def test_quota_applies_to_overwrite_copy_and_multipart_and_rolls_back(self):
        await self.client.set_bucket_quota('test', 10)
        await self.client.put_object_bytes('test', 'first', b'123456')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:507'):
            await self.client.put_object_bytes('test', 'second', b'12345')
        await self.client.put_object_bytes('test', 'first', b'1234')
        await self.client.put_object_bytes('test', 'second', b'12345')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:507'):
            await self.client.copy_object('test', 'first', 'test', 'copy')
        upload = (await self.client.create_multipart_upload('test', 'parts'))['upload_id']
        part = await self.client.upload_part(upload, 1, b'12')
        with self.assertRaisesRegex(RuntimeError, 'STATUS:507'):
            await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual((await self.client.list_parts(upload))['parts'][0]['size'], 2)
        usage = await self.client.bucket_usage('test')
        self.assertEqual((usage['object_count'], usage['logical_bytes'], usage['pending_part_bytes']), (2, 9, 2))
        await self.client.delete_objects('test', ['second'])
        await self.client.complete_multipart_upload(upload, [part])
        self.assertEqual((await self.client.bucket_usage('test'))['logical_bytes'], 6)
        # Shrinking below current usage still permits shrinking writes and deletion.
        await self.client.set_bucket_quota('test', 3)
        await self.client.put_object_bytes('test', 'first', b'1')
        await self.client.delete_objects('test', ['parts'])
        self.assertEqual((await self.client.bucket_usage('test'))['logical_bytes'], 1)

    async def test_quota_is_atomic_across_two_processes_and_persists_restart(self):
        await self.client.set_bucket_quota('test', 10)
        other, _ = await self.start()
        async def put(client, key):
            try:
                await client.put_object_bytes('test', key, b'123456')
                return True
            except RuntimeError as e:
                self.assertIn('STATUS:507', str(e))
                return False
        self.assertEqual(sum(await asyncio.gather(put(self.client, 'one'), put(other, 'two'))), 1)
        third, _ = await self.start()
        usage = await third.bucket_usage('test')
        self.assertEqual((usage['logical_bytes'], usage['max_bytes']), (6,10))
        await third.set_bucket_quota('test', 0)
        await third.put_object_bytes('test', 'three', b'additional')

    async def test_lifecycle_preview_paging_disabled_rules_and_fresh_overwrite(self):
        for key in ['tmp/old-a', 'tmp/old-b', 'tmp/fresh', 'permanent/old', '目录/%_old']:
            await self.client.put_object_bytes('test', key, b'content')
        self.age(['tmp/old-a', 'tmp/old-b', 'tmp/fresh', 'permanent/old', '目录/%_old'])
        await self.client.put_object_bytes('test', 'tmp/fresh', b'new')
        rules = [
            {'id':'temp','prefix':'tmp/','expire_after_secs':3600},
            {'id':'permanent-disabled','prefix':'permanent/','expire_after_secs':3600,'enabled':False},
            {'id':'unicode','prefix':'目录/%_','expire_after_secs':3600},
            {'id':'overlap','prefix':'tmp/old','expire_after_secs':3600},
        ]
        await self.client.set_lifecycle('test', rules)
        first = await self.client.preview_expiration('test', limit=1)
        seen = [v['key'] for v in first['objects']]
        while first['is_truncated']:
            first = await self.client.preview_expiration('test', limit=1, after_id=first['next_after_id'])
            seen.extend(v['key'] for v in first['objects'])
        self.assertEqual(set(seen), {'tmp/old-a','tmp/old-b','目录/%_old'})
        self.assertEqual(len(seen), 3)
        self.assertEqual((await self.client.bucket_usage('test'))['object_count'], 5)
        cleaner, _ = await self.start('--lifecycle-interval-secs','1')
        for key in seen:
            await self.wait_missing(cleaner, key)
        self.assertEqual((await cleaner.get_object_bytes('test','tmp/fresh'))[0], b'new')
        self.assertEqual((await cleaner.get_object_bytes('test','permanent/old'))[0], b'content')
        self.assertEqual((await cleaner.bucket_usage('test'))['object_count'], 2)
        await cleaner.clear_lifecycle('test')
        self.assertEqual((await cleaner.get_lifecycle('test'))['rules'], [])

    async def test_lifecycle_pause_and_invalid_configuration(self):
        for rules in [
            [{'id':'x','expire_after_secs':0}],
            [{'id':'x','expire_after_secs':1},{'id':'x','expire_after_secs':2}],
            [{'id':'x','prefix':'/','expire_after_secs':1}],
        ]:
            with self.assertRaisesRegex(RuntimeError, 'STATUS:400'):
                await self.client.set_lifecycle('test', rules)
        await self.client.put_object_bytes('test','old',b'x')
        self.age(['old'])
        await self.client.set_lifecycle('test',[{'id':'all','expire_after_secs':3600,'enabled':False}])
        self.assertEqual((await self.client.preview_expiration('test'))['objects'], [])
        self.assertEqual(len((await self.client.preview_expiration('test', include_disabled=True))['objects']),1)
        await self.client.set_lifecycle('test',[{'id':'all','expire_after_secs':3600}])
        paused, _ = await self.start('--lifecycle-interval-secs','0')
        self.assertEqual((await paused.get_object_bytes('test','old'))[0],b'x')
        self.assertEqual(len((await paused.preview_expiration('test'))['objects']),1)

    async def test_disk_reserve_blocks_put_parts_and_assembly_but_allows_reads_and_delete(self):
        await self.client.put_object_bytes('test','existing',b'keep')
        upload = (await self.client.create_multipart_upload('test','result'))['upload_id']
        part = await self.client.upload_part(upload,1,b'part')
        low, _ = await self.start('--min-free-bytes',str(2**64-1))
        with self.assertRaisesRegex(RuntimeError,'STATUS:503'):
            await low.ready()
        self.assertEqual((await low.health())['status'],'ok')
        for key, body in [('new',b'x'),('existing',b'new'),('empty',b'')]:
            with self.assertRaisesRegex(RuntimeError,'STATUS:507'):
                await low.put_object_bytes('test',key,body)
        with self.assertRaisesRegex(RuntimeError,'STATUS:507'):
            await low.upload_part(upload,2,b'new part')
        with self.assertRaisesRegex(RuntimeError,'STATUS:507'):
            await low.complete_multipart_upload(upload,[part])
        self.assertEqual((await low.get_object_bytes('test','existing'))[0],b'keep')
        self.assertEqual(len((await low.list_parts(upload))['parts']),1)
        self.assertEqual((await low.delete_objects('test',['existing']))['deleted_count'],1)

    async def test_metrics_stats_and_authentication(self):
        secured, port = await self.start('--auth-enabled','true','--service-token','token','--user-auth-secret','secret')
        auth = ALocalS3ClientAsync(f'http://127.0.0.1:{port}', disable_proxy=True, service_token='token')
        self.clients.append(auth)
        for method in [secured.metrics,secured.storage_stats]:
            with self.assertRaisesRegex(RuntimeError,'STATUS:401'):
                await method()
        self.assertEqual((await secured.ready())['status'],'ready')
        await auth.put_object_bytes('test','secret-object-name',b'1234')
        await auth.get_object_bytes('test','secret-object-name')
        await auth.set_bucket_quota('test',20)
        stats = await auth.storage_stats()
        self.assertEqual((stats['object_count'],stats['logical_bytes']),(1,4))
        text = await auth.metrics()
        self.assertNotIn('secret-object-name',text)
        self.assertIn('alocals3_storage_logical_bytes 4',text)
        self.assertIn('alocals3_filesystem_available_bytes ',text)
        self.assertIn('alocals3_http_response_headers_seconds_bucket{',text)
        self.assertRegex(text,r'alocals3_http_request_body_bytes_total [1-9][0-9]*')
        def headers():
            with urlopen(Request(f'http://127.0.0.1:{port}/s3',headers={'Authorization':'Bearer token'})) as response:
                return response.headers
        response_headers = await asyncio.to_thread(headers)
        self.assertRegex(response_headers['x-request-id'],r'^[0-9a-f]{32}$')


if __name__ == '__main__':
    unittest.main()
