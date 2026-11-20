import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from shadow_projection import open_shadow
from shadow_worker import schema,page,code_fingerprint,storage_bytes,ReusableClient
from test_shadow_projection import Fixture, SOURCE, AUTHORITY, compiler


class WorkerFixture(Fixture):
    def execute(self,sql,params=None,settings=None):
        if 'SELECT source_id,file_id,metadata FROM sync_generations' in sql:
            cursor=(params['source'],params['file'])
            return [(source,file_id,meta) for (source,file_id),meta in sorted(self.ledger.items()) if (source,file_id)>cursor][:64]
        return super().execute(sql,params,settings)


class ShadowWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.db_path=self.root/'shadow.sqlite';self.db=open_shadow(self.db_path);schema(self.db)
    def tearDown(self):self.db.close();self.temp.cleanup()
    def run_page(self,fixture,now=1000):
        return page(fixture,self.db,AUTHORITY,'`chunks`',self.root/'spool',compiler,self.db_path,now=now)
    def test_restartable_scan_replays_without_duplicate_occurrences(self):
        fixture=WorkerFixture((b'{"type":"session","id":"s"}\n',
                               b'{"type":"message","message":{"role":"user","content":[{"text":"hello"}]}}\n'))
        result=self.run_page(fixture)
        self.assertEqual(result['processed'],2);self.assertEqual(result['failed'],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],2)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_vcc_generation').fetchone()[0],2)
        self.assertEqual(self.run_page(fixture)['phase'],'cycle_complete')
        self.assertEqual(self.run_page(fixture)['skipped'],2)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],2)
    def test_poison_stays_pending_with_backoff_and_no_success_receipt(self):
        fixture=WorkerFixture((b'not-json\n',))
        result=self.run_page(fixture)
        self.assertEqual(result['failed'],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_generation').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT error_class,attempts FROM shadow_failure').fetchone(),('PoisonRecord',1))
        self.run_page(fixture);self.assertEqual(self.run_page(fixture,now=1001)['skipped'],1)
        self.assertEqual(self.db.execute('SELECT attempts FROM shadow_failure').fetchone()[0],1)
    def test_same_generation_changed_metadata_fails_closed(self):
        fixture=WorkerFixture((b'{"type":"session","id":"s"}\n',));self.run_page(fixture);self.run_page(fixture)
        item=json.loads(fixture.ledger[(SOURCE,'v0')]);item['parent']='other'
        fixture.ledger[(SOURCE,'v0')]=json.dumps(item,sort_keys=True,separators=(',',':'))
        result=self.run_page(fixture,now=2000)
        self.assertEqual(result['failed'],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_generation').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT error_class FROM shadow_failure').fetchone()[0],'ValueError')
    def test_connection_reused_and_recreated_after_invalidation(self):
        made=[]
        class Client:
            def __init__(self):self.closed=False
            def disconnect(self):self.closed=True
        def factory():
            c=Client();made.append(c);return c
        conn=ReusableClient(factory)
        first=conn.get()
        for _ in range(100):self.assertIs(conn.get(),first)
        self.assertEqual(len(made),1)
        conn.invalidate();self.assertTrue(first.closed)
        self.assertIsNot(conn.get(),first);self.assertEqual(len(made),2)
        conn.invalidate();conn.invalidate()
    def test_disconnect_error_does_not_prevent_reconnect(self):
        class Client:
            def disconnect(self):raise OSError('failed close')
        conn=ReusableClient(Client);first=conn.get();conn.invalidate()
        self.assertIsNot(conn.get(),first)
    def test_code_fingerprint_changes_and_prior_shadows_count_toward_budget(self):
        source=self.root/'compiler.py';source.write_text('one')
        first=code_fingerprint([source]);source.write_text('two')
        self.assertNotEqual(first,code_fingerprint([source]))
        before=storage_bytes(self.root)
        (self.root/'previous.sqlite').write_bytes(b'x'*4096)
        self.assertGreaterEqual(storage_bytes(self.root),before+4096)
    def test_backpressure_does_not_advance_scan_cursor(self):
        fixture=WorkerFixture((b'{"type":"session","id":"s"}\n',))
        with patch('shadow_worker.MAX_SHADOW_BYTES',1):result=self.run_page(fixture)
        self.assertEqual(result['phase'],'backpressure')
        self.assertEqual(self.db.execute('SELECT source_cursor,file_cursor FROM shadow_worker_state').fetchone(),('',''))
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_generation').fetchone()[0],0)


if __name__=='__main__':unittest.main()
