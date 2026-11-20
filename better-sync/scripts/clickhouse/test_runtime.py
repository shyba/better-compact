import hashlib
import http.client
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer

from backup import backup_sql, repository_url, poll, BackupFailed, retryable_backup_failure, assert_no_server_jobs
from receiver import handler
from store import metadata, segment_identity, Store, Conflict

TOKEN='test-token-long-enough'
SOURCE='a'*32
DATA=b'original bytes\xff\n'


class FakeClient:
    def __init__(self):
        self.receipt=None
        self.calls=[]
        self.fail=False
    def execute(self,sql,params=None):
        self.calls.append(sql)
        if sql.startswith('SELECT metadata'):
            return [(self.receipt,)] if self.receipt else []
        if sql.startswith('SELECT raw_size'):
            return []
        if sql.startswith('INSERT INTO sync_segments'):
            if self.fail:raise RuntimeError('lost acknowledgement')
            self.receipt=params[0][-1]
        return []
    def disconnect(self):pass


class StoreTests(unittest.TestCase):
    def test_receipt_last_and_immutable(self):
        c=FakeClient()
        s=Store(factory=lambda:c)
        m=metadata(SOURCE,'file','path',0,True,'pi-jsonl','root',len(DATA),hashlib.sha256(DATA).hexdigest())
        receipt=s.commit(m,io.BytesIO(DATA))
        self.assertTrue(receipt['committed'])
        self.assertTrue(c.calls[-1].startswith('INSERT INTO sync_segments'))
        self.assertEqual(s.commit(m,io.BytesIO(DATA)),receipt)
        with self.assertRaises(Conflict):s.commit({**m,'path':'other'},io.BytesIO(DATA))
    def test_existing_receipt_replays_references_before_acknowledgement(self):
        c=FakeClient();s=Store(factory=lambda:c)
        m=metadata(SOURCE,'file','path',0,True,'pi-jsonl','root',len(DATA),hashlib.sha256(DATA).hexdigest())
        s.commit(m,io.BytesIO(DATA));c.calls.clear()
        self.assertTrue(s.commit(m,io.BytesIO(DATA))['committed'])
        self.assertTrue(any(sql.startswith('INSERT INTO sync_refs') for sql in c.calls))
        self.assertFalse(any(sql.startswith('INSERT INTO sync_segments') for sql in c.calls))
    def test_failed_commit_has_no_success(self):
        c=FakeClient();c.fail=True;s=Store(factory=lambda:c)
        m=metadata(SOURCE,'file','path',0,True,'pi-jsonl','root',len(DATA),hashlib.sha256(DATA).hexdigest())
        with self.assertRaises(RuntimeError):s.commit(m,io.BytesIO(DATA))
        self.assertIsNone(c.receipt)
    def test_identity_preserves_source(self):
        self.assertNotEqual(segment_identity(SOURCE,'x'),segment_identity('b'*32,'x'))
    def test_invalid_replacement(self):
        with self.assertRaises(ValueError):metadata(SOURCE,'f','p',1,True,'pi','root',0,'0'*64)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.commits=[]
        class FakeStore:
            def authority(self):return 'test-authority-00000001'
            def commit(_,meta,stream):
                self.commits.append(stream.read())
                return dict(store='clickhouse',committed=True,**meta)
            def inventory(_,source):return []
            def verify_raw(_,item):return {'status':'verified','scope':'raw replacement'}
            def commit_generation(_,item):return {'committed':True,**item}
            def generation_status(_,source,file_id):return {'source_id':source,'file_id':file_id} if file_id=='known' else None
        self.temp=tempfile.TemporaryDirectory()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),handler(FakeStore(),TOKEN,Path(self.temp.name)))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join();self.temp.cleanup()
    def request(self,method,path,body=None,headers=None):
        conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        conn.request(method,path,body,headers or {})
        response=conn.getresponse();data=json.loads(response.read());status=response.status;conn.close()
        return status,data
    def test_auth(self):self.assertEqual(self.request('GET','/health')[0],401)
    def test_capabilities(self):
        status,body=self.request('GET','/capabilities',headers={'Authorization':'Bearer '+TOKEN})
        self.assertEqual(status,200);self.assertEqual(body['store'],'clickhouse')
    def upload(self,sha):
        return self.request('PUT','/file/f',DATA,{'Authorization':'Bearer '+TOKEN,'x-vcc-source-id':SOURCE,
            'x-vcc-source-kind':'pi-jsonl','x-vcc-source-locator':'root','x-vcc-path':'path',
            'x-vcc-sha256':sha,'x-vcc-full':'true'})
    def test_checksum_before_commit(self):
        self.assertEqual(self.upload('0'*64)[0],400)
        self.assertEqual(self.commits,[])
    def test_commit_receipt(self):
        status,body=self.upload(hashlib.sha256(DATA).hexdigest())
        self.assertEqual(status,201);self.assertTrue(body['committed']);self.assertEqual(self.commits,[DATA])
    def test_generation_status_distinguishes_missing_from_receipt(self):
        for file_id,expected in (('known',200),('missing',404)):
            payload=json.dumps({'source_id':SOURCE,'file_id':file_id}).encode()
            status,body=self.request('POST','/generation/status',payload,{'Authorization':'Bearer '+TOKEN})
            self.assertEqual(status,expected)
            if expected==200:self.assertEqual(body['authority_id'],'test-authority-00000001')
    def test_generation_receipt_contains_authority_and_scope(self):
        payload=json.dumps({'file_id':'version'}).encode()
        status,body=self.request('POST','/generation',payload,{'Authorization':'Bearer '+TOKEN})
        self.assertEqual(status,201);self.assertTrue(body['committed'])
        self.assertEqual(body['store'],'clickhouse');self.assertEqual(body['authority_id'],'test-authority-00000001')
    def test_verify_separates_raw_from_unverified_backup_projection(self):
        payload=json.dumps({'files':[{'file_id':'version'}]}).encode()
        status,body=self.request('POST','/verify',payload,{'Authorization':'Bearer '+TOKEN})
        self.assertEqual(status,200)
        self.assertEqual(body['files'][0]['archive']['status'],'verified')
        for stage in ('backup','loaded','indexed'):
            self.assertEqual(body['files'][0][stage]['status'],'unknown')
    def test_verify_rejects_multiple_files(self):
        payload=json.dumps({'files':[{},{}]}).encode()
        status,_=self.request('POST','/verify',payload,{'Authorization':'Bearer '+TOKEN})
        self.assertEqual(status,400)


class BackupTests(unittest.TestCase):
    def test_unknown_or_running_server_job_prevents_new_backup(self):
        class Client:
            def __init__(self,rows):self.rows=rows
            def execute(self,*args):return self.rows
        assert_no_server_jobs(Client([(0,)]))
        for rows in ([],[(1,)],[(None,)],[(0,),(0,)]):
            with self.assertRaises(RuntimeError):assert_no_server_jobs(Client(rows))
    def test_only_confirmed_terminal_conflict_is_retryable(self):
        self.assertTrue(retryable_backup_failure(BackupFailed('BACKUP_FAILED',598)))
        for error in (BackupFailed('CREATING_BACKUP',598),BackupFailed('RESTORE_FAILED',598),
                      BackupFailed('BACKUP_FAILED',241),RuntimeError('unknown submission')):
            self.assertFalse(retryable_backup_failure(error))
    def test_server_error_code_extracted_without_leaking_details(self):
        class Client:
            def execute(self,*args):return [('BACKUP_FAILED','Code: 598. private destination')]
        with self.assertRaises(BackupFailed) as caught:poll(Client(),'job')
        self.assertEqual(caught.exception.code,598)
        self.assertNotIn('private destination',str(caught.exception))
    def test_terminal_failure_is_observed_before_exception(self):
        class Client:
            def execute(self,*args):return [('BACKUP_FAILED','server details')]
        seen=[]
        with self.assertRaises(RuntimeError):poll(Client(),'job',seen.append)
        self.assertEqual(seen,['BACKUP_FAILED'])
    def test_unknown_job_is_not_reported_as_terminal(self):
        class Client:
            def execute(self,*args):return []
        seen=[]
        with self.assertRaises(RuntimeError):poll(Client(),'job',seen.append)
        self.assertEqual(seen,[])
    def test_bare_endpoint_is_normalized(self):
        self.assertEqual(repository_url('192.0.2.1:9000/bucket/backups/'),'http://192.0.2.1:9000/bucket/backups')
        self.assertEqual(repository_url('https://example.org/bucket/repo'),'https://example.org/bucket/repo')
    def test_invalid_repository_rejected(self):
        for value in ('http:///bucket','https://example.org','https://user:secret@example.org/bucket','https://example.org/bucket?secret=x'):
            with self.assertRaises(ValueError):repository_url(value)

    def test_incremental_sql_is_native(self):
        sql=backup_sql('session_center',True)
        self.assertIn('base_backup=S3',sql);self.assertTrue(sql.endswith('ASYNC'))
        self.assertNotIn('EXPORT',sql)
    def test_full_has_no_base(self):self.assertNotIn('base_backup',backup_sql('session_center',False))
    def test_identifier_validation(self):
        with self.assertRaises(ValueError):backup_sql('db;DROP DATABASE x',False)


if __name__=='__main__':unittest.main()
