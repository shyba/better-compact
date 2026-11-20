import hashlib
import json
from types import SimpleNamespace
import unittest

from historical_frame_parity import read_frame,compare_frame
from test_handoff_parity import PG

ARCHIVE='a'*64;SOURCE='b'*32


class Client:
    def __init__(self,body=b'{"type":"message"}\n'):
        self.body=body;self.digest=hashlib.sha256(body).hexdigest();self.have_receipt=True;self.corrupt=False
        self.meta={'source_id':SOURCE,'source_kind':'pi-jsonl','path':'fixture.jsonl',
                   'full':True,'start':0,'bytes':len(body),'sha256':self.digest}
    def execute(self,sql,params,settings=None):
        if 'archive_migration_v1_archives' in sql:return [(1,)] if self.have_receipt else []
        if 'archive_migration_v1_frames' in sql:return [(json.dumps(self.meta),len(self.body),self.digest,1)]
        if 'archive_migration_v1_refs' in sql:return [(0,0,self.digest,len(self.body))]
        if 'archive_migration_v1_chunks' in sql:
            return [(self.digest.encode(),len(self.body),b'x'*len(self.body) if self.corrupt else self.body)]
        raise AssertionError(sql)


class Compiler:
    @staticmethod
    def build_ir_opencode(records,path):
        return [SimpleNamespace(ord=0,kind='user',wnum=1,text='same',brief='same',ts=None)],{}


class HistoricalFrameParityTests(unittest.TestCase):
    def test_committed_historical_frame_is_verified_and_comparison_is_scoped(self):
        result=compare_frame(PG([('user',1,'same','same')]),Client(),Compiler,ARCHIVE,0)
        self.assertEqual(result['status'],'matched')
        self.assertIn('not necessarily latest',result['input_scope'])
    def test_missing_commit_corruption_and_non_full_frames_fail(self):
        for failure in ('receipt','chunk','full'):
            c=Client()
            if failure=='receipt':c.have_receipt=False
            if failure=='chunk':c.corrupt=True
            if failure=='full':c.meta['full']=False
            with self.assertRaises(ValueError):read_frame(c,ARCHIVE,0)
    def test_nonmatching_or_ambiguous_legacy_session_cannot_pass(self):
        self.assertEqual(compare_frame(PG([('user',1,'different','same')]),Client(),Compiler,ARCHIVE,0)['status'],'mismatch')
        self.assertEqual(compare_frame(PG([],sessions=('one','two')),Client(),Compiler,ARCHIVE,0)['status'],'unknown')
    def test_incomplete_jsonl_tail_cannot_pass(self):
        with self.assertRaises(ValueError):compare_frame(PG([]),Client(b'{"type":"message"}'),Compiler,ARCHIVE,0)


if __name__=='__main__':unittest.main()
