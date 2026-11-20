import json
import unittest

from generation_ledger import commit_generation, lineage
from raw_coverage import CoverageMismatch, CoverageUnknown
from store import Store
from test_raw_coverage import LineageFixture, SOURCE


class LedgerFixture(LineageFixture):
    def __init__(self):
        super().__init__();self.ledger={};self.calls=[]
    def generation(self,index):
        return {'source_id':SOURCE,'path':'session.jsonl',**self.versions[index]}
    def execute(self,sql,params=None,settings=None):
        self.calls.append(sql)
        if 'SELECT metadata FROM sync_generations' in sql:
            value=self.ledger.get((params['source'],params['file']))
            return [(value,)] if value else []
        if sql.startswith('INSERT INTO sync_generations'):
            source,file_id,path,metadata=params[0]
            self.ledger[(source,file_id)]=metadata
            return []
        return super().execute(sql,params,settings)
    def commit(self,index):return commit_generation(self,'`chunks`',self.generation(index))
    def disconnect(self):pass


class LedgerTests(unittest.TestCase):
    def test_verified_root_and_append_receipts_are_last(self):
        f=LedgerFixture()
        for i in range(3):
            result=f.commit(i)
            self.assertTrue(result['committed'])
            self.assertTrue(f.calls[-1].startswith('INSERT INTO sync_generations'))
        self.assertEqual([x['file_id'] for x in lineage(f,f.generation(2))],['v0','v1','v2'])
        inserts=sum(x.startswith('INSERT INTO sync_generations') for x in f.calls)
        self.assertTrue(f.commit(2)['committed'])
        self.assertEqual(sum(x.startswith('INSERT INTO sync_generations') for x in f.calls),inserts)
    def test_store_verifies_committed_append_but_not_projection_or_backup(self):
        f=LedgerFixture()
        for i in range(3):f.commit(i)
        result=Store('chunks',factory=lambda:f).verify_raw(f.item)
        self.assertEqual(result['status'],'verified')
        self.assertIn('explicit lineage',result['scope'])
        wrong={**f.item,'sha256':'0'*64}
        self.assertEqual(Store('chunks',factory=lambda:f).verify_raw(wrong)['status'],'mismatch')
    def test_append_after_empty_root_is_valid(self):
        from test_raw_coverage import sha
        f=LedgerFixture()
        root=f.generation(0);root.update(size=0,sha256=sha(b''))
        root_segment='v0.0';root_id=sha((SOURCE+'\0'+root_segment).encode())
        f.rows['v0.']=[(root_segment,'session.jsonl',0,0,True,sha(b''),root_id)]
        f.refs[root_id]=[]
        self.assertTrue(commit_generation(f,'`chunks`',root)['committed'])
        child=f.generation(1);child.update(start=0,size=4,sha256=sha(b'abcd'))
        child_segment='v1.0';child_id=sha((SOURCE+'\0'+child_segment).encode())
        f.rows['v1.']=[(child_segment,'session.jsonl',0,4,False,sha(b'abcd'),child_id)]
        f.refs[child_id]=[(0,sha(b'abcd'),4)]
        self.assertTrue(commit_generation(f,'`chunks`',child)['committed'])
    def test_lost_generation_acknowledgement_replays_without_duplicate(self):
        class LostAck(LedgerFixture):
            failed=False
            def execute(self,sql,params=None,settings=None):
                result=super().execute(sql,params,settings)
                if sql.startswith('INSERT INTO sync_generations') and not self.failed:
                    self.failed=True
                    raise RuntimeError('simulated lost acknowledgement')
                return result
        f=LostAck()
        with self.assertRaises(RuntimeError):f.commit(0)
        self.assertEqual(len(f.ledger),1)
        self.assertTrue(f.commit(0)['committed'])
        self.assertEqual(sum(x.startswith('INSERT INTO sync_generations') for x in f.calls),1)
    def test_missing_parent_never_creates_receipt(self):
        f=LedgerFixture()
        with self.assertRaises(CoverageUnknown):f.commit(1)
        self.assertEqual(f.ledger,{})
    def test_corrupt_chunk_never_creates_receipt(self):
        f=LedgerFixture();f.payloads.clear()
        with self.assertRaises(CoverageMismatch):f.commit(0)
        self.assertEqual(f.ledger,{})
    def test_identity_conflict_and_wrong_lineage_rejected(self):
        f=LedgerFixture();f.commit(0)
        changed=f.generation(0);changed['sha256']='0'*64
        with self.assertRaises(CoverageMismatch):commit_generation(f,'`chunks`',changed)
        changed=f.generation(1);changed['path']='another'
        with self.assertRaises(CoverageMismatch):commit_generation(f,'`chunks`',changed)
        self.assertEqual(len(f.ledger),1)
    def test_cyclic_ancestor_does_not_loop(self):
        f=LedgerFixture();ancestor=f.generation(0);ancestor['parent']='v1'
        f.ledger[(SOURCE,'v0')]=json.dumps(ancestor,sort_keys=True,separators=(',',':'))
        with self.assertRaises(CoverageMismatch):f.commit(1)


if __name__=='__main__':unittest.main()
