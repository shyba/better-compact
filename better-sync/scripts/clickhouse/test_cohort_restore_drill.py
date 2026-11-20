import json
import unittest

from cohort_restore_drill import gather,copy_cohort
from test_raw_coverage import LineageFixture,SOURCE


class Client:
    def __init__(self):
        self.f=LineageFixture();self.authority='test-authority-000001';self.corrupt=False;self.calls=[]
        self.versions=[{**v,'source_id':SOURCE,'path':'session.jsonl'} for v in self.f.versions]
    def execute(self,sql,params=None,settings=None):
        self.calls.append((sql,params,settings))
        if 'sync_authority' in sql:return [(self.authority,)]
        if 'sync_generations' in sql:return [(json.dumps(v),) for v in self.versions if v['file_id']==params['file']]
        if 'sync_segments' in sql:
            rows=self.f.rows.get(params['prefix'],[])
            return [(r[-1],) for r in rows] if 'SELECT segment_id' in sql else rows
        if 'sync_refs' in sql:
            rows=self.f.refs[params['id']]
            return [(r[1],) for r in rows] if 'SELECT hash FROM' in sql else rows
        if 'archive_migration_v1_chunks' in sql:
            return [(k.encode(),len(self.f.payloads[k]),b'x'*len(self.f.payloads[k]) if self.corrupt else self.f.payloads[k])
                    for k in params['hashes'] if k in self.f.payloads]
        raise AssertionError(sql)
    def intent(self):return {**self.versions[-1],'authority':self.authority,'committed_at':1}


class CohortTests(unittest.TestCase):
    def test_gather_requires_full_verified_lineage(self):
        c=Client();leaf,versions,segments,hashes,result=gather(c,c.intent())
        self.assertEqual((len(versions),len(segments),len(hashes)),(3,3,2))
        self.assertEqual(result['sha256'],leaf['sha256'])
        c.corrupt=True
        with self.assertRaises(ValueError):gather(c,c.intent())
    def test_missing_intent_or_authority_fails(self):
        c=Client();intent=c.intent();intent.pop('committed_at')
        with self.assertRaises(ValueError):gather(c,intent)
        intent=c.intent();intent['authority']='different-authority'
        with self.assertRaises(ValueError):gather(c,intent)
    def test_copy_is_bounded_and_uses_key_prewhere(self):
        c=Client();leaf,versions,segments,hashes,_=gather(c,c.intent())
        calls=[]
        class Writer:
            def execute(self,sql,params=None,settings=None):calls.append((sql,params,settings))
        names={key:'isolated_'+key for key in ('sync_authority','sync_generations','sync_segments','sync_refs','sync_chunks')}
        copy_cohort(Writer(),names,leaf,versions,segments,hashes)
        self.assertEqual(len(calls),5)
        self.assertTrue(all(sql.lstrip().startswith('INSERT INTO') for sql,_,_ in calls))
        self.assertIn('PREWHERE hash IN',calls[-1][0])
        self.assertEqual(calls[-1][2]['max_threads'],1)


if __name__=='__main__':unittest.main()
