import json
import unittest

from restore_production_metadata import inspect_metadata
from test_raw_coverage import LineageFixture,SOURCE

NAMES={'sync_segments':'isolated_segments','sync_refs':'isolated_refs',
       'sync_generations':'isolated_generations','sync_authority':'isolated_authority'}


class Client:
    def __init__(self):
        self.fixture=LineageFixture();self.authority='authority-123456789';self.missing_ref=False
        self.versions=[{**v,'source_id':SOURCE,'path':'session.jsonl'} for v in self.fixture.versions]
    def execute(self,sql,params=None,settings=None):
        if 'isolated_authority' in sql:return [(self.authority,)]
        if 'isolated_generations' in sql:
            return [(json.dumps(v),) for v in self.versions if v['file_id']==params['file']]
        if 'isolated_segments' in sql:return self.fixture.rows.get(params['prefix'],[])
        if 'isolated_refs' in sql:return [] if self.missing_ref else self.fixture.refs[params['id']]
        raise AssertionError(sql)
    def intent(self):return {**self.versions[-1],'authority':self.authority}


class MetadataRestoreTests(unittest.TestCase):
    def test_verified_metadata_does_not_claim_payload_inclusion(self):
        c=Client();result=inspect_metadata(c,NAMES,c.intent())
        self.assertEqual((result['versions'],result['segments'],result['references']),(3,3,3))
        self.assertIn('never release outbox',result['scope'])
    def test_missing_refs_or_authority_mismatch_fail(self):
        c=Client();intent=c.intent();c.missing_ref=True
        with self.assertRaises(ValueError):inspect_metadata(c,NAMES,intent)
        c=Client();intent=c.intent();c.authority='different'
        with self.assertRaises(ValueError):inspect_metadata(c,NAMES,intent)
    def test_changed_generation_identity_fails(self):
        c=Client();intent=c.intent();intent['sha256']='0'*64
        with self.assertRaises(ValueError):inspect_metadata(c,NAMES,intent)


if __name__=='__main__':unittest.main()
