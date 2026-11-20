import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cohort_proof_audit import audit
from cohort_restore_drill import KEYS

SOURCE='a'*32;AUTH='test-authority-000001';TAG='sync_cohort_drill_'+'c'*32
BODY=b'hello\n';DIGEST=hashlib.sha256(BODY).hexdigest()
LEAF={'source_id':SOURCE,'file_id':'file','path':'file.jsonl','start':0,'size':len(BODY),'sha256':DIGEST,'parent':None}


class Client:
    def __init__(self):self.missing_job=False
    def execute(self,sql,params=None,settings=None):
        if 'system.backups' in sql:
            if self.missing_job:return []
            status='BACKUP_CREATED' if params['id']=='one' else 'RESTORED'
            return [(status,"S3('https://example/bucket/backup/full/')")]
        if 'sync_cohort_drill_' in sql and '_segments' in sql:
            meta={'source_id':SOURCE,'file_id':'file.0','path':'file.jsonl','start':0,'size':len(BODY),
                  'sha256':DIGEST,'full':True,'source_kind':'pi-jsonl','locator':'root'}
            return [('file.0','file.jsonl',0,len(BODY),1,DIGEST,json.dumps(meta))]
        raise AssertionError(sql)


class ProofAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.state={'phase':'verified','active':None,'authority':AUTH,'leaf':LEAF,'repository':'https://example/bucket/backup',
                    'jobs':[{'kind':'backup','terminal_status':'BACKUP_CREATED','id':'one'}, {'kind':'restore','terminal_status':'RESTORED','id':'two'}],
                    'tables':{k:TAG+'_'+k[5:] for k in KEYS},'restored_tables':{k:TAG+'_'+k[5:]+'_restored' for k in KEYS}}
        key=hashlib.sha256((SOURCE+'\nfile.0').encode()).hexdigest()
        self.body=self.root/(key+'.body');self.descriptor=self.root/(key+'.json');self.body.write_bytes(BODY)
        self.descriptor.write_text(json.dumps({'authority':AUTH,'fileID':'file.0','sourcePath':'file.jsonl','start':0,
            'full':True,'size':len(BODY),'sha256':DIGEST,'committed_at':1,'source':{'id':SOURCE,'kind':'pi-jsonl','locator':'root'}}))
    def tearDown(self):self.temp.cleanup()
    def check(self,c):
        with patch('cohort_proof_audit.verify_cohort',return_value={'sha256':DIGEST}),patch('cohort_proof_audit.lineage',return_value=[LEAF]):
            return audit(c,self.state,self.root)
    def test_exact_segment_and_body_match_but_release_is_not_authorized(self):
        result=self.check(Client());self.assertEqual(result['segments'],1);self.assertIn('no release authorized',result['scope'])
        self.assertTrue(self.body.exists());self.assertTrue(self.descriptor.exists())
    def test_corrupt_body_or_missing_job_fails_closed(self):
        self.body.write_bytes(b'xxxxxx')
        with self.assertRaises(ValueError):self.check(Client())
        self.body.write_bytes(BODY);c=Client();c.missing_job=True
        with self.assertRaises(ValueError):self.check(c)
    def test_conflicting_descriptor_or_table_identity_fails(self):
        d=json.loads(self.descriptor.read_text());d['source']['locator']='other';self.descriptor.write_text(json.dumps(d))
        with self.assertRaises(ValueError):self.check(Client())
        self.state['restored_tables']['sync_refs']='other'
        with self.assertRaises(ValueError):self.check(Client())


if __name__=='__main__':unittest.main()
