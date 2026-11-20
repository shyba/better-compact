import json
from pathlib import Path
import tempfile
import unittest

from backup import BackupFailed
from restore_lineage_drill import Namespace,submit


class RestoreDrillTests(unittest.TestCase):
    def test_namespace_maps_only_exact_table_identifiers(self):
        class Client:
            def execute(self,sql,params=None,settings=None):return sql,params,settings
        p=Namespace(Client(),{'sync_segments':'isolated_segments','sync_refs':'isolated_refs'})
        sql,_,_=p.execute('SELECT * FROM sync_segments JOIN sync_refs USING(segment_id) WHERE name=sync_segments_extra')
        self.assertIn('`isolated_segments`',sql);self.assertIn('`isolated_refs`',sql)
        self.assertIn('sync_segments_extra',sql)
    def test_job_identity_is_durable_before_submission(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'state.json';state={'jobs':[],'active':None}
            class Client:
                def execute(self,sql,params=None,settings=None):
                    if 'count()' in sql:return [(0,)]
                    if sql.startswith('BACKUP'):
                        saved=json.loads(path.read_text())
                        assert saved['active']['id']==params['id']
                        return []
                    if 'SELECT status,error' in sql:return [('BACKUP_CREATED','')]
                    raise AssertionError(sql)
            submit(Client(),state,path,'BACKUP example',{},'full','BACKUP_CREATED')
            self.assertNotIn('active',state);self.assertEqual(state['jobs'][0]['terminal_status'],'BACKUP_CREATED')
    def test_terminal_failure_is_recorded_without_clearing_job(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'state.json';state={'jobs':[],'active':None}
            class Client:
                def execute(self,sql,params=None,settings=None):
                    if 'count()' in sql:return [(0,)]
                    if sql.startswith('BACKUP'):return []
                    if 'SELECT status,error' in sql:return [('BACKUP_FAILED','Code: 598. private destination')]
                    raise AssertionError(sql)
            with self.assertRaises(BackupFailed):submit(Client(),state,path,'BACKUP example',{},'full','BACKUP_CREATED')
            active=json.loads(path.read_text())['active']
            self.assertEqual(active['observed_status'],'BACKUP_FAILED');self.assertEqual(active['error_code'],598)
            self.assertNotIn('private destination',json.dumps(active))
    def test_unknown_submission_preserves_active_identity(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'state.json';state={'jobs':[],'active':None}
            class Client:
                def execute(self,sql,params=None,settings=None):
                    if 'count()' in sql:return [(0,)]
                    raise OSError('lost acknowledgement')
            with self.assertRaisesRegex(RuntimeError,'acknowledgement uncertain'):submit(Client(),state,path,'BACKUP example',{},'full','BACKUP_CREATED')
            self.assertEqual(json.loads(path.read_text())['active']['id'],state['active']['id'])
            self.assertEqual(state['jobs'],[])


if __name__=='__main__':unittest.main()
