import hashlib
import importlib.util
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest

from shadow_projection import open_shadow, project_shadow
from projection_records import PoisonRecord
from raw_coverage import CoverageMismatch

SOURCE='a'*32
AUTHORITY='shadow-authority-00001'
spec=importlib.util.spec_from_file_location('codex_vcc_shadow_projection_test',Path(__file__).resolve().parents[4]/'codex-vcc.py')
compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)

def sha(value):return hashlib.sha256(value).hexdigest()


class Fixture:
    def __init__(self,pieces):
        self.refs={};self.payloads={};self.rows={};self.ledger={};self.generations=[];self.meta={};self.source_kind='pi-jsonl'
        data=b''
        for index,piece in enumerate(pieces):
            start=len(data);data+=piece;file='v'+str(index);segment=file+'.'+str(start)
            segment_id=sha((SOURCE+'\0'+segment).encode());key=sha(piece)
            self.rows[file+'.']=[(segment,'session.jsonl',start,len(piece),index==0,sha(piece),segment_id)]
            self.meta[segment_id]={'source_id':SOURCE,'file_id':segment,'path':'session.jsonl','locator':'/test'}
            self.refs[segment_id]=[(0,key,len(piece))] if piece else []
            if piece:self.payloads[key]=piece
            version={'source_id':SOURCE,'file_id':file,'path':'session.jsonl','start':start,
                     'size':len(data),'sha256':sha(data),'parent':None if index==0 else 'v'+str(index-1)}
            self.generations.append(version)
            self.ledger[(SOURCE,file)]=json.dumps(version,sort_keys=True,separators=(',',':'))
    def execute(self,sql,params=None,settings=None):
        if 'SELECT metadata FROM sync_generations' in sql:
            value=self.ledger.get((params['source'],params['file']))
            return [(value,)] if value else []
        if 'SELECT metadata FROM sync_segments' in sql:
            item=self.meta.get(params['id'])
            return [(json.dumps({**item,'source_kind':self.source_kind}),)] if item else []
        if 'FROM sync_segments' in sql:return self.rows.get(params['prefix'],[])
        if 'FROM sync_refs' in sql:return self.refs[params['id']]
        if 'FROM `chunks`' in sql:return [(k.encode(),len(self.payloads[k]),self.payloads[k]) for k in params['hashes'] if k in self.payloads]
        raise AssertionError(sql)


class ShadowProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)
        self.db=open_shadow(self.path/'shadow.sqlite')
    def tearDown(self):self.db.close();self.temp.cleanup()
    def project(self,fixture,index):
        return project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[index],self.db,self.path/'spool')
    def test_append_preserves_distinct_equal_occurrences_and_partial_tail(self):
        first=b'{"type":"a"}\n{"type":"a"}\n{"type":'
        fixture=Fixture((first,b'"b"}\n'))
        root=self.project(fixture,0)
        self.assertEqual(root['records_inserted'],2);self.assertGreater(root['trailing_bytes'],0)
        self.assertEqual(self.project(fixture,0)['records_inserted'],0)
        appended=self.project(fixture,1)
        self.assertEqual(appended['records_inserted'],1);self.assertEqual(appended['trailing_bytes'],0)
        rows=self.db.execute('SELECT byte_offset,event_id,raw_sha256 FROM shadow_event ORDER BY byte_offset').fetchall()
        self.assertEqual(len(rows),3);self.assertEqual(rows[0][2],rows[1][2]);self.assertNotEqual(rows[0][1],rows[1][1])
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_generation').fetchone()[0],2)
    def test_vcc_shadow_compiles_and_replays_without_collapsing_equal_messages(self):
        msg=b'{"type":"message","message":{"role":"user","content":[{"text":"same"}]}}\n'
        fixture=Fixture((b'{"type":"session","id":"s"}\n'+msg+msg,))
        def compile_once():
            return project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[0],self.db,self.path/'spool',compiler,'pi-jsonl')
        first=compile_once();self.assertEqual(first['shadow_vcc_blocks'],3)
        users=self.db.execute("SELECT event_id,record_offset,content FROM shadow_vcc_block WHERE kind='user' ORDER BY sequence").fetchall()
        self.assertEqual(len(users),2);self.assertEqual(users[0][2],users[1][2]);self.assertNotEqual(users[0][0],users[1][0])
        self.assertEqual(compile_once()['records_inserted'],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_vcc_block').fetchone()[0],3)
    def test_vcc_shadow_ambiguous_ordinals_roll_back_occurrences(self):
        fixture=Fixture((b'{"type":"event_msg","ordinal":1}\n{"type":"event_msg","ordinal":1}\n',));fixture.source_kind='codex-jsonl'
        with self.assertRaisesRegex(ValueError,'duplicate Codex ordinal'):
            project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[0],self.db,self.path/'spool',compiler,'codex-jsonl')
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_vcc_generation').fetchone()[0],0)
    def test_vcc_shadow_refuses_source_locator_change_in_lineage(self):
        fixture=Fixture((b'{"type":"session","id":"s"}\n',b'{"type":"message"}\n'))
        segment_id=fixture.rows['v1.'][0][-1];fixture.meta[segment_id]['locator']='/changed'
        with self.assertRaisesRegex(ValueError,'metadata changed'):
            project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[1],self.db,self.path/'spool',compiler)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
    def test_vcc_shadow_refuses_wrong_source_dialect(self):
        fixture=Fixture((b'{"type":"session","id":"s"}\n',))
        with self.assertRaisesRegex(ValueError,'source kind mismatch'):
            project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[0],self.db,self.path/'spool',compiler,'codex-jsonl')
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
    def test_vcc_shadow_rejects_oversized_before_reading_payload(self):
        fixture=Fixture((b'{}\n',));fixture.generations[0]['size']=9*1024*1024
        with self.assertRaisesRegex(ValueError,'bounded shadow compiler'):
            project_shadow(fixture,'`chunks`',AUTHORITY,fixture.generations[0],self.db,self.path/'spool',compiler,'pi-jsonl')
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
    def test_poison_record_rolls_back_whole_shadow_generation(self):
        fixture=Fixture((b'{"type":"a"}\nnot-json\n',))
        with self.assertRaises(PoisonRecord):self.project(fixture,0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_generation').fetchone()[0],0)
    def test_existing_generation_identity_conflict_fails_closed(self):
        fixture=Fixture((b'{"type":"a"}\n',));self.project(fixture,0)
        self.db.execute("UPDATE shadow_generation SET sha256='wrong'");self.db.commit()
        with self.assertRaisesRegex(ValueError,'generation identity conflict'):self.project(fixture,0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT sha256 FROM shadow_generation').fetchone()[0],'wrong')
    def test_corrupt_payload_never_commits_shadow_events(self):
        fixture=Fixture((b'{"type":"a"}\n',));key=next(iter(fixture.payloads))
        fixture.payloads[key]=b'x'*len(fixture.payloads[key])
        with self.assertRaises(CoverageMismatch):self.project(fixture,0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],0)
    def test_refuses_unrecognized_existing_database(self):
        path=self.path/'unrelated.sqlite'
        other=sqlite3.connect(path);other.execute('CREATE TABLE important(value TEXT)');other.execute("INSERT INTO important VALUES ('kept')");other.commit();other.close()
        with self.assertRaisesRegex(ValueError,'unrecognized'):open_shadow(path)
        other=sqlite3.connect(path)
        try:self.assertEqual(other.execute('SELECT value FROM important').fetchone()[0],'kept')
        finally:other.close()
    def test_authority_namespaces_same_source_and_generation(self):
        fixture=Fixture((b'{"type":"a"}\n',));self.project(fixture,0)
        result=project_shadow(fixture,'`chunks`','another-authority-0001',fixture.generations[0],self.db,self.path/'spool')
        self.assertEqual(result['records_inserted'],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM shadow_event').fetchone()[0],2)


if __name__=='__main__':unittest.main()
