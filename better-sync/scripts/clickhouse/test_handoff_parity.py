import tempfile
from pathlib import Path
import unittest

from handoff_parity import compare_shadow
from shadow_projection import open_shadow

AUTH='shadow-authority-00001';SOURCE='a'*32;LEAF='leaf'


class Cursor:
    def __init__(self,pg,name=None):self.pg=pg;self.name=name;self.itersize=None;self.rows=[]
    def __enter__(self):return self
    def __exit__(self,*_):pass
    def execute(self,sql,params):
        self.pg.calls.append((sql,params,self.name))
        if 'FROM vcc.session' in sql:self.rows=[(x,) for x in self.pg.sessions]
        elif 'FROM vcc.block' in sql:
            self.rows=[(*row,len(row[2].encode()),len(row[3].encode()) if row[3] is not None else 0) for row in self.pg.blocks]
        else:raise AssertionError(sql)
    def fetchall(self):return self.rows
    def __iter__(self):return iter(self.rows)


class PG:
    def __init__(self,blocks,sessions=('session',)):self.blocks=blocks;self.sessions=sessions;self.calls=[]
    def cursor(self,name=None):return Cursor(self,name)


class HandoffParityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=open_shadow(Path(self.temp.name)/'shadow.sqlite')
        self.db.execute('INSERT INTO shadow_generation VALUES (?,?,?,?,?,?,?,?,?,?)',
                        (AUTH,SOURCE,'root',LEAF,'session.jsonl',2,'f'*64,2,0,'0'*64))
        self.db.execute('INSERT INTO shadow_vcc_generation VALUES (?,?,?,?,?)',(AUTH,SOURCE,LEAF,'pi-jsonl',2))
        for seq in range(2):
            self.db.execute('INSERT INTO shadow_vcc_block VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                            (AUTH,SOURCE,LEAF,seq,'event'+str(seq),seq,0,'user',1,'same','same','null'))
        self.db.commit()
    def tearDown(self):self.db.close();self.temp.cleanup()
    def compare(self,pg):return compare_shadow(pg,self.db,AUTH,SOURCE,LEAF)
    def test_exact_parity_preserves_distinct_duplicate_blocks(self):
        pg=PG([('user',1,'same','same'),('user',1,'same','same')]);r=self.compare(pg)
        self.assertEqual(r['status'],'matched');self.assertEqual(r['shadow_blocks'],2)
        self.assertIn('excludes',r['scope'])
        self.assertTrue(all(sql.lstrip().upper().startswith('SELECT') for sql,_args,_name in pg.calls))
        self.assertEqual(pg.calls[-1][2],'better_sync_shadow_parity')
    def test_legacy_dedup_or_content_change_is_mismatch(self):
        for rows in ([('user',1,'same','same')],[('user',1,'same','same'),('user',1,'other','same')]):
            self.assertEqual(self.compare(PG(rows))['status'],'mismatch')
    def test_incomplete_tail_or_ambiguous_session_is_unknown(self):
        self.db.execute('UPDATE shadow_generation SET trailing_size=1');self.db.commit()
        self.assertEqual(self.compare(PG([]))['status'],'unknown')
        self.db.execute('UPDATE shadow_generation SET trailing_size=0');self.db.commit()
        self.assertEqual(self.compare(PG([],sessions=('a','b')))['status'],'unknown')
        self.assertEqual(self.compare(PG([],sessions=()))['status'],'unknown')
    def test_shadow_receipt_count_conflict_is_not_matched(self):
        self.db.execute('UPDATE shadow_vcc_generation SET block_count=3');self.db.commit()
        self.assertEqual(self.compare(PG([('user',1,'same','same')]*2))['status'],'unknown')


if __name__=='__main__':unittest.main()
