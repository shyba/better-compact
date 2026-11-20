import hashlib
import unittest

from raw_coverage import verify_replacement, verify_lineage, CoverageMismatch, CoverageUnknown

SOURCE='a'*32

def sha(data):return hashlib.sha256(data).hexdigest()


class Fixture:
    def __init__(self, pieces=(b'abcd',b'efgh')):
        self.rows=[];self.refs={};self.payloads={};start=0
        for i,data in enumerate(pieces):
            file_id='version.'+str(start)
            segment_id=sha((SOURCE+'\0'+file_id).encode())
            self.rows.append((file_id,'session.jsonl',start,len(data),i==0,sha(data),segment_id))
            key=sha(data)
            self.refs[segment_id]=[(0,key,len(data))] if data else []
            self.payloads[key]=data
            start+=len(data)
        self.item={'source_id':SOURCE,'file_id':'version','path':'session.jsonl',
                   'size':start,'sha256':sha(b''.join(pieces))}
    def execute(self,sql,params=None,settings=None):
        if 'FROM sync_segments' in sql:return self.rows
        if 'FROM sync_refs' in sql:return self.refs[params['id']]
        if 'FROM `chunks`' in sql:
            assert 'FINAL PREWHERE hash IN' in sql
            assert settings['max_block_size']==32 and settings['max_threads']==1
            return [(key.encode(),len(self.payloads[key]),self.payloads[key])
                    for key in params['hashes'] if key in self.payloads]
        raise AssertionError(sql)
    def verify(self):return verify_replacement(self,'`chunks`',self.item)


class CoverageTests(unittest.TestCase):
    def test_exact_full_reconstruction(self):
        fixture=Fixture()
        self.assertEqual(fixture.verify()['status'],'verified')
    def test_empty_file(self):
        self.assertEqual(Fixture((b'',)).verify()['status'],'verified')
    def test_missing_segment_and_overlap_fail(self):
        fixture=Fixture();fixture.rows.pop(0)
        with self.assertRaises(CoverageUnknown):fixture.verify()
        fixture=Fixture();fixture.rows.append(fixture.rows[-1])
        with self.assertRaises(CoverageMismatch):fixture.verify()
        fixture=Fixture();fixture.rows.pop()
        with self.assertRaises(CoverageMismatch):fixture.verify()
    def test_append_alone_is_not_a_verified_whole_file(self):
        fixture=Fixture();fixture.rows=fixture.rows[1:]
        with self.assertRaisesRegex(CoverageUnknown,'append lineage'):fixture.verify()
    def test_missing_or_corrupt_chunk_fails(self):
        fixture=Fixture();fixture.payloads.clear()
        with self.assertRaises(CoverageMismatch):fixture.verify()
        fixture=Fixture();key=next(iter(fixture.payloads));fixture.payloads[key]=b'xxxx'
        with self.assertRaises(CoverageMismatch):fixture.verify()
    def test_wrong_source_path_or_checksum_fails(self):
        for field,value in [('path','other'),('sha256','0'*64)]:
            fixture=Fixture();fixture.item[field]=value
            with self.assertRaises(CoverageMismatch):fixture.verify()
    def test_bad_reference_order_fails(self):
        fixture=Fixture();seg=fixture.rows[0][-1];ref=fixture.refs[seg][0]
        fixture.refs[seg]=[(1,ref[1],ref[2])]
        with self.assertRaises(CoverageMismatch):fixture.verify()
    def test_unbounded_file_is_unknown(self):
        fixture=Fixture();fixture.item['size']=2**40
        with self.assertRaises(CoverageUnknown):fixture.verify()


class LineageFixture:
    def __init__(self):
        self.refs={};self.payloads={};self.rows={};self.versions=[]
        data=b''
        for index,piece in enumerate((b'abcd',b'abcd',b'ef')):
            start=len(data);data+=piece
            name='v'+str(index);seg=name+'.'+str(start)
            segment_id=sha((SOURCE+'\0'+seg).encode());key=sha(piece)
            self.rows[name+'.']=[(seg,'session.jsonl',start,len(piece),index==0,sha(piece),segment_id)]
            self.refs[segment_id]=[(0,key,len(piece))];self.payloads[key]=piece
            self.versions.append({'file_id':name,'parent':None if index==0 else 'v'+str(index-1),
                                  'start':start,'size':len(data),'sha256':sha(data)})
        self.item={'source_id':SOURCE,'file_id':'v2','path':'session.jsonl','size':len(data),'sha256':sha(data)}
    def execute(self,sql,params=None,settings=None):
        if 'FROM sync_segments' in sql:return self.rows.get(params['prefix'],[])
        if 'FROM sync_refs' in sql:return self.refs[params['id']]
        if 'FROM `chunks`' in sql:return [(k.encode(),len(self.payloads[k]),self.payloads[k]) for k in params['hashes'] if k in self.payloads]
        raise AssertionError(sql)
    def verify(self):return verify_lineage(self,'`chunks`',self.item,self.versions)


class LineageTests(unittest.TestCase):
    def test_explicit_append_chain_reconstructs_repeated_payload_occurrences(self):
        f=LineageFixture();self.assertEqual(f.verify()['status'],'verified')
        self.assertEqual(len(f.payloads),2) # identical bytes share a chunk, not an occurrence
        self.assertEqual(len(f.versions),3)
    def test_missing_wrong_or_cyclic_parent_fails(self):
        for mutation in ('missing','wrong','cycle'):
            f=LineageFixture()
            if mutation=='missing':f.versions.pop(1)
            elif mutation=='wrong':f.versions[1]['parent']='v2'
            else:f.versions[2]['file_id']='v0'
            with self.assertRaises((CoverageUnknown,CoverageMismatch)):f.verify()
    def test_intermediate_checksum_and_range_must_match(self):
        for field,value in (('sha256','0'*64),('start',5),('size',9)):
            f=LineageFixture();f.versions[1][field]=value
            with self.assertRaises(CoverageMismatch):f.verify()
    def test_absent_segment_and_corrupt_shared_chunk_fail(self):
        f=LineageFixture();f.rows['v1.']=[]
        with self.assertRaises(CoverageUnknown):f.verify()
        f=LineageFixture();f.payloads[sha(b'abcd')]=b'xxxx'
        with self.assertRaises(CoverageMismatch):f.verify()
    def test_leaf_must_be_requested_version(self):
        f=LineageFixture();f.item['file_id']='wrong'
        with self.assertRaises(CoverageMismatch):f.verify()


if __name__=='__main__':unittest.main()
