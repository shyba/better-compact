"""Single-authority raw-segment storage; receipts are committed last.

A segment is an immutable upload occurrence, NOT a deduplicated event. Raw
chunks share content by SHA256; references retain positions and rewrites.
FINAL is required for logical reads until background deduplication completes.
"""
import hashlib
import json
import re
import threading

from connection import connect
from raw_coverage import verify_replacement, verify_lineage, CoverageMismatch, CoverageUnknown, _validate_item
from generation_ledger import commit_generation, lineage

CHUNK_BYTES = 256*1024
IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z_0-9]*$')


class Conflict(ValueError):
    pass


def segment_identity(source_id, file_id):
    return hashlib.sha256((source_id+'\0'+file_id).encode()).hexdigest()


def metadata(source_id, file_id, path, start, full, source_kind, locator, size, sha256):
    if not re.fullmatch(r'[a-f0-9]{16,64}',source_id):
        raise ValueError('invalid source id')
    if not isinstance(file_id,str) or not 1<=len(file_id)<=300:
        raise ValueError('invalid file id')
    if not isinstance(path,str) or not 1<=len(path)<=1024:
        raise ValueError('invalid path')
    if type(start) is not int or start<0 or type(size) is not int or size<0 or start+size>2**53-1:
        raise ValueError('invalid source byte range')
    if type(full) is not bool or (full and start!=0):
        raise ValueError('replacement must start at zero')
    if not re.fullmatch(r'[a-f0-9]{64}',sha256):
        raise ValueError('invalid payload checksum')
    if not all(isinstance(x,str) and 1<=len(x)<=1024 for x in (source_kind,locator)):
        raise ValueError('source kind and locator required')
    return dict(source_id=source_id,file_id=file_id,path=path,start=start,full=full,
                source_kind=source_kind,locator=locator,size=size,sha256=sha256)


class Store:
    def __init__(self, blob_table='sync_chunks', factory=connect):
        if not IDENTIFIER.fullmatch(blob_table):
            raise ValueError('invalid blob table identifier')
        self.blobs = '`'+blob_table+'`'
        self.factory = factory
        self.writer = threading.Lock()

    def schema(self):
        c=self.factory()
        try:
            c.execute(f'''CREATE TABLE IF NOT EXISTS {self.blobs} (
                hash FixedString(64) CODEC(ZSTD(3)),raw_size UInt32 CODEC(ZSTD(3)),
                data String CODEC(ZSTD(3))) ENGINE=ReplacingMergeTree ORDER BY hash''')
            c.execute('''CREATE TABLE IF NOT EXISTS sync_refs (
                segment_id FixedString(64),ordinal UInt32 CODEC(Delta,ZSTD(3)),
                hash FixedString(64),raw_size UInt32) ENGINE=ReplacingMergeTree
                ORDER BY (segment_id,ordinal)''')
            c.execute('''CREATE TABLE IF NOT EXISTS sync_segments (
                source_id String, segment_id FixedString(64),file_id String,path String,
                start UInt64,size UInt64,full UInt8,sha256 FixedString(64),metadata String CODEC(ZSTD(3)),
                committed_at DateTime64(3) DEFAULT now64(3)) ENGINE=ReplacingMergeTree
                ORDER BY (source_id,segment_id)''')
            c.execute('''CREATE TABLE IF NOT EXISTS sync_authority (
                singleton UInt8,authority_id String) ENGINE=ReplacingMergeTree ORDER BY singleton''')
            c.execute('''CREATE TABLE IF NOT EXISTS sync_generations (
                source_id String,file_id String,path String,metadata String CODEC(ZSTD(3)),
                committed_at DateTime64(3) DEFAULT now64(3)) ENGINE=ReplacingMergeTree
                ORDER BY (source_id,file_id)''')
        finally:c.disconnect()

    def authority(self):
        c=self.factory()
        try:
            rows=c.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
            if len(rows)!=1:
                raise RuntimeError('administrator must provision exactly one persistent sync authority ID')
            return rows[0][0]
        finally:c.disconnect()

    def commit(self, meta, stream):
        key=segment_identity(meta['source_id'],meta['file_id'])
        encoded=json.dumps(meta,sort_keys=True,separators=(',',':'))
        # One receiver writer. Multiple service instances require an explicit
        # coordination strategy; CH keys alone are not atomic uniqueness locks.
        with self.writer:
            c=self.factory()
            try:
                existing=c.execute('''SELECT metadata FROM sync_segments FINAL
                    WHERE source_id=%(source)s AND segment_id=%(id)s''',
                    {'source':meta['source_id'],'id':key})
                if existing and (len(existing)!=1 or existing[0][0]!=encoded):
                    raise Conflict('immutable upload identity reused for different bytes/metadata')
                # A restored database can contain a receipt but be missing a
                # referenced row or chunk. Replay the verified payload and
                # repair missing payloads/references before acknowledging it;
                # receipt presence alone is not proof of recoverable bytes.
                ordinal=offset=0
                pending={}
                refs=[]
                buffered=0
                def flush():
                    nonlocal buffered
                    if not refs:return
                    found=dict(c.execute(f'SELECT hash,raw_size FROM {self.blobs} FINAL WHERE hash IN %(hashes)s',
                                         {'hashes':tuple(pending)}))
                    missing=[]
                    for sha,data in pending.items():
                        if sha in found and found[sha]!=len(data):raise Conflict('chunk size conflict')
                        if sha not in found:missing.append((sha,len(data),data))
                    if missing:c.execute(f'INSERT INTO {self.blobs} VALUES',missing)
                    c.execute('INSERT INTO sync_refs VALUES',refs)
                    pending.clear();refs.clear();buffered=0
                while offset<meta['size']:
                    size=min(CHUNK_BYTES-((meta['start']+offset)%CHUNK_BYTES),meta['size']-offset)
                    data=stream.read(size)
                    if len(data)!=size:
                        raise ValueError('spooled segment truncated')
                    sha=hashlib.sha256(data).hexdigest()
                    if sha not in pending:
                        pending[sha]=data
                        buffered+=size
                    refs.append((key,ordinal,sha,size))
                    ordinal+=1
                    offset+=size
                    if buffered>=8*1024*1024 or len(refs)>=1024:flush()
                flush()
                # Receipt last. Native synchronous inserts acknowledge server
                # commit; actual persistence/replication policy is a deployment
                # contract, not implied fsync/replica quorum by this code.
                if not existing:
                    c.execute('''INSERT INTO sync_segments
                        (source_id,segment_id,file_id,path,start,size,full,sha256,metadata) VALUES''',
                        [(meta['source_id'],key,meta['file_id'],meta['path'],meta['start'],meta['size'],int(meta['full']),meta['sha256'],encoded)])
                return dict(store='clickhouse',committed=True,segment_id=key,**meta)
            finally:c.disconnect()

    def generation_status(self, source, file_id):
        if not isinstance(source,str) or not re.fullmatch(r'[a-f0-9]{16,64}',source):
            raise ValueError('invalid source id')
        if not isinstance(file_id,str) or not 1<=len(file_id)<=260:
            raise ValueError('invalid generation id')
        c=self.factory()
        try:
            rows=c.execute('''SELECT metadata FROM sync_generations FINAL
                WHERE source_id=%(source)s AND file_id=%(file)s''',{'source':source,'file':file_id})
            if len(rows)>1:raise CoverageMismatch('ambiguous generation receipt')
            return json.loads(rows[0][0]) if rows else None
        finally:c.disconnect()

    def commit_generation(self, item):
        # Share the segment writer's lock; a separate receiver instance still
        # requires external coordination and must not share this namespace.
        with self.writer:
            c=self.factory()
            try:return commit_generation(c,self.blobs,item)
            finally:c.disconnect()

    def verify_raw(self, item):
        c=self.factory()
        try:
            try:
                _validate_item(item)
                rows=c.execute('''SELECT metadata FROM sync_generations FINAL
                    WHERE source_id=%(source)s AND file_id=%(file)s''',
                    {'source':item.get('source_id'),'file':item.get('file_id')})
                if len(rows)>1:raise CoverageMismatch('ambiguous generation receipt')
                if rows:
                    generation=json.loads(rows[0][0])
                    if any(generation.get(key)!=item.get(key) for key in ('source_id','file_id','path','size','sha256')):
                        raise CoverageMismatch('requested file does not match committed generation')
                    return verify_lineage(c,self.blobs,item,lineage(c,generation))
                return verify_replacement(c,self.blobs,item)
            except CoverageUnknown as exc:
                return {'status':'unknown','reason':str(exc)}
            except CoverageMismatch as exc:
                return {'status':'mismatch','reason':str(exc)}
        finally:c.disconnect()

    def inventory(self, source):
        if not re.fullmatch(r'[a-f0-9]{16,64}',source):
            raise ValueError('invalid source id')
        c=self.factory()
        try:
            rows=c.execute('''SELECT file_id,path,start,size,full,sha256,segment_id
                FROM sync_segments FINAL WHERE source_id=%(source)s''',{'source':source})
            return [dict(zip(('file_id','path','start','size','full','sha256','segment_id'),row)) for row in rows]
        finally:c.disconnect()
