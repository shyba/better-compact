#!/usr/bin/env python3
"""Compare a verified, bounded historical full frame with legacy VCC read-only.

A match compares rendered fields only. A historical frame may not be the latest
source generation and this cannot authorize a handoff or prove backup coverage.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import re

from connection import connect
from handoff_parity import compare_legacy_blocks
from projection_records import complete_records
from shadow_vcc import compile_vcc

MAX_FRAME_BYTES=1024*1024
CHUNK_BYTES=256*1024


def read_frame(client,archive_id,frame_no):
    if not re.fullmatch(r'[a-f0-9]{64}',archive_id) or type(frame_no) is not int or frame_no<0:
        raise ValueError('invalid archive/frame identity')
    params={'archive':archive_id,'frame':frame_no}
    receipts=client.execute('''SELECT frames FROM archive_migration_v1_archives FINAL
        WHERE archive_id=%(archive)s LIMIT 2''',params)
    if len(receipts)!=1 or frame_no>=receipts[0][0]:raise ValueError('committed archive receipt missing or ambiguous')
    frames=client.execute('''SELECT header,raw_size,sha256,segments FROM archive_migration_v1_frames FINAL
        WHERE archive_id=%(archive)s AND frame_no=%(frame)s LIMIT 2''',params)
    if len(frames)!=1:raise ValueError('frame receipt missing or ambiguous')
    header,size,digest,segments=frames[0]
    if not 0<size<=MAX_FRAME_BYTES or segments!=(size+CHUNK_BYTES-1)//CHUNK_BYTES:
        raise ValueError('frame exceeds bounded full-frame limits')
    meta=json.loads(header)
    if (not isinstance(meta,dict) or meta.get('full') is not True
        or not (meta.get('start') is None or (type(meta['start']) is int and meta['start']==0))
        or type(meta.get('bytes')) is not int or meta['bytes']!=size or meta.get('sha256')!=digest
        or not re.fullmatch(r'[a-f0-9]{16,64}',meta.get('source_id') or '')
        or meta.get('source_kind') not in ('pi-jsonl','codex-jsonl','codex-jsonl-sessions')
        or not isinstance(meta.get('path'),str) or not meta['path']):
        raise ValueError('frame lacks explicit full-frame identity')
    refs=client.execute('''SELECT ordinal,frame_offset,hash,raw_size FROM archive_migration_v1_refs FINAL
        WHERE archive_id=%(archive)s AND frame_no=%(frame)s ORDER BY ordinal LIMIT 6''',params)
    if len(refs)!=segments:raise ValueError('frame reference count mismatch')
    chunks=client.execute('''SELECT hash,raw_size,data FROM archive_migration_v1_chunks FINAL
        PREWHERE hash IN %(hashes)s''',{'hashes':tuple(set(row[2] for row in refs))},
        settings={'strings_as_bytes':True,'max_block_size':32,'preferred_block_size_bytes':8388608,'max_threads':1})
    by_hash={key.decode('ascii'):(n,data) for key,n,data in chunks}
    body=bytearray();offset=0
    for index,(ordinal,start,key,n) in enumerate(refs):
        if ordinal!=index or start!=offset or n!=min(CHUNK_BYTES,size-offset):raise ValueError('frame reference order or size mismatch')
        value=by_hash.get(key)
        if value is None or value[0]!=n or len(value[1])!=n or hashlib.sha256(value[1]).hexdigest()!=key:
            raise ValueError('frame chunk missing or corrupt')
        body.extend(value[1]);offset+=n
    if offset!=size or hashlib.sha256(body).hexdigest()!=digest:raise ValueError('frame checksum mismatch')
    return meta,bytes(body)


def compare_frame(pg,client,compiler,archive_id,frame_no):
    meta,body=read_frame(client,archive_id,frame_no)
    events,tail,_=complete_records(meta['source_id'],archive_id+':'+str(frame_no),0,body)
    if tail or len(events)>50000:raise ValueError('frame has incomplete tail or too many records')
    blocks,_=compile_vcc(compiler,meta['source_kind'],meta['path'],events)
    if len(blocks)>100000:raise ValueError('frame exceeds bounded compiler block limit')
    expected=((b['kind'],b['window'],b['text'],b['brief']) for b in blocks)
    result=compare_legacy_blocks(pg,meta['source_id'],meta['path'],expected,len(blocks))
    result.update(input_scope='verified historical full frame only; not necessarily latest generation',
                  frame_bytes=len(body),frame_sha256=hashlib.sha256(body).hexdigest())
    return result


def main():
    import psycopg2
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-id',required=True)
    parser.add_argument('--frame',type=int,required=True)
    parser.add_argument('--compiler',default=os.path.join(os.path.dirname(__file__),'../../../../codex-vcc.py'))
    args=parser.parse_args()
    spec=importlib.util.spec_from_file_location('historical_vcc',args.compiler)
    compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)
    client=connect()
    try:
        pg=psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=int(os.environ.get('POSTGRES_PORT','5432')),
            dbname=os.environ['POSTGRES_DB'],user=os.environ['DB_READER_USER'],password=os.environ['DB_READER_PASSWORD'],
            connect_timeout=3,options='-c statement_timeout=5000 -c default_transaction_read_only=on')
        try:result=compare_frame(pg,client,compiler,args.archive_id,args.frame)
        finally:pg.close()
    finally:client.disconnect()
    print(json.dumps(result))
    return 0 if result['status']=='matched' else 1


if __name__=='__main__':raise SystemExit(main())
