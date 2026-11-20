#!/usr/bin/env python3
"""Isolated occurrence-index pilot, not the production VCC/search projection.

Reads a committed ClickHouse generation lineage into bounded temporary staging,
verifies every raw byte before writing a local SQLite shadow index, and commits
complete JSONL records with source/root-generation/byte-offset identity. It
never updates PostgreSQL VCC/search, source checkpoints, or the sync outbox.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile

from connection import connect
from generation_ledger import lineage, validate_generation
from projection_records import complete_records
from raw_coverage import verify_lineage
from shadow_vcc import compile_vcc

MAX_SHADOW_COMPILE_BYTES=8*1024*1024
MAX_SHADOW_EVENTS=50000
MAX_SHADOW_BLOCKS=100000

IDENTIFIER=re.compile(r'^[A-Za-z_][A-Za-z_0-9]*$')


def open_shadow(path):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    created=False
    try:
        fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        os.close(fd);created=True
    except FileExistsError:
        if not stat.S_ISREG(path.lstat().st_mode):raise ValueError('shadow path is not a regular file')
    db=sqlite3.connect(path,timeout=5)
    if not created:
        try:
            marker=db.execute("SELECT value FROM shadow_meta WHERE key='format'").fetchone()
            if marker!=('better-sync-shadow-v1',):raise ValueError('not a better-sync shadow database')
        except (sqlite3.Error,ValueError):
            db.close()
            raise ValueError('refusing to alter an unrecognized SQLite database') from None
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''CREATE TABLE IF NOT EXISTS shadow_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        INSERT OR IGNORE INTO shadow_meta(key,value) VALUES ('format','better-sync-shadow-v1');
        CREATE TABLE IF NOT EXISTS shadow_event (
        authority TEXT NOT NULL, source_id TEXT NOT NULL, root_id TEXT NOT NULL,
        byte_offset INTEGER NOT NULL, event_id TEXT NOT NULL, raw_sha256 TEXT NOT NULL,
        raw_size INTEGER NOT NULL, record_type TEXT NOT NULL,
        PRIMARY KEY(authority,source_id,root_id,byte_offset), UNIQUE(event_id));
        CREATE TABLE IF NOT EXISTS shadow_generation (
        authority TEXT NOT NULL,source_id TEXT NOT NULL,root_id TEXT NOT NULL,leaf_id TEXT NOT NULL,
        path TEXT NOT NULL,size INTEGER NOT NULL,sha256 TEXT NOT NULL,complete_offset INTEGER NOT NULL,
        trailing_size INTEGER NOT NULL,trailing_sha256 TEXT NOT NULL,
        PRIMARY KEY(authority,source_id,leaf_id));
        CREATE TABLE IF NOT EXISTS shadow_vcc_block (
        authority TEXT NOT NULL,source_id TEXT NOT NULL,leaf_id TEXT NOT NULL,sequence INTEGER NOT NULL,
        event_id TEXT NOT NULL,record_offset INTEGER NOT NULL,block_index INTEGER NOT NULL,
        kind TEXT NOT NULL,window_no INTEGER NOT NULL,content TEXT NOT NULL,brief TEXT NOT NULL,ts_json TEXT NOT NULL,
        PRIMARY KEY(authority,source_id,leaf_id,sequence));
        CREATE TABLE IF NOT EXISTS shadow_vcc_generation (
        authority TEXT NOT NULL,source_id TEXT NOT NULL,leaf_id TEXT NOT NULL,source_kind TEXT NOT NULL,
        block_count INTEGER NOT NULL,PRIMARY KEY(authority,source_id,leaf_id));''')
    return db


def lineage_source_kind(client, chain):
    """Derive the compiler dialect from each committed version's first segment."""
    identity=None
    for version in chain:
        file_id=version['file_id']+'.'+str(version['start'])
        segment_id=hashlib.sha256((version['source_id']+'\0'+file_id).encode()).hexdigest()
        rows=client.execute('''SELECT metadata FROM sync_segments FINAL
            WHERE source_id=%(source)s AND segment_id=%(id)s''',
            {'source':version['source_id'],'id':segment_id})
        if len(rows)!=1:raise ValueError('shadow source metadata unavailable or ambiguous')
        meta=json.loads(rows[0][0])
        if meta.get('source_id')!=version['source_id'] or meta.get('file_id')!=file_id or meta.get('path')!=version['path']:
            raise ValueError('shadow source metadata identity mismatch')
        current=(meta.get('source_kind'),meta.get('locator'))
        if not all(isinstance(value,str) and value for value in current):raise ValueError('shadow source metadata incomplete')
        if identity is not None and current!=identity:raise ValueError('shadow source metadata changed within lineage')
        identity=current
    return identity[0]


def project_shadow(client, blobs, authority, generation, db, spool_dir, compiler=None, source_kind=None):
    """Verify lineage and atomically index occurrences and optional VCC shadow."""
    if not isinstance(authority,str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,128}',authority):
        raise ValueError('invalid authority')
    validate_generation(generation)
    if compiler is not None and generation['size']>MAX_SHADOW_COMPILE_BYTES:
        raise ValueError('generation exceeds bounded shadow compiler input')
    chain=lineage(client,generation)
    if compiler is not None:
        actual_kind=lineage_source_kind(client,chain)
        if source_kind is not None and source_kind!=actual_kind:raise ValueError('shadow source kind mismatch')
        source_kind=actual_kind
    root=chain[0]['file_id']
    spool_dir=Path(spool_dir)
    spool_dir.mkdir(parents=True,exist_ok=True,mode=0o700)
    with tempfile.TemporaryFile(dir=spool_dir) as spool:
        # A sink is temporary until every segment and intermediate hash passes.
        verify_lineage(client,blobs,generation,chain,on_chunk=spool.write)
        spool.seek(0)
        db.execute('BEGIN IMMEDIATE')
        try:
            pending=b'';offset=0;inserted=0;seen=0;compile_events=[]
            while True:
                chunk=spool.read(64*1024)
                if not chunk:break
                records,pending,offset=complete_records(generation['source_id'],authority+'\0'+root,offset,pending+chunk)
                for record in records:
                    if compiler is not None:
                        if len(compile_events)>=MAX_SHADOW_EVENTS:raise ValueError('too many shadow compiler events')
                        compile_events.append(record)
                    digest=hashlib.sha256(record['raw']).hexdigest()
                    key=(authority,generation['source_id'],root,record['offset'])
                    existing=db.execute('''SELECT event_id,raw_sha256,raw_size FROM shadow_event
                        WHERE authority=? AND source_id=? AND root_id=? AND byte_offset=?''',key).fetchone()
                    expected=(record['id'],digest,record['size'])
                    if existing and existing!=expected:
                        raise ValueError('shadow occurrence identity conflict')
                    if not existing:
                        kind=record['value'].get('type')
                        db.execute('''INSERT INTO shadow_event(authority,source_id,root_id,byte_offset,event_id,
                            raw_sha256,raw_size,record_type) VALUES (?,?,?,?,?,?,?,?)''',
                            (*key,record['id'],digest,record['size'],kind if isinstance(kind,str) else ''))
                        inserted+=1
                    seen+=1
            if offset+len(pending)!=generation['size']:
                raise ValueError('shadow byte accounting mismatch')
            block_count=None
            if compiler is not None:
                blocks,_stats=compile_vcc(compiler,source_kind,generation['path'],compile_events)
                if len(blocks)>MAX_SHADOW_BLOCKS:raise ValueError('too many shadow compiler blocks')
                block_rows=[(authority,generation['source_id'],generation['file_id'],b['sequence'],b['event_id'],
                             b['record_offset'],b['block_index'],b['kind'],b['window'],b['text'],b['brief'],
                             json.dumps(b['timestamp'],ensure_ascii=True,separators=(',',':'))) for b in blocks]
                old=db.execute('''SELECT authority,source_id,leaf_id,sequence,event_id,record_offset,block_index,
                    kind,window_no,content,brief,ts_json FROM shadow_vcc_block
                    WHERE authority=? AND source_id=? AND leaf_id=? ORDER BY sequence''',
                    (authority,generation['source_id'],generation['file_id'])).fetchall()
                marker=db.execute('''SELECT source_kind,block_count FROM shadow_vcc_generation
                    WHERE authority=? AND source_id=? AND leaf_id=?''',
                    (authority,generation['source_id'],generation['file_id'])).fetchone()
                if marker:
                    if marker!=(source_kind,len(block_rows)) or old!=block_rows:
                        raise ValueError('shadow VCC output changed for immutable generation')
                else:
                    if old:raise ValueError('shadow VCC blocks exist without a generation marker')
                    db.executemany('''INSERT INTO shadow_vcc_block(authority,source_id,leaf_id,sequence,event_id,
                        record_offset,block_index,kind,window_no,content,brief,ts_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',block_rows)
                    db.execute('''INSERT INTO shadow_vcc_generation(authority,source_id,leaf_id,source_kind,block_count)
                        VALUES (?,?,?,?,?)''',(authority,generation['source_id'],generation['file_id'],source_kind,len(block_rows)))
                block_count=len(block_rows)
            value=(root,generation['path'],generation['size'],generation['sha256'],offset,
                   len(pending),hashlib.sha256(pending).hexdigest())
            existing=db.execute('''SELECT root_id,path,size,sha256,complete_offset,trailing_size,trailing_sha256
                FROM shadow_generation WHERE authority=? AND source_id=? AND leaf_id=?''',
                (authority,generation['source_id'],generation['file_id'])).fetchone()
            if existing and existing!=value:raise ValueError('shadow generation identity conflict')
            if not existing:
                db.execute('''INSERT INTO shadow_generation(authority,source_id,root_id,leaf_id,path,size,sha256,
                    complete_offset,trailing_size,trailing_sha256) VALUES (?,?,?,?,?,?,?,?,?,?)''',
                    (authority,generation['source_id'],root,generation['file_id'],generation['path'],
                     generation['size'],generation['sha256'],offset,len(pending),hashlib.sha256(pending).hexdigest()))
            db.commit()
        except BaseException:
            db.rollback()
            raise
    return {'scope':'isolated shadow occurrence/VCC index only; production VCC/search not updated',
            'source_id':generation['source_id'],'root_id':root,'leaf_id':generation['file_id'],
            'records_seen':seen,'records_inserted':inserted,'complete_offset':offset,'trailing_bytes':len(pending),
            'shadow_vcc_blocks':block_count}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True)
    parser.add_argument('--generation',required=True)
    parser.add_argument('--shadow-db',type=Path,required=True)
    parser.add_argument('--spool-dir',type=Path,required=True)
    parser.add_argument('--blob-table',default='archive_migration_v1_chunks')
    parser.add_argument('--shadow-vcc',action='store_true',help='also compile into the isolated shadow database')
    args=parser.parse_args()
    if not IDENTIFIER.fullmatch(args.blob_table):raise ValueError('invalid blob table')
    compiler=None
    if args.shadow_vcc:
        home=Path(os.environ['VCC_HOME']) if os.environ.get('VCC_HOME') else Path(__file__).resolve().parents[4]
        spec=importlib.util.spec_from_file_location('shadow_codex_vcc',home/'codex-vcc.py')
        if spec is None or spec.loader is None:raise ValueError('VCC compiler unavailable')
        compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)
    client=connect()
    try:
        authority_rows=client.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
        if len(authority_rows)!=1:raise ValueError('authority unavailable or ambiguous')
        rows=client.execute('''SELECT metadata FROM sync_generations FINAL
            WHERE source_id=%(source)s AND file_id=%(file)s''',{'source':args.source,'file':args.generation})
        if len(rows)!=1:raise ValueError('generation unavailable or ambiguous')
        generation=json.loads(rows[0][0])
        if generation.get('source_id')!=args.source or generation.get('file_id')!=args.generation:
            raise ValueError('generation identity mismatch')
        db=open_shadow(args.shadow_db)
        try:print(json.dumps(project_shadow(client,'`'+args.blob_table+'`',authority_rows[0][0],generation,db,args.spool_dir,compiler)))
        finally:db.close()
    finally:client.disconnect()


if __name__=='__main__':main()
