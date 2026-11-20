#!/usr/bin/env python3
"""Continuous isolated ClickHouse → local VCC shadow, never production VCC.

Scans committed generations by stable key in bounded pages and revisits the
whole keyspace each cycle. A failed generation remains explicitly pending with
bounded retry backoff; no source/outbox/production checkpoint is advanced.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import time

from connection import connect
from shadow_projection import open_shadow, project_shadow

PAGE_SIZE=64
MAX_SHADOW_BYTES=512*1024*1024  # 2026-11-16: codex pilot added ~480 committed files; the 64 MiB budget hit backpressure within hours. Host disk is plentiful.


class ReusableClient:
    """Avoid repeated TLS allocation; discard connections after any error."""
    def __init__(self,factory):self.factory=factory;self.client=None
    def get(self):
        if self.client is None:self.client=self.factory()
        return self.client
    def invalidate(self):
        client,self.client=self.client,None
        if client is not None:
            try:client.disconnect()
            except Exception:pass  # cleanup must not hide the original failure


def schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS shadow_worker_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),source_cursor TEXT NOT NULL,file_cursor TEXT NOT NULL,cycles INTEGER NOT NULL);
        INSERT OR IGNORE INTO shadow_worker_state VALUES (1,'','',0);
        CREATE TABLE IF NOT EXISTS shadow_verified_meta (
        authority TEXT NOT NULL,source_id TEXT NOT NULL,file_id TEXT NOT NULL,metadata_sha256 TEXT NOT NULL,
        PRIMARY KEY(authority,source_id,file_id));
        CREATE TABLE IF NOT EXISTS shadow_failure (
        authority TEXT NOT NULL,source_id TEXT NOT NULL,file_id TEXT NOT NULL,metadata_sha256 TEXT NOT NULL,
        attempts INTEGER NOT NULL,next_attempt REAL NOT NULL,error_class TEXT NOT NULL,
        PRIMARY KEY(authority,source_id,file_id));''')


def storage_bytes(root):
    # Keep prior compiler-version shadows and account for every SQLite file,
    # WAL and shared-memory sidecar. This is a backpressure proxy, not a quota.
    return sum(path.stat().st_size for path in Path(root).glob('*.sqlite*') if path.is_file())


def code_fingerprint(paths):
    digest=hashlib.sha256()
    for path in paths:
        path=Path(path)
        digest.update(path.name.encode()+b'\0')
        with path.open('rb') as stream:
            while chunk:=stream.read(256*1024):digest.update(chunk)
        digest.update(b'\0')
    return digest.hexdigest()


def page(client,db,authority,blobs,spool,compiler,db_path,now=None):
    """One bounded page; failures never become successful shadow receipts."""
    now=time.time() if now is None else now
    source_cursor,file_cursor,cycles=db.execute('SELECT source_cursor,file_cursor,cycles FROM shadow_worker_state WHERE singleton=1').fetchone()
    rows=client.execute('''SELECT source_id,file_id,metadata FROM sync_generations FINAL
        WHERE (source_id,file_id) > (%(source)s,%(file)s)
        ORDER BY source_id,file_id LIMIT 64''',{'source':source_cursor,'file':file_cursor})
    if not rows:
        db.execute("UPDATE shadow_worker_state SET source_cursor='',file_cursor='',cycles=cycles+1 WHERE singleton=1")
        db.commit()
        return {'phase':'cycle_complete','processed':0,'failed':0,'skipped':0,'cycles':cycles+1}
    result={'phase':'scanning','processed':0,'failed':0,'skipped':0,'cycles':cycles}
    for source,file_id,raw in rows:
        if storage_bytes(Path(db_path).parent)>=MAX_SHADOW_BYTES:
            result['phase']='backpressure'
            break
        fingerprint=hashlib.sha256(raw.encode()).hexdigest()
        try:
            generation=json.loads(raw)
            if generation.get('source_id')!=source or generation.get('file_id')!=file_id:
                raise ValueError('generation identity mismatch')
            existing=db.execute('''SELECT path,size,sha256 FROM shadow_generation
                WHERE authority=? AND source_id=? AND leaf_id=?''',(authority,source,file_id)).fetchone()
            compiled=db.execute('''SELECT 1 FROM shadow_vcc_generation
                WHERE authority=? AND source_id=? AND leaf_id=?''',(authority,source,file_id)).fetchone()
            marker=db.execute('''SELECT metadata_sha256 FROM shadow_verified_meta
                WHERE authority=? AND source_id=? AND file_id=?''',(authority,source,file_id)).fetchone()
            if existing and existing!=(generation.get('path'),generation.get('size'),generation.get('sha256')):
                raise ValueError('shadow generation identity conflict')
            if marker and marker[0]!=fingerprint:
                raise ValueError('shadow generation immutable metadata changed')
            if existing and compiled and marker:
                result['skipped']+=1
            else:
                failure=db.execute('''SELECT metadata_sha256,attempts,next_attempt FROM shadow_failure
                    WHERE authority=? AND source_id=? AND file_id=?''',(authority,source,file_id)).fetchone()
                if failure and failure[0]==fingerprint and failure[2]>now:
                    result['skipped']+=1
                else:
                    project_shadow(client,blobs,authority,generation,db,spool,compiler)
                    db.execute('''INSERT INTO shadow_verified_meta(authority,source_id,file_id,metadata_sha256)
                        VALUES (?,?,?,?) ON CONFLICT(authority,source_id,file_id) DO NOTHING''',(authority,source,file_id,fingerprint))
                    db.execute('DELETE FROM shadow_failure WHERE authority=? AND source_id=? AND file_id=?',(authority,source,file_id))
                    db.commit();result['processed']+=1
        except Exception as exc:
            db.rollback()
            failure=db.execute('''SELECT metadata_sha256,attempts FROM shadow_failure
                WHERE authority=? AND source_id=? AND file_id=?''',(authority,source,file_id)).fetchone()
            attempts=1 if not failure or failure[0]!=fingerprint else min(failure[1]+1,20)
            delay=min(3600,30*(2**min(attempts-1,7)))
            db.execute('''INSERT INTO shadow_failure(authority,source_id,file_id,metadata_sha256,attempts,next_attempt,error_class)
                VALUES (?,?,?,?,?,?,?) ON CONFLICT(authority,source_id,file_id) DO UPDATE SET
                metadata_sha256=excluded.metadata_sha256,attempts=excluded.attempts,next_attempt=excluded.next_attempt,error_class=excluded.error_class''',
                (authority,source,file_id,fingerprint,attempts,now+delay,type(exc).__name__))
            db.commit();result['failed']+=1
        db.execute('UPDATE shadow_worker_state SET source_cursor=?,file_cursor=? WHERE singleton=1',(source,file_id))
        db.commit()
    return result


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    temp=path.with_suffix('.tmp')
    with temp.open('w') as f:
        os.chmod(temp,0o600);json.dump(value,f);f.flush();os.fsync(f.fileno())
    os.replace(temp,path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--blob-table',default='archive_migration_v1_chunks')
    p.add_argument('--interval',type=float,default=30)
    args=p.parse_args()
    if not args.blob_table.replace('_','a').isalnum() or args.blob_table[0].isdigit():raise ValueError('invalid blob table')
    if args.interval<1:raise ValueError('interval must be at least one second')
    home=Path(os.environ['VCC_HOME']) if os.environ.get('VCC_HOME') else Path(__file__).resolve().parents[4]
    spec=importlib.util.spec_from_file_location('shadow_codex_vcc',home/'codex-vcc.py')
    if spec is None or spec.loader is None:raise ValueError('VCC compiler unavailable')
    compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)
    code=[home/'codex-vcc.py']+[Path(__file__).with_name(name) for name in
        ('shadow_worker.py','shadow_projection.py','shadow_vcc.py','projection_records.py','raw_coverage.py','generation_ledger.py')]
    fingerprint=code_fingerprint(code)
    args.root.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (args.root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        # Never reuse compiler-derived output across code changes. Preserve the
        # older shadow and rebuild into a separately named local database.
        db_path=args.root/('shadow-'+fingerprint+'.sqlite');db=open_shadow(db_path);schema(db)
        stop=False
        def halt(*_):
            nonlocal stop
            stop=True
        signal.signal(signal.SIGTERM,halt);signal.signal(signal.SIGINT,halt)
        connection=ReusableClient(connect)
        try:
            while not stop:
                started=time.time()
                try:
                    client=connection.get()
                    authorities=client.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
                    if len(authorities)!=1:raise ValueError('authority unavailable or ambiguous')
                    authority=authorities[0][0]
                    result=page(client,db,authority,'`'+args.blob_table+'`',args.root/'spool',compiler,db_path)
                    result.update(heartbeat_at=time.time(),shadow_bytes=storage_bytes(args.root),
                        shadow_db=str(db_path),compiler_fingerprint=fingerprint,
                        generation_failures=db.execute('SELECT count(*) FROM shadow_failure WHERE authority=?',(authority,)).fetchone()[0],
                        shadow_generations=db.execute('SELECT count(*) FROM shadow_vcc_generation WHERE authority=?',(authority,)).fetchone()[0],
                        shadow_events=db.execute('SELECT count(*) FROM shadow_event WHERE authority=?',(authority,)).fetchone()[0],
                        scope='isolated shadow only; production VCC/search unchanged')
                except Exception as exc:
                    connection.invalidate()
                    result={'phase':'error','error_class':type(exc).__name__,'heartbeat_at':time.time(),
                            'scope':'isolated shadow only; production VCC/search unchanged'}
                save(args.root/'status.json',result)
                print(json.dumps(result),flush=True)
                deadline=time.monotonic()+args.interval
                while not stop and time.monotonic()<deadline:time.sleep(max(0,min(1,deadline-time.monotonic())))
        finally:
            connection.invalidate()
            db.close()


if __name__=='__main__':main()
