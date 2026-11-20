#!/usr/bin/env python3
"""Restore only tiny sync metadata tables from an existing production backup.

This is a readback experiment, not backup-inclusion proof: chunk payloads are
not restored, no outbox bytes are released, and production-chain recoverability
and full-restore capacity remain unknown. All scratch tables and prefixes stay.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import time
import uuid

from backup import assert_no_server_jobs, repository_url, save
from connection import connect
from generation_ledger import lineage
from restore_lineage_drill import Namespace, submit
from raw_coverage import CHUNK_BYTES
from store import segment_identity


def inspect_metadata(client,names,intent):
    restored=Namespace(client,names)
    auth=restored.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
    if auth!=[(intent['authority'],)]:raise ValueError('restored authority mismatch')
    rows=restored.execute('SELECT metadata FROM sync_generations FINAL WHERE source_id=%(source)s AND file_id=%(file)s',
                          {'source':intent['source_id'],'file':intent['file_id']})
    if len(rows)!=1:raise ValueError('requested generation not present in restored metadata')
    leaf=json.loads(rows[0][0])
    for key in ('source_id','file_id','path','start','size','sha256','parent'):
        if leaf.get(key)!=intent.get(key):raise ValueError('restored generation differs from retained intent')
    versions=lineage(restored,leaf);offset=segments=references=0
    for vi,version in enumerate(versions):
        if version['start']!=offset or (vi>0 and version['size']<=offset):raise ValueError('restored generation lineage range mismatch')
        rows=restored.execute('''SELECT file_id,path,start,size,full,sha256,segment_id FROM sync_segments FINAL
            WHERE source_id=%(source)s AND startsWith(file_id,%(prefix)s) ORDER BY start LIMIT 4097''',
            {'source':intent['source_id'],'prefix':version['file_id']+'.'})
        if not rows or len(rows)>4096 or segments+len(rows)>4096:raise ValueError('restored segment count invalid')
        for index,(file,path,start,size,full,digest,segment) in enumerate(rows):
            if (file!=version['file_id']+'.'+str(start) or path!=intent['path'] or start!=offset or
                segment!=segment_identity(intent['source_id'],file) or full not in (0,1) or bool(full)!=(vi==0 and index==0) or
                not re.fullmatch(r'[a-f0-9]{64}',digest) or size<0 or size>64*1024*1024 or start+size>version['size'] or
                (size==0 and (version['size']!=0 or len(rows)!=1 or vi!=0))):
                raise ValueError('restored segment identity or range mismatch')
            refs=restored.execute('''SELECT ordinal,hash,raw_size FROM sync_refs FINAL
                WHERE segment_id=%(id)s ORDER BY ordinal LIMIT 258''',{'id':segment})
            if len(refs)>257:raise ValueError('restored segment has too many references')
            within=0
            for ordinal,(actual,key,n) in enumerate(refs):
                expected=min(CHUNK_BYTES-((start+within)%CHUNK_BYTES),size-within)
                if actual!=ordinal or expected<=0 or n!=expected or not re.fullmatch(r'[a-f0-9]{64}',key):
                    raise ValueError('restored reference identity or range mismatch')
                within+=n
            if within!=size:raise ValueError('restored reference coverage incomplete')
            offset+=size;segments+=1;references+=len(refs)
        if offset!=version['size']:raise ValueError('restored generation range incomplete')
    return {'versions':len(versions),'segments':segments,'references':references,'size':offset,
            'scope':'production-backup metadata only; chunk payloads and full restore not verified; never release outbox'}


def run(root,backup_state,intent_path):
    if not root.is_dir() or root.stat().st_mode & 0o077:raise ValueError('private existing root required')
    path=root/'state.json'
    if path.exists():raise ValueError('existing evidence must not be overwritten')
    intent=json.loads(intent_path.read_text())
    if not intent.get('committed_at'):raise ValueError('committed retained intent required')
    db=os.environ['CLICKHOUSE_DATABASE']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',db):raise ValueError('invalid database')
    repo=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
    if not repo.startswith('https://') and os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1':raise ValueError('private HTTP not approved')
    with (backup_state/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        scheduler=json.loads((backup_state/'state.json').read_text())
        if scheduler.get('active') or not scheduler.get('successful'):raise ValueError('backup state unavailable or active')
        backup=scheduler['successful'][-1]
        if not backup['destination'].startswith(repo+'/') or intent['committed_at']>=backup['started_at']*1000:
            raise ValueError('backup selection or intent timing not eligible for readback')
        client=connect()
        try:
            assert_no_server_jobs(client)
            # Bound the actual current physical rows, not just logical FINAL
            # rows. This is a conservative proxy, not a disk-space guarantee.
            for table in ('sync_authority','sync_generations','sync_segments','sync_refs'):
                count=client.execute(f'SELECT count() FROM {table}',settings={'max_threads':1,'max_execution_time':5})[0][0]
                if count>10000:raise ValueError('metadata table exceeds tiny-restore limit')
            tag='sync_prod_metadata_drill_'+uuid.uuid4().hex
            names={key:tag+'_'+key[5:] for key in ('sync_authority','sync_generations','sync_segments','sync_refs')}
            state={'format':1,'phase':'restore','started_at':time.time(),'jobs':[],'active':None,
                   'backup_id':backup['id'],'destination':backup['destination'],'tables':names,
                   'intent':{k:intent.get(k) for k in ('authority','source_id','file_id','path','start','size','sha256','parent')},
                   'scope':'metadata-only; not backup-inclusion proof'}
            save(path,state)
            tables=', '.join(f'TABLE `{db}`.`{key}` AS `{db}`.`{name}`' for key,name in names.items())
            params={'dest':backup['destination'],'access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY']}
            submit(client,state,path,f'RESTORE {tables} FROM S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'restore','RESTORED')
            evidence=inspect_metadata(client,names,intent)
            state.update(phase='verified_metadata_only',finished_at=time.time(),evidence=evidence);save(path,state)
            print(json.dumps({'verified_metadata':True,**evidence}),flush=True)
        finally:client.disconnect()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True,type=Path)
    p.add_argument('--backup-state',required=True,type=Path);p.add_argument('--intent',required=True,type=Path)
    a=p.parse_args();run(a.root,a.backup_state,a.intent)


if __name__=='__main__':main()
