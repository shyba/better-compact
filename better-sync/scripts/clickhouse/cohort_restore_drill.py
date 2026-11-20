#!/usr/bin/env python3
"""Back up and restore one bounded retained generation as an isolated cohort.

The cohort is a native ClickHouse backup, not a second live sync authority.
This pilot preserves every scratch table and backup prefix and never releases
outbox data. It does not prove recovery of the main production backup chain.
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
from raw_coverage import verify_lineage
from restore_lineage_drill import Namespace,submit

MAX_BYTES=1024*1024
MAX_ITEMS=64
SETTINGS={'max_threads':1,'max_execution_time':30,'max_memory_usage':268435456,
          'max_block_size':32,'preferred_block_size_bytes':8388608}
KEYS=('sync_authority','sync_generations','sync_segments','sync_refs','sync_chunks')


def gather(client,intent):
    if not intent.get('committed_at') or not isinstance(intent.get('authority'),str):
        raise ValueError('retained committed intent required')
    if type(intent.get('size')) is not int or intent['size']>MAX_BYTES:
        raise ValueError('cohort exceeds bounded raw size')
    auth=client.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
    if auth!=[(intent['authority'],)]:raise ValueError('authority mismatch')
    rows=client.execute('SELECT metadata FROM sync_generations FINAL WHERE source_id=%(source)s AND file_id=%(file)s',
                        {'source':intent['source_id'],'file':intent['file_id']})
    if len(rows)!=1:raise ValueError('generation receipt unavailable or ambiguous')
    leaf=json.loads(rows[0][0])
    if any(leaf.get(k)!=intent.get(k) for k in ('source_id','file_id','path','start','size','sha256','parent')):
        raise ValueError('generation receipt differs from retained intent')
    versions=lineage(client,leaf)
    if len(versions)>MAX_ITEMS:raise ValueError('too many cohort generations')
    verified=verify_lineage(client,'`archive_migration_v1_chunks`',leaf,versions)
    segments=[];hashes=set()
    for version in versions:
        found=client.execute('''SELECT segment_id FROM sync_segments FINAL
            WHERE source_id=%(source)s AND startsWith(file_id,%(prefix)s) ORDER BY start LIMIT 65''',
            {'source':leaf['source_id'],'prefix':version['file_id']+'.'})
        segments.extend(row[0] for row in found)
        if len(segments)>MAX_ITEMS:raise ValueError('too many cohort segments')
    for segment in segments:
        refs=client.execute('SELECT hash FROM sync_refs FINAL WHERE segment_id=%(id)s ORDER BY ordinal LIMIT 65',{'id':segment})
        hashes.update(row[0] for row in refs)
        if len(hashes)>MAX_ITEMS or len(refs)>MAX_ITEMS:raise ValueError('too many cohort chunks or references')
    return leaf,versions,segments,sorted(hashes),verified


def copy_cohort(client,names,leaf,versions,segments,hashes):
    # Table identities come exclusively from a random internal tag. Source
    # values are parameters; FINAL preserves the existing logical semantics.
    client.execute(f'INSERT INTO `{names["sync_authority"]}` SELECT * FROM sync_authority FINAL WHERE singleton=1',settings=SETTINGS)
    client.execute(f'''INSERT INTO `{names['sync_generations']}` SELECT * FROM sync_generations FINAL
        WHERE source_id=%(source)s AND file_id IN %(files)s''',
        {'source':leaf['source_id'],'files':tuple(v['file_id'] for v in versions)},settings=SETTINGS)
    client.execute(f'''INSERT INTO `{names['sync_segments']}` SELECT * FROM sync_segments FINAL
        WHERE source_id=%(source)s AND segment_id IN %(segments)s''',
        {'source':leaf['source_id'],'segments':tuple(segments)},settings=SETTINGS)
    client.execute(f'''INSERT INTO `{names['sync_refs']}` SELECT * FROM sync_refs FINAL
        WHERE segment_id IN %(segments)s''',{'segments':tuple(segments)},settings=SETTINGS)
    if hashes:
        client.execute(f'''INSERT INTO `{names['sync_chunks']}` SELECT * FROM archive_migration_v1_chunks FINAL
            PREWHERE hash IN %(hashes)s''',{'hashes':tuple(hashes)},settings=SETTINGS)


def verify_cohort(client,names,leaf,authority):
    scoped=Namespace(client,{k:v for k,v in names.items() if k!='sync_chunks'})
    if scoped.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')!=[(authority,)]:
        raise ValueError('cohort authority mismatch')
    rows=scoped.execute('SELECT metadata FROM sync_generations FINAL WHERE source_id=%(source)s AND file_id=%(file)s',
                        {'source':leaf['source_id'],'file':leaf['file_id']})
    if len(rows)!=1 or json.loads(rows[0][0])!=leaf:raise ValueError('cohort leaf receipt mismatch')
    return verify_lineage(scoped,'`'+names['sync_chunks']+'`',leaf,lineage(scoped,leaf))


def run(root,backup_state,intent_path):
    if not root.is_dir() or root.stat().st_mode & 0o077:raise ValueError('private existing root required')
    path=root/'state.json'
    if path.exists():raise ValueError('existing cohort state must not be overwritten')
    intent=json.loads(intent_path.read_text());db=os.environ['CLICKHOUSE_DATABASE']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',db):raise ValueError('invalid database')
    repo=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
    if not repo.startswith('https://') and os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1':raise ValueError('private HTTP not approved')
    with (backup_state/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        scheduler=json.loads((backup_state/'state.json').read_text())
        if scheduler.get('active'):raise ValueError('scheduler has unresolved active backup')
        client=connect()
        try:
            assert_no_server_jobs(client)
            leaf,versions,segments,hashes,verified=gather(client,intent)
            tag='sync_cohort_drill_'+uuid.uuid4().hex
            names={key:tag+'_'+key[5:] for key in KEYS}
            restored={key:value+'_restored' for key,value in names.items()}
            state={'format':1,'phase':'creating','started_at':time.time(),'active':None,'jobs':[],
                   'tables':names,'restored_tables':restored,'created_tables':[],'repository':repo+'/'+tag,
                   'authority':intent['authority'],'leaf':leaf,'scope':'bounded cohort pilot; outbox release disabled'}
            save(path,state)
            for key,name in names.items():
                original='archive_migration_v1_chunks' if key=='sync_chunks' else key
                client.execute(f'CREATE TABLE `{db}`.`{name}` AS `{db}`.`{original}`')
                state['created_tables'].append(name);save(path,state)
            copy_cohort(client,names,leaf,versions,segments,hashes)
            if verify_cohort(client,names,leaf,intent['authority'])['sha256']!=verified['sha256']:
                raise ValueError('copied cohort does not match live lineage')
            params={'dest':state['repository']+'/full/','access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY']}
            tables=', '.join(f'TABLE `{db}`.`{name}`' for name in names.values())
            state['phase']='backup';save(path,state)
            submit(client,state,path,f'BACKUP {tables} TO S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'backup','BACKUP_CREATED')
            tables=', '.join(f'TABLE `{db}`.`{names[k]}` AS `{db}`.`{restored[k]}`' for k in KEYS)
            state['phase']='restore';save(path,state)
            submit(client,state,path,f'RESTORE {tables} FROM S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'restore','RESTORED')
            result=verify_cohort(client,restored,leaf,intent['authority'])
            if result['sha256']!=leaf['sha256'] or result['size']!=leaf['size']:raise ValueError('restored cohort differs')
            evidence={'versions':len(versions),'segments':len(segments),'unique_chunks':len(hashes),
                      'restored_bytes':result['size'],'restored_sha256':result['sha256'],
                      'scope':'independently restored bounded cohort only; production-chain coverage and outbox release remain unverified'}
            state.update(phase='verified',finished_at=time.time(),evidence=evidence);save(path,state)
            print(json.dumps({'verified':True,**evidence}),flush=True)
        finally:client.disconnect()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',required=True,type=Path)
    p.add_argument('--backup-state',required=True,type=Path);p.add_argument('--intent',required=True,type=Path)
    a=p.parse_args();run(a.root,a.backup_state,a.intent)


if __name__=='__main__':main()
