#!/usr/bin/env python3
"""Isolated native full/incremental lineage restore drill; never releases outbox.

Uses tiny synthetic tables in the configured database. Preserves all tables,
backup prefixes, jobs and local evidence even on success. It does not establish
production-chain coverage or safe capacity for a production restore.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import time
import uuid

from backup import BackupFailed, assert_no_server_jobs, poll, repository_url, save
from connection import connect
from generation_ledger import lineage
from projection_records import complete_records
from raw_coverage import verify_lineage
from shadow_vcc import compile_vcc
from store import Store, metadata


class Namespace:
    """Remap only known internal table names to isolated, validated names."""
    def __init__(self,client,names):self.client=client;self.names=names
    def execute(self,sql,params=None,settings=None):
        for source,target in self.names.items():
            sql=re.sub(r'(?<![A-Za-z0-9_])'+re.escape(source)+r'(?![A-Za-z0-9_])',f'`{target}`',sql)
        return self.client.execute(sql,params,settings=settings)
    def disconnect(self):pass


def sha(data):return hashlib.sha256(data).hexdigest()


SOURCE='a'*32
SESSION_PATH='synthetic-session.jsonl'
FIRST=b'{"type":"session","id":"synthetic-restore-drill"}\n{"type":"message","message":{"role":"user","content":[{"text":"same"}]}}\n'
SECOND=b'{"type":"message","message":{"role":"user","content":[{"text":"same"}]}}\n{"type":"message","message":{"role":"assistant","content":[{"text":"later"}]}}\n'


def verify_restored(client,state,compiler_path):
    names=state['tables'];restored=state['restored_tables'];tag=state['tag']
    if not re.fullmatch(r'sync_lineage_drill_[a-f0-9]{32}',tag):raise ValueError('invalid drill tag')
    keys={'sync_segments','sync_refs','sync_generations','sync_authority','sync_chunks'}
    if set(names)!=keys or set(restored)!=keys:raise ValueError('invalid drill table map')
    for key in keys:
        if names[key]!=tag+'_'+key[5:] or restored[key]!=names[key]+'_restored':raise ValueError('invalid drill table identity')
    jobs=[j for j in state['jobs'] if j['kind']=='restore' and j['terminal_status']=='RESTORED']
    if len(jobs)!=1 or poll(client,jobs[0]['id'])!='RESTORED':raise ValueError('restore job cannot be independently confirmed')
    original=Namespace(client,names);recovered=Namespace(client,restored)
    authority=original.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
    got=recovered.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
    if len(authority)!=1 or got!=authority:raise ValueError('restored authority differs')
    leaf={'source_id':SOURCE,'file_id':'append','path':SESSION_PATH,'start':len(FIRST),
          'size':len(FIRST+SECOND),'sha256':sha(FIRST+SECOND),'parent':'root'}
    rows=recovered.execute('SELECT metadata FROM sync_generations FINAL WHERE source_id=%(source)s AND file_id=%(file)s',{'source':SOURCE,'file':'append'})
    if len(rows)!=1 or json.loads(rows[0][0])!=leaf:raise ValueError('restored generation differs')
    payload=bytearray();result=verify_lineage(recovered,'`'+restored['sync_chunks']+'`',leaf,lineage(recovered,leaf),on_chunk=payload.extend)
    if bytes(payload)!=FIRST+SECOND or result['status']!='verified':raise ValueError('restored bytes differ')
    events,tail,_=complete_records(SOURCE,'root',0,bytes(payload))
    if tail or len(events)!=4 or events[1]['id']==events[2]['id'] or events[1]['value']!=events[2]['value']:
        raise ValueError('restored occurrence identity mismatch')
    spec=importlib.util.spec_from_file_location('restore_drill_vcc',compiler_path)
    compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)
    blocks,_=compile_vcc(compiler,'pi-jsonl',SESSION_PATH,events)
    repeated=[block for block in blocks if block['event_id'] in (events[1]['id'],events[2]['id'])]
    if len(repeated)!=2 or repeated[0]['event_id']==repeated[1]['event_id'] or repeated[0]['text']!=repeated[1]['text']:
        raise ValueError('restored VCC projection mismatch')
    return {'restored_sha256':sha(payload),'restored_bytes':len(payload),'events':len(events),'blocks':len(blocks)}


def wait(client,state,path,job,expected,timeout=600):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            status=poll(client,job['id'])
        except BackupFailed as exc:
            state['active']['observed_status']=exc.status
            state['active']['error_code']=exc.code
            state['heartbeat_at']=time.time();save(path,state)
            raise
        state['active']['observed_status']=status
        state['heartbeat_at']=time.time();save(path,state)
        if status==expected:
            state['jobs'].append({**state.pop('active'),'terminal_status':status,'finished_at':time.time()})
            save(path,state);return
        time.sleep(2)
    raise TimeoutError('restore drill job outcome unknown; preserve state and repository')


def submit(client,state,path,sql,params,kind,expected):
    assert_no_server_jobs(client)
    job={'id':str(uuid.uuid4()),'kind':kind,'started_at':time.time()}
    state['active']=job;save(path,state)  # ID durable before SQL; never infer failure from lost ACK
    settings={'max_execution_time':0,'max_threads':1,'backup_threads':1,'max_memory_usage':268435456,
              's3_max_inflight_parts_for_one_file':1,'s3_min_upload_part_size':8388608,
              's3_max_upload_part_size':16777216,'s3_max_single_part_upload_size':16777216,
              'max_backup_bandwidth':16777216}
    try:
        client.execute(sql,{**params,'id':job['id']},settings=settings)
    except Exception:
        # Submission outcome is unknown; preserve the job ID and do not leak
        # parameterized repository credentials through server error messages.
        raise RuntimeError('drill submission failed or acknowledgement uncertain; preserve job and prefix') from None
    wait(client,state,path,job,expected)


def run(root,backup_state,compiler_path):
    if not root.is_dir():raise ValueError('drill root must exist and be private')
    if root.stat().st_mode & 0o077:raise ValueError('drill root must be private')
    state_path=root/'state.json'
    if state_path.exists():raise RuntimeError('existing drill evidence must never be overwritten or resubmitted')
    db=os.environ['CLICKHOUSE_DATABASE']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',db):raise ValueError('invalid database')
    repo=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
    if not repo.startswith('https://') and os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1':
        raise ValueError('backup repository requires HTTPS or explicit private-network HTTP approval')
    tag='sync_lineage_drill_'+uuid.uuid4().hex
    names={key:tag+'_'+key[5:] for key in ('sync_segments','sync_refs','sync_generations','sync_authority')}
    names['sync_chunks']=tag+'_chunks'
    restored={key:value+'_restored' for key,value in names.items()}
    state={'format':1,'scope':'synthetic isolated lineage drill; not production-chain proof',
           'tag':tag,'tables':names,'restored_tables':restored,'repository':repo+'/'+tag,
           'jobs':[],'created_tables':[],'active':None,'phase':'initializing','started_at':time.time()}
    # Coordinate with the existing local backup scheduler. If killed, the
    # scheduler's server-job check also prevents overlap with an active job.
    with (backup_state/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        scheduler=json.loads((backup_state/'state.json').read_text())
        if scheduler.get('active'):raise RuntimeError('scheduler has unresolved active backup')
        client=connect()
        try:
            assert_no_server_jobs(client);save(state_path,state)
            for key,name in names.items():
                original='archive_migration_v1_chunks' if key=='sync_chunks' else key
                client.execute(f'CREATE TABLE `{db}`.`{name}` AS `{db}`.`{original}`')
                state['created_tables'].append(name);save(state_path,state)
            actual=client.execute('SELECT authority_id FROM sync_authority FINAL WHERE singleton=1')
            if len(actual)!=1:raise ValueError('production authority is absent or ambiguous')
            authority=actual[0][0]
            client.execute(f'INSERT INTO `{names["sync_authority"]}` VALUES',[(1,authority)])
            isolated=Namespace(client,names)
            store=Store(blob_table=names['sync_chunks'],factory=lambda:isolated)
            source=SOURCE;path=SESSION_PATH;first=FIRST;second=SECOND
            def insert(version,data,start,parent,total):
                segment=metadata(source,version+'.'+str(start),path,start,start==0,'pi-jsonl','synthetic-drill',len(data),sha(data))
                store.commit(segment,io.BytesIO(data))
                item={'source_id':source,'file_id':version,'path':path,'start':start,'size':len(total),'sha256':sha(total),'parent':parent}
                store.commit_generation(item)
                return item
            root_generation=insert('root',first,0,None,first)
            del root_generation
            params={'access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY'],
                    'base':state['repository']+'/full/','dest':state['repository']+'/full/'}
            tables=', '.join(f'TABLE `{db}`.`{name}`' for name in names.values())
            state['phase']='full_backup';save(state_path,state)
            submit(client,state,state_path,f'BACKUP {tables} TO S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'full','BACKUP_CREATED')
            leaf=insert('append',second,len(first),'root',first+second)
            params['dest']=state['repository']+'/incremental/'
            state['phase']='incremental_backup';save(state_path,state)
            submit(client,state,state_path,f'BACKUP {tables} TO S3(%(dest)s,%(access)s,%(secret)s) SETTINGS base_backup=S3(%(base)s,%(access)s,%(secret)s), id=%(id)s ASYNC',params,'incremental','BACKUP_CREATED')
            restore_tables=', '.join(f'TABLE `{db}`.`{names[key]}` AS `{db}`.`{restored[key]}`' for key in names)
            state['phase']='restore';save(state_path,state)
            submit(client,state,state_path,f'RESTORE {restore_tables} FROM S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'restore','RESTORED')
            evidence=verify_restored(client,state,compiler_path)
            state.update(phase='verified',finished_at=time.time(),**evidence,
                         scope='synthetic isolated full/incremental restore only; excludes production-chain coverage and search')
            save(state_path,state)
            print(json.dumps({'verified':True,**evidence,'scope':state['scope']}),flush=True)
        finally:client.disconnect()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True);p.add_argument('--backup-state',type=Path,required=True)
    p.add_argument('--compiler',type=Path,required=True);p.add_argument('--verify-existing',action='store_true');args=p.parse_args()
    if not args.verify_existing:
        return run(args.root,args.backup_state,args.compiler)
    path=args.root/'state.json';state=json.loads(path.read_text())
    if state.get('active') or state.get('phase')!='restore':raise ValueError('drill is not awaiting restored readback')
    client=connect()
    try:evidence=verify_restored(client,state,args.compiler)
    finally:client.disconnect()
    state.update(phase='verified',finished_at=time.time(),**evidence,
                 scope='synthetic isolated full/incremental restore only; excludes production-chain coverage and search')
    save(path,state)
    print(json.dumps({'verified':True,**evidence,'scope':state['scope']}),flush=True)


if __name__=='__main__':main()
