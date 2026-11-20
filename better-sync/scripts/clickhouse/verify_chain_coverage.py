#!/usr/bin/env python3
"""Scheduled read-only check: which retained generations the production chain covers.

Restores only the four small sync metadata tables from the latest successful
native backup and compares every committed retained intent under the configured
roots. Reference/lineage presence is proven; chunk bytes, full restore capacity
and outbox release remain explicitly unknown and untouched.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit
import time

from connection import connect
from restore_production_metadata import inspect_metadata
from restore_lineage_drill import Namespace, submit
from backup import assert_no_server_jobs, repository_url, save


def retained_intents(roots):
    intents=[]
    for root in roots:
        # two layouts: pilot dirs (<dir>/outbox/<x>/*.generation.json) and the
        # production sender outbox (<authority>/*.generation.json)
        paths=list(Path(root).glob('*/outbox/*/*.generation.json'))
        paths+=list(Path(root).glob('*/*.generation.json'))
        paths+=list(Path(root).glob('*.generation.json'))
        for path in paths:
            try:value=json.loads(path.read_text())
            except (OSError,ValueError):continue
            if type(value.get('committed_at')) is int and value['committed_at']>0:
                intents.append({'intent_path':str(path),**{k:value.get(k) for k in
                    ('authority','source_id','file_id','path','start','size','sha256','parent','committed_at')}})
    return intents


def run(root,backup_state,outroots):
    if not root.is_dir() or root.stat().st_mode & 0o077:raise ValueError('private existing root required')
    path=root/'state.json'
    if path.exists():raise ValueError('verification root must be fresh per run')
    def acquire(lock):
        # The hourly native backup holds this lock for its whole run; colliding
        # runs must wait, not fail. Bounded: ~15 minutes, then fail for the watch.
        deadline=time.time()+900
        while True:
            try:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);return
            except BlockingIOError:
                if time.time()>=deadline:raise ValueError('backup-state lock busy for 15 minutes; hourly backup wedged?')
                time.sleep(30)
    with (backup_state/'lock').open('a') as lock:
        acquire(lock)
        scheduler=json.loads((backup_state/'state.json').read_text())
        if scheduler.get('active') or not scheduler.get('successful'):
            raise ValueError('backup state unavailable or has an active job')
        backup=scheduler['successful'][-1]
    backup_url=urlsplit(backup['destination'])
    repo_url=os.environ.get('CLICKHOUSE_BACKUP_S3_URL')
    if not repo_url:
        scheme='https' if os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1' else 'http'
        if backup_url.scheme not in (scheme,'https','http'):raise ValueError('unexpected backup destination scheme')
        repo_url=f"{backup_url.scheme}://{backup_url.netloc}{backup_url.path}".rstrip('/')
        repo_url=repo_url[:repo_url.rfind('/')]
    repo=repository_url(repo_url)
    if not repo.startswith('https://') and os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1':
        raise ValueError('private HTTP not approved')
    db=os.environ['CLICKHOUSE_DATABASE']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',db):raise ValueError('invalid database')
    with (backup_state/'lock').open('a') as lock:
        acquire(lock)
        if not backup['destination'].startswith(repo+'/'):raise ValueError('backup outside configured repository')
        client=connect()
        try:
            assert_no_server_jobs(client)
            intents=[i for i in retained_intents(outroots) if i['committed_at']<backup['started_at']*1000]
            intents.sort(key=lambda i:i['committed_at'])
            tag='sync_chain_coverage_'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'_'+os.urandom(4).hex()
            names={key:tag+'_'+key[5:] for key in ('sync_authority','sync_generations','sync_segments','sync_refs')}
            state={'format':1,'phase':'restore','started_at':time.time(),'jobs':[],'active':None,
                   'backup_id':backup['id'],'tables':names,'intents':intents,'results':[],
                   'scope':'chain metadata coverage only; chunk bytes and full restore unknown; release disabled'}
            save(path,state)
            tables=', '.join(f'TABLE `{db}`.`{key}` AS `{db}`.`{name}`' for key,name in names.items())
            params={'dest':backup['destination'],'access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY']}
            submit(client,state,path,f'RESTORE {tables} FROM S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC',params,'restore','RESTORED')
            covered=0
            for intent in intents:
                try:
                    inspect_metadata(client,names,intent)
                    state['results'].append({'file_id':intent['file_id'],'status':'chain_covered_references'});covered+=1
                except ValueError as exc:
                    state['results'].append({'file_id':intent.get('file_id'),'status':'not_covered','reason':str(exc)[:200]})
                save(path,state)
            state.update(phase='completed',finished_at=time.time(),covered=covered,total=len(intents),
                         chunk_bytes_status='unknown_pending_capacity_verified_full_restore')
            save(path,state)
            print(json.dumps({'covered_references':covered,'total':len(intents),'backup_id':backup['id'],
                              'chunk_bytes':state['chunk_bytes_status']}),flush=True)
            return 0 if covered==len(intents) and intents else 1
        finally:client.disconnect()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True);p.add_argument('--backup-state',type=Path,required=True)
    p.add_argument('--outbox-roots',nargs='+',type=Path,required=True)
    a=p.parse_args();raise SystemExit(run(a.root,a.backup_state,a.outbox_roots))


if __name__=='__main__':main()
