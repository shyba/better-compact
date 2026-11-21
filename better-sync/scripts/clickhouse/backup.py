#!/usr/bin/env python3
"""Local single-flight scheduler for native compressed-part S3 backups.

Does not export JSON or maintain ingestion receipts. No automatic retention:
base backups must remain available while any retained incremental depends on
those bases. A backup/restore acceptance test is required before cutover.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import random
import time
import uuid

from connection import connect
from urllib.parse import urlsplit, urlunsplit


def repository_url(value):
    value=value if '://' in value else 'http://'+value
    parsed=urlsplit(value)
    if parsed.scheme not in ('http','https') or not parsed.hostname or not parsed.path.strip('/'):
        raise ValueError('backup URL must include endpoint, bucket and prefix')
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError('backup URL must not embed credentials/query/fragment')
    return urlunsplit(parsed).rstrip('/')


def save(path, state):
    temporary=path.with_suffix('.tmp')
    with temporary.open('w') as f:
        os.chmod(temporary,0o600)
        json.dump(state,f,indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary,path)
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)


def backup_sql(database, incremental):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',database):
        raise ValueError('invalid database')
    sql=f'BACKUP DATABASE `{database}` TO S3(%(destination)s,%(access)s,%(secret)s)'
    if incremental:
        sql+=' SETTINGS base_backup=S3(%(base)s,%(access)s,%(secret)s), id=%(id)s'
    else:
        sql+=' SETTINGS id=%(id)s'
    return sql+' ASYNC'


class BackupFailed(RuntimeError):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(f'native backup/restore ended with {status}, code {code}; inspect protected server logs')


def retryable_backup_failure(error):
    # Retry only a confirmed terminal operation. Never retry unknown/running
    # job outcomes or reuse their destination prefixes.
    return isinstance(error, BackupFailed) and error.status=='BACKUP_FAILED' and error.code==598


def poll(client, job_id, observed=None):
    rows=client.execute('SELECT status,error FROM system.backups WHERE id=%(id)s',{'id':job_id})
    if len(rows)!=1:
        raise RuntimeError('backup job cannot be located; refusing overlapping backup')
    status,error=rows[0]
    if observed is not None:
        observed(status)
    if status in ('BACKUP_FAILED','RESTORE_FAILED','BACKUP_CANCELLED','RESTORE_CANCELLED'):
        match=re.search(r'Code:\s*(\d+)',error)
        raise BackupFailed(status,int(match.group(1)) if match else None)
    return status


def assert_no_server_jobs(client):
    # The local lock cannot outlive a killed process. A foreign or orphaned
    # native job may still be running even when our scheduler state is idle.
    rows=client.execute("SELECT count() FROM system.backups WHERE status IN ('CREATING_BACKUP','RESTORING')")
    if len(rows)!=1 or len(rows[0])!=1 or type(rows[0][0]) is not int or rows[0][0]!=0:
        raise RuntimeError('native backup or restore may still be active; refusing overlap')


def run(root, full=False, acknowledge_restart=False):
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (root/'lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state_path=root/'state.json'
        state=json.loads(state_path.read_text()) if state_path.exists() else {'successful':[]}
        client=connect()
        try:
            # Recover a known in-flight job before attempting any new backup.
            # Persist the chosen ID before issuing SQL. If SQL acknowledgement
            # is lost, never guess that the backup failed and overlap it.
            if state.get('active'):
                previous=state['active']
                known=client.execute('SELECT status FROM system.backups WHERE id=%(id)s',{'id':previous['id']})
                if not known and acknowledge_restart:
                    server_started=client.execute('SELECT toUnixTimestamp(now())-uptime()')[0][0]
                    running=client.execute("SELECT count() FROM system.backups WHERE status IN ('CREATING_BACKUP','RESTORING')")[0][0]
                    if previous['started_at']>=server_started-5 or running:
                        raise RuntimeError('cannot prove previous backup was interrupted by server restart')
                    state.setdefault('failed',[]).append({**previous,
                        'terminal_status':'SERVER_RESTARTED_OUTCOME_UNKNOWN',
                        'repository_retained':True,'server_started_at':server_started})
                    state.pop('active')
                    save(state_path,state)
                if known and known[0][0] in ('BACKUP_FAILED','BACKUP_CANCELLED'):
                    state.setdefault('failed',[]).append({**previous,'terminal_status':known[0][0]})
                    state.pop('active')
                    save(state_path,state)
                # Only a server-confirmed terminal job may be retired. Unknown
                # or running jobs remain active and cannot be overlapped.
            if state.get('active'):
                job=state['active']
            else:
                assert_no_server_jobs(client)
                repo=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
                if not repo.startswith('https://') and os.environ.get('CLICKHOUSE_BACKUP_ALLOW_HTTP')!='1':
                    raise ValueError('backup repository requires HTTPS or explicit private-network HTTP approval')
                now=time.time()
                previous=state['successful'][-1] if state['successful'] else None
                new_full=full or previous is None or now-previous['full_started_at']>=7*86400
                job_id=str(uuid.uuid4())
                job={'id':job_id,'destination':repo+'/'+job_id+'/', 'started_at':now,
                     'base':None if new_full else previous['destination'],
                     'full_started_at':now if new_full else previous['full_started_at']}
                state['active']=job
                save(state_path,state)
                params={'destination':job['destination'],'base':job['base'],'id':job_id,
                        'access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY']}
                try:
                    client.execute(backup_sql(os.environ['CLICKHOUSE_DATABASE'],bool(job['base'])),params,
                                   settings={'max_execution_time':0,'max_threads':1,'backup_threads':1,
                                             'max_memory_usage':268435456,
                                             's3_use_adaptive_timeouts':False,
                                             's3_request_timeout_ms':120000,
                                             's3_connect_timeout_ms':5000,
                                             's3_max_inflight_parts_for_one_file':1,
                                             's3_min_upload_part_size':8388608,
                                             's3_max_upload_part_size':16777216,
                                             's3_max_single_part_upload_size':16777216,
                                             'max_backup_bandwidth':16777216})
                except Exception:
                    # Unknown submission outcome is left active deliberately.
                    raise RuntimeError('backup submission failed or acknowledgement uncertain; inspect job ID before clearing active state') from None
            def observed(status):
                state['last_status']=status
                state['heartbeat_at']=time.time()
                save(state_path,state)
            while True:
                try:
                    status=poll(client,job['id'],observed)
                except BackupFailed:
                    raise
                except Exception as exc:
                    # Poll failures (Code 202 contention, dropped connections
                    # during the weekly full's own S3 traffic) do not affect
                    # the server-side job: note liveness, wait, resume.
                    state['heartbeat_at']=time.time()
                    save(state_path,state)
                    print(json.dumps({'poll_retry':type(exc).__name__,'detail':str(exc)[:140]}),flush=True)
                    time.sleep(30);continue
                if status=='BACKUP_CREATED':
                    job['finished_at']=time.time()
                    state['successful'].append(job)
                    state.pop('active')
                    save(state_path,state)
                    print(json.dumps({'backup_id':job['id'],'status':status,'incremental':bool(job['base'])}),flush=True)
                    return
                time.sleep(10)
        finally:client.disconnect()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir',type=Path,required=True)
    p.add_argument('--full',action='store_true')
    p.add_argument('--acknowledge-server-restart',action='store_true',
                   help='retire an unknown job only after proving a newer server epoch and no running backups; retain its repository')
    args=p.parse_args()
    for attempt in range(3):
        try:
            return run(args.state_dir,args.full,args.acknowledge_server_restart)
        except BackupFailed as exc:
            if not retryable_backup_failure(exc) or attempt==2:
                raise
            # run() confirmed failure and released its lock. The next run
            # rechecks the terminal state and archives it before choosing a
            # fresh UUID/prefix and the last SUCCESSFUL base.
            print(json.dumps({'retrying_terminal_backup':True,'code':exc.code,
                              'attempt':attempt+1,'failed_prefix_retained':True}),flush=True)
            time.sleep(10*(attempt+1)+random.uniform(0,2))


if __name__=='__main__':main()
