#!/usr/bin/env python3
"""Read-only, bounded inventory of successful native backup prefixes.

Does not list historical archives, inspect/remove locks, delete objects, or alter
backup state. A failed/partial listing is explicitly incomplete and cannot be
used as retention evidence. Run under a supervised service with a time limit.
"""
import argparse
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

from minio import Minio
import urllib3
from connection import connect


def save(path, state):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    tmp=path.with_suffix('.tmp')
    with tmp.open('w') as f:
        os.chmod(tmp,0o600)
        json.dump(state,f,indent=2)
        f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    state=json.loads(args.state.read_text())
    jobs=state.get('successful',[])
    result={'phase':'running','started_at':time.time(),'total':len(jobs),'jobs':[],
            'failed_prefixes_not_listed':len(state.get('failed',[])),
            'active_at_start':bool(state.get('active')),'errors':[]}
    save(args.output,result)
    destinations={j['destination'] for j in jobs}
    if len(destinations)!=len(jobs):raise ValueError('duplicate destination in successful state')
    c=connect()
    try:
        rows=c.execute('SELECT id,status,compressed_size,uncompressed_size,total_size FROM system.backups WHERE id IN %(ids)s',
                       {'ids':tuple(j['id'] for j in jobs)}) if jobs else []
    finally:c.disconnect()
    server={row[0]:row[1:] for row in rows}
    for job in jobs:
        try:
            parsed=urlsplit(job['destination'])
            repo=urlsplit(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
            if parsed.scheme not in ('http','https') or not parsed.netloc or not parsed.path or parsed.query or parsed.fragment:
                raise ValueError('invalid destination')
            if (parsed.scheme,parsed.netloc)!=(repo.scheme,repo.netloc):
                raise ValueError('destination not in configured repository')
            parts=parsed.path.lstrip('/').split('/',1)
            repo_parts=repo.path.lstrip('/').split('/',1)
            if len(parts)!=2 or len(repo_parts)!=2 or parts[0]!=repo_parts[0] or not parts[1].startswith(repo_parts[1].rstrip('/')+'/'):
                raise ValueError('destination outside backup prefix')
            key=parts[1]
            if not key.endswith('/') or key.rsplit('/',2)[-2]!=job['id']:
                raise ValueError('destination does not match backup ID')
            if job.get('base') and job['base'] not in destinations:
                raise ValueError('incremental base is absent from state')
            observed=server.get(job['id'])
            if not observed or observed[0]!='BACKUP_CREATED':
                raise ValueError('successful job not confirmed by ClickHouse')
            mc=Minio(parsed.netloc,access_key=os.environ['S3_ACCESS_KEY'],secret_key=os.environ['S3_SECRET_KEY'],
                     secure=parsed.scheme=='https',http_client=urllib3.PoolManager(
                         timeout=urllib3.Timeout(connect=3,read=30),retries=False))
            for attempt in range(3):
                count=total=0
                try:
                    for obj in mc.list_objects(parts[0],prefix=key,recursive=True):
                        if not obj.object_name.startswith(key):raise ValueError('unexpected listing key')
                        count+=1;total+=obj.size
                    break
                except (urllib3.exceptions.HTTPError, OSError):
                    if attempt==2:raise
                    time.sleep(2*(attempt+1))
            result['jobs'].append({'id':job['id'],'base':job.get('base'),
                'kind':'incremental' if job.get('base') else 'full',
                'objects':count,'s3_bytes':total,'server_compressed_bytes':observed[1],
                'server_uncompressed_bytes':observed[2],'server_logical_bytes':observed[3]})
        except Exception as exc:
            result['errors'].append({'id':job['id'],'type':type(exc).__name__,
                                     'reason':str(exc).split('\n')[0][:300]})
        result['heartbeat_at']=time.time()
        save(args.output,result)
        time.sleep(0.1)
    result['phase']='completed' if len(result['jobs'])==len(jobs) and not result['errors'] else 'incomplete'
    result['finished_at']=time.time()
    result['totals']={kind:{'jobs':sum(j['kind']==kind for j in result['jobs']),
                            'objects':sum(j['objects'] for j in result['jobs'] if j['kind']==kind),
                            's3_bytes':sum(j['s3_bytes'] for j in result['jobs'] if j['kind']==kind)}
                      for kind in ('full','incremental')}
    save(args.output,result)
    print(json.dumps({'phase':result['phase'],'totals':result['totals'],'errors':len(result['errors'])}),flush=True)
    if result['phase']!='completed':raise SystemExit(1)


if __name__=='__main__':main()
