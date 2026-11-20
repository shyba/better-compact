#!/usr/bin/env python3
"""Read-only HEAD audit of objects referenced by one native backup manifest.

This does not enumerate extra/unknown objects, verify data checksums, establish
backup restoration, or permit deletion. Failed/unknown prefixes are untouched.
"""
import argparse
import json
import os
from pathlib import Path
import time
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

from minio import Minio
import urllib3


def save(path,state):
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    tmp=path.with_suffix('.tmp')
    with tmp.open('w') as f:
        os.chmod(tmp,0o600);json.dump(state,f,indent=2);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args()
    jobs=json.loads(args.state.read_text())['successful']
    job=next((j for j in jobs if not j.get('base')),None)
    if not job:raise ValueError('no completed full backup')
    dest=urlsplit(job['destination']);repo=urlsplit(os.environ['CLICKHOUSE_BACKUP_S3_URL'])
    if (dest.scheme,dest.netloc)!=(repo.scheme,repo.netloc):raise ValueError('destination outside repository')
    parts=dest.path.lstrip('/').split('/',1);repo_parts=repo.path.lstrip('/').split('/',1)
    if (len(parts)!=2 or len(repo_parts)!=2 or parts[0]!=repo_parts[0] or
        not parts[1].startswith(repo_parts[1].rstrip('/')+'/') or
        not parts[1].endswith('/') or parts[1].rsplit('/',2)[-2]!=job['id']):
        raise ValueError('invalid owned destination')
    mc=Minio(dest.netloc,access_key=os.environ['S3_ACCESS_KEY'],secret_key=os.environ['S3_SECRET_KEY'],
             secure=dest.scheme=='https',http_client=urllib3.PoolManager(
                 timeout=urllib3.Timeout(connect=3,read=5),retries=False))
    r=mc.get_object(parts[0],parts[1]+'.backup')
    try:data=r.read(32*1024*1024+1)
    finally:r.close();r.release_conn()
    if len(data)>32*1024*1024:raise ValueError('manifest exceeds limit')
    root=ET.fromstring(data)
    if root.findtext('backup_id')!=job['id']:raise ValueError('manifest backup id mismatch')
    objects={}
    for entry in root.findall('./contents/file'):
        name=entry.findtext('data_file') or entry.findtext('name')
        size=int(entry.findtext('size','-1'))
        if not name or name.startswith('/') or '..' in name.split('/') or size<0:
            raise ValueError('invalid manifest object')
        if name in objects and objects[name]!=size:raise ValueError('conflicting object sizes')
        objects[name]=size
    state={'phase':'running','backup_id':job['id'],'started_at':time.time(),
           'objects':len(objects),'checked':0,'bytes':len(data),'manifest_bytes':len(data),
           'errors':[],'scope':'manifest-referenced object HEAD sizes; no checksum, restore, or complete bucket listing'}
    save(args.output,state)
    for name,expected in sorted(objects.items()):
        try:
            obj=mc.stat_object(parts[0],parts[1]+name)
            if obj.size!=expected:raise ValueError('size mismatch')
            state['checked']+=1;state['bytes']+=obj.size
        except Exception as exc:
            state['errors'].append({'object':name,'type':type(exc).__name__,
                                    'reason':str(exc).split('\n')[0][:200]})
        state['heartbeat_at']=time.time();save(args.output,state)
        time.sleep(0.05)
    state['phase']='completed' if not state['errors'] and state['checked']==state['objects'] else 'incomplete'
    state['finished_at']=time.time();save(args.output,state)
    print(json.dumps({'phase':state['phase'],'checked':state['checked'],
                      'objects':state['objects'],'bytes':state['bytes'],'errors':len(state['errors'])}),flush=True)
    if state['phase']!='completed':raise SystemExit(1)


if __name__=='__main__':main()
