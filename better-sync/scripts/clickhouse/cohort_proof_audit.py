#!/usr/bin/env python3
"""Read-only audit of a restored cohort against retained local outbox bytes.

This produces no release receipt and never unlinks files. It checks native job
identity, restored payload/lineage and each immutable local descriptor and body.
An earlier production backup chain and automated release remain unproven.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time

from cohort_restore_drill import KEYS,verify_cohort
from connection import connect
from generation_ledger import lineage
from restore_lineage_drill import Namespace


def digest(data):return hashlib.sha256(data).hexdigest()


def audit(client,state,outbox):
    if state.get('phase')!='verified' or state.get('active'):
        raise ValueError('cohort has no completed restored proof')
    names=state['tables'];restored=state['restored_tables']
    if set(names)!=set(KEYS) or set(restored)!=set(KEYS):raise ValueError('invalid table map')
    match=re.fullmatch(r'(sync_cohort_drill_[a-f0-9]{32})_authority',names['sync_authority'])
    if not match:raise ValueError('invalid cohort namespace')
    for key in KEYS:
        if names[key]!=match.group(1)+'_'+key[5:] or restored[key]!=names[key]+'_restored':
            raise ValueError('cohort table identity mismatch')
    jobs=state.get('jobs',[])
    if len(jobs)!=2 or [(j.get('kind'),j.get('terminal_status')) for j in jobs]!=[('backup','BACKUP_CREATED'),('restore','RESTORED')]:
        raise ValueError('cohort job evidence incomplete')
    prefix=state['repository']+'/full/'
    for job in jobs:
        rows=client.execute('SELECT status,name FROM system.backups WHERE id=%(id)s',{'id':job['id']})
        if len(rows)!=1 or rows[0][0]!=job['terminal_status'] or prefix not in rows[0][1]:
            raise ValueError('cohort native job identity or status unavailable')
    leaf=state['leaf'];authority=state['authority']
    result=verify_cohort(client,restored,leaf,authority)
    scoped=Namespace(client,{k:v for k,v in restored.items() if k!='sync_chunks'})
    versions=lineage(scoped,leaf);segments=0;local_bytes=0
    for version in versions:
        rows=scoped.execute('''SELECT file_id,path,start,size,full,sha256,metadata FROM sync_segments FINAL
            WHERE source_id=%(source)s AND startsWith(file_id,%(prefix)s) ORDER BY start LIMIT 65''',
            {'source':leaf['source_id'],'prefix':version['file_id']+'.'})
        if not rows or segments+len(rows)>64:raise ValueError('invalid cohort segment count')
        for file,path,start,size,full,checksum,encoded in rows:
            meta=json.loads(encoded)
            key=digest((leaf['source_id']+'\n'+file).encode())
            descriptor=json.loads((outbox/(key+'.json')).read_text())
            expected={'authority':authority,'fileID':file,'sourcePath':path,'start':start,
                      'full':bool(full),'size':size,'sha256':checksum}
            if any(descriptor.get(k)!=v for k,v in expected.items()) or type(descriptor.get('committed_at')) is not int or descriptor['committed_at']<=0:
                raise ValueError('retained outbox descriptor differs from restored segment')
            source=descriptor.get('source') or {}
            if (source.get('id')!=leaf['source_id'] or source.get('kind')!=meta.get('source_kind') or
                source.get('locator')!=meta.get('locator') or meta.get('source_id')!=leaf['source_id'] or
                meta.get('file_id')!=file or meta.get('path')!=path or meta.get('start')!=start or
                meta.get('size')!=size or meta.get('sha256')!=checksum or meta.get('full')!=bool(full)):
                raise ValueError('restored source metadata differs from retained descriptor')
            body=(outbox/(key+'.body')).read_bytes()
            if len(body)!=size or digest(body)!=checksum:raise ValueError('retained body does not match restored segment')
            local_bytes+=size;segments+=1
    return {'status':'matched','versions':len(versions),'segments':segments,'retained_bytes':local_bytes,
            'whole_sha256':result['sha256'],'scope':'restored isolated cohort versus retained pilot outbox only; no release authorized'}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--state',type=Path,required=True);p.add_argument('--outbox',type=Path,required=True)
    p.add_argument('--evidence',type=Path,help='create a private, non-overwriting local audit record')
    a=p.parse_args();state_bytes=a.state.read_bytes();state=json.loads(state_bytes);client=connect()
    try:result=audit(client,state,a.outbox)
    finally:client.disconnect()
    if a.evidence:
        evidence={'checked_at':time.time(),'state_sha256':digest(state_bytes),'state_path':str(a.state),
                  'jobs':[{'id':j['id'],'kind':j['kind']} for j in state['jobs']],'result':result}
        fd=os.open(a.evidence,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w') as stream:
            json.dump(evidence,stream,indent=2);stream.flush();os.fsync(stream.fileno())
        directory=os.open(a.evidence.parent,os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    print(json.dumps(result))


if __name__=='__main__':main()
