#!/usr/bin/env python3
"""Prove native full -> incremental -> isolated table restore before cutover.

Creates only uniquely named canary tables in the configured database and writes
backups under a unique repository subdirectory. Never overwrites migrated/live
tables and never deletes backup objects. Credentials are not printed.
"""
import hashlib
import json
import os
import time
import uuid

from backup import poll, repository_url
from connection import connect


def wait(client, job):
    deadline=time.monotonic()+600
    while time.monotonic()<deadline:
        status=poll(client,job)
        if status in ('BACKUP_CREATED','RESTORED'):return
        time.sleep(2)
    raise TimeoutError('backup/restore drill deadline; inspect job before rerunning')


def main():
    import re
    db=os.environ['CLICKHOUSE_DATABASE']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*',db):raise ValueError('invalid database')
    prefix='sync_drill_'+uuid.uuid4().hex
    source=f'`{db}`.`{prefix}`'
    restored=f'`{db}`.`{prefix}_restored`'
    base=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])+'/'+prefix
    params={'access':os.environ['S3_ACCESS_KEY'],'secret':os.environ['S3_SECRET_KEY']}
    client=connect()
    created=[]
    try:
        client.execute(f'CREATE TABLE {source} (seq UInt32,data String CODEC(ZSTD(3))) ENGINE=MergeTree ORDER BY seq')
        created.append(source)
        first=b'first preserved original bytes\xff\n'*10000
        second=b'next chunk after first backup\x00\n'*10000
        client.execute(f'INSERT INTO {source} VALUES',[(1,first)])
        for n in (1,2):
            if n==2:client.execute(f'INSERT INTO {source} VALUES',[(2,second)])
            job=str(uuid.uuid4())
            sql=f'BACKUP TABLE {source} TO S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s'
            if n==2:sql+=', base_backup=S3(%(base)s,%(access)s,%(secret)s)'
            sql+=' ASYNC'
            client.execute(sql,{**params,'dest':base+f'/{n}/','base':base+'/1/','id':job})
            wait(client,job)
        job=str(uuid.uuid4())
        client.execute(f'''RESTORE TABLE {source} AS {restored}
            FROM S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s ASYNC''',
            {**params,'dest':base+'/2/','id':job})
        created.append(restored)
        wait(client,job)
        rows=client.execute(f'SELECT seq,hex(SHA256(data)) FROM {restored} ORDER BY seq')
        expected=[(1,hashlib.sha256(first).hexdigest().upper()),(2,hashlib.sha256(second).hexdigest().upper())]
        if rows!=expected:raise ValueError('restored payload checksum mismatch')
        print(json.dumps({'verified':True,'full_incremental_restore':True,'canary_prefix':prefix,
                          'backup_prefix':base,'restored_rows':len(rows)}),flush=True)
    finally:
        # If restore is still active/uncertain do not race it by deleting its
        # destination. A failed drill leaves isolated canaries for inspection.
        if 'rows' in locals() and rows==expected:
            for name in reversed(created):client.execute('DROP TABLE '+name+' SYNC')
        client.disconnect()


if __name__=='__main__':main()
