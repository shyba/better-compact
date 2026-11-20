#!/usr/bin/env python3
"""Small native S3 backup lock diagnostic. Unique canaries/prefixes only.

Synchronous canary backups permit collection of query-local trace logs. Only
lock-related messages are retained; credentials are redacted. No existing
backup objects are removed. Canary table is dropped once operations terminate.
"""
import json
import os
from pathlib import Path
import time
import uuid

from backup import repository_url
from connection import connect

ROOT=Path('/pg-hot/data/better-sync/backup-lock-diagnostic')


def main():
    ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
    state={'phase':'running','attempts':[],'trace':[]}
    def save():
        state['heartbeat_at']=time.time()
        p=ROOT/'status.json'
        p.write_text(json.dumps(state,indent=2)+'\n')
        p.chmod(0o600)
    save()
    client=connect()
    client.connection.send_receive_timeout=150
    from clickhouse_driver.protocol import ServerPacketTypes
    original=client.connection.receive_packet
    def receive(*a,**kw):
        packet=original(*a,**kw)
        if packet.type==ServerPacketTypes.LOG and packet.block:
            names=[col[0] for col in packet.block.columns_with_types]
            for row in packet.block.get_rows():
                text=dict(zip(names,row)).get('text','')
                if 'lock' in text.lower() and ('backup' in text.lower() or 'contents' in text.lower() or '.lock' in text):
                    for key in ('S3_ACCESS_KEY','S3_SECRET_KEY','CLICKHOUSE_PASSWORD'):
                        if os.environ.get(key):text=text.replace(os.environ[key],'[redacted]')
                    state['trace'].append(text[:1500])
        return packet
    client.connection.receive_packet=receive
    name='sync_lock_probe_'+uuid.uuid4().hex
    repo=repository_url(os.environ['CLICKHOUSE_BACKUP_S3_URL'])+'/lock-diagnostics/'+name
    created=False
    try:
        client.execute(f'CREATE TABLE `{name}` (n UInt32) ENGINE=MergeTree ORDER BY n')
        created=True
        client.execute(f'INSERT INTO `{name}` VALUES',[(1,)])
        for i in range(6):
            job=str(uuid.uuid4())
            destination=repo+'/'+str(i)+'/'
            params={'dest':destination,'access':os.environ['S3_ACCESS_KEY'],
                    'secret':os.environ['S3_SECRET_KEY'],'id':job,'base':repo+'/0/'}
            sql=f'BACKUP TABLE `{name}` TO S3(%(dest)s,%(access)s,%(secret)s) SETTINGS id=%(id)s'
            if i:sql+=',base_backup=S3(%(base)s,%(access)s,%(secret)s)'
            result={'id':job,'incremental':bool(i),'destination':destination}
            try:
                rows=client.execute(sql,params,settings={
                    'max_execution_time':120,'backup_threads':1,'max_threads':1,
                    's3_use_adaptive_timeouts':False,'s3_request_timeout_ms':120000,
                    's3_connect_timeout_ms':5000,
                    's3_max_inflight_parts_for_one_file':1,'s3_max_upload_part_size':16777216,
                    's3_min_upload_part_size':8388608,'send_logs_level':'trace'})
                result['status']=str(rows)
            except Exception as exc:
                result['error_code']=getattr(exc,'code',None)
                result['status']='failed'
                # A socket timeout would leave a possibly running operation.
                # Do not continue to another canary or delete its table then.
                if result['error_code'] is None:
                    created=False
                    raise
            state['attempts'].append(result)
            save()
            if i==0 and result['status']=='failed':break
        state['phase']='completed'
    except Exception as exc:
        state['phase']='failed'
        state['error_type']=type(exc).__name__
        state['error_code']=getattr(exc,'code',None)
        raise
    finally:
        if created:client.execute(f'DROP TABLE `{name}` SYNC')
        client.disconnect()
        save()


if __name__=='__main__':main()
