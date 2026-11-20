#!/usr/bin/env python3
"""Authenticated bounded HTTP ingest. No S3 client, SQLite ledger or PG writes."""
import argparse
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from receiver_lock import authority_lock
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
from urllib.parse import unquote, urlsplit, parse_qs

from store import Store, Conflict, metadata

MAX_BYTES=64*1024*1024


def handler(store, token, spool_dir):
    slots=threading.BoundedSemaphore(2)
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self, fmt, *args):
            # URLs/source paths may be private. Log status, not request tokens.
            pass

        def reply(self,status,body):
            raw=json.dumps(body,separators=(',',':')).encode()
            self.send_response(status)
            self.send_header('content-type','application/json')
            self.send_header('content-length',str(len(raw)))
            self.send_header('connection','close')
            self.send_header('x-sync-store','clickhouse')
            self.end_headers()
            self.wfile.write(raw)
            self.close_connection=True

        def do_GET(self): self.dispatch()
        def do_PUT(self): self.dispatch()
        def do_POST(self): self.dispatch()

        def dispatch(self):
            self.connection.settimeout(30)
            given=self.headers.get('authorization','')
            if not hmac.compare_digest(given.encode(),('Bearer '+token).encode()):
                return self.reply(401,{'error':'bearer token required'})
            if not slots.acquire(blocking=False):
                return self.reply(429,{'error':'receiver busy; retain outbox and retry'})
            try:
                url=urlsplit(self.path)
                if url.path=='/capabilities' and self.command=='GET':
                    return self.reply(200,{'store':'clickhouse','protocol':1,'authority_id':store.authority(),
                                          'max_segment_bytes':MAX_BYTES,'receipt':'committed_segment',
                                          'generation_protocol':1,'generation_status_protocol':1})
                if url.path=='/health' and self.command=='GET':
                    return self.reply(200,{'ok':True,'store':'clickhouse','authority_id':store.authority()})
                if url.path=='/manifest' and self.command=='GET':
                    source=parse_qs(url.query).get('source',[''])[0]
                    return self.reply(200,{'source':source,'files':store.inventory(source),
                                          'scope':'committed immutable segments; no projection or backup guarantee'})
                if url.path=='/generation/status' and self.command=='POST':
                    if self.headers.get('transfer-encoding'):
                        return self.reply(400,{'error':'explicit content-length required'})
                    length=int(self.headers.get('content-length','-1'))
                    if length<1 or length>16384:
                        return self.reply(413,{'error':'generation status request exceeds limit'})
                    item=json.loads(self.rfile.read(length))
                    if not isinstance(item,dict) or set(item)!={'source_id','file_id'}:
                        return self.reply(400,{'error':'source_id and file_id required'})
                    result=store.generation_status(item['source_id'],item['file_id'])
                    if result is None:
                        return self.reply(404,{'error':'generation receipt absent'})
                    return self.reply(200,{'store':'clickhouse','authority_id':store.authority(),
                                           'committed':True,**result})
                if url.path=='/generation' and self.command=='POST':
                    if self.headers.get('transfer-encoding'):
                        return self.reply(400,{'error':'explicit content-length required'})
                    length=int(self.headers.get('content-length','-1'))
                    if length<1 or length>16384:
                        return self.reply(413,{'error':'generation request exceeds limit'})
                    item=json.loads(self.rfile.read(length))
                    receipt=store.commit_generation(item)
                    receipt['store']='clickhouse'
                    receipt['authority_id']=store.authority()
                    return self.reply(201,receipt)
                if url.path=='/verify' and self.command=='POST':
                    if self.headers.get('transfer-encoding'):
                        return self.reply(400,{'error':'explicit content-length required'})
                    length=int(self.headers.get('content-length','-1'))
                    if length<1 or length>16384:
                        return self.reply(413,{'error':'verification request exceeds limit'})
                    body=json.loads(self.rfile.read(length))
                    files=body.get('files') if isinstance(body,dict) else None
                    if not isinstance(files,list) or len(files)!=1 or not isinstance(files[0],dict):
                        return self.reply(400,{'error':'exactly one file required'})
                    item=files[0]
                    raw=store.verify_raw(item)
                    unknown=lambda reason: {'status':'unknown','reason':reason}
                    return self.reply(200,{'store':'clickhouse','authority_id':store.authority(),
                        'files':[{'file_id':item['file_id'],'archive':raw,
                                  'backup':unknown('backup inclusion has not been independently verified'),
                                  'loaded':unknown('ClickHouse projection handoff is not implemented'),
                                  'indexed':unknown('ClickHouse projection handoff is not implemented')}]})
                if not url.path.startswith('/file/') or self.command not in ('PUT','POST'):
                    return self.reply(404,{'error':'not found'})
                if self.headers.get('transfer-encoding'):
                    return self.reply(400,{'error':'explicit content-length required'})
                size=int(self.headers.get('content-length','-1'))
                if size<0 or size>MAX_BYTES:
                    return self.reply(413,{'error':'segment must be 0..64 MiB'})
                file_id=unquote(url.path[len('/file/'):])
                expected=self.headers.get('x-vcc-sha256','')
                meta=metadata(self.headers.get('x-vcc-source-id',''),file_id,
                    self.headers.get('x-vcc-path',''),int(self.headers.get('x-vcc-start','0')),
                    self.headers.get('x-vcc-full','false') in ('true','1'),
                    self.headers.get('x-vcc-source-kind',''),self.headers.get('x-vcc-source-locator',''),size,expected)
                # Spool bounded input before any receipt or data insert. The
                # producer's durable outbox protects failures/restarts; local
                # receiver spool is temporary and never a sync authority.
                with tempfile.TemporaryFile(dir=spool_dir) as spool:
                    digest=hashlib.sha256()
                    remaining=size
                    while remaining:
                        data=self.rfile.read(min(256*1024,remaining))
                        if not data:
                            raise ValueError('incomplete upload')
                        spool.write(data)
                        digest.update(data)
                        remaining-=len(data)
                    if digest.hexdigest()!=expected:
                        raise ValueError('upload SHA256 mismatch')
                    spool.seek(0)
                    receipt=store.commit(meta,spool)
                receipt['authority_id']=store.authority()
                return self.reply(201,receipt)
            except Conflict as exc:
                self.reply(409,{'error':str(exc)})
            except (ValueError,TypeError) as exc:
                self.reply(400,{'error':str(exc)})
            except (socket.timeout,TimeoutError):
                self.reply(408,{'error':'upload timed out; retain outbox'})
            except Exception:
                # Do not leak SQL, credentials or private source paths.
                self.reply(503,{'error':'store unavailable; retain outbox and retry'})
            finally:slots.release()
    return Handler


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host',default='127.0.0.1')
    p.add_argument('--port',type=int,default=4176)
    p.add_argument('--spool',type=Path,required=True)
    p.add_argument('--blob-table',default='sync_chunks')
    args=p.parse_args()
    token=os.environ['S3_SYNC_TOKEN']
    if len(token)<16:raise ValueError('token too short')
    args.spool.mkdir(parents=True,exist_ok=True,mode=0o700)
    store=Store(args.blob_table)
    store.schema()
    authority=store.authority()  # fail closed: never silently replace authority on boot
    # The in-process writer mutex is insufficient if a second receiver starts
    # on another port. Hold one kernel lock for this authority for the life of
    # the process. Distributed/multi-user deployments still need coordination.
    with authority_lock(authority):
        server=ThreadingHTTPServer((args.host,args.port),handler(store,token,args.spool))
        server.daemon_threads=True
        server.serve_forever()


if __name__=='__main__':main()
