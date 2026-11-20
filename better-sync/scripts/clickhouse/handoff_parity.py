#!/usr/bin/env python3
"""Read-only VCC shadow/legacy block comparison; never performs a handoff.

A block match alone does not establish source-byte coverage, archive drainage,
backup inclusion, index completeness or safe production cutover.
"""
import argparse
import itertools
import json
import os
import sqlite3


def _pg_text(value):
    return value.replace('\x00','\\x00') if isinstance(value,str) else value


def compare_shadow(pg,shadow,authority,source_id,leaf_id):
    item=shadow.execute('''SELECT path,size,sha256,complete_offset,trailing_size FROM shadow_generation
        WHERE authority=? AND source_id=? AND leaf_id=?''',(authority,source_id,leaf_id)).fetchone()
    if not item:return {'status':'unknown','reason':'verified shadow generation unavailable'}
    path,size,digest,complete_offset,trailing_size=item
    if trailing_size or complete_offset!=size:
        return {'status':'unknown','reason':'generation ends with an incomplete JSONL record'}
    marker=shadow.execute('''SELECT source_kind,block_count FROM shadow_vcc_generation
        WHERE authority=? AND source_id=? AND leaf_id=?''',(authority,source_id,leaf_id)).fetchone()
    if not marker:return {'status':'unknown','reason':'VCC shadow compiler output unavailable'}
    expected=shadow.execute('''SELECT kind,window_no,content,brief FROM shadow_vcc_block
        WHERE authority=? AND source_id=? AND leaf_id=? ORDER BY sequence''',(authority,source_id,leaf_id))
    result=compare_legacy_blocks(pg,source_id,path,expected,marker[1])
    if result['status']=='matched':
        result.update(generation_bytes=size,generation_sha256=digest)
    return result


def compare_legacy_blocks(pg,source_id,path,expected,expected_count):
    """Compare only ordered rendered fields; caller establishes input origin."""
    # This is an exact source/path match, never a guessed session identity.
    with pg.cursor() as cur:
        cur.execute('''SELECT session_id FROM vcc.session
            WHERE source_id=%s AND path=%s ORDER BY session_id LIMIT 2''',(source_id,_pg_text(path)))
        sessions=cur.fetchall()
    if len(sessions)!=1:
        return {'status':'unknown','reason':'legacy source/path does not identify exactly one session'}
    sentinel=object();count=0
    # A named server cursor avoids buffering a whole legacy session in the
    # driver. Oversized fields are not transferred at all and remain unknown.
    with pg.cursor(name='better_sync_shadow_parity') as cur:
        cur.itersize=1
        cur.execute('''SELECT kind,window_no,
            CASE WHEN octet_length(content)>16777216 THEN NULL ELSE content END,
            CASE WHEN octet_length(brief)>16777216 THEN NULL ELSE brief END,
            octet_length(content),coalesce(octet_length(brief),0)
            FROM vcc.block WHERE session_id=%s ORDER BY seq''',(sessions[0][0],))
        for actual,wanted in itertools.zip_longest(cur,expected,fillvalue=sentinel):
            if actual is sentinel or wanted is sentinel:
                result={'status':'mismatch','shadow_blocks':expected_count,'reason':'legacy and shadow block counts differ'}
                if actual is sentinel:result['legacy_blocks']=count
                else:result['legacy_blocks_at_least']=count+1
                return result
            if actual[4]>16777216 or actual[5]>16777216:
                return {'status':'unknown','reason':'legacy block exceeds bounded comparison limit'}
            if tuple(actual[:4])!=tuple(_pg_text(value) for value in wanted):
                return {'status':'mismatch','shadow_blocks':expected_count,'first_difference_index':count,
                        'reason':'ordered VCC block output differs'}
            count+=1
    if count!=expected_count:return {'status':'unknown','reason':'shadow block count does not match receipt'}
    return {'status':'matched','legacy_blocks':count,'shadow_blocks':count,
            'scope':'ordered kind/window/content/brief only; excludes timestamps, source/archive coverage, backups and search'}


def main():
    import psycopg2
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shadow-db',required=True)
    parser.add_argument('--authority',required=True)
    parser.add_argument('--source',required=True)
    parser.add_argument('--generation',required=True)
    args=parser.parse_args()
    shadow=sqlite3.connect('file:'+args.shadow_db+'?mode=ro',uri=True)
    try:
        marker=shadow.execute("SELECT value FROM shadow_meta WHERE key='format'").fetchone()
        if marker!=('better-sync-shadow-v1',):raise ValueError('unrecognized shadow database')
        pg=psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=int(os.environ.get('POSTGRES_PORT','5432')),
            dbname=os.environ['POSTGRES_DB'],user=os.environ['DB_READER_USER'],password=os.environ['DB_READER_PASSWORD'],
            connect_timeout=3,options='-c statement_timeout=5000 -c default_transaction_read_only=on')
        try:result=compare_shadow(pg,shadow,args.authority,args.source,args.generation)
        finally:pg.close()
    finally:shadow.close()
    print(json.dumps(result))
    return 0 if result['status']=='matched' else 1


if __name__=='__main__':raise SystemExit(main())
