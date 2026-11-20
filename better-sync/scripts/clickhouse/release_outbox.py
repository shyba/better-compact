#!/usr/bin/env python3
"""Release retained outbox segment bodies whose bytes are proven durable.

A body is unlinked only when ALL of the following hold, each checked
independently:
  1. the descriptor is committed (sync receipt) and its body exists;
  2. the latest daily chain-coverage run completed with full coverage against
     a backup that STARTED AFTER this segment was committed;
  3. the segment belongs to the committed lineage of its file in ClickHouse
     (sync_segments row identity matches the descriptor exactly);
  4. read-back: the chunk data stored in ClickHouse for this segment
     concatenates to exactly the descriptor's sha256.

The descriptor JSON is kept (it is the audit trail); only the body is
unlinked, and each release is logged to a JSONL ledger. Bodies without
descriptors are never touched. Bounded per run.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connection import connect  # noqa: E402


def chain_gate(backup_state, max_age_hours):
    # coverage history lives in chain-coverage-history, sibling of the
    # backup-state directory (backup_state may be the dir or its state.json)
    base = backup_state.parent.parent if backup_state.name == 'state.json' else backup_state.parent
    history = base / 'chain-coverage-history'
    states = sorted(history.glob('run-*/state.json'))
    if not states: raise ValueError('no chain-coverage runs exist')
    s = json.load(open(states[-1]))
    if s.get('phase') != 'completed' or s.get('covered') != s.get('total') or not s.get('total'):
        raise ValueError(f'chain coverage incomplete: {s.get("phase")} {s.get("covered")}/{s.get("total")}')
    if time.time() - s.get('finished_at', 0) > max_age_hours * 3600:
        raise ValueError('chain coverage is stale')
    state_file = backup_state if backup_state.name == 'state.json' else backup_state / 'state.json'
    scheduler = json.load(open(state_file))
    backup = next((b for b in scheduler.get('successful', []) if b['id'] == s.get('backup_id')), None)
    if not backup: raise ValueError('verified backup not found in scheduler history')
    gate = {'backup_id': backup['id'], 'backup_started_at': backup['started_at'],
            'coverage_finished': s['finished_at'], 'covered': s['covered']}
    return gate


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--outbox-root', type=Path, required=True, help='authority outbox directory')
    p.add_argument('--backup-state', type=Path, required=True)
    p.add_argument('--ledger', type=Path, required=True)
    p.add_argument('--max-age-hours', type=float, default=26)
    p.add_argument('--limit', type=int, default=400)
    p.add_argument('--dry-run', action='store_true')
    a = p.parse_args()
    load_paths = ('/secondary/data/session_center/.env', '/home/user/.config/better-sync/clickhouse.env')
    for path in load_paths:
        try:
            for line in open(path):
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line: continue
                key, _, value = line.partition('=')
                os.environ.setdefault(key.strip(), value.strip().strip('"'))
        except OSError: pass

    gate = chain_gate(a.backup_state, a.max_age_hours)
    client = connect()
    released = kept_missing_receipt = kept_not_in_chain = kept_mismatch = errors = 0
    freed_bytes = 0
    reasons = {}
    try:
        descriptors = sorted(a.outbox_root.glob('*.json'))
        candidates = []
        for descriptor in descriptors:
            if descriptor.name.endswith('.generation.json'): continue
            body = descriptor.with_suffix('.body')
            if not body.exists(): continue
            try: meta = json.loads(descriptor.read_text())
            except ValueError: errors += 1; continue
            if 'fileID' not in meta or 'source' not in meta:
                continue  # unrelated JSON (e.g. committed markers from other flows)
            if meta.get('committed_at') is None: kept_missing_receipt += 1; continue
            if meta['committed_at'] / 1000 >= gate['backup_started_at']:
                kept_not_in_chain += 1; continue  # committed after the verified backup started
            candidates.append((descriptor, body, meta))
            if len(candidates) >= a.limit: break
        for descriptor, body, meta in candidates:
            source = meta['source']['id']
            file_id = meta['fileID']
            segment_id = hashlib.sha256(f'{source}\0{file_id}'.encode()).hexdigest()
            try:
                rows = client.execute('''SELECT segment_id,size,sha256 FROM sync_segments FINAL
                    WHERE source_id=%(s)s AND segment_id=%(i)s''', {'s': source, 'i': segment_id})
                if len(rows) != 1 or rows[0][1] != meta['size'] or rows[0][2] != meta['sha256']:
                    reasons.setdefault('segment_identity', []).append(file_id); kept_mismatch += 1; continue
                refs = client.execute('''SELECT ordinal,hash,raw_size FROM sync_refs FINAL
                    WHERE segment_id=%(i)s ORDER BY ordinal''', {'i': segment_id})
                if not refs or sum(r[2] for r in refs) != meta['size']:
                    reasons.setdefault('refs', []).append(file_id); kept_mismatch += 1; continue
                # read blob data per hash: point queries are exact and
                # consistent on the ReplacingMergeTree blob table (IN-list
                # and FINAL reads over the multi-GB table proved flaky under
                # the 512 MiB per-query limit); content-addressed rows make
                # any returned copy with a matching hash authoritative.
                chunks = {}
                blobs_table = "`archive_migration_v1_chunks`"
                for chash in (r[1] for r in refs):
                    rows_b = client.execute(
                        f'SELECT data FROM {blobs_table} WHERE hash=%(h)s', {'h': chash},
                        settings={'max_threads': 1, 'strings_as_bytes': True})
                    if rows_b: chunks[chash] = rows_b[0][0]
                digest = hashlib.sha256()
                ok = True
                for ordinal, chash, csize in refs:
                    data = chunks.get(chash)
                    if data is None or len(data) != csize or hashlib.sha256(data).hexdigest() != chash:
                        ok = False; break
                    digest.update(data)
                if not ok or digest.hexdigest() != meta['sha256']:
                    reasons.setdefault('readback', []).append(file_id); kept_mismatch += 1; continue
                size = body.stat().st_size
                if size != meta['size']:
                    reasons.setdefault('body_size', []).append(file_id); kept_mismatch += 1; continue
                if a.dry_run:
                    released += 1; freed_bytes += size; continue
                body.unlink()
                released += 1; freed_bytes += size
                a.ledger.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                with a.ledger.open('a') as log:
                    log.write(json.dumps({'released_at': time.time(), 'segment_id': segment_id,
                        'file_id': file_id, 'size': meta['size'], 'sha256': meta['sha256'],
                        'backup_id': gate['backup_id']}) + '\n')
            except Exception as exc:
                errors += 1
                if errors <= 3: print('error', file_id, type(exc).__name__, file=sys.stderr)
    finally:
        client.disconnect()
    print(json.dumps({'gate_backup': gate['backup_id'], 'released': released,
        'freed_bytes': freed_bytes, 'awaiting_backup': kept_not_in_chain,
        'no_receipt': kept_missing_receipt, 'mismatch_kept': kept_mismatch, 'errors': errors,
        'reasons': {k: v[:3] for k, v in reasons.items()},
        'dry_run': a.dry_run}), flush=True)
    return 0 if errors == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
