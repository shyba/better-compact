#!/usr/bin/env python3
"""Production VCC projector for one pi source, reading only committed,
lineage-verified ClickHouse generations.

Reuses the production loader's own write functions (vcc_load) and the same
compiler dialect, so rows land exactly as the legacy loader would write them
(parity already proven against production rows). A hard per-source fence
refuses to run while the legacy feeder unit for this source is active, so
there is never a second writer for the same source in PostgreSQL.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from connection import connect  # noqa: E402
from generation_ledger import lineage  # noqa: E402
from projection_records import complete_records  # noqa: E402
from raw_coverage import verify_lineage  # noqa: E402


def load_env(paths):
    for path in paths:
        try:
            for line in open(path):
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line: continue
                key, _, value = line.partition('=')
                os.environ.setdefault(key.strip(), value.strip().strip('"'))
        except OSError: pass


def load_compiler(path):
    spec = importlib.util.spec_from_file_location('codex_vcc', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_loader(root):
    loader_path = root / 's3_sync' / 'scripts' / 'vcc_load.py'
    if not loader_path.exists():
        loader_path = Path('/secondary/data/session_center/s3_sync/scripts/vcc_load.py')
    spec = importlib.util.spec_from_file_location('vcc_load', loader_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fence_active(unit):
    state = subprocess.run(['systemctl', 'is-active', '--quiet', unit]).returncode == 0
    return state


class Fence:
    """Refuse all work while the legacy feeder unit for this source is active.
    Re-checked between files; a mid-run activation aborts before the next
    write. The check and the surrounding transaction are not atomic across
    processes, but the feeder side stops feeding long before any operator
    restarts this projector, and block writes are idempotent by hash."""

    def __init__(self, unit):
        self.unit = unit

    def check(self):
        if fence_active(self.unit):
            raise SystemExit(f'fence: legacy feeder {self.unit} is active; refusing to project')


def source_fsid(client, source_id):
    rows = client.execute('''SELECT metadata FROM sync_segments FINAL
        WHERE source_id=%(s)s ORDER BY committed_at DESC LIMIT 1''', {'s': source_id})
    if not rows: raise ValueError('source has no segments in ClickHouse')
    meta = json.loads(rows[0][0])
    kind, locator = meta.get('source_kind'), meta.get('locator')
    if not kind or not locator: raise ValueError('source metadata incomplete')
    fsid = hashlib.sha256(f'{kind}\n{locator}'.encode()).hexdigest()[:32]
    if fsid != source_id: raise ValueError(f'source id mismatch: derived {fsid}')
    return fsid, kind, locator


def file_state(pgcur, fsid, path):
    pgcur.execute('select byte_end from vcc.file_cursor where source_id=%s and path=%s', (fsid, path))
    row = pgcur.fetchone()
    return int(row[0]) if row else 0


def reconstruct(client, blobs, generation, spool_dir):
    chain = lineage(client, generation)
    with tempfile.TemporaryFile(dir=spool_dir) as spool:
        verify_lineage(client, blobs, generation, chain, on_chunk=spool.write)
        spool.seek(0)
        return spool.read()

def project_generation(client, blobs, loader, vcc, pg, pgcur, fsid, kind, locator,
                       generation, spool_dir, dry_run, a_replace=False):
    rel = generation['path']
    path = locator.rstrip('/') + '/' + rel  # production stores absolute paths
    raw = reconstruct(client, '`archive_migration_v1_chunks`', generation, spool_dir)
    records, _pending, offset = complete_records(generation['source_id'], generation['file_id'], 0, raw)
    if _pending:
        raise ValueError('trailing partial record in committed generation')
    # shape adapter: build the exact dicts parse_records_releasing_lines would
    # hand the loader and compiler (parsed JSON with _ln and ordinal merged)
    parsed = []
    for index, r in enumerate(records):
        value = dict(r['value']) if isinstance(r['value'], dict) else {}
        value['_ln'] = index + 1
        value.setdefault('ordinal', None)
        parsed.append(value)
    cursor = file_state(pgcur, fsid, path)
    existing_session = None
    pgcur.execute('select session_id from vcc.session where source_id=%s and path=%s order by created_at desc nulls last, session_id limit 1', (fsid, path))
    row = pgcur.fetchone()
    if row: existing_session = row[0]
    if generation['size'] < cursor:
        return {'path': path, 'status': 'behind_cursor', 'blocks': 0}
    if generation['size'] == cursor and not a_replace:
        return {'path': path, 'status': 'already_current', 'blocks': 0}
    delta = cursor > 0 and kind in ('codex-jsonl', 'codex-jsonl-sessions')
    if delta:
        # Continuation of the loader's frame-wise semantics: compile only the
        # records after the legacy byte cursor. The codex compiler is
        # context-sensitive (whole-file output differs from frame-wise output
        # and would drop real records), so history is never recompiled; the
        # projector is a drop-in continuation for bytes the loader never saw.
        kept = []
        for index, r in enumerate(records):
            start, end = r['offset'], r['offset'] + r['size']
            if end <= cursor: continue
            if start < cursor:
                raise ValueError(f'byte cursor falls inside a JSONL record for {path}; frame remains pending')
            value = dict(parsed[index])
            value['_ln'] = len(kept) + 1
            kept.append(value)
        parsed = kept
        if not parsed:
            return {'path': path, 'status': 'already_current', 'blocks': 0}
    # The loader's append semantics: sequence continues from stored blocks;
    # block_hash idempotency makes replays land exactly once. On cutover
    # (--replace), legacy rows carry per-frame ordinals that cannot be
    # reproduced from whole-file generations; their content is identical
    # (proven for the record-local pi compiler), so the session is replaced
    # once with file-wide ordinals. Codex never replaces (delta mode above).
    full = bool(a_replace and existing_session and not delta)
    if 'opencode' in kind or kind in ('pi-jsonl', 'codex-jsonl', 'codex-jsonl-sessions'):
        if kind == 'pi-jsonl' or 'opencode' in kind:
            blocks, _stats = vcc.build_ir_opencode(parsed, path)
        else:
            blocks, _stats = vcc.build_ir(parsed, path)
    else:
        raise ValueError(f'unsupported source kind {kind}')
    loader._insert_blocks  # touch to fail fast if loader shape changed
    native = None if (full or delta) else loader._native_session_id(blocks, parsed, path)
    if full or delta:
        # replace keeps the legacy identity; delta continuation: the loader
        # already mapped this path to a session
        pgcur.execute('select session_id from vcc.session where source_id=%s and path=%s order by created_at desc nulls last, session_id limit 1', (fsid, path))
        row = pgcur.fetchone()
        if not row: raise ValueError(f'{"replace" if full else "delta"} found no session mapping for {path}')
        sess = row[0]
    elif native.startswith('path:') or not native:
        pgcur.execute('select session_id from vcc.session where source_id=%s and path=%s order by created_at desc nulls last, session_id limit 1', (fsid, path))
        row = pgcur.fetchone()
        if row:
            # mirror the loader: bind to the most recent session mapped to
            # this path and keep its identity (native becomes None)
            sess = row[0]
        else:
            # loader fallback: deterministic identity from the path itself
            sess = loader.sid(fsid, native)
    else:
        sess = loader.sid(fsid, native)
    if dry_run:
        pg.rollback()
        return {'path': path, 'status': 'dry_run_blocks', 'blocks': len(blocks)}
    pgcur.execute('''insert into vcc.source(source_id, kind, locator, origin_host, origin_id) values (%s,%s,%s,%s,%s)
                    on conflict (source_id) do update set locator=excluded.locator,
                      origin_host=coalesce(vcc.source.origin_host, excluded.origin_host),
                      origin_id=coalesce(vcc.source.origin_id, excluded.origin_id)''',
                 (fsid, kind, locator, None, None))
    loader._upsert_session(pgcur, sess, fsid, native, path, parsed, blocks, {}, None)
    loader._insert_blocks(pgcur, sess, blocks, full=full)
    pgcur.execute('''insert into vcc.file_cursor(source_id,path,byte_end) values (%s,%s,%s)
        on conflict (source_id,path) do update set byte_end=greatest(vcc.file_cursor.byte_end, excluded.byte_end), updated_at=now()''',
        (fsid, path, generation['size']))
    pg.commit()
    return {'path': path, 'status': 'projected', 'blocks': len(blocks), 'bytes': generation['size'] - cursor}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-id', required=True)
    p.add_argument('--leaf-limit', type=int, default=64)
    p.add_argument('--fence-unit', default='vcc-feed-pi.service')
    p.add_argument('--scratch', default=None, help='PostgreSQL DSN override (scratch schema tests)')
    p.add_argument('--ignore-fence-for-scratch', action='store_true',
                   help='run while the legacy feeder is active; requires --scratch, refuses production DSN')
    p.add_argument('--replace', action='store_true',
                   help='cutover mode: re-project sessions already present from '
                        'legacy feeding once, replacing per-frame-ordinal rows '
                        'with whole-file ordinals (content identical)')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--state', type=Path, default=Path('/pg-hot/data/better-sync/pi-projection/state.sqlite'))
    a = p.parse_args()
    repo = Path(__file__).resolve().parents[3]
    compiler_path = Path('/secondary/data/session_center/codex-vcc.py')
    if not compiler_path.exists():
        compiler_path = repo / 'codex-vcc.py'
    _spec = importlib.util.spec_from_file_location('codex_vcc', compiler_path)
    load_env([repo / '.env', '/home/user/.config/better-sync/clickhouse.env'])
    vcc = load_compiler(compiler_path)
    loader = load_loader(repo)
    fence = Fence(a.fence_unit)
    if a.ignore_fence_for_scratch:
        if not a.scratch or a.scratch == os.environ.get('SOURCE_DATABASE_URL'):
            raise SystemExit('--ignore-fence-for-scratch requires a --scratch DSN different from SOURCE_DATABASE_URL')
    else:
        fence.check()
    client = connect()
    import psycopg2
    # mirror the loader's DSN preference: writer role over read-only source URL
    if a.scratch:
        dsn = a.scratch
    elif os.environ.get('POSTGRES_HOST') and os.environ.get('DB_WRITER_USER'):
        dsn = (f"postgresql://{os.environ['DB_WRITER_USER']}:{os.environ['DB_WRITER_PASSWORD']}"
               f"@{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT'] or 5432}/{os.environ['POSTGRES_DB']}")
    else:
        dsn = os.environ['SOURCE_DATABASE_URL']
    pg = psycopg2.connect(dsn)
    pgcur = pg.cursor()
    try:
        fsid, kind, locator = source_fsid(client, a.source_id)
        print('source', fsid, kind, locator)
        projected = 0
        rows = client.execute('''SELECT file_id, metadata FROM sync_generations FINAL
            WHERE source_id=%(s)s ORDER BY committed_at DESC LIMIT %(n)s''',
            {'s': a.source_id, 'n': a.leaf_limit})
        seen_paths = set()
        for file_id, raw in rows:
            generation = json.loads(raw)
            path = generation['path']
            if path in seen_paths: continue
            seen_paths.add(path)
            if not a.ignore_fence_for_scratch: fence.check()
            result = project_generation(client, '`archive_migration_v1_chunks`', loader, vcc, pg, pgcur, fsid, kind,
                                        locator, generation, '/pg-hot/data/better-sync/pi-projection', a.dry_run,
                                        a.replace)
            print(json.dumps(result), flush=True)
            if result['status'] == 'projected': projected += 1
        print('projected_files', projected)
    finally:
        pgcur.close(); pg.close(); client.disconnect()


if __name__ == '__main__':
    main()
