"""Bounded byte-exact verification of ClickHouse raw segment chains.

``verify_lineage`` is a pure reconstruction primitive; callers must separately
establish that its ordered version metadata is durable and authoritative. The
current receiver only exposes replacement verification until durable lineage
and its commit protocol exist. Never infer append ancestry from range ends.
"""
import hashlib
import re

CHUNK_BYTES = 256 * 1024
MAX_VERIFY_BYTES = 256 * 1024 * 1024
MAX_SEGMENTS = 4096
MAX_VERSIONS = 4096


class CoverageMismatch(ValueError):
    pass


class CoverageUnknown(ValueError):
    pass


def _validate_item(item):
    source = item.get('source_id')
    file_id = item.get('file_id')
    path = item.get('path')
    size = item.get('size')
    expected = item.get('sha256')
    if not isinstance(source, str) or not re.fullmatch(r'[a-f0-9]{16,64}', source):
        raise ValueError('invalid source id')
    if not isinstance(file_id, str) or not 1 <= len(file_id) <= 260:
        raise ValueError('invalid file id')
    if not isinstance(path, str) or not 1 <= len(path) <= 1024:
        raise ValueError('invalid path')
    if type(size) is not int or not 0 <= size <= 2**53-1:
        raise ValueError('invalid size')
    if not isinstance(expected, str) or not re.fullmatch(r'[a-f0-9]{64}', expected):
        raise ValueError('invalid whole-file checksum')
    if size > MAX_VERIFY_BYTES:
        raise CoverageUnknown('file exceeds bounded online verification limit')


def verify_lineage(client, blobs, item, versions, on_chunk=None):
    """Reconstruct explicit root-to-leaf versions and verify every prefix.

    `versions` must be obtained from a durable authoritative commit ledger,
    not guessed from segments. Each entry has file_id, parent, start, size and
    sha256. This function does not write such a ledger or issue a receipt.
    """
    _validate_item(item)
    if not isinstance(versions, list) or not versions:
        raise CoverageUnknown('generation lineage is unavailable')
    if len(versions) > MAX_VERSIONS:
        raise CoverageUnknown('generation lineage exceeds bounded online verification limit')
    source, path = item['source_id'], item['path']
    whole = hashlib.sha256()
    offset = 0
    total_segments = 0
    previous = None
    seen = set()
    for version_index, version in enumerate(versions):
        if not isinstance(version, dict):
            raise ValueError('invalid generation metadata')
        current = {**item, 'file_id': version.get('file_id'),
                   'size': version.get('size'), 'sha256': version.get('sha256')}
        _validate_item(current)
        file_id, size = current['file_id'], current['size']
        if (file_id in seen or version.get('parent') != previous or
            type(version.get('start')) is not int or version['start'] != offset or
            (version_index > 0 and size <= offset) or size < offset):
            raise CoverageMismatch('generation parent, identity, or range mismatch')
        seen.add(file_id)
        rows = client.execute('''SELECT file_id,path,start,size,full,sha256,segment_id
            FROM sync_segments FINAL WHERE source_id=%(source)s
            AND startsWith(file_id,%(prefix)s) ORDER BY start LIMIT 4097''',
            {'source': source, 'prefix': file_id+'.'})
        if not rows:
            raise CoverageUnknown('no committed segments for this file version')
        total_segments += len(rows)
        if total_segments > MAX_SEGMENTS:
            raise CoverageUnknown('file has too many segments for bounded online verification')
        if version_index == 0 and (rows[0][2] != 0 or not rows[0][4]):
            raise CoverageUnknown('append lineage is not persisted; full coverage cannot be established')
        for index, (segment_file, segment_path, start, length, full, segment_sha, segment_id) in enumerate(rows):
            expected_id = hashlib.sha256((source+'\0'+segment_file).encode()).hexdigest()
            expected_full = version_index == 0 and index == 0
            if (segment_file != file_id+'.'+str(start) or segment_id != expected_id or
                segment_path != path or start != offset or full not in (0,1) or bool(full) != expected_full or
                length < 0 or length > 64*1024*1024 or start+length > size or
                (length == 0 and (size != 0 or len(rows) != 1 or version_index != 0))):
                raise CoverageMismatch('segment identity or contiguous range mismatch')
            refs = client.execute('''SELECT ordinal,hash,raw_size FROM sync_refs FINAL
                WHERE segment_id=%(id)s ORDER BY ordinal LIMIT 258''', {'id': segment_id})
            if len(refs) > 257:
                raise CoverageMismatch('too many chunk references for a segment')
            segment_hash = hashlib.sha256()
            within = 0
            for base in range(0, len(refs), 32):
                batch = refs[base:base+32]
                hashes = tuple(set(row[1] for row in batch))
                # The sort key is hash. PREWHERE filters before reading large
                # String payloads; an ordinary WHERE on FINAL can materialize
                # hundreds of MiB of unrelated chunks for a tiny lookup.
                payload_rows = client.execute(f'SELECT hash,raw_size,data FROM {blobs} FINAL PREWHERE hash IN %(hashes)s',
                                              {'hashes': hashes}, settings={'strings_as_bytes': True, 'max_block_size': 32,
                                                                          'preferred_block_size_bytes': 8*1024*1024,
                                                                          'max_threads': 1})
                payloads = {key.decode('ascii'): (raw_size, data) for key, raw_size, data in payload_rows}
                for expected_ordinal, (ordinal, key, raw_size) in enumerate(batch, start=base):
                    expected_length = min(CHUNK_BYTES-((start+within) % CHUNK_BYTES), length-within)
                    if ordinal != expected_ordinal or expected_length <= 0 or raw_size != expected_length:
                        raise CoverageMismatch('chunk order or length mismatch')
                    payload = payloads.get(key)
                    if payload is None or payload[0] != raw_size or len(payload[1]) != raw_size:
                        raise CoverageMismatch('missing chunk or chunk length mismatch')
                    data = payload[1]
                    if hashlib.sha256(data).hexdigest() != key:
                        raise CoverageMismatch('chunk checksum mismatch')
                    if on_chunk is not None:
                        # The sink must be temporary: final generation and
                        # prefix checks have not completed yet.
                        on_chunk(data)
                    segment_hash.update(data)
                    whole.update(data)
                    within += raw_size
            if within != length or segment_hash.hexdigest() != segment_sha:
                raise CoverageMismatch('segment checksum or byte coverage mismatch')
            offset += length
        if offset != size or whole.hexdigest() != current['sha256']:
            raise CoverageMismatch('whole-file checksum or byte coverage mismatch')
        previous = file_id
    if previous != item['file_id'] or offset != item['size'] or whole.hexdigest() != item['sha256']:
        raise CoverageMismatch('requested version does not match lineage leaf')
    return {'status': 'verified', 'size': offset, 'sha256': whole.hexdigest(),
            'scope': 'ClickHouse raw bytes for this explicit lineage only'}


def verify_replacement(client, blobs, item):
    result = verify_lineage(client, blobs, item, [{'file_id': item.get('file_id'), 'parent': None,
                              'start': 0, 'size': item.get('size'), 'sha256': item.get('sha256')}])
    result['scope'] = 'ClickHouse raw bytes for this replacement only'
    return result
