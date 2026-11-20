"""Committed whole-file version metadata for the staged ClickHouse receiver.

This module does not expose an HTTP endpoint. The caller must serialize all
writers sharing this namespace: ClickHouse primary keys are not uniqueness
constraints. Receipts are inserted only after byte-exact lineage verification.
"""
import json
from raw_coverage import verify_lineage, CoverageUnknown, CoverageMismatch, MAX_VERSIONS, _validate_item

FIELDS=frozenset(('source_id','file_id','path','start','size','sha256','parent'))


def validate_generation(item):
    if not isinstance(item,dict) or set(item)!=FIELDS:
        raise ValueError('generation metadata fields differ from contract')
    _validate_item(item)
    start=item['start'];parent=item['parent']
    if type(start) is not int or start<0 or start>item['size']:
        raise ValueError('invalid generation start')
    if parent is None:
        if start!=0:raise ValueError('replacement generation must start at zero')
    elif not isinstance(parent,str) or not 1<=len(parent)<=260:
        raise ValueError('append requires a valid parent')
    return item


def lineage(client,item):
    """Read immutable ancestors by exact identity, never by maximum end."""
    validate_generation(item)
    chain=[item];seen={item['file_id']};parent=item['parent']
    while parent is not None:
        if parent in seen:raise CoverageMismatch('cyclic generation lineage')
        if len(chain)>=MAX_VERSIONS:raise CoverageUnknown('generation lineage exceeds verification limit')
        seen.add(parent)
        rows=client.execute('''SELECT metadata FROM sync_generations FINAL
            WHERE source_id=%(source)s AND file_id=%(file)s''',
            {'source':item['source_id'],'file':parent})
        if len(rows)!=1:raise CoverageUnknown('parent generation receipt missing or ambiguous')
        ancestor=validate_generation(json.loads(rows[0][0]))
        if (ancestor['source_id']!=item['source_id'] or ancestor['path']!=item['path'] or
            ancestor['file_id']!=parent):
            raise CoverageMismatch('generation ancestor identity mismatch')
        chain.append(ancestor);parent=ancestor['parent']
    chain.reverse()
    return chain


def commit_generation(client,blobs,item):
    """Verify all ancestor bytes, then insert the immutable generation receipt."""
    validate_generation(item)
    encoded=json.dumps(item,sort_keys=True,separators=(',',':'))
    rows=client.execute('''SELECT metadata FROM sync_generations FINAL
        WHERE source_id=%(source)s AND file_id=%(file)s''',
        {'source':item['source_id'],'file':item['file_id']})
    if len(rows)>1:raise CoverageMismatch('ambiguous generation receipt')
    if rows:
        if rows[0][0]!=encoded:raise CoverageMismatch('immutable generation identity conflict')
        return {'committed':True,**item}
    versions=lineage(client,item)
    verify_lineage(client,blobs,item,versions)
    # Commit last: the native insert receipt only represents raw ClickHouse
    # state, not an independently verified S3 backup or projection checkpoint.
    client.execute('''INSERT INTO sync_generations (source_id,file_id,path,metadata) VALUES''',
                   [(item['source_id'],item['file_id'],item['path'],encoded)])
    return {'committed':True,**item}
