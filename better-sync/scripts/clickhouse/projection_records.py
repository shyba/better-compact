"""Pure, position-stable JSONL record extraction for the future projection.

This module is not a projection worker and never advances a database checkpoint.
Callers must provide a verified immutable generation and retain incomplete or
poison bytes. Identical payloads at different positions are different records.
"""
import hashlib
import json

MAX_RECORD_BYTES = 16 * 1024 * 1024


class PoisonRecord(ValueError):
    def __init__(self, offset, reason):
        self.offset = offset
        super().__init__(f'JSONL record at byte {offset}: {reason}')


def occurrence_id(source_id, generation_id, offset):
    if not isinstance(source_id,str) or not source_id or not isinstance(generation_id,str) or not generation_id:
        raise ValueError('source and generation identities are required')
    if type(offset) is not int or offset < 0:
        raise ValueError('invalid record offset')
    value=json.dumps([source_id,generation_id,offset],ensure_ascii=True,separators=(',',':')).encode()
    return hashlib.sha256(value).hexdigest()


def _invalid_constant(value):
    raise ValueError('non-standard JSON number')


def complete_records(source_id, generation_id, start, data):
    """Return (complete records, trailing bytes, trailing absolute offset).

    `start` is the absolute position of data[0], including any saved trailing
    bytes from a prior read. A poison or oversized record raises instead of
    returning partial success, so callers must not advance past it silently.
    """
    occurrence_id(source_id,generation_id,start)  # validate identities/offset
    if not isinstance(data,bytes):raise TypeError('raw bytes required')
    records=[];position=0
    while True:
        end=data.find(b'\n',position)
        if end<0:
            trailing=data[position:]
            if len(trailing)>MAX_RECORD_BYTES:raise PoisonRecord(start+position,'trailing record exceeds limit')
            return records,trailing,start+position
        raw=data[position:end+1]
        if len(raw)>MAX_RECORD_BYTES:raise PoisonRecord(start+position,'record exceeds limit')
        try:
            value=json.loads(raw.decode('utf-8'),parse_constant=_invalid_constant)
        except (UnicodeDecodeError,ValueError):
            raise PoisonRecord(start+position,'invalid UTF-8 or JSON') from None
        if not isinstance(value,dict):raise PoisonRecord(start+position,'JSON record is not an object')
        offset=start+position
        records.append({'id':occurrence_id(source_id,generation_id,offset),'offset':offset,
                        'size':len(raw),'raw':raw,'value':value})
        position=end+1
