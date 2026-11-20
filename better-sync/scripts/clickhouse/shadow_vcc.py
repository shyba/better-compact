"""VCC shadow compilation with explicit mapping back to raw occurrences.

This is a comparison primitive, not a PostgreSQL writer or production
projection. It consumes one already-verified, complete generation at once;
callers must enforce a conservative memory budget and discard all results if
compilation or mapping fails.
"""
from collections import defaultdict


def compile_vcc(compiler, kind, path, events):
    if kind not in ('pi-jsonl','codex-jsonl','codex-jsonl-sessions'):
        raise ValueError('unsupported VCC source kind')
    records=[]
    for index,event in enumerate(events):
        if not isinstance(event.get('value'),dict) or not isinstance(event.get('id'),str) or type(event.get('offset')) is not int:
            raise ValueError('invalid verified shadow occurrence')
        record=dict(event['value'])
        record['_ln']=index+1
        record.setdefault('ordinal',None)
        records.append(record)
    if kind=='pi-jsonl':
        session_positions=[index for index,r in enumerate(records) if r.get('type')=='session']
        if session_positions and session_positions!=[0]:
            raise ValueError('ambiguous Pi session header position')
        blocks,stats=compiler.build_ir_opencode(records,path)
        positions={index:event for index,event in enumerate(events)}
    else:
        positions={}
        for index,record in enumerate(records):
            key=record.get('ordinal')
            if key is None:key='L'+str(index+1)
            try:
                if key in positions:raise ValueError('ambiguous duplicate Codex ordinal')
                positions[key]=events[index]
            except TypeError:
                raise ValueError('unhashable Codex ordinal') from None
        blocks,stats=compiler.build_ir(records,path)
    per_record=defaultdict(int);result=[]
    for seq,block in enumerate(blocks):
        try:event=positions.get(block.ord)
        except TypeError:raise ValueError('unhashable VCC block ordinal') from None
        if event is None:raise ValueError('VCC block cannot be mapped to a verified occurrence')
        if not isinstance(block.kind,str) or not isinstance(block.text,str) or not isinstance(block.brief,str):
            raise ValueError('invalid VCC block output')
        if type(block.wnum) is not int:raise ValueError('non-integer VCC window')
        index=per_record[event['id']];per_record[event['id']]+=1
        result.append({'sequence':seq,'event_id':event['id'],'record_offset':event['offset'],
                       'block_index':index,'kind':block.kind,'window':block.wnum,
                       'text':block.text,'brief':block.brief,'timestamp':block.ts})
    return result,stats
