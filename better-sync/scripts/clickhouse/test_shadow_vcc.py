import importlib.util
from pathlib import Path
import unittest

from projection_records import complete_records
from shadow_vcc import compile_vcc

ROOT=Path(__file__).resolve().parents[4]
spec=importlib.util.spec_from_file_location('codex_vcc_shadow_test',ROOT/'codex-vcc.py')
compiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler)


def events(raw):return complete_records('source','authority\0root',0,raw)[0]


class ShadowVCCTests(unittest.TestCase):
    def test_pi_equal_payload_messages_keep_distinct_occurrences(self):
        # One session header, then two identical message records.
        raw=b'{"type":"session","id":"s"}\n'+b'{"type":"message","message":{"role":"user","content":[{"text":"same"}]}}\n'*2
        result,_=compile_vcc(compiler,'pi-jsonl','session.jsonl',events(raw))
        users=[b for b in result if b['kind']=='user']
        self.assertEqual(len(users),2);self.assertEqual(users[0]['text'],users[1]['text'])
        self.assertNotEqual(users[0]['event_id'],users[1]['event_id'])
        self.assertNotEqual(users[0]['record_offset'],users[1]['record_offset'])
    def test_codex_matches_existing_compiler_in_order_and_text(self):
        raw=(b'{"type":"session_meta","ordinal":1,"payload":{"id":"s","cwd":"/x"}}\n'
             b'{"type":"response_item","ordinal":2,"payload":{"type":"message","role":"user","content":[{"text":"hello"}]}}\n'
             b'{"type":"compacted","ordinal":3,"payload":{"window_number":5}}\n'
             b'{"type":"response_item","ordinal":4,"payload":{"type":"message","role":"assistant","content":[{"text":"done"}]}}\n')
        rows=events(raw);shadow,stats=compile_vcc(compiler,'codex-jsonl','session.jsonl',rows)
        records=[]
        for i,event in enumerate(rows):
            r=dict(event['value']);r['_ln']=i+1;r.setdefault('ordinal',None);records.append(r)
        baseline,baseline_stats=compiler.build_ir(records,'session.jsonl')
        self.assertEqual([(b['kind'],b['text'],b['window'],b['timestamp']) for b in shadow],
                         [(b.kind,b.text,b.wnum,b.ts) for b in baseline])
        self.assertEqual(stats,baseline_stats)
    def test_ambiguous_ordinal_and_header_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'duplicate Codex ordinal'):
            compile_vcc(compiler,'codex-jsonl','x',events(b'{"ordinal":1}\n{"ordinal":1}\n'))
        with self.assertRaisesRegex(ValueError,'session header'):
            compile_vcc(compiler,'pi-jsonl','x',events(b'{"type":"message"}\n{"type":"session"}\n'))
    def test_unsupported_kind_never_claims_projection(self):
        with self.assertRaises(ValueError):compile_vcc(compiler,'unknown','x',[])


if __name__=='__main__':unittest.main()
