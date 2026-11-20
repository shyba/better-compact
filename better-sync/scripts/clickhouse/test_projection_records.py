import unittest
from projection_records import complete_records, occurrence_id, PoisonRecord


class ProjectionRecordTests(unittest.TestCase):
    def test_duplicate_payload_positions_remain_distinct_and_replay_is_stable(self):
        data=b'{"type":"message","text":"same"}\n'*2
        records,trailing,offset=complete_records('source','generation',0,data)
        self.assertEqual(trailing,b'');self.assertEqual(offset,len(data))
        self.assertNotEqual(records[0]['id'],records[1]['id'])
        self.assertEqual(records,complete_records('source','generation',0,data)[0])
        self.assertNotEqual(records[0]['id'],occurrence_id('source','rewrite',0))
    def test_partial_line_preserved_until_completion(self):
        first=b'{"type":"session"}\n{"type":"mess'
        records,trailing,start=complete_records('source','generation',0,first)
        self.assertEqual(len(records),1);self.assertEqual(start,len(first)-len(trailing))
        second,trailing,end=complete_records('source','generation',start,trailing+b'age"}\n')
        self.assertEqual(len(second),1);self.assertEqual(second[0]['offset'],start)
        self.assertEqual(trailing,b'');self.assertGreater(end,start)
    def test_invalid_line_does_not_return_advanced_records(self):
        data=b'{"valid":true}\nnot-json\n'
        with self.assertRaises(PoisonRecord) as caught:complete_records('source','generation',0,data)
        self.assertEqual(caught.exception.offset,len(b'{"valid":true}\n'))
    def test_invalid_utf8_and_nonobject_are_poison(self):
        for data in (b'\xff\n',b'[]\n',b'\n',b'{"x":NaN}\n'):
            with self.assertRaises(PoisonRecord):complete_records('source','generation',12,data)
    def test_binary_trailing_data_is_preserved_exactly(self):
        raw=b'{"incomplete":"\xff'
        self.assertEqual(complete_records('source','generation',5,raw),( [],raw,5))


if __name__=='__main__':unittest.main()
