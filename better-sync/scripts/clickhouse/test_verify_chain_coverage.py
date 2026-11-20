import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from verify_chain_coverage import retained_intents

SOURCE='a'*32;AUTH='test-authority-000001'


class RetainedIntentTests(unittest.TestCase):
    def test_only_committed_intents_are_collected(self):
        with tempfile.TemporaryDirectory() as root:
            base=Path(root)/'pilot'/'outbox'/'x';base.mkdir(parents=True)
            good={'authority':AUTH,'source_id':SOURCE,'file_id':'v','path':'p','start':0,'size':1,
                  'sha256':'0'*64,'parent':None,'committed_at':1}
            (base/'a.generation.json').write_text(json.dumps(good))
            (base/'b.generation.json').write_text(json.dumps({**good,'committed_at':None}))
            (base/'c.generation.json').write_text('not-json')
            (base/'d.generation.json').write_text(json.dumps({**good,'committed_at':'x'}))
            found=retained_intents([Path(root)])
            self.assertEqual(len(found),1)
            self.assertEqual(found[0]['file_id'],'v');self.assertEqual(found[0]['intent_path'],str(base/'a.generation.json'));self.assertEqual(found[0]['path'],'p')


if __name__=='__main__':unittest.main()
