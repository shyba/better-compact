import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest

from receiver_lock import authority_lock

AUTH='test-authority-000001'


class ReceiverLockTests(unittest.TestCase):
    def test_process_exclusion_and_crash_release_preserve_lock_file(self):
        with tempfile.TemporaryDirectory() as root:
            code='from receiver_lock import authority_lock; import sys,time\nwith authority_lock(sys.argv[1],sys.argv[2]):\n print("ready",flush=True); time.sleep(30)'
            env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parent)}
            child=subprocess.Popen([sys.executable,'-c',code,AUTH,root],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
            try:
                self.assertTrue(select.select([child.stdout],[],[],3)[0]);self.assertEqual(child.stdout.readline().strip(),'ready')
                with self.assertRaisesRegex(RuntimeError,'already locked'):
                    with authority_lock(AUTH,root):pass
                child.kill();child.wait(timeout=3)
                with authority_lock(AUTH,root):pass
                self.assertEqual(len(list(Path(root).glob('*.lock'))),1)
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=3)
                child.stdout.close();child.stderr.close()
    def test_different_authorities_can_lock_separately(self):
        with tempfile.TemporaryDirectory() as root:
            with authority_lock(AUTH,root), authority_lock('different-authority-02',root):pass
    def test_insecure_or_symlink_root_rejected(self):
        with tempfile.TemporaryDirectory() as parent:
            root=Path(parent)/'loose';root.mkdir(mode=0o755);root.chmod(0o755)
            with self.assertRaises(ValueError):
                with authority_lock(AUTH,root):pass
            link=Path(parent)/'link';link.symlink_to(root,target_is_directory=True)
            with self.assertRaises(ValueError):
                with authority_lock(AUTH,link):pass


if __name__=='__main__':unittest.main()
