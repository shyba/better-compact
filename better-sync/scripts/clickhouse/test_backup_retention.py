import unittest
from backup_retention import plan


def job(name, base=None, started=0):
    return {'id':name,'destination':'repo/'+name+'/','base':None if base is None else 'repo/'+base+'/',
            'started_at':started}


class RetentionTests(unittest.TestCase):
    def test_unknown_and_active_never_produce_candidates(self):
        jobs=[job('a'),job('b',started=1),job('c',started=2)]
        for kwargs in ({'active':True,'repository_complete':True,'unknown_prefixes':False},
                       {'unknown_jobs':True,'repository_complete':True,'unknown_prefixes':False},
                       {'repository_complete':False,'unknown_prefixes':False},
                       {'repository_complete':True,'unknown_prefixes':True}):
            self.assertEqual(plan(jobs,restore_verified_ids=['b'],**kwargs)['candidates'],[])
    def test_protect_fulls_newest_chains_and_restored_chain(self):
        jobs=[job('a',started=1),job('a1','a'),job('a2','a1'),job('b',started=2),job('b1','b'),
              job('c',started=3),job('c1','c'),job('d',started=4),job('d1','d')]
        result=plan(jobs,repository_complete=True,unknown_prefixes=False,restore_verified_ids=['b1'])
        self.assertEqual(result['status'],'dry-run')
        self.assertEqual(result['candidates'],['a2','a1'])
        self.assertEqual(set(result['protected_fulls']),{'a','b','c','d'})
    def test_insufficient_fulls_or_missing_restore_proof_preserves_all(self):
        jobs=[job('a'),job('a1','a')]
        self.assertEqual(plan(jobs,repository_complete=True,unknown_prefixes=False,restore_verified_ids=['a'])['candidates'],[])
        jobs.append(job('b',started=1))
        self.assertEqual(plan(jobs,repository_complete=True,unknown_prefixes=False)['candidates'],[])
    def test_missing_dependency_cycle_and_duplicate_fail_closed(self):
        bad=[job('a'),job('b',started=1),job('c','missing')]
        self.assertIn('missing',plan(bad,repository_complete=True,unknown_prefixes=False,restore_verified_ids=['a'])['reason'])
        cyclic=[job('a'),job('b',started=1),job('c','d'),job('d','c')]
        self.assertIn('cyclic',plan(cyclic,repository_complete=True,unknown_prefixes=False,restore_verified_ids=['a'])['reason'])
        self.assertIn('duplicate',plan([job('a'),job('a')],repository_complete=True,unknown_prefixes=False,restore_verified_ids=['a'])['reason'])


if __name__=='__main__':unittest.main()
