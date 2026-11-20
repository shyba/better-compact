"""Conservative native-backup retention *planning*, never deletion.

An inventory of known successful prefixes alone is not enough to prove absence
of external/unknown descendants. Callers must establish complete ownership and
repository enumeration separately; uncertainty yields no deletion candidates.
"""


def plan(successful, *, active=False, unknown_jobs=False, repository_complete=False,
         unknown_prefixes=True, restore_verified_ids=()):
    if active or unknown_jobs:
        return {'status':'blocked','reason':'active or unknown job may have dependencies','candidates':[]}
    if not repository_complete or unknown_prefixes:
        return {'status':'blocked','reason':'repository ownership and descendants are not fully known','candidates':[]}
    by_destination={j['destination']:j for j in successful}
    by_id={j['id']:j for j in successful}
    if len(by_destination)!=len(successful) or len(by_id)!=len(successful):
        return {'status':'blocked','reason':'duplicate backup identity','candidates':[]}
    roots={}
    depths={}
    for job in successful:
        current=job;seen=set();depth=0
        while current.get('base'):
            if current['id'] in seen:
                return {'status':'blocked','reason':'cyclic backup dependency','candidates':[]}
            seen.add(current['id']);depth+=1
            current=by_destination.get(current['base'])
            if current is None:
                return {'status':'blocked','reason':'missing backup dependency','candidates':[]}
        roots[job['id']]=current['id'];depths[job['id']]=depth
    fulls=sorted((j for j in successful if not j.get('base')),
                 key=lambda j:(j['started_at'],j['id']),reverse=True)
    if len(fulls)<2:
        return {'status':'blocked','reason':'fewer than two full chains','candidates':[]}
    if not restore_verified_ids or any(x not in by_id for x in restore_verified_ids):
        return {'status':'blocked','reason':'restore-verified recovery point unknown','candidates':[]}
    protected_roots={j['id'] for j in fulls[:2]}
    protected_roots.update(roots[job_id] for job_id in restore_verified_ids)
    # Protect every full base forever and every incremental in protected chains.
    # Remaining chains are removed leaf-first; no retained child loses its base.
    candidates=[j for j in successful if j.get('base') and roots[j['id']] not in protected_roots]
    candidates.sort(key=lambda j:(depths[j['id']],j['id']),reverse=True)
    return {'status':'dry-run','reason':'complete owned graph; deletion not implemented',
            'candidates':[j['id'] for j in candidates],
            'protected_fulls':[j['id'] for j in fulls],
            'protected_chain_roots':sorted(protected_roots)}
