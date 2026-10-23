"""Deterministically assign immutable logical subtrees within worker budgets."""
import math


def plan_placement(subtrees, workers, previous=None):
    """Use largest-first normalized load, with stable worker and subtree ties.

    Estimates include the complete destination replica, rather than transfer
    deltas. Capacity is an admission constraint; an infeasible greedy result
    fails closed and does not claim that no possible packing exists.
    """
    resources=('storage_bytes','memory_bytes','load')
    def checked(row,key):
        value=row.get(key)
        if type(value) not in (int,float) or not math.isfinite(value) or value<0:
            raise ValueError('nonnegative finite resource required: '+key)
        return value
    worker_by_id={w['worker_id']:w for w in workers}
    if not workers or len(worker_by_id)!=len(workers):raise ValueError('unique workers required')
    subtree_by_id={s['subtree_id']:s for s in subtrees}
    if not subtrees or len(subtree_by_id)!=len(subtrees):raise ValueError('unique subtrees required')
    capacities={wid:{key:checked(w,key) for key in resources} for wid,w in worker_by_id.items()}
    for row in subtrees:
        for key in resources:checked(row,key)
    used={wid:{key:checked(worker_by_id[wid],'retained_'+key) if 'retained_'+key in worker_by_id[wid] else 0 for key in resources} for wid in worker_by_id};owners={}
    if any(used[wid][key]>capacities[wid][key] for wid in used for key in resources):raise ValueError('retained generations already exceed worker capacity')
    def weight(row):
        return max((row[key]/max(c[key] for c in capacities.values()) if max(c[key] for c in capacities.values()) else (math.inf if row[key] else 0)) for key in resources)
    for subtree in sorted(subtrees,key=lambda row:(-weight(row),row['subtree_id'])):
        options=[]
        for wid,caps in capacities.items():
            after={key:used[wid][key]+subtree[key] for key in resources}
            if any(after[key]>caps[key] for key in resources):continue
            ratios=[after[key]/caps[key] if caps[key] else 0 for key in resources]
            options.append((max(ratios),sum(ratios),0 if previous and previous.get(subtree['subtree_id'])==wid else 1,wid,after))
        if not options:raise ValueError('greedy placement cannot admit subtree '+subtree['subtree_id'])
        _,_,_,wid,after=min(options);owners[subtree['subtree_id']]=wid;used[wid]=after
    return dict(owners=owners,estimated_resources=used,policy='largest-normalized-first-stable-v1')
