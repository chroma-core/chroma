"""Read coarse save profiles and balance deltas without summing nested scopes."""
import json
import sys
from pathlib import Path


def seconds(value):
    if isinstance(value, dict):
        return {k.removesuffix('_nanos') + '_seconds' if k.endswith('_nanos') else k:
                v / 1e9 if k.endswith('_nanos') else seconds(v)
                for k, v in value.items()}
    return value


def summarize(path):
    data = json.loads(Path(path).read_text())
    cycles = []
    for cycle in data['cycles']:
        before = {o['owner']: o for o in cycle['before_balance']}
        after = {o['owner']: o for o in cycle['before_placement']}
        assert all(o['vectors'] == 0 for owner, o in after.items() if owner != 1)
        scopes = {}
        for name, counters in after[1]['work']['inclusive_scopes'].items():
            if name.startswith('LocalTransfer'):
                old = before[1]['work']['inclusive_scopes'].get(name, {})
                scopes[name] = seconds({k: v - old.get(k, 0) for k, v in counters.items() if not k.startswith('max_')})
        cycles.append(dict(rows=cycle['rows_after'], producer_dependency_queue_visits=cycle.get('scheduler_producer_dependencies'), reassignment_coalescing=cycle.get('reassignment_coalescing'), transfer_inclusive_scopes=scopes))
    return dict(input=str(path), save_seconds=data['save_seconds'], save=seconds(data.get('save_profile')), cycles=cycles, interpretation=['Transfer wall times sum concurrent operations and can exceed elapsed balance.', 'Cleanup includes terminalization; do not sum nested scopes.', 'Store sync times belong inside save phases, not in addition to them.', 'Dependency visits are not unique jobs or waiting duration.', 'Missing profile fields mean unavailable, not zero.', 'Coalescing counts queued jobs combined and inter-request duplicate IDs; these are not guaranteed execution rows saved.'])


if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), indent=2))
