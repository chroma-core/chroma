"""Retain completed phases and failed balance snapshots without implying a valid saved index."""
import json
import runpy
import sys
from pathlib import Path

phase = runpy.run_path(str(Path(__file__).with_name('summarize-distributed-construction.py')))['phase']


def summarize(path):
    events = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    failures = [event for event in events if event['event'] == 'balance_failed']
    assert len(failures) == 1, 'This analysis requires exactly one failed balance.'
    cycles = []
    for event in events:
        if event['event'] != 'cycle_complete':
            continue
        cycle = event['cycle']
        row = {key: cycle[key] for key in ['rows_before', 'rows_after', 'add_seconds', 'balance_seconds', 'placement_seconds']}
        row['insertion'] = phase(cycle['before_add'], cycle['after_add'])
        row['balance'] = phase(cycle['before_balance'], cycle['before_placement'])
        row['owners_after_placement'] = {owner['owner']: owner['vectors'] for owner in cycle['after_placement']}
        cycles.append(row)
    failure = failures[0]
    adds = [event for event in events if event['event'] == 'add_complete' and event['rows_after'] == failure['rows']]
    starts = [event for event in events if event['event'] == 'balance_started' and event['rows'] == failure['rows']]
    assert len(adds) == len(starts) == 1
    add, start = adds[0], starts[0]
    assert add['monotonic_ns'] < start['monotonic_ns'] < failure['monotonic_ns']
    assert not cycles or cycles[-1]['rows_after'] == add['rows_before']
    # A failed statistics request is not a zero counter snapshot.
    assert set(failure['owners']) == {'Ok'}, 'Failed owner statistics: preserve raw evidence without computing deltas.'
    return dict(complete=False, cycles=cycles,
                final_add=dict(rows_after=add['rows_after'], wall_seconds=add['seconds'],
                               insertion=phase(add['before'], add['after']), rpc_before=add['rpc_before'], rpc_after=add['rpc_after']),
                failed_balance=dict(wall_seconds=failure['seconds'], error=failure['error'],
                                    partial=phase(start['owners'], failure['owners']['Ok'])))


if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), indent=2))
