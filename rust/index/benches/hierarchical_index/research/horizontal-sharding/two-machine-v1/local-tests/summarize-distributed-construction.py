"""Attribute a two-process construction run without treating owner-local counters as cluster totals."""
import json
import sys
from pathlib import Path


def delta(after, before):
    result = {key: value - before.get(key, 0) for key, value in after.items() if not key.startswith('max_')}
    assert all(value >= 0 for value in result.values()), 'Cumulative work counter decreased.'
    return result


def phase(before_rows, after_rows):
    before = {row['owner']: row for row in before_rows}
    after = {row['owner']: row for row in after_rows}
    assert set(before) == set(after) == {1, 2}, 'This analysis requires the verified two-owner/two-process fixture.'
    owners = {}
    for owner in sorted(after):
        old, new = before[owner], after[owner]
        kernel = delta(new['clustering_process'], old['clustering_process'])
        cpu = new['process_cpu_seconds'] - old['process_cpu_seconds']
        assert cpu >= 0
        activity = {key: new[key] - old[key] for key in ['jobs_started', 'jobs_completed', 'splits', 'transfers', 'distances', 'expansions', 'vector_fetches']}
        owners[owner] = dict(process_cpu_seconds=cpu, activity=activity, clustering=kernel,
                             clustering_cpu_seconds=kernel['thread_cpu_nanos']/1e9,
                             other_cpu_seconds=cpu-kernel['thread_cpu_nanos']/1e9,
                             vectors_before=old['vectors'], vectors_after=new['vectors'],
                             work=delta(new['work']['counters'], old['work']['counters']),
                             inclusive_scopes={name: delta(fields, old['work']['inclusive_scopes'].get(name, {}))
                                               for name, fields in new['work']['inclusive_scopes'].items()},
                             cumulative_process_peak_rss_bytes=new['process_peak_rss_bytes'])
    return dict(owners=owners, process_cpu_seconds=sum(row['process_cpu_seconds'] for row in owners.values()),
                clustering_cpu_seconds=sum(row['clustering_cpu_seconds'] for row in owners.values()),
                other_cpu_seconds=sum(row['other_cpu_seconds'] for row in owners.values()))


def summarize(path):
    data = json.loads(Path(path).read_text())
    assert 'arm' not in data, 'Use the reference summarizer for the reference arm.'
    cycles = []
    for cycle in data['cycles']:
        insertion = phase(cycle['before_add'], cycle['after_add'])
        balance = phase(cycle['before_balance'], cycle['before_placement'])
        after = {row['owner']: row['vectors'] for row in cycle['after_placement']}
        assert sum(after.values()) == cycle['rows_after']
        cycles.append(dict(rows_before=cycle['rows_before'], rows_after=cycle['rows_after'],
                           add_seconds=cycle['add_seconds'], balance_seconds=cycle['balance_seconds'],
                           placement_seconds=cycle['placement_seconds'], insertion=insertion, balance=balance,
                           owner_vectors_after_placement=after, rpc_phases=cycle['rpc_phases'],
                           scheduler_producer_dependencies=cycle.get('scheduler_producer_dependencies'),
                           reassignment_coalescing=cycle.get('reassignment_coalescing')))
    totals = {key: sum(cycle[key] for cycle in cycles) for key in ['add_seconds', 'balance_seconds', 'placement_seconds']}
    for name in ['insertion', 'balance']:
        totals[name] = {key: sum(cycle[name][key] for cycle in cycles)
                        for key in ['process_cpu_seconds', 'clustering_cpu_seconds', 'other_cpu_seconds']}
    return dict(input_result=str(path), rows=data['rows'], cycles=cycles, totals=totals,
                settings=data['settings'], final_shape=data['routing_shape'],
                save_seconds=data['save_seconds'], construction_and_save_seconds=data['construction_and_save_seconds'],
                validated=data['validation_complete'], minimum_owner_share=data['minimum_owner_share'],
                interpretation=[
                    'Require independent receipts proving one owner process on each physical machine before interpreting summed CPU.',
                    'Owner process CPU spans sequential before/after snapshots and includes boundary/control work; wall balance uses the coordinator timer.',
                    'Clustering process deltas are summed once per owner. The coordinator-only clustering_local_process_delta is deliberately not used.',
                    'Other CPU is process CPU minus clustering thread CPU over these observation boundaries, not an isolated exclusive scope.',
                    'Inclusive scopes overlap and must not be added to elapsed time.',
                    'RPC phase totals already count each two-host connection once; do not add peer transport counters again.',
                    'Per-process peak RSS is cumulative. Summing peaks does not establish simultaneous cluster peak memory.',
                ])


if __name__ == '__main__':
    print(json.dumps(summarize(sys.argv[1]), indent=2))
