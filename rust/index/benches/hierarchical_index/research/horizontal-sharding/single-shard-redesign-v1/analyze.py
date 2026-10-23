"""Summarize retained matched runs without treating unequal work as fixed parity."""
import argparse
import json
from pathlib import Path
from statistics import mean

parser = argparse.ArgumentParser()
parser.add_argument('results', type=Path, nargs='+')
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
runs = [(path, json.loads(path.read_text())) for path in args.results]
keys = ('input_sha256_operator_verified', 'rows', 'dimensions', 'workers', 'metric', 'add_batch_rows', 'settings')
reference_settings = {key: runs[0][1][key] for key in keys}
for path, run in runs:
    if {key: run[key] for key in keys} != reference_settings:
        raise SystemExit(f'Configuration or input mismatch: {path}')
    if run['validation']['exact_once_vectors'] != run['rows']:
        raise SystemExit(f'Incomplete validation: {path}')

rows = []
for path, run in runs:
    phases = run['phases']
    rows.append({
        'source_result': str(path), 'arm': run['arm'],
        'phase_wall_seconds': {name: value['wall_seconds'] for name, value in phases.items()},
        'phase_cpu_cores': {name: value['effective_cpu_cores'] for name, value in phases.items()},
        'build_and_save_seconds': run['build_and_save_total']['wall_seconds'],
        'total_seconds': run['total']['wall_seconds'],
        'peak_rss_bytes': run['total']['process_peak_rss_bytes'],
        'levels': run['shape']['level_node_counts'],
        'leaf_count': len(run['shape']['leaf_sizes']),
        'transferred_vectors': run['work']['transferred_vectors'],
        'navigate_distances': run['work']['navigate_distances'],
        'coordination': run['coordination'],
    })
summary = {'matched_configuration': reference_settings, 'runs_in_supplied_order': rows,
    'interpretation_limits': ['Two repetitions do not estimate a stable performance distribution.',
        'Different random trees and scheduler orders can do different work.',
        'Worker durations overlap; CPU cores and job occupancy are different measurements.',
        'Fresh 100K construction does not measure cache-capacity crossing or physical distribution.',
        'Query smoke is not a held-out quality comparison.']}
reference = [r for r in rows if r['arm'] == 'unsharded_reference']
candidate = [r for r in rows if r['arm'] == 'single_shard_owner']
if reference and candidate:
    summary['descriptive_build_ratio_candidate_over_reference'] = mean(r['build_and_save_seconds'] for r in candidate) / mean(r['build_and_save_seconds'] for r in reference)
args.output.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
