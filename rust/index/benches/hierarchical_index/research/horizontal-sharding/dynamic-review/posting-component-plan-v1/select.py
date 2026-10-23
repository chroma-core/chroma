"""Select a typical owner from retained counts; never execute a replay."""
import hashlib
import itertools
import json
from pathlib import Path
import statistics
import tarfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
READY = ROOT / 'pilot/results/performance-readiness'
def sha(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()
counts_path = READY / 'fixed16-score-counter-distribution.json'
storage_path = READY / 'selected10m-owner-copy-accounting.json'
fixed_path = READY / 'fixed16-candidate-reuse-summary.json'
query_path = ROOT / 'fleet/prep10m/stage/test.jsonl'
counts = json.loads(counts_path.read_text())
storage = json.loads(storage_path.read_text())
fixed = json.loads(fixed_path.read_text())
owners = {r['subtree_id']: r for r in storage['rows']}
grouped = {}
for row in counts['rows']:
    grouped.setdefault(row['subtree_id'], []).append(row)
ranked = []
for owner, rows in grouped.items():
    if {r['query_id'] for r in rows} != set(fixed['query_ids']) or len(rows) != 16:
        continue
    median = statistics.median(r['unique_candidates_scanned'] for r in rows)
    ranked.append((abs(median-counts['unique_candidates_scanned']['median']),
                   owners[owner]['full_owned_copy_logical_bytes'], int(owner), median))
ranked.sort()
chosen = str(ranked[0][2])
queries = {}
with query_path.open() as stream:
    for line in stream:
        row = json.loads(line)
        if row['query_id'] in fixed['query_ids']:
            assert row['query_id'] not in queries
            queries[row['query_id']] = row['vector']
assert set(queries) == set(fixed['query_ids'])
trace = Path(fixed['source_archive'])
assert sha(trace) == fixed['source_archive_sha256']
requests = []
with tarfile.open(trace) as tar:
    lines = list(itertools.islice(tar.extractfile(fixed['member']), 16))
assert hashlib.sha256(b''.join(lines)).hexdigest() == fixed['selected_raw_lines_sha256']
for line in lines:
    row = json.loads(line)
    leaves = [r['node_id'] for r in row['result']['reached_leaves'] if r['subtree_id'] == chosen]
    metrics = [r['metrics'] for r in row['result']['worker_metrics']
               if r['subtree_id'] == chosen and r['op'] == 'score_selected_leaves']
    assert len(metrics) == 1 and len(leaves) == metrics[0]['leaves_scanned']
    assert len(set(leaves)) == len(leaves)
    assert len(queries[row['query_id']]) == 768
    requests.append(dict(query_id=row['query_id'], subtree_id=chosen,
                         expected_unique_candidates=metrics[0]['unique_candidates_scanned'],
                         payload=dict(op='score_selected_leaves', nodes=leaves,
                                      query=queries[row['query_id']], candidate_budget=1000,
                                      generation=metrics[0]['generation'])))
(HERE / 'requests.json').write_text(json.dumps(requests, separators=(',', ':'))+'\n')
result = dict(status='component-plan-only-not-runtime-admitted',
    rule='Require all fixed16 queries; minimize absolute owner median-U distance from global call median; break ties by checkpoint bytes then numeric owner ID. Selection uses no timing or speedup.',
    eligible_owners=len(ranked), global_call_median=counts['unique_candidates_scanned']['median'],
    selected_owner=owners[chosen], selected_owner_median=ranked[0][3],
    selected_unique_range=[min(r['expected_unique_candidates'] for r in requests), max(r['expected_unique_candidates'] for r in requests)],
    comparison_scope='Typical candidate-count component only; not representative tail latency, full-index quality, whole-query speed or 100B capacity.',
    requests_scope='Reconstructed payloads from exact held-out query vectors and ordered reached leaves; fresh request IDs and exact binary/snapshot admission still required. Not captured historical wire bodies.',
    inputs={str(p): sha(p) for p in (counts_path, storage_path, fixed_path, query_path, trace)},
    requests_sha256=sha(HERE/'requests.json'),
    validation='Sixteen query identities, dimensions, per-owner leaf counts and leaf uniqueness match saved records.',
    independent_review='Not independently reviewed; no implementation acceptance or runtime permission implied.')
(HERE/'selection.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps({k: result[k] for k in ('status','eligible_owners','global_call_median','selected_owner_median','selected_unique_range')}))
print(json.dumps(result['selected_owner']))
