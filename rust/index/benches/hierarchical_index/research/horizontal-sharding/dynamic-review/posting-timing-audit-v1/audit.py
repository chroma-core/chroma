"""Audit retained timing evidence without running search or changing its sources."""
import ast
import hashlib
import itertools
import json
from pathlib import Path
import tarfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
CHECKOUT = Path('/private/tmp/chroma-hierarchical-spann-sharding')
FIELDS = ('load_ns', 'quantize_ns', 'distance_ns', 'dedup_ns', 'select_ns', 'sort_ns')

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def profiles(value):
    if isinstance(value, dict):
        if 'posting_selection_profile' in value:
            yield value['posting_selection_profile']
        for child in value.values():
            yield from profiles(child)
    elif isinstance(value, list):
        for child in value:
            yield from profiles(child)

native = ROOT / 'dynamic-review/posting-candidate-selection-v2/runtime-attempt1/native.json'
archive = ROOT / 'pilot/results/fineweb10m-heldout-v2-small.tar.gz'
fixed = ROOT / 'pilot/results/performance-readiness/fixed16-candidate-reuse-summary.json'
expected = json.loads(fixed.read_text())
assert digest(archive) == expected['source_archive_sha256']
with tarfile.open(archive) as tar:
    stream = tar.extractfile(expected['member'])
    lines = list(itertools.islice(stream, 16))
assert hashlib.sha256(b''.join(lines)).hexdigest() == expected['selected_raw_lines_sha256']
records = [json.loads(line) for line in lines]
assert [r['query_id'] for r in records] == expected['query_ids']

samples = []
for operation in json.loads(native.read_text()):
    response = operation['response']
    profile = response.get('posting_selection_profile')
    if profile is None:
        continue
    assert operation['request']['op'] == 'score_selected_leaves'
    assert all(type(profile[k]) is int and profile[k] >= 0 for k in FIELDS)
    stage_ns = sum(profile[k] for k in FIELDS)
    envelope_ns = round(response['elapsed_ms'] * 1_000_000)
    assert stage_ns <= envelope_ns
    samples.append(dict(mode=operation['mode'], subtree_id=operation['subtree_id'],
                        request_id=response['request_id'], stages_ns={k: profile[k] for k in FIELDS},
                        envelope_ns=envelope_ns, unassigned_ns=envelope_ns-stage_ns))
assert len(samples) == 16

# Evaluate only the two saved-report projection expressions, never the benchmark module.
benchmark = CHECKOUT / 'tools/hierarchical_sharding/subtree_benchmark.py'
tree = ast.parse(benchmark.read_text())
projection_checks = {}
sentinel = {'elapsed_ms': 1, 'adapter_timing': {}, 'retained_postings': {},
            'posting_selection_policy': 'full-sort-inclusive',
            'posting_selection_profile': {'load_ns': 17}, 'candidates': []}
for function in tree.body:
    if not isinstance(function, ast.FunctionDef) or function.name not in ('compact', 'load_process'):
        continue
    for node in ast.walk(function):
        if isinstance(node, ast.DictComp) and 'op' in ast.unparse(node) and "op['result']" in ast.unparse(node):
            value = eval(compile(ast.Expression(node), str(benchmark), 'eval'),
                         {'__builtins__': {}}, {'op': {'result': sentinel}})
            projection_checks[function.name] = dict(line=node.lineno, retained_keys=sorted(value),
                retains_posting_profile='posting_selection_profile' in value)
assert projection_checks['compact']['retains_posting_profile']
assert not projection_checks['load_process']['retains_posting_profile']

sources = [benchmark] + [CHECKOUT / p for p in (
    'rust/index/benches/hierarchical_index/reader/reader.rs',
    'rust/index/benches/hierarchical_index/reader/remote.rs',
    'rust/index/benches/hierarchical_index/reader/persistance.rs',
    'rust/index/benches/hierarchical_index/reader/selection.rs',
    'rust/index/benches/hierarchical_spann_shard.rs',
    'tools/hierarchical_sharding/subtree_transport.py',
    'tools/hierarchical_sharding/owner_reduction.py')]
result = dict(status='passed-offline-evidence-and-reporting-audit',
    source_hashes={str(p): digest(p) for p in sources},
    evidence_hashes={str(p): digest(p) for p in (native, archive, fixed)},
    fixed16_query_ids=expected['query_ids'],
    real_trace_profile_count=sum(len(list(profiles(r))) for r in records),
    retained_native_profile_count=len(samples), native_samples=samples,
    projection_checks=projection_checks,
    interpretation='Tiny retained native samples check timer containment only; they cannot rank real-data bottlenecks or establish speedup. Stage timers are wall-clock intervals, not CPU time. Concurrent worker times must not be summed into query latency.',
    independent_review='Not independently reviewed; analysis only. Existing accepted implementation and runtime receipts remain unchanged.',
    next_decision='Use compact held-out reporting for a future representative profile replay; repair concurrent-load projection only if that measurement is selected. No new search or cloud operation is necessary to establish this reporting gap.')
(HERE / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: result[k] for k in ('status', 'real_trace_profile_count', 'retained_native_profile_count', 'projection_checks')}))
