"""Reject known incompatible binaries before any cloud lifecycle operation."""
import hashlib
import json
from pathlib import Path

BINARY_SHA = '67439496d6d5eb33d7210a347a90b68f934ce7dac012acc35fa99ef485a7d528'

def verify_file(root, pin):
    path = root / pin['path']
    if path.is_symlink() or not path.is_file():
        raise ValueError('missing or symlink input')
    if path.stat().st_size != pin['bytes'] or hashlib.sha256(path.read_bytes()).hexdigest() != pin['sha256']:
        raise ValueError('input integrity mismatch')
    return path

def admit(spec, capabilities, shape):
    if spec['policy'] != {'navigation': 'fp', 'beam': 32, 'k': 100, 'rerank_candidates': 400}:
        raise ValueError('fixed policy differs')
    if spec['rows'] != 2000000 or spec['queries'] != 16:
        raise ValueError('fixed input scope differs')
    if capabilities['binary_sha256'] != spec['binary_sha256']:
        raise ValueError('binary binding differs')
    if shape['nodes'] != shape['internal_nodes'] + shape['leaves']:
        raise ValueError('inconsistent topology')
    if not 1 <= shape['depth'] <= 16 or not 32 < shape['leaves'] <= 4096:
        raise ValueError('missing depth or insufficient leaf competition')
    if shape['nodes'] > capabilities['fp_node_cap'] or shape['nodes'] * 768 * 4 > capabilities['fp_payload_cap_bytes']:
        raise ValueError('known FP preparation cap rejects tree')
    if spec['rows'] > capabilities['fp_row_cap'] or spec['rows'] > capabilities['stage_row_cap']:
        raise ValueError('known reader row cap rejects tree')
    if capabilities['manifest_schema'] != spec['manifest_schema']:
        raise ValueError('construction and reader manifest schemas differ')
    if capabilities['trace_kind'] != 'bounded-gt-intersections-v1':
        raise ValueError('full candidate trace is not admitted for2M')
    return {'decision': 'admitted-local-contract-only', 'internal_pruning_claim': False,
            'question': 'leaf-selection and candidate economics', 'remote_admission_required': True}

if __name__ == '__main__':
    here = Path(__file__).resolve().parent
    spec = json.loads((here / 'runtime-spec.proposed.json').read_text())
    caps = json.loads((here / 'retained-capabilities.json').read_text())
    try:
        admit(spec, caps, {'nodes': 1780, 'internal_nodes': 28, 'leaves': 1752, 'depth': 3})
    except ValueError as error:
        print(json.dumps({'decision': 'blocked-before-cloud', 'error': str(error)}))
        raise SystemExit(2)

def validate_summary(reply, query_id, supplied_gt, snapshot_sha):
    if reply.get('query_id') != query_id or reply.get('snapshot_sha256') != snapshot_sha or reply.get('error'):
        raise ValueError('reply identity mismatch')
    gt=reply.get('gt',{});expected=set(supplied_gt)
    if len(supplied_gt)!=100 or len(expected)!=100 or gt.get('complete') is not True or gt.get('ground_truth_count')!=100:
        raise ValueError('GT cohort or completeness')
    sets=[]
    for key in ('reached_ids','retained_ids','returned_ids'):
        ids=gt.get(key,[])
        if len(ids)!=len(set(ids)) or not set(ids)<=expected:raise ValueError('stage IDs outside supplied GT')
        sets.append(set(ids))
    reached,retained,returned=sets
    if not returned<=retained<=reached or returned != (set(reply.get('ids',[])) & expected):raise ValueError('stage membership inconsistent')
    if len(reply.get('ids',[]))!=100 or len(set(reply['ids']))!=100 or len(reply.get('distance_bits',[]))!=100:raise ValueError('returned hit count')
    counts=dict(navigation_loss=100-len(reached),trim_loss=len(reached)-len(retained),exact_loss=len(retained)-len(returned))
    if [gt.get(k) for k in ('navigation_missing','trim_missing','exact_missing')]!=list(counts.values()):raise ValueError('stage arithmetic differs')
    trace=reply.get('work',{}).get('navigation_level_trace')
    if not trace or trace.get('complete') is not True or len(trace.get('levels',[]))>16:raise ValueError('incomplete navigation trace')
    if not 0<reply.get('selected_leaves',0)<=4096 or not 0<reply.get('posting_rows',0)<=4000000:raise ValueError('work cap exceeded')
    return counts
