"""One explicitly versioned query vector per adapter batch; no persistent cache."""
import copy
import json
import math

FORMAT='shared-vector-v1'


def vector(value,dimension=None):
    if not isinstance(value,list) or not 1<=len(value)<=16384 or (dimension is not None and (type(dimension) is not int or dimension!=len(value))):raise ValueError('shared query dimension differs')
    if any(type(x) not in (int,float) or not math.isfinite(x) for x in value):raise ValueError('shared query requires finite numbers')
    return tuple(value)


def shared_vector(items):
    if not isinstance(items,list) or not 1<=len(items)<=128:raise ValueError('bounded shared batch required')
    if any(not isinstance(item,dict) or not isinstance(item.get('payload'),dict) for item in items):raise ValueError('shared operation schema differs')
    vectors=[vector(item['payload'].get('query')) for item in items]
    encoded=[json.dumps(v,allow_nan=False) for v in vectors]
    if any(value!=encoded[0] for value in encoded):raise ValueError('mixed batch queries')
    return vectors[0]


def encode_batch(payload,enabled=False):
    if type(enabled) is not bool:raise ValueError('shared query opt-in must be boolean')
    if not enabled:return json.dumps(payload,allow_nan=False).encode()
    if set(payload)!= {'tree_sha256','request_id','requests','timeout_s'}:raise ValueError('shared batch envelope differs')
    query=shared_vector(payload['requests'])
    # Preserve numeric values, types and negative zero without f32 conversion.
    result=copy.deepcopy(payload)
    for item in result['requests']:del item['payload']['query']
    result.update(query_format=FORMAT,shared_query=list(query))
    return json.dumps(result,allow_nan=False).encode()


def expand_batch(payload,enabled,readers):
    """Validate the complete opt-in batch before returning owned worker items."""
    has_shared='query_format' in payload or 'shared_query' in payload
    if not has_shared:return payload.get('requests'),None
    if not enabled or payload.get('query_format')!=FORMAT or set(payload)!={'tree_sha256','request_id','requests','timeout_s','query_format','shared_query'}:raise ValueError('unsupported shared query batch schema')
    query=vector(payload.get('shared_query'));items=payload.get('requests')
    if not isinstance(items,list) or not 1<=len(items)<=128:raise ValueError('bounded shared batch required')
    seen=set()
    for item in items:
        if not isinstance(item,dict) or not isinstance(item.get('payload'),dict) or 'query' in item['payload']:raise ValueError('inline and shared queries cannot mix')
        sid=item.get('subtree_id')
        if not isinstance(sid,str) or sid not in readers or sid in seen:raise ValueError('shared subtree identity differs')
        seen.add(sid);reader=readers[sid]
        if item.get('snapshot_sha256')!=reader.descriptor['snapshot_sha256']:raise ValueError('shared snapshot differs')
        vector(list(query),reader.snapshot['dimension'])
        if item['payload'].get('op') not in ('describe_root','expand','score_selected_leaves','rerank_selected'):raise ValueError('unsupported shared operation')
    owned=copy.deepcopy(items)
    for item in owned:item['payload']['query']=list(query)
    return owned,FORMAT
