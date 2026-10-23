"""Publish one fenced immutable tree and worker placement for dynamic changes.

This authority is a single filesystem shared by publishers. Readers copy one
record, then keep its tree and placement pinned; publication never revokes old
readers or deletes checkpoint data.
"""
import fcntl
import json
from pathlib import Path
from .tree_manifest import canonical_bytes,content_hash,validate_tree,checked_artifact,_atomic_write


def load_deployment(pointer):
    pointer=Path(pointer);head=json.loads(pointer.read_text())
    record=json.loads((pointer.parent/head['manifest']).read_text())
    if content_hash(record)!=head['deployment_sha256'] or record['fence']!=head['fence']:
        raise ValueError('deployment pointer identity mismatch')
    return record


def validate_record(record,base='.'):
    if record.get('format_version')!=1 or type(record.get('fence')) is not int or record['fence']<1:
        raise ValueError('positive deployment fencing token required')
    report=validate_tree(record['tree'],base)
    vectors=checked_artifact(record['canonical_vectors'],base)
    if vectors.stat().st_size!=record['tree']['source']['row_count']*record['tree']['dimension']*4:raise ValueError('canonical dense FP32 corpus size differs')
    if record.get('tree_sha256')!=report['tree_sha256']:raise ValueError('deployment tree identity mismatch')
    slots={s['subtree_id']:s for s in record['tree']['subtrees']}
    owners=record.get('owners',{})
    if set(owners)!=set(slots):raise ValueError('placement must own every subtree exactly once')
    for sid,owner in owners.items():
        if not isinstance(owner.get('worker_id'),str) or not owner['worker_id'] or not isinstance(owner.get('checkpoint_dir'),str):
            raise ValueError('worker identity and immutable checkpoint location required')
        if owner.get('snapshot_sha256')!=slots[sid]['snapshot']['sha256']:
            raise ValueError('placement references a different subtree snapshot')
    return report


def publish_deployment(pointer,record,expected_parent,probe,base='.'):
    """Probe all destinations, then serialize topology/placement publication.

    The caller supplies an actual worker reader probe. Its successful response
    must match the pinned immutable root, map and generation. No readiness bit
    from a saved planning file substitutes for this call.
    """
    pointer=Path(pointer);pointer.parent.mkdir(parents=True,exist_ok=True)
    validate_record(record,base)
    for subtree in record['tree']['subtrees']:
        sid=subtree['subtree_id'];ready=probe(sid,record['owners'][sid],subtree)
        expected=dict(snapshot_sha256=subtree['snapshot']['sha256'],generation=subtree['generation'],root_ref=subtree['root_ref'],vector_count=subtree['vector_count'],map_sha256=subtree['local_to_global']['sha256'])
        if any(ready.get(k)!=v for k,v in expected.items()):raise ValueError('destination readiness mismatch: '+sid)
    with open(str(pointer)+'.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        old=load_deployment(pointer) if pointer.exists() else None
        actual=content_hash(old) if old else None
        if actual!=expected_parent or record.get('parent_sha256')!=actual:raise ValueError('deployment compare-and-swap conflict')
        if record['fence']!=(old['fence']+1 if old else 1):raise ValueError('stale deployment fencing token')
        if old:
            if record.get('canonical_vectors')!=old.get('canonical_vectors'):raise ValueError('canonical corpus identity changed')
            before,after=old['tree'],record['tree']
            if any(before[k]!=after[k] for k in ('source','dimension','metric')):raise ValueError('dynamic transaction must preserve source and live ID set')
            changed=content_hash(before)!=content_hash(after)
            if changed and after['tree_generation']<=before['tree_generation']:raise ValueError('topology generation must increase')
            if not changed and record.get('operation')!='placement':raise ValueError('unchanged topology requires placement operation')
        digest=content_hash(record);name='deployment-'+digest+'.json';target=pointer.parent/name;payload=canonical_bytes(record)
        if target.exists() and target.read_bytes()!=payload:raise ValueError('immutable deployment collision')
        if not target.exists():_atomic_write(target,payload)
        _atomic_write(pointer,canonical_bytes(dict(deployment_sha256=digest,manifest=name,fence=record['fence'])))
    return dict(deployment_sha256=digest,fence=record['fence'],manifest=name)


def make_record(tree,owners,parent=None,operation='placement',canonical_vectors=None):
    return dict(canonical_vectors=canonical_vectors or (parent.get('canonical_vectors') if parent else None),format_version=1,fence=parent['fence']+1 if parent else 1,parent_sha256=content_hash(parent) if parent else None,operation=operation,tree_sha256=content_hash(tree),tree=tree,owners=owners)
