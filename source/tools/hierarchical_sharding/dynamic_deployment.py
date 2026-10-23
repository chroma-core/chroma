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


def resolved_tree(record):
    """Resolve immutable snapshot/map pins through this deployment's replicas.

    Physical worker paths are not logical tree identity. The replacement paths
    are checked against the original byte/hash descriptors by validate_tree.
    """
    import copy
    tree=copy.deepcopy(record['tree'])
    for subtree in tree['subtrees']:
        owner=record['owners'][subtree['subtree_id']];directory=Path(owner['checkpoint_dir'])
        manifest=directory/('manifest-'+str(subtree['generation'])+'.json')
        subtree['snapshot']['path']=str(manifest)
        snapshot=json.loads(checked_artifact(subtree['snapshot'],'.').read_text())
        expected_blocks={'posting_list_id','scalar_metadata_id','vector_data_id','leaf_node_id','internal_node_id'}
        if set(snapshot.get('block_ids',{}))!=expected_blocks:raise ValueError('all five persisted lower references are required')
        audit=snapshot.get('integrity_audit',{});coverage=audit.get('coverage',{})
        if audit.get('block_ids')!=snapshot['block_ids'] or audit.get('generation')!=subtree['generation'] or audit.get('id_map_sha256')!=subtree['local_to_global']['sha256'] or coverage.get('expected_rows')!=subtree['vector_count'] or coverage.get('unique_live_rows')!=subtree['vector_count']:raise ValueError('snapshot lacks complete version-aware ownership audit')
        mapping=Path(snapshot['id_mapping']['path'])
        if mapping.is_absolute() or len(mapping.parts)!=1:raise ValueError('replica map must be a checkpoint-local filename')
        subtree['local_to_global']['path']=str(directory/mapping)
    return tree

def validate_record(record,base='.'):
    if record.get('format_version')!=1 or type(record.get('fence')) is not int or record['fence']<1:
        raise ValueError('positive deployment fencing token required')
    if record.get('operation') not in ('placement','topology','compaction'):raise ValueError('explicit dynamic operation required')
    try:report=validate_tree(resolved_tree(record),base)
    except OSError as error:raise ValueError('replica artifact is missing or unreadable') from error
    report['tree_sha256']=content_hash(record['tree'])
    semantics=dict(dtype='little-endian-f32',row_order='dense-global-id',normalization='worker-f64-l2-to-f32' if record['tree']['metric']=='cosine' else 'identity',metric=record['tree']['metric'])
    if record.get('vector_semantics')!=semantics:raise ValueError('canonical vector interpretation/normalization is not pinned')
    vectors=checked_artifact(record['canonical_vectors'],base)
    if vectors.stat().st_size!=record['tree']['source']['row_count']*record['tree']['dimension']*4:raise ValueError('canonical dense FP32 corpus size differs')
    if record.get('tree_sha256')!=report['tree_sha256']:raise ValueError('deployment tree identity mismatch')
    slots={s['subtree_id']:s for s in record['tree']['subtrees']}
    owners=record.get('owners',{})
    if set(owners)!=set(slots):raise ValueError('placement must own every subtree exactly once')
    counts={}
    for owner in owners.values():counts[owner.get('worker_id')]=counts.get(owner.get('worker_id'),0)+1
    if any(count>128 for count in counts.values()):raise ValueError('worker exceeds the supported128-subtree batch bound')
    for sid,owner in owners.items():
        if not isinstance(owner.get('worker_id'),str) or not owner['worker_id'] or not isinstance(owner.get('checkpoint_dir'),str):
            raise ValueError('worker identity and immutable checkpoint location required')
        if owner.get('endpoint') is not None and (not isinstance(owner['endpoint'],str) or not owner['endpoint'].startswith(('http://','https://')) or owner.get('serving_tree_sha256')!=record['tree_sha256']):raise ValueError('generation-specific serving tree binding required')
        if owner.get('snapshot_sha256')!=slots[sid]['snapshot']['sha256']:
            raise ValueError('placement references a different subtree snapshot')
    return report


def verify_canonical_append(before,after,dimension):
    old=checked_artifact(before,'.');new=checked_artifact(after,'.')
    if new.stat().st_size<=old.stat().st_size or new.stat().st_size%(dimension*4):raise ValueError('canonical append must extend complete FP32 rows')
    with old.open('rb') as a,new.open('rb') as b:
        for chunk in iter(lambda:a.read(1024*1024),b''):
            if b.read(len(chunk))!=chunk:raise ValueError('canonical append changes existing vector bytes')
    return new

def publish_deployment(pointer,record,expected_parent,probe,base='.'):
    """Probe all destinations, then serialize topology/placement publication.

    The caller supplies an actual worker reader probe. Its successful response
    must match the pinned immutable root, map and generation. No readiness bit
    from a saved planning file substitutes for this call.
    """
    pointer=Path(pointer);pointer.parent.mkdir(parents=True,exist_ok=True)
    validate_record(record,base)
    from .dynamic_runtime import durable_file
    for desc in (record['tree']['model'],record['canonical_vectors']):durable_file(checked_artifact(desc,base))
    for subtree in record['tree']['subtrees']:
        sid=subtree['subtree_id']
        try:ready=probe(sid,record['owners'][sid],subtree)
        except Exception as error:raise ValueError('destination readiness failed: '+sid) from error
        expected=dict(snapshot_sha256=subtree['snapshot']['sha256'],generation=subtree['generation'],root_ref=subtree['root_ref'],vector_count=subtree['vector_count'],map_sha256=subtree['local_to_global']['sha256'])
        if any(ready.get(k)!=v for k,v in expected.items()):raise ValueError('destination readiness mismatch: '+sid)
    with open(str(pointer)+'.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        old=load_deployment(pointer) if pointer.exists() else None
        actual=content_hash(old) if old else None
        if actual!=expected_parent or record.get('parent_sha256')!=actual:raise ValueError('deployment compare-and-swap conflict')
        if record['fence']!=(old['fence']+1 if old else 1):raise ValueError('stale deployment fencing token')
        if old:
            if old.get('vector_semantics')!=record.get('vector_semantics'):raise ValueError('vector normalization policy changed')
            before,after=old['tree'],record['tree']
            if any(before[k]!=after[k] for k in ('dimension','metric')) or before['source']['source_id']!=after['source']['source_id']:raise ValueError('source identity or vector interpretation changed')
            changed=content_hash(before)!=content_hash(after)
            operation=record['operation']
            if operation=='compaction':
                verify_canonical_append(old['canonical_vectors'],record['canonical_vectors'],before['dimension'])
                if after['source']['row_count']<=before['source']['row_count']:raise ValueError('compaction requires a growing source prefix')
                for key in ('model','upper_root','routing_boundary_scheme','construction_epoch'):
                    if before[key]!=after[key]:raise ValueError('compaction cannot change routing topology')
                previous={s['subtree_id']:s for s in resolved_tree(old)['subtrees']}
                if set(previous)!={s['subtree_id'] for s in after['subtrees']}:raise ValueError('compaction cannot change logical owners')
                for subtree in after['subtrees']:
                    a=checked_artifact(previous[subtree['subtree_id']]['local_to_global'],'.');b=checked_artifact(subtree['local_to_global'],base)
                    with a.open('rb') as src,b.open('rb') as dst:
                        for chunk in iter(lambda:src.read(1024*1024),b''):
                            if dst.read(len(chunk))!=chunk:raise ValueError('compaction changes pinned local ID prefix')
            elif record.get('canonical_vectors')!=old.get('canonical_vectors') or before['source']!=after['source']:raise ValueError('topology/placement must preserve canonical live corpus')
            if operation=='placement' and changed:raise ValueError('placement must preserve complete tree identity')
            if operation=='topology' and not changed:raise ValueError('topology operation requires a changed tree')
            if changed and after['tree_generation']<=before['tree_generation']:raise ValueError('topology generation must increase')
        digest=content_hash(record);name='deployment-'+digest+'.json';target=pointer.parent/name;payload=canonical_bytes(record)
        if target.exists() and target.read_bytes()!=payload:raise ValueError('immutable deployment collision')
        if not target.exists():_atomic_write(target,payload)
        _atomic_write(pointer,canonical_bytes(dict(deployment_sha256=digest,manifest=name,fence=record['fence'])))
    return dict(deployment_sha256=digest,fence=record['fence'],manifest=name)


def make_record(tree,owners,parent=None,operation='placement',canonical_vectors=None):
    return dict(vector_semantics=dict(dtype='little-endian-f32',row_order='dense-global-id',normalization='worker-f64-l2-to-f32' if tree['metric']=='cosine' else 'identity',metric=tree['metric']),canonical_vectors=canonical_vectors or (parent.get('canonical_vectors') if parent else None),format_version=1,fence=parent['fence']+1 if parent else 1,parent_sha256=content_hash(parent) if parent else None,operation=operation,tree_sha256=content_hash(tree),tree=tree,owners=owners)


def deployment_contains(pointer,digest,max_generations=100000):
    """Prove commitment from the durable head's immutable parent chain."""
    pointer=Path(pointer);record=load_deployment(pointer)
    for _ in range(max_generations):
        if content_hash(record)==digest:return True
        parent=record.get('parent_sha256')
        if parent is None:return False
        if not isinstance(parent,str) or len(parent)!=64 or any(c not in '0123456789abcdef' for c in parent):raise ValueError('invalid deployment ancestry')
        previous=json.loads((pointer.parent/('deployment-'+parent+'.json')).read_text())
        if content_hash(previous)!=parent or previous['fence']!=record['fence']-1:raise ValueError('broken deployment ancestry')
        record=previous
    raise ValueError('deployment ancestry exceeds reconciliation bound')

def run_transaction(pointer,parent,prepare,probe,journal,parameters=None):
    """Durably journal a private build and publish against its original fence.

    A failed transaction retains its private outputs. Retrying requires a new
    journal/output namespace and explicit planning from a newly pinned parent;
    this function never substitutes a newer parent after a CAS conflict.
    """
    import copy
    journal=Path(journal);journal.parent.mkdir(parents=True,exist_ok=True)
    with journal.open('xb') as initial:initial.write(b'{}\n');initial.flush();__import__('os').fsync(initial.fileno())
    original=content_hash(parent);state=dict(status='preparing',parent_sha256=original,fence=parent['fence']+1,parameters=parameters or {})
    _atomic_write(journal,canonical_bytes(state));record=None;receipt=None;publication_attempted=False
    try:
        record=prepare(copy.deepcopy(parent))
        if record.get('parent_sha256')!=original or record.get('fence')!=parent['fence']+1:raise ValueError('prepared transaction changed its original parent authorization')
        state.update(status='prepared',deployment_sha256=content_hash(record),prepared_record=record);_atomic_write(journal,canonical_bytes(state))
        publication_attempted=True
        receipt=publish_deployment(pointer,record,original,probe)
        state.update(status='published',publication=receipt);_atomic_write(journal,canonical_bytes(state));return receipt
    except BaseException as error:
        state.update(status='failed_unpublished',error=type(error).__name__+': '+str(error),private_outputs_retained=True)
        # A successful receipt remains definitive even after another publisher
        # advances the head. Otherwise prove ancestry or preserve uncertainty.
        if receipt is not None:
            state.update(status='published_reconciliation_required',publication=receipt)
        elif record is not None and publication_attempted:
            try:
                if deployment_contains(pointer,content_hash(record)):state['status']='published_reconciliation_required'
                elif not isinstance(error,ValueError):state['status']='publication_state_unknown'
            except (OSError,ValueError,KeyError):state['status']='publication_state_unknown'
        _atomic_write(journal,canonical_bytes(state));raise
