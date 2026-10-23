"""Prepare local topology changes through the captured collection authority.

Record identity stays fixed. Only one complete collection registration can make
new vector ownership and its namespace descriptors visible. This is an opt-in
internal adapter, not a remote execution or production authentication service.
"""
import copy
from pathlib import Path
import uuid
from .compactor_materialized import checked,load_json,integer
from .compactor_namespace import current_view,_same,_submit_namespace_proposal,reconcile_namespace_placement
from .compactor_registration import HIERARCHY_BUNDLE,_segments
from .dynamic_deployment import validate_record
from .dynamic_topology import prepare_topology_record,owned_ids
from .tree_manifest import canonical_bytes,content_hash,artifact,_atomic_write


def _capture_health(delegate,context):
    collection=context['parent']['collection']['collection_id']
    if not _same(delegate.capture(collection),context) or not _same(delegate.observe(collection),context['parent']):raise ValueError('topology capture is stale')
    if delegate.current_session(collection)!={k:context['compaction'][k] for k in ('job_id','session_id')}:raise ValueError('topology job session changed')


def _proposal(context,reference):
    c=context['parent']['collection'];segments=_segments(context['parent']['segments'])
    segments[context['vector_segment_id']]['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(reference).decode().strip()]
    proposal=dict(tenant_id=c['tenant_id'],database_name=c['database_name'],collection_id=c['collection_id'],collection_version=c['version'],log_position=c['log_position'],segment_flush_info=[dict(segment_id=sid,file_paths=segments[sid]['file_paths']) for sid in sorted(segments)],total_records_post_compaction=c['total_records_post_compaction'],size_bytes_post_compaction=c['size_bytes_post_compaction'],schema=c.get('schema'))
    candidate=copy.deepcopy(context['parent']);candidate['collection']['version']+=1
    for segment in candidate['segments']:segment['file_paths']=copy.deepcopy(segments[segment['segment_id']]['file_paths'])
    return proposal,candidate


def _candidate_context(context,candidate):
    result=copy.deepcopy(context);result['parent']=copy.deepcopy(candidate)
    result['record_collection_binding']['wire']['version']+=1
    return result


def _verify_candidate(context,reference,replacements):
    parent_view=current_view(context);parent=load_json(checked(parent_view['registered_bundle'],32*1024*1024))['deployment']
    bundle=load_json(checked(reference,32*1024*1024));record=bundle['deployment'];validate_record(record)
    if bundle.get('operation')!='topology' or not _same(bundle['collection_parent'],context['parent']) or not _same(bundle['compaction'],context['compaction']):raise ValueError('topology bundle context differs')
    if record['operation']!='topology' or record['fence']!=parent['fence']+1 or record['parent_sha256']!=content_hash(parent):raise ValueError('topology parent fence differs')
    if not _same(record['canonical_vectors'],parent['canonical_vectors']) or record['tree']['source']!=parent['tree']['source'] or record['tree']['dimension']!=parent['tree']['dimension'] or record['tree']['metric']!=parent['tree']['metric']:raise ValueError('topology changes source identity or values')
    old_slots={s['subtree_id']:s for s in parent['tree']['subtrees']};new_slots={s['subtree_id']:s for s in record['tree']['subtrees']}
    affected={sid for item in replacements.values() for sid in item['from']}
    if set(new_slots)!=(set(old_slots)-affected)|set(replacements):raise ValueError('topology owner identities differ')
    for sid,item in replacements.items():
        if list(owned_ids(new_slots[sid]))!=sorted(item['ids']):raise ValueError('topology replacement membership differs')
    for sid in set(old_slots)-affected:
        if list(owned_ids(old_slots[sid]))!=list(owned_ids(new_slots[sid])):raise ValueError('topology changes unaffected ID ownership')
    metadata=bundle['hierarchy_metadata'];root=load_json(checked(metadata['namespace_root'],2*1024*1024));before=parent_view['namespace']
    for key in ('mapping','identity_source','canonical_vectors','record_segment_id','record_files','row_count'):
        if not _same(root[key],before[key]):raise ValueError('topology changes record namespace identity')
    if root['parent_namespace_sha256']!=parent_view['metadata']['namespace_root']['sha256'] or root['construction_parent_deployment_sha256']!=content_hash(parent) or root['construction_parent_ownership_generation']!=parent['fence']:raise ValueError('topology namespace parent differs')
    proposal,candidate=_proposal(context,reference);target=_candidate_context(context,candidate)
    if not _same(root['collection'],target['record_collection_binding']):raise ValueError('topology namespace target collection differs')
    view=current_view(target)
    if not _same(view['registered_bundle'],reference):raise ValueError('topology candidate resolver differs')
    return proposal,candidate,target


def _preflight(context,parent,replacements,output):
    c=context['parent']['collection'];job=context['compaction']
    if not integer(c['version'],2**31-2) or not integer(parent['fence'],2**64-2,1):raise ValueError('topology version/fence exhausted')
    for key in ('job_id','session_id'):uuid.UUID(job[key])
    if not _same([job['pulled_log_offset'],job['target_total_records'],job['target_logical_size_bytes']],[c['log_position'],c['total_records_post_compaction'],c['size_bytes_post_compaction']]):raise ValueError('topology must preserve log/count/size')
    if not isinstance(replacements,dict) or not replacements:raise ValueError('explicit topology replacement required')
    # Preserve the existing local preparation boundary before any directory or
    # writer is created. Logical worker labels do not establish remote support.
    for item in replacements.values():
        if not isinstance(item,dict) or set(item)!={'from','ids'} or not isinstance(item['from'],list) or not item['from']:raise ValueError('exact replacement inputs required')
        sources=[parent['owners'][sid] for sid in item['from']]
        if any(o.get('remote') for o in sources) or len({o['worker_id'] for o in sources})>1:raise ValueError('cross-worker topology requires explicit execution binding')
    protected=[Path(o['checkpoint_dir']).resolve() for o in parent['owners'].values()]
    view=current_view(context)
    protected += [Path(parent['canonical_vectors']['path']).resolve(),Path(view['registered_bundle']['path']).resolve()]
    protected += [Path(view['namespace'][key]['path']).resolve() for key in ('mapping','identity_source')]
    protected += [Path(view['metadata'][key]['path']).resolve() for key in ('namespace_root','construction_receipt')]
    if any(output==p or output in p.parents or (p.is_dir() and p in output.parents) for p in protected):raise ValueError('topology output overlaps retained parent')


def prepare_namespace_topology(delegate,collection_id,replacements,output,binary,record_auditor,timeout=30):
    """Return eligibility only after real vector preparation and record audit.

    record_auditor is an internal actual-reader adapter, not a caller receipt.
    The native fixture pins its executable and retains every input/output.
    """
    context=copy.deepcopy(delegate.capture(collection_id));replacements=copy.deepcopy(replacements)
    if context['parent']['collection']['collection_id']!=collection_id:raise ValueError('topology collection differs')
    _capture_health(delegate,context);view=current_view(context);parent=load_json(checked(view['registered_bundle'],32*1024*1024))['deployment'];output=Path(output).resolve()
    _preflight(context,parent,replacements,output);output.mkdir(parents=True,exist_ok=False)
    journal=output/'preparation.json';intent=dict(status='preparing-private-topology',current_bundle=view['registered_bundle'],replacements=replacements)
    _atomic_write(journal,canonical_bytes(intent))
    try:
        record=prepare_topology_record(binary,parent,replacements,output/'vectors',timeout=timeout)
        namespace=copy.deepcopy(view['namespace']);namespace.update(owner_maps=[s['local_to_global'] for s in sorted(record['tree']['subtrees'],key=lambda s:s['subtree_id'])],construction_parent_deployment_sha256=content_hash(parent),construction_parent_ownership_generation=parent['fence'],parent_namespace_sha256=view['metadata']['namespace_root']['sha256'])
        namespace['collection']=copy.deepcopy(context['record_collection_binding']);namespace['collection']['wire']['version']+=1
        path=output/'namespace.json';_atomic_write(path,canonical_bytes(namespace));ns=artifact(path)
        construction=dict(namespace_root=ns,construction_parent_deployment_sha256=content_hash(parent),construction_parent_ownership_generation=parent['fence'],parent_namespace_sha256=view['metadata']['namespace_root']['sha256'])
        path=output/'construction.json';_atomic_write(path,canonical_bytes(construction));metadata=dict(record_segment_id=view['metadata']['record_segment_id'],namespace_root=ns,construction_receipt=artifact(path))
        bundle=dict(format_version=1,operation='topology',collection_parent=context['parent'],compaction=context['compaction'],segment_outputs=_segments(context['parent']['segments']),deployment=record,hierarchy_metadata=metadata)
        path=output/'bundle.json';_atomic_write(path,canonical_bytes(bundle));reference=artifact(path)
        proposal,candidate,target=_verify_candidate(context,reference,replacements)
        audit=record_auditor(target,ns,output/'prepare-audit')
        _capture_health(delegate,context)
        prepared=dict(context=context,replacements=replacements,descriptor=reference,proposal=proposal,candidate_state=candidate,proposal_sha256=content_hash(proposal),expected_current_bundle=view['registered_bundle'],journal_path=str(output/'submission.json'),attempt_id=None,preparation_journal=str(journal),audit=copy.deepcopy(audit))
        _atomic_write(output/'prepared.json',canonical_bytes(prepared));_atomic_write(journal,canonical_bytes(dict(intent,status='prepared-private-topology',descriptor=reference)))
        return prepared
    except BaseException as error:
        _atomic_write(journal,canonical_bytes(dict(intent,status='failed-private-topology',error=repr(error))))
        raise


def _topology_health(delegate,prepared,record_auditor):
    from .dynamic_runtime import durable_namespace
    import os
    marker=Path(prepared['preparation_journal']).parent/'validation-started.json'
    if marker.exists():raise ValueError('topology attempt is not eligible after validation started')
    # An exclusive durable marker blocks restart/retry even if writing the
    # later failure classification itself fails. Ambiguous submitted attempts
    # bypass this health callback and only reconcile their original request.
    with marker.open('xb') as stream:
        stream.write(canonical_bytes(dict(proposal_sha256=prepared['proposal_sha256'])))
        stream.flush();os.fsync(stream.fileno())
    durable_namespace(marker.parent)
    try:
        return _validate_topology_health(delegate,prepared,record_auditor)
    except BaseException as error:
        try:
            state=load_json(prepared['preparation_journal'])
            state.update(status='failed-submit-validation',error=repr(error))
            _atomic_write(prepared['preparation_journal'],canonical_bytes(state))
        except BaseException as journal_error:
            error.add_note('failure classification could not be saved: '+repr(journal_error))
        raise


def _validate_topology_health(delegate,prepared,record_auditor):
    context=prepared['context'];_capture_health(delegate,context)
    if load_json(prepared['preparation_journal']).get('status')!='prepared-private-topology':raise ValueError('topology attempt is not eligible')
    view=current_view(context)
    if not _same(view['registered_bundle'],prepared['expected_current_bundle']):raise ValueError('topology registered bundle differs')
    proposal,candidate,target=_verify_candidate(context,prepared['descriptor'],prepared['replacements'])
    if not _same(proposal,prepared['proposal']) or content_hash(proposal)!=prepared['proposal_sha256'] or not _same(candidate,prepared['candidate_state']):raise ValueError('topology complete proposal differs')
    metadata=load_json(checked(prepared['descriptor'],32*1024*1024))['hierarchy_metadata']
    record_auditor(target,metadata['namespace_root'],Path(prepared['journal_path']).parent/('submit-audit-'+str(uuid.uuid4())))
    # An actual reader audit takes time; observe the authority again immediately
    # before the shared helper journals and issues the one registration request.
    _capture_health(delegate,context)


def submit_namespace_topology(delegate,prepared,record_auditor):
    return _submit_namespace_proposal(delegate,prepared,lambda d,p:_topology_health(d,p,record_auditor))


def reconcile_namespace_topology(delegate,prepared):
    return reconcile_namespace_placement(delegate,prepared)
