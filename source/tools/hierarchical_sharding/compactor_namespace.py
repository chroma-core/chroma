"""Bind an optional namespace to the existing captured collection registration.

This adapter validates local artifacts. The compactor delegate supplies authority;
it does not turn file receipts into production authentication or immutable storage.
"""
import copy
import json
from .compactor_materialized import checked, load_json, integer
from .compactor_registration import HIERARCHY_BUNDLE, _segments
from .dynamic_deployment import validate_record
from .tree_manifest import canonical_bytes, content_hash


def _same(a,b):return canonical_bytes(a)==canonical_bytes(b)

def current_view(context):
    """Resolve all current references from one already captured segment set."""
    state=context['parent'];collection=state['collection'];segments=_segments(state['segments'])
    vector=segments[context['vector_segment_id']]
    references=vector['file_paths'].get(HIERARCHY_BUNDLE)
    if not isinstance(references,list) or len(references)!=1:raise ValueError('one current registered bundle required')
    reference=json.loads(references[0]);bundle=load_json(checked(reference,32*1024*1024))
    if 'deployment' not in bundle or 'hierarchy_metadata' not in bundle:raise ValueError('current namespace bundle required; no implicit legacy bootstrap')
    deployment=bundle['deployment'];validate_record(deployment)
    before=bundle['collection_parent']['collection']
    if before['collection_id']!=collection['collection_id'] or before['version']+1!=collection['version'] or bundle['compaction']['pulled_log_offset']!=collection['log_position'] or deployment['tree']['source']['row_count']!=collection['total_records_post_compaction']:raise ValueError('current bundle collection fence differs')
    metadata=bundle['hierarchy_metadata']
    if set(metadata)!={'record_segment_id','namespace_root','construction_receipt'}:raise ValueError('exact namespace metadata required')
    record=segments[metadata['record_segment_id']]
    if record.get('scope')!='RECORD' or record.get('type')!='urn:chroma:segment/record/blockfile':raise ValueError('record segment type/scope differs')
    root=load_json(checked(metadata['namespace_root'],2*1024*1024))
    receipt=load_json(checked(metadata['construction_receipt'],2*1024*1024))
    # The construction snapshot is retained, not deserialized as today's
    # collection. Its version/log/time can differ after placement publication.
    current_binding=context['record_collection_binding']
    current_wire=current_binding['wire']
    if not _same([current_wire['id'],current_wire['version'],current_wire['log_position'],current_wire['dimension'],current_wire.get('schema'),current_binding['total_records_post_compaction']],
                 [collection['collection_id'],collection['version'],collection['log_position'],deployment['tree']['dimension'],collection.get('schema'),collection['total_records_post_compaction']]):raise ValueError('captured actual record collection differs')
    def stable(binding):
        value=copy.deepcopy(binding)
        for key in ('version','log_position'):value['wire'].pop(key)
        for key in ('total_records_post_compaction','size_bytes_post_compaction','last_compaction_time_secs','version_file_path','lineage_file_path','updated_at','compaction_failure_count'):value.pop(key)
        return value
    if not _same(stable(root['collection']),stable(current_binding)):raise ValueError('namespace immutable collection configuration differs')
    wire=root['collection']['wire']
    if wire['id']!=collection['collection_id'] or wire['dimension']!=deployment['tree']['dimension'] or wire.get('schema')!=collection.get('schema'):raise ValueError('namespace immutable collection identity/schema differs')
    if root['record_segment_id']!=record['segment_id'] or not _same(root['record_files'],record['file_paths']) or root['row_count']!=collection['total_records_post_compaction']:raise ValueError('namespace current record roots/count differ')
    maps=[s['local_to_global'] for s in sorted(deployment['tree']['subtrees'],key=lambda s:s['subtree_id'])]
    if not _same(root['canonical_vectors'],deployment['canonical_vectors']) or not _same(root['owner_maps'],maps):raise ValueError('namespace current canonical/ownership roots differ')
    expected=dict(namespace_root=metadata['namespace_root'],construction_parent_deployment_sha256=root['construction_parent_deployment_sha256'],construction_parent_ownership_generation=root['construction_parent_ownership_generation'],parent_namespace_sha256=root['parent_namespace_sha256'])
    if not _same(receipt,expected):raise ValueError('namespace construction receipt differs')
    return copy.deepcopy(dict(registered_bundle=reference,metadata=metadata,namespace=root,record_segment=record,current_parent=dict(deployment_sha256=content_hash(deployment),ownership_generation=deployment['fence'],next_global_id=root['row_count'])))


class NamespacePreparation:
    """An internal preparation hook; no registration or independent pointer.

    The injected producer must consume the retained actual materializer result,
    flush its record writer, fully audit saved output and return exact artifacts.
    A fake producer establishes only the hook contract, never saved-record proof.
    """
    def __init__(self,delegate,producer):
        self.delegate=delegate;self.producer=producer;self.context=None;self.view=None;self.prepared=None
    def __getattr__(self,name):return getattr(self.delegate,name)
    def capture(self,collection_id):
        context=copy.deepcopy(self.delegate.capture(collection_id))
        view=current_view(context)
        if self.context is not None and (not _same(context,self.context) or not _same(view,self.view)):raise ValueError('captured namespace view changed')
        self.context=context;self.view=view
        return copy.deepcopy(context)
    def _check(self,context):
        if self.context is None or not _same(context,self.context) or not _same(current_view(context),self.view):raise ValueError('current namespace binding changed')
    def prepare_hierarchy_metadata(self,context,deployment):
        self._check(context)
        if self.prepared is not None:raise ValueError('private namespace preparation cannot repeat')
        # Mark the attempt used BEFORE a producer can mutate any writer.
        self.prepared={}
        value=self.producer.prepare(copy.deepcopy(context),copy.deepcopy(self.view),copy.deepcopy(deployment))
        if not isinstance(value,dict) or set(value)!={'metadata','record_files'}:raise ValueError('complete saved record preparation required')
        self.prepared=copy.deepcopy(value)
        return copy.deepcopy(value['metadata'])
    def segment_outputs(self,context):
        self._check(context)
        outputs=_segments(self.delegate.segment_outputs(context))
        if not self.prepared:raise ValueError('saved namespace preparation unavailable')
        sid=self.view['record_segment']['segment_id']
        if sid not in outputs or outputs[sid]['scope']!='RECORD' or outputs[sid]['type']!=self.view['record_segment']['type']:raise ValueError('candidate record segment differs')
        outputs[sid]['file_paths']=copy.deepcopy(self.prepared['record_files'])
        return list(outputs.values())
    def validate_hierarchy_metadata(self,context,deployment,outputs,metadata):
        self._check(context)
        if not self.prepared or not _same(metadata,self.prepared['metadata']):raise ValueError('prepared namespace metadata differs')
        segments=_segments(list(outputs.values()) if isinstance(outputs,dict) else outputs)
        sid=self.view['record_segment']['segment_id']
        if not _same(segments[sid]['file_paths'],self.prepared['record_files']):raise ValueError('registered record files differ from saved candidate')
        # Revalidate actual artifacts before eligibility AND immediately before
        # registration submission. No old caller audit receipt is accepted.
        self.producer.validate(copy.deepcopy(context),copy.deepcopy(self.view),copy.deepcopy(deployment),copy.deepcopy(self.prepared))

# This exact source list is emitted by the actual Rust exporter. A runtime
# receipt is local provenance, not a service authentication mechanism.
NAMESPACE_PRODUCERS=(
    'rust/index/tests/hierarchical_namespace_export.rs',
    'rust/worker/src/execution/orchestration/hierarchical_record_namespace.rs',
    'rust/worker/src/execution/orchestration/hierarchical_materialized_input.rs',
    'rust/segment/src/types.rs','rust/segment/src/blockfile_record.rs',
)

class ExportedNamespaceAuthority:
    """Read actual non-identity materialization; the default schema stays strict."""
    def __init__(self,delegate,export,receipt,runtime):
        self.delegate=delegate;self.export=copy.deepcopy(export);self.receipt=copy.deepcopy(receipt);self.runtime=copy.deepcopy(runtime);self.context=None
    def __getattr__(self,name):return getattr(self.delegate,name)
    def capture(self,collection):
        context=copy.deepcopy(self.delegate.capture(collection))
        if self.context is not None and not _same(context,self.context):raise ValueError('materialized namespace capture changed')
        self.context=context;return copy.deepcopy(context)
    def load(self):
        from pathlib import Path
        from .tree_manifest import file_hash
        export=load_json(checked(self.export,2*1024*1024));proof=load_json(checked(self.receipt,65536))
        if set(proof)!={'format_version','export','input','runtime_binary','producer_sources'} or type(proof['format_version']) is not int or proof['format_version']!=1 or not _same(proof['export'],self.export) or not _same(proof['runtime_binary'],self.runtime):raise ValueError('namespace export provenance differs')
        checked(proof['input'],65536);checked(self.runtime,256*1024*1024)
        root=Path(__file__).resolve().parents[2]
        if set(proof['producer_sources'])!=set(NAMESPACE_PRODUCERS) or any(file_hash(root/p)!=proof['producer_sources'][p] for p in NAMESPACE_PRODUCERS):raise ValueError('namespace producer source differs')
        return export
    def source_logs(self,context):
        import struct
        import math
        if self.context is None or not _same(context,self.context):raise ValueError('namespace source capture differs')
        view=current_view(context);export=self.load();captured=export['captured'];c=context['parent']['collection'];parent=view['current_parent'];target=context['compaction']['pulled_log_offset']
        expected=dict(collection_id=c['collection_id'],collection_version=c['version'],parent_log_position=c['log_position'],pulled_log_offset=target,parent_deployment_sha256=parent['deployment_sha256'],ownership_generation=parent['ownership_generation'])
        if any(not _same(captured[k],v) for k,v in expected.items()) or not _same(export['parent_namespace'],view['metadata']['namespace_root']) or not _same(export['parent_record_files'],view['record_segment']['file_paths']) or not _same(export['parent_collection_binding'],context['record_collection_binding']):raise ValueError('actual materialized parent binding differs')
        if export.get('capture_kind')=='captured-current':
            if not _same(export.get('registered_bundle'),view['registered_bundle']):raise ValueError('actual export registered bundle is stale')
        elif export.get('capture_kind')!='constructed-genesis' or export.get('registered_bundle') is not None:raise ValueError('explicit namespace capture kind required')
        rows=captured['rows'];first=parent['next_global_id'];dimension=view['namespace']['collection']['wire']['dimension']
        if not isinstance(rows,list) or len(rows)!=target-c['log_position'] or not 0<len(rows)<=100000 or first+len(rows)>100000:raise ValueError('namespace source coverage differs')
        users=set();offsets=set();result=[]
        for i,row in enumerate(rows):
            if set(row)!={'log_offset','global_id','record_offset_id','user_id','embedding_bits'} or not _same(row['log_offset'],c['log_position']+i+1) or not _same(row['global_id'],first+i) or not integer(row['record_offset_id'],2**32-1,1) or row['record_offset_id'] in offsets or not isinstance(row['user_id'],str) or not row['user_id'] or row['user_id'] in users:raise ValueError('actual namespace row differs')
            bits=row['embedding_bits']
            if not isinstance(bits,list) or len(bits)!=dimension or any(not integer(v,2**32-1) for v in bits):raise ValueError('namespace vector bits differ')
            vector=list(struct.unpack('<'+'f'*dimension,struct.pack('<'+'I'*dimension,*bits)))
            if any(not math.isfinite(v) for v in vector):raise ValueError('namespace vector nonfinite')
            users.add(row['user_id']);offsets.add(row['record_offset_id']);result.append(dict(operation='add',materialized_operation='add_new',log_offset=row['log_offset'],global_id=row['global_id'],user_id=row['user_id'],embedding=vector))
        return result
    def source_binding(self,context):
        self.source_logs(context)
        return copy.deepcopy(dict(export=self.export,receipt=self.receipt,runtime=self.runtime,parent_namespace=current_view(context)['metadata']['namespace_root']))


class SavedNamespaceProducer:
    """Compose saved record roots and run the actual full record auditor.

    The exporter owns the real materializer result and already flushes the
    private record roots. This hook binds them to the actual saved vector output.
    """
    def __init__(self,export_authority,output):
        from pathlib import Path
        self.source=export_authority;self.output=Path(output).resolve();self.serial=0;self.saved=None
    def prepare(self,context,view,deployment):
        from .tree_manifest import _atomic_write,artifact
        self.source.source_logs(context);export=self.source.load()
        self.output.mkdir(parents=True,exist_ok=False)
        parent=view['namespace'];rows=export['namespace_rows'];captured=export['captured']['rows'];old=load_json(checked(parent['mapping'],24*1024*1024))
        if rows[:len(old)]!=old or len(rows)!=len(old)+len(captured):raise ValueError('saved namespace changes parent mapping')
        for row,actual in zip(rows[len(old):],captured):
            if not _same(row,{k:actual[k] for k in ('global_id','record_offset_id','user_id')}):raise ValueError('saved mapping differs from actual materialization')
        mapping=self.output/'mapping.json';_atomic_write(mapping,canonical_bytes(rows))
        identity=self.output/'identities.json';_atomic_write(identity,canonical_bytes([{k:r[k] for k in ('global_id','user_id')} for r in rows]))
        root=copy.deepcopy(parent);root.update(construction_parent_deployment_sha256=view['current_parent']['deployment_sha256'],construction_parent_ownership_generation=view['current_parent']['ownership_generation'],record_files=copy.deepcopy(export['candidate_record_files']),row_count=len(rows),mapping=artifact(mapping),identity_source=artifact(identity),canonical_vectors=copy.deepcopy(deployment['canonical_vectors']),owner_maps=[copy.deepcopy(s['local_to_global']) for s in sorted(deployment['tree']['subtrees'],key=lambda s:s['subtree_id'])],parent_namespace_sha256=view['metadata']['namespace_root']['sha256'])
        root['collection']=copy.deepcopy(context['record_collection_binding']);root['collection']['wire'].update(version=context['parent']['collection']['version']+1,log_position=context['compaction']['pulled_log_offset']);root['collection'].update(total_records_post_compaction=len(rows),size_bytes_post_compaction=context['compaction']['target_logical_size_bytes'])
        path=self.output/'namespace.json';_atomic_write(path,canonical_bytes(root));reference=artifact(path)
        receipt=self.output/'construction.json';_atomic_write(receipt,canonical_bytes(dict(namespace_root=reference,construction_parent_deployment_sha256=root['construction_parent_deployment_sha256'],construction_parent_ownership_generation=root['construction_parent_ownership_generation'],parent_namespace_sha256=root['parent_namespace_sha256'])))
        self.saved=dict(metadata=dict(record_segment_id=view['record_segment']['segment_id'],namespace_root=reference,construction_receipt=artifact(receipt)),record_files=copy.deepcopy(export['candidate_record_files']))
        return copy.deepcopy(self.saved)
    def validate(self,context,view,deployment,prepared):
        import os
        import subprocess
        from .tree_manifest import _atomic_write,artifact
        self.source.source_logs(context);export=self.source.load()
        if self.saved is None or not _same(prepared,self.saved):raise ValueError('saved namespace preparation differs')
        root=load_json(checked(prepared['metadata']['namespace_root'],2*1024*1024));validate_record(deployment)
        construction=load_json(checked(prepared['metadata']['construction_receipt'],2*1024*1024))
        expected_construction=dict(namespace_root=prepared['metadata']['namespace_root'],construction_parent_deployment_sha256=root['construction_parent_deployment_sha256'],construction_parent_ownership_generation=root['construction_parent_ownership_generation'],parent_namespace_sha256=root['parent_namespace_sha256'])
        if not _same(construction,expected_construction):raise ValueError('candidate construction receipt differs')
        if not _same(root['canonical_vectors'],deployment['canonical_vectors']) or not _same(root['owner_maps'],[s['local_to_global'] for s in sorted(deployment['tree']['subtrees'],key=lambda s:s['subtree_id'])]) or not _same(root['record_files'],prepared['record_files']):raise ValueError('candidate complete roots differ')
        if root['row_count']!=deployment['tree']['source']['row_count'] or root['collection']['wire']['version']!=context['parent']['collection']['version']+1 or root['collection']['wire']['log_position']!=context['compaction']['pulled_log_offset'] or root['collection']['size_bytes_post_compaction']!=context['compaction']['target_logical_size_bytes']:raise ValueError('candidate target registration differs')
        self.serial+=1;input_path=self.output/('audit-input-'+str(self.serial)+'.json');result=self.output/('audit-result-'+str(self.serial)+'.json')
        value=dict(candidate=prepared['metadata']['namespace_root'],parent=view['metadata']['namespace_root'],export=self.source.export,store=export['record_store'],output=str(result),current_bundle=view['registered_bundle'],current_collection_binding=context['record_collection_binding'],vector_segment_id=context['vector_segment_id']);_atomic_write(input_path,canonical_bytes(value))
        env=os.environ.copy();env['SPANN_NAMESPACE_AUDIT_INPUT']=str(input_path)
        command=[str(checked(self.source.runtime,256*1024*1024)),'--ignored','--exact','audit_saved_namespace_candidate','--nocapture']
        run=subprocess.run(command,env=env,capture_output=True,timeout=10)
        (self.output/('audit-'+str(self.serial)+'.stdout')).write_bytes(run.stdout);(self.output/('audit-'+str(self.serial)+'.stderr')).write_bytes(run.stderr)
        if run.returncode:raise ValueError('actual saved record audit failed')
        report=load_json(result)
        if not _same(report['current_bundle'],view['registered_bundle']) or not _same(report['current_parent_sha256'],view['current_parent']['deployment_sha256']) or not _same(report['current_parent_fence'],view['current_parent']['ownership_generation']) or not _same(report['candidate'],prepared['metadata']['namespace_root']) or not _same(report['parent'],view['metadata']['namespace_root']) or not _same(report['export'],self.source.export) or report['all_three_maps_and_vectors_verified'] is not True:raise ValueError('saved record audit response differs')
        checked(prepared['metadata']['namespace_root'],2*1024*1024);checked(self.source.export,2*1024*1024)


def prepare_namespace_placement(delegate,collection_id,assignments,output):
    """Prepare a logical assignment change through the same collection bundle.

    This local adapter changes worker labels, not physical checkpoint locations.
    Destination transfer/readiness belongs to the existing separate fleet path.
    """
    import uuid
    from pathlib import Path
    from .dynamic_deployment import make_record
    from .tree_manifest import _atomic_write,artifact
    context=copy.deepcopy(delegate.capture(collection_id));state=context['parent']
    if state['collection']['collection_id']!=collection_id:raise ValueError('placement collection differs')
    if not _same(delegate.observe(state['collection']['collection_id']),state):raise ValueError('placement capture is stale')
    view=current_view(context);bundle=load_json(checked(view['registered_bundle'],32*1024*1024));parent=bundle['deployment']
    if not integer(state['collection']['version'],2**31-2) or not integer(parent['fence'],2**64-2,1):raise ValueError('placement version/fence exhausted')
    if set(assignments)!=set(parent['owners']) or any(not isinstance(w,str) or not w for w in assignments.values()):raise ValueError('complete logical placement assignment required')
    owners=copy.deepcopy(parent['owners'])
    for sid,worker in assignments.items():owners[sid]['worker_id']=worker
    record=make_record(copy.deepcopy(parent['tree']),owners,parent,'placement');validate_record(record)
    if not _same(record['canonical_vectors'],parent['canonical_vectors']) or not _same(record['tree'],parent['tree']):raise ValueError('logical placement changes index content')
    output=Path(output).resolve()
    protected=[Path(owner['checkpoint_dir']).resolve() for owner in parent['owners'].values()]+[Path(parent['canonical_vectors']['path']).resolve(),Path(view['registered_bundle']['path']).resolve(),Path(view['metadata']['namespace_root']['path']).resolve()]
    if any(output==p or output in p.parents or (p.is_dir() and p in output.parents) for p in protected):raise ValueError('placement output overlaps retained parent')
    output.mkdir(parents=True,exist_ok=False)
    job=copy.deepcopy(context['compaction']);c=state['collection']
    for key in ('job_id','session_id'):uuid.UUID(job[key])
    if not _same([job['pulled_log_offset'],job['target_total_records'],job['target_logical_size_bytes']],[c['log_position'],c['total_records_post_compaction'],c['size_bytes_post_compaction']]):raise ValueError('placement must preserve log/count/size')
    if delegate.current_session(collection_id)!={key:job[key] for key in ('job_id','session_id')}:raise ValueError('placement job session changed')
    outputs=_segments(state['segments']);next_bundle=dict(format_version=1,collection_parent=state,compaction=job,segment_outputs=outputs,deployment=record,hierarchy_metadata=copy.deepcopy(view['metadata']),operation='logical-placement')
    path=output/'bundle.json';_atomic_write(path,canonical_bytes(next_bundle));reference=artifact(path);outputs[context['vector_segment_id']]['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(reference).decode().strip()]
    c=state['collection'];proposal=dict(tenant_id=c['tenant_id'],database_name=c['database_name'],collection_id=c['collection_id'],collection_version=c['version'],log_position=c['log_position'],segment_flush_info=[dict(segment_id=sid,file_paths=outputs[sid]['file_paths']) for sid in sorted(outputs)],total_records_post_compaction=c['total_records_post_compaction'],size_bytes_post_compaction=c['size_bytes_post_compaction'],schema=c.get('schema'))
    candidate=copy.deepcopy(state);candidate['collection']['version']+=1
    for segment in candidate['segments']:segment['file_paths']=copy.deepcopy(outputs[segment['segment_id']]['file_paths'])
    prepared=dict(context=context,descriptor=reference,proposal=proposal,candidate_state=candidate,proposal_sha256=content_hash(proposal),expected_current_bundle=view['registered_bundle'],journal_path=str(output/'submission.json'),attempt_id=None)
    _atomic_write(output/'prepared.json',canonical_bytes(prepared));return prepared


def _placement_health(delegate,prepared):
    context=prepared['context'];collection=context['parent']['collection']['collection_id']
    if not _same(delegate.capture(collection),context) or not _same(delegate.observe(collection),context['parent']):raise ValueError('placement publication is stale')
    if delegate.current_session(collection)!={key:context['compaction'][key] for key in ('job_id','session_id')}:raise ValueError('placement job session changed')
    view=current_view(context)
    if not _same(view['registered_bundle'],prepared['expected_current_bundle']):raise ValueError('placement registered bundle changed')
    bundle=load_json(checked(prepared['descriptor'],32*1024*1024));validate_record(bundle['deployment'])
    before=load_json(checked(view['registered_bundle'],32*1024*1024))['deployment'];after=bundle['deployment']
    if not _same(bundle['collection_parent'],context['parent']) or not _same(bundle['compaction'],context['compaction']) or not _same(bundle['hierarchy_metadata'],view['metadata']):raise ValueError('placement candidate context/namespace differs')
    if after['fence']!=before['fence']+1 or after['parent_sha256']!=content_hash(before) or not _same(after['tree'],before['tree']) or not _same(after['canonical_vectors'],before['canonical_vectors']):raise ValueError('placement candidate changes saved content')
    if set(after['owners'])!=set(before['owners']):raise ValueError('placement owner coverage differs')
    for sid,owner in before['owners'].items():
        if not _same({k:v for k,v in owner.items() if k!='worker_id'},{k:v for k,v in after['owners'][sid].items() if k!='worker_id'}):raise ValueError('logical placement changes physical snapshot')
    if content_hash(prepared['proposal'])!=prepared['proposal_sha256']:raise ValueError('placement proposal changed')
    expected=_segments(context['parent']['segments']);expected[context['vector_segment_id']]['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(prepared['descriptor']).decode().strip()]
    proposed={s['segment_id']:s['file_paths'] for s in prepared['proposal']['segment_flush_info']}
    if not _same(proposed,{sid:s['file_paths'] for sid,s in expected.items()}):raise ValueError('placement complete segment files differ')
    c=context['parent']['collection'];proposal=prepared['proposal']
    expected_proposal=dict(tenant_id=c['tenant_id'],database_name=c['database_name'],collection_id=c['collection_id'],collection_version=c['version'],log_position=c['log_position'],segment_flush_info=[dict(segment_id=sid,file_paths=expected[sid]['file_paths']) for sid in sorted(expected)],total_records_post_compaction=c['total_records_post_compaction'],size_bytes_post_compaction=c['size_bytes_post_compaction'],schema=c.get('schema'))
    if not _same(proposal,expected_proposal):raise ValueError('placement version/log/count/size differs')
    candidate=copy.deepcopy(context['parent']);candidate['collection']['version']+=1
    for segment in candidate['segments']:segment['file_paths']=copy.deepcopy(expected[segment['segment_id']]['file_paths'])
    if not _same(candidate,prepared['candidate_state']):raise ValueError('placement candidate registration differs')



def reconcile_namespace_placement(delegate,prepared):
    from .tree_manifest import _atomic_write
    collection=prepared['context']['parent']['collection']['collection_id']
    try:
        observed=delegate.observe(collection)
        status='committed' if _same(observed,prepared['candidate_state']) else 'pending-or-unknown'
    except BaseException as error:
        status='pending-or-unknown'
    result=dict(status=status,attempt_id=prepared['attempt_id'],proposal_sha256=prepared['proposal_sha256'])
    _atomic_write(prepared['journal_path'],canonical_bytes(result));return result


def submit_namespace_placement(delegate,prepared):
    """Submit once; an ambiguous outcome never authorizes a registration retry."""
    return _submit_namespace_proposal(delegate,prepared,_placement_health)


def _submit_namespace_proposal(delegate,prepared,health):
    """Share the existing submit-once journal and ambiguity rules internally."""
    import uuid
    from .tree_manifest import _atomic_write
    if prepared['attempt_id'] is not None:return reconcile_namespace_placement(delegate,prepared)
    from pathlib import Path
    if Path(prepared['journal_path']).exists():
        previous=load_json(prepared['journal_path'])
        if previous.get('proposal_sha256')!=prepared['proposal_sha256'] or not isinstance(previous.get('attempt_id'),str):raise ValueError('placement submission remains unreconciled')
        prepared['attempt_id']=previous['attempt_id'];return reconcile_namespace_placement(delegate,prepared)
    health(delegate,prepared)
    prepared['attempt_id']=str(uuid.uuid4())
    _atomic_write(prepared['journal_path'],canonical_bytes(dict(status='registration-submitted',attempt_id=prepared['attempt_id'],proposal_sha256=prepared['proposal_sha256'])))
    try:delegate.start_registration(prepared['attempt_id'],copy.deepcopy(prepared['proposal']))
    except BaseException as error:
        _atomic_write(prepared['journal_path'],canonical_bytes(dict(status='pending-or-unknown',attempt_id=prepared['attempt_id'],proposal_sha256=prepared['proposal_sha256'],error=repr(error))))
    return reconcile_namespace_placement(delegate,prepared)


def prepare_current_namespace_export(delegate,collection_id,expected_bundle,rows,store,output):
    """Capture the current parent before launching any allocator or writer."""
    from pathlib import Path
    from .tree_manifest import _atomic_write,artifact
    context=copy.deepcopy(delegate.capture(collection_id));collection=context['parent']['collection']
    if collection['collection_id']!=collection_id:raise ValueError('namespace collection differs')
    if not _same(delegate.observe(collection['collection_id']),context['parent']):raise ValueError('namespace current capture is stale')
    view=current_view(context)
    if not _same(view['registered_bundle'],expected_bundle):raise ValueError('requested namespace bundle is stale')
    if view['current_parent']['next_global_id']!=18 or len(rows)!=2:raise ValueError('bounded second-append fixture only')
    if any(set(r)!={'log_offset','user_id','embedding_bits'} or not _same(r['log_offset'],collection['log_position']+i+1) for i,r in enumerate(rows)):raise ValueError('exact current source window required')
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    path=output/'input.json';_atomic_write(path,canonical_bytes(dict(capture=context,expected_registered_bundle=expected_bundle,rows=rows,store=str(Path(store).resolve()))))
    return artifact(path)
