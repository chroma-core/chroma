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
