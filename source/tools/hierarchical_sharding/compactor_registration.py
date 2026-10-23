"""Opt-in bridge from private hierarchical writes to collection registration.

The authority is an internal compactor adapter, not a caller-provided trust flag.
No production RPC is installed here. The existing collection version is the
only publication authority; this module never advances a deployment pointer.
"""
import copy
import json
import math
from pathlib import Path
import struct
import uuid
from .dynamic_deployment import validate_record
from .dynamic_runtime import durable_directory
from .dynamic_write import prepare_global_append
from .tree_manifest import artifact, checked_artifact, canonical_bytes, content_hash, _atomic_write

HIERARCHY_BUNDLE='hierarchical_spann_bundle_v1'

def _integer(value,minimum=0):
    return type(value) is int and minimum<=value<=2**63-1

def _bits(vector,dimension):
    if not isinstance(vector,list) or len(vector)!=dimension or any(type(v) not in (int,float) or not math.isfinite(v) for v in vector):raise ValueError('finite source embedding required')
    encoded=struct.pack('<'+'f'*dimension,*vector)
    if any(not math.isfinite(v) for v in struct.unpack('<'+'f'*dimension,encoded)):raise ValueError('source embedding overflows float32')
    return list(struct.unpack('<'+'I'*dimension,encoded))

def _segments(rows):
    if not isinstance(rows,list) or not rows:raise ValueError('complete segment set required')
    result={}
    for row in rows:
        sid=row['segment_id'];uuid.UUID(sid)
        if sid in result or not isinstance(row['file_paths'],dict):raise ValueError('duplicate or malformed segment')
        if any(not isinstance(k,str) or not isinstance(v,list) or any(not isinstance(p,str) for p in v) for k,v in row['file_paths'].items()):raise ValueError('malformed segment paths')
        result[sid]=copy.deepcopy(row)
    return result

class CompactorRegistration:
    """One add-only local task session, sourced by the existing compactor.

    Authority methods supply captured context, pulled source rows, staged normal
    segment outputs, registration attempt observations and the existing log
    offset update. Fake implementations establish an in-process contract only.
    """
    def __init__(self,authority,collection_id,planner_binary,reader_binary,output):
        uuid.UUID(collection_id)
        self.authority=authority
        self.context=copy.deepcopy(authority.capture(collection_id))
        self.parent_state=copy.deepcopy(self.context['parent'])
        c=self.parent_state['collection'];job=self.context['compaction']
        if c['collection_id']!=collection_id or not c['tenant_id'] or not c['database_name'] or not _integer(c['version']) or c['version']>=2**31-1 or not _integer(c['log_position'],-1):raise ValueError('invalid captured collection')
        for key in ('job_id','session_id'):uuid.UUID(job[key])
        self.parent_segments=_segments(self.parent_state['segments'])
        self.vector_id=self.context['vector_segment_id'];selected=self.parent_segments[self.vector_id]
        if selected.get('scope')!='VECTOR' or selected.get('type')!='urn:chroma:segment/vector/spann':raise ValueError('captured vector segment type/scope differs')
        references=selected['file_paths'].get(HIERARCHY_BUNDLE)
        if not isinstance(references,list) or len(references)!=1:raise ValueError('registered hierarchy parent required; legacy bootstrap unsupported')
        self.parent_reference=json.loads(references[0]);parent_path=checked_artifact(self.parent_reference,'.')
        loaded=json.loads(parent_path.read_text());self.parent=loaded.get('deployment',loaded);validate_record(self.parent)
        if 'deployment' in loaded and (loaded['collection_parent']['collection']['collection_id']!=collection_id or loaded['collection_parent']['collection']['version']+1!=c['version'] or loaded['compaction']['pulled_log_offset']!=c['log_position']):raise ValueError('registered bundle collection generation differs')
        tree=self.parent['tree']
        if tree['metric']!='l2':raise ValueError('first compactor contract supports L2 adds only')
        if tree['source']['row_count']!=c['total_records_post_compaction']:raise ValueError('registered hierarchy count differs from collection')
        target=job['pulled_log_offset']
        if not _integer(target) or target<=c['log_position']:raise ValueError('nonempty captured source window required')
        source=copy.deepcopy(authority.source_logs(self.context))
        if len(source)!=target-c['log_position']:raise ValueError('complete contiguous add-only source window required')
        users=set();self.rows=[]
        for i,row in enumerate(source):
            if row.get('operation')!='add' or row.get('materialized_operation')!='add_new' or row.get('metadata') is not None or row.get('document') is not None or not _integer(row.get('log_offset')) or not _integer(row.get('global_id')) or row.get('log_offset')!=c['log_position']+i+1 or row.get('global_id')!=tree['source']['row_count']+i or not isinstance(row.get('user_id'),str) or not row['user_id'] or row['user_id'] in users:raise ValueError('source add/offset/stable ID binding differs')
            users.add(row['user_id']);self.rows.append(dict(log_offset=row['log_offset'],global_id=row['global_id'],user_id=row['user_id'],embedding_bits=_bits(row['embedding'],tree['dimension'])))
        if not _integer(job['target_total_records']) or job['target_total_records']!=tree['source']['row_count']+len(self.rows) or not _integer(job['target_logical_size_bytes']):raise ValueError('normal compactor count/size differs')
        self.materialized_binding=copy.deepcopy(authority.source_binding(self.context)) if hasattr(authority,'source_binding') else None
        self.output=Path(output).resolve()
        protected=[Path(o['checkpoint_dir']).resolve() for o in self.parent['owners'].values()]+[Path(self.parent['canonical_vectors']['path']).resolve(),Path(tree['model']['path']).resolve(),parent_path.resolve()]
        if any(self.output==p or self.output in p.parents or (p.is_dir() and p in self.output.parents) for p in protected):raise ValueError('private output overlaps retained parent')
        self.output.mkdir(parents=True,exist_ok=False)
        self.planner_binary=planner_binary;self.reader_binary=reader_binary
        self.phase='captured';self.task_id=None;self.task_bytes=None;self.reply=None;self.proposal=None;self.attempt=None;self.events=[]
        self._journal('captured')

    def _journal(self,phase,**detail):
        self.phase=phase;self.events.append(dict(phase=phase,**detail))
        journal=dict(context=self.context,events=self.events)
        if getattr(self,'materialized_binding',None) is not None:journal['materialized_source_binding']=copy.deepcopy(self.materialized_binding)
        _atomic_write(self.output/'session.json',canonical_bytes(journal))

    def task_spec(self):
        spec=dict(op='prepare_global_append',collection_id=self.context['parent']['collection']['collection_id'],job_id=self.context['compaction']['job_id'],session_id=self.context['compaction']['session_id'],parent_version=self.parent_state['collection']['version'],parent_descriptor=self.parent_reference,pulled_log_offset=self.context['compaction']['pulled_log_offset'],model=self.parent['tree']['model'],ownership_generation=self.parent['fence'],owners=self.parent['owners'],rows=self.rows)
        if self.materialized_binding is not None:spec['materialized_source_binding']=copy.deepcopy(self.materialized_binding)
        return copy.deepcopy(spec)

    def _health(self):
        if canonical_bytes(self.authority.observe(self.parent_state['collection']['collection_id']))!=canonical_bytes(self.parent_state):raise ValueError('collection registration parent changed')
        if self.authority.current_session(self.parent_state['collection']['collection_id'])!={key:self.context['compaction'][key] for key in ('job_id','session_id')}:raise ValueError('compactor task session changed')
        checked_artifact(self.parent_reference,'.');validate_record(self.parent)
        if self.materialized_binding is not None and canonical_bytes(self.authority.source_binding(self.context))!=canonical_bytes(self.materialized_binding):raise ValueError('captured materializer source changed')

    def prepare(self,task_id,payload):
        uuid.UUID(task_id);encoded=canonical_bytes(payload)
        if self.task_id is not None:
            if task_id!=self.task_id or encoded!=self.task_bytes:raise ValueError('conflicting task replay')
            if self.reply is None:raise ValueError('poisoned task session requires a fresh immutable-parent attempt')
            return copy.deepcopy(self.reply)
        if encoded!=canonical_bytes(self.task_spec()):raise ValueError('task differs from captured source/vector/routing fence')
        self._health();self.task_id=task_id;self.task_bytes=encoded
        # Intent exists before canonical copying, owner forks or lower writes.
        self._journal('preparing',task_id=task_id,task_sha256=content_hash(payload))
        try:
            canonical=self.output/'canonical.f32'
            with checked_artifact(self.parent['canonical_vectors'],'.').open('rb') as source,canonical.open('xb') as dest:
                for part in iter(lambda:source.read(1024*1024),b''):dest.write(part)
                for row in self.rows:dest.write(struct.pack('<'+'I'*len(row['embedding_bits']),*row['embedding_bits']))
            record=prepare_global_append(self.planner_binary,self.reader_binary,self.parent,canonical,self.output/'private-writers',timeout=30)
            validate_record(record)
            plan_path=self.output/'private-writers/write-plan.stdout';plan=json.loads(plan_path.read_text())
            if [r['id'] for r in plan['routes']]!=[r['global_id'] for r in self.rows]:raise ValueError('saved routes differ from source IDs')
            self._health()
            outputs=_segments(self.authority.segment_outputs(self.context))
            if set(outputs)!=set(self.parent_segments) or outputs[self.vector_id]['file_paths']!=self.parent_segments[self.vector_id]['file_paths']:raise ValueError('complete segment output set or selected segment keys differ')
            bundle=dict(format_version=1,collection_parent=self.parent_state,compaction=self.context['compaction'],source_rows=self.rows,task_id=task_id,task_sha256=content_hash(payload),writer_execution=artifact(plan_path),segment_outputs=outputs,deployment=record)
            if self.materialized_binding is not None:bundle['materialized_source_binding']=copy.deepcopy(self.materialized_binding)
            path=self.output/'hierarchy-bundle.json';_atomic_write(path,canonical_bytes(bundle));self.bundle_reference=artifact(path)
            outputs[self.vector_id]['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(self.bundle_reference).decode().strip()]
            c=self.parent_state['collection'];job=self.context['compaction']
            self.proposal=dict(tenant_id=c['tenant_id'],database_name=c['database_name'],collection_id=c['collection_id'],collection_version=c['version'],log_position=job['pulled_log_offset'],segment_flush_info=[dict(segment_id=sid,file_paths=outputs[sid]['file_paths']) for sid in sorted(outputs)],total_records_post_compaction=record['tree']['source']['row_count'],size_bytes_post_compaction=job['target_logical_size_bytes'],schema=c.get('schema'))
            self.candidate_state=copy.deepcopy(self.parent_state)
            self.candidate_state['collection'].update(version=c['version']+1,log_position=self.proposal['log_position'],total_records_post_compaction=self.proposal['total_records_post_compaction'],size_bytes_post_compaction=self.proposal['size_bytes_post_compaction'])
            for row in self.candidate_state['segments']:row['file_paths']=copy.deepcopy(outputs[row['segment_id']]['file_paths'])
            durable_directory(self.output)
            self.proposal_sha256=content_hash(self.proposal)
            self.reply=dict(task_id=task_id,descriptor=self.bundle_reference,registration=copy.deepcopy(self.proposal),routes=plan['routes'])
            self._journal('prepared',descriptor=self.bundle_reference,registration_sha256=content_hash(self.proposal))
            return copy.deepcopy(self.reply)
        except BaseException as error:
            self._journal('poisoned-private-attempt',error=repr(error));raise

    def submit(self):
        if self.attempt is not None:return self.reconcile()
        if self.phase!='prepared':raise ValueError('complete prepared task required')
        self._health();checked_artifact(self.bundle_reference,'.')
        if content_hash(self.proposal)!=self.proposal_sha256:raise ValueError('prepared registration changed')
        bundle=json.loads(Path(self.bundle_reference['path']).read_text());validate_record(bundle['deployment'])
        self.attempt=str(uuid.uuid4());self._journal('registration-submitted',attempt_id=self.attempt,registration_sha256=content_hash(self.proposal))
        try:self.authority.start_registration(self.attempt,copy.deepcopy(self.proposal))
        except BaseException as error:self._journal('registration-outcome-unknown',attempt_id=self.attempt,error=repr(error))
        return self.reconcile()

    def reconcile(self):
        if self.attempt is None:raise ValueError('no registration attempt')
        try:
            observed=self.authority.observe(self.parent_state['collection']['collection_id'])
            if canonical_bytes(observed)==canonical_bytes(self.candidate_state):
                checked_artifact(self.bundle_reference,'.')
                log=self.authority.log_position(self.parent_state['collection']['collection_id'])
                if not _integer(log,-1):raise ValueError('invalid authoritative log position')
                result=dict(status='committed',log_synchronized=log>=self.proposal['log_position'])
            elif canonical_bytes(observed)==canonical_bytes(self.parent_state):
                terminal=self.authority.attempt_status(self.attempt,content_hash(self.proposal))
                result=dict(status='terminal-rejected' if terminal=='authoritative-terminal-rejection' else 'pending-or-unknown',log_synchronized=False)
            else:result=dict(status='pending-or-unknown',log_synchronized=False)
        except BaseException as error:result=dict(status='pending-or-unknown',log_synchronized=False,error=repr(error))
        self._journal('reconciled',attempt_id=self.attempt,**result)
        return result

    def reconcile_log(self):
        state=self.reconcile()
        if state['status']!='committed':raise ValueError('cannot advance log without exact authoritative candidate publication')
        # No registration or private writer retry occurs here.
        # This contract must compare the exact candidate and serialize against newer
        # registration/log updates. The existing generic Log trait does not supply
        # this guarantee; a production backend must leave repair unsupported.
        self.authority.reconcile_log_if_current(copy.deepcopy(self.candidate_state),self.proposal['log_position'])
        return self.reconcile()
