"""Opt-in tiny worker transport for one captured collection proposal.

Complete local mirrors retain the existing full validation oracle. They are
fixture evidence, not a replication policy or a production transport service.
"""
import base64
import copy
from pathlib import Path
import uuid
from .compactor_materialized import checked,load_json,integer
from .compactor_namespace import current_view,_same,_submit_namespace_proposal,reconcile_namespace_placement
from .compactor_topology import _capture_health,_proposal,_candidate_context
from .dynamic_archive import export_checkpoint,import_checkpoint
from .dynamic_remote import RemoteBuilder
from .dynamic_runtime import inventory,seal_owner
from .dynamic_deployment import make_record,validate_record
from .tree_manifest import artifact,canonical_bytes,content_hash,file_hash,_atomic_write


def _metadata(row,subtree,expected_inventory):
    if row['subtree']['subtree_id']!=subtree['subtree_id'] or row['subtree']['generation']!=subtree['generation'] or row['subtree']['snapshot']['sha256']!=subtree['snapshot']['sha256'] or row['subtree']['local_to_global']['sha256']!=subtree['local_to_global']['sha256'] or row['owner']['inventory_sha256']!=expected_inventory or content_hash(row['inventory'])!=expected_inventory:raise ValueError('agent source snapshot/map/inventory differs')
    if base64.b64decode(row['manifest_b64'],validate=True)!=checked(subtree['snapshot'],2*1024**2).read_bytes() or base64.b64decode(row['map_b64'],validate=True)!=checked(subtree['local_to_global'],1024).read_bytes():raise ValueError('agent source bytes differ')


def _owner(directory,subtree,worker,replica,client):
    owner=seal_owner(directory,subtree,worker);owner['remote']=dict(worker_id=worker,replica_id=replica,instance_id=client.instance_id,agent_run_id=client.run_id);return owner


class TransportLedger:
    """Record owned operation identities before any mutating agent request."""
    def __init__(self,root,parent):
        self.root=Path(root);self.root.mkdir(parents=True,exist_ok=False);self.path=self.root/'operations.json';self.state=dict(parent_deployment_sha256=content_hash(parent),parent_fence=parent['fence'],operations=[]);self.save()
    def save(self):_atomic_write(self.path,canonical_bytes(self.state))
    def start(self,worker,kind,**identity):
        row=dict(operation_id=uuid.uuid4().hex,worker=worker,kind=kind,status='started',**identity);self.state['operations'].append(row);self.save();return row
    def finish(self,row,**result):row.update(status='completed',**result);self.save()
    def unknown(self,row,error):row.update(status='unreconciled',error=repr(error));self.save()


class TrackedClient:
    def __init__(self,client,ledger):self.client=client;self.ledger=ledger
    def __getattr__(self,name):return getattr(self.client,name)
    def rpc(self,payload):
        if payload['op'] in ('info','metadata','probe-generation','batch','selected-write-status'):return self.client.rpc(payload)
        identity={k:payload[k] for k in ('replica_id','generation_id') if k in payload};row=self.ledger.start(self.worker_id,payload['op'],**identity)
        try:
            result=self.client.rpc(payload);self.ledger.finish(row,returned_handles={k:result[k] for k in ('export_id','ticket','generation_id','replica_id') if k in result});return result
        except BaseException as error:self.ledger.unknown(row,error);raise
    def upload(self,replica,archive,receipt,subtree):
        row=self.ledger.start(self.worker_id,'upload',replica_id=replica,snapshot_sha256=subtree['snapshot']['sha256'],receipt=receipt)
        try:
            result=self.client.upload(replica,archive,receipt,subtree);self.ledger.finish(row);return result
        except BaseException as error:self.ledger.unknown(row,error);raise
    def mirror(self,replica,expected_inventory,target,archive):
        exported=self.rpc(dict(op='export',replica_id=replica,expected_inventory_sha256=expected_inventory));self.download(exported['export_id'],exported['receipt'],archive);import_checkpoint(archive,target,exported['receipt']);return exported['receipt']


class TrackedGeneration:
    """Retain caller-chosen generation IDs even if readiness acknowledgements fail."""
    def __init__(self,record,clients,ledger):
        self.record=copy.deepcopy(record);self.clients=clients;self.ledger=ledger;self.generations={};self.closed=False
        groups={}
        for subtree in record['tree']['subtrees']:
            remote=record['owners'][subtree['subtree_id']]['remote'];worker=remote['worker_id'];client=clients[worker]
            if remote['instance_id']!=client.instance_id or remote['agent_run_id']!=client.run_id:raise ValueError('generation physical tuple differs')
            groups.setdefault(worker,[]).append(dict(subtree_id=subtree['subtree_id'],replica_id=remote['replica_id'],snapshot_sha256=subtree['snapshot']['sha256']))
        try:
            for worker,items in sorted(groups.items()):
                handle='generation-'+uuid.uuid4().hex;self.generations[worker]=handle
                # The owned handle is durable before the request, not after ack.
                ledger.state.setdefault('generations',{})[worker]=handle;ledger.save()
                clients[worker].rpc(dict(op='prepare-generation',generation_id=handle,tree_sha256=record['tree_sha256'],subtrees=items))
            self.verify()
        except BaseException:
            try:self.close()
            except BaseException:pass
            raise
    def verify(self):
        if self.closed:raise ValueError('owned generation is closed')
        for subtree in self.record['tree']['subtrees']:
            sid=subtree['subtree_id'];owner=self.record['owners'][sid];remote=owner['remote'];client=self.clients[remote['worker_id']]
            row=client.rpc(dict(op='metadata',replica_id=remote['replica_id']));_metadata(row,subtree,owner['inventory_sha256'])
            request_id=uuid.uuid4().hex;payload=dict(tree_sha256=self.record['tree_sha256'],request_id=request_id,timeout_s=10,requests=[dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root'))])
            result=client.rpc(dict(op='probe-generation',generation_id=self.generations[remote['worker_id']],payload=payload))
            if result.get('request_id')!=request_id or result.get('tree_sha256')!=self.record['tree_sha256'] or result.get('owner_id')!=remote['worker_id']:raise ValueError('generation response differs')
            rows=result.get('responses',[])
            if len(rows)!=1 or rows[0]['snapshot_sha256']!=subtree['snapshot']['sha256'] or rows[0]['result']['vector_count']!=subtree['vector_count']:raise ValueError('generation readiness differs')
    def close(self):
        if self.closed:return
        errors=[]
        for worker,handle in list(self.generations.items()):
            try:self.clients[worker].rpc(dict(op='retire',generation_id=handle));del self.generations[worker]
            except BaseException as error:errors.append(repr(error))
        self.ledger.state['cleanup']=dict(status='unreconciled' if errors else 'reconciled',remaining_generations=copy.deepcopy(self.generations),errors=errors);self.ledger.save()
        if errors:raise ValueError('owned generation cleanup unreconciled: '+str(errors))
        self.closed=True


class RemoteTopologyExecution:
    """Execute affected rebuilds only from exact captured remote source replicas."""
    def __init__(self,parent,clients,assignments,output,health):
        self.parent=copy.deepcopy(parent);self.raw_clients=dict(clients);self.assignments=copy.deepcopy(assignments);self.output=Path(output);self.health=health;self.ledger=None;self.clients={};self.builder=None;self.generation=None;self.readiness_started=False;self.sources={}
        if not 1<=len(clients)<=2:raise ValueError('one or two bounded agent clients required')
        for sid,owner in parent['owners'].items():
            remote=owner.get('remote')
            if not remote or remote['worker_id']!=owner['worker_id']:raise ValueError('remote topology requires captured remote source owners')
            client=clients[remote['worker_id']]
            if remote['instance_id']!=client.instance_id or remote['agent_run_id']!=client.run_id:raise ValueError('captured source physical tuple differs')
            self.sources[sid]=copy.deepcopy(dict(remote=remote,snapshot_sha256=owner['snapshot_sha256'],inventory_sha256=owner['inventory_sha256']))
    def binding(self):return dict(parent_sha256=content_hash(self.parent),fence=self.parent['fence'],sources=copy.deepcopy(self.sources),assignments=copy.deepcopy(self.assignments))
    def preflight(self,parent,replacements):
        if not _same(parent,self.parent):raise ValueError('remote execution parent differs')
        if parent['tree'].get('routing_boundary_scheme')!='centroid-content-v1':raise ValueError('remote topology requires committed per-centroid boundaries')
        if len(parent['owners'])>8 or parent['tree']['source']['row_count']>128 or parent['tree']['dimension']>16:raise ValueError('tiny remote topology admission exceeded')
        from .dynamic_topology import owned_ids
        slots={s['subtree_id']:s for s in parent['tree']['subtrees']};affected={s for item in replacements.values() for s in item['from']}
        if not affected<=set(slots):raise ValueError('remote source owner differs')
        expected={i for sid in affected for i in owned_ids(slots[sid])};seen=set()
        for item in replacements.values():
            allowed={i for sid in item['from'] for i in owned_ids(slots[sid])}
            if not item['ids'] or any(type(i) is not int or i not in allowed or i in seen for i in item['ids']) or len(item['ids'])!=len(set(item['ids'])):raise ValueError('remote replacement ownership differs')
            seen.update(item['ids'])
        if seen!=expected or len(set(slots)-affected)+len(replacements)>8:raise ValueError('remote replacement coverage differs')
        if set(self.assignments)!=set(replacements) or any(w not in self.raw_clients for w in self.assignments.values()):raise ValueError('exact affected destination assignment required')
        self.check_sources()
    def check_sources(self):
        self.health();validate_record(self.parent)
        for subtree in self.parent['tree']['subtrees']:
            owner=self.parent['owners'][subtree['subtree_id']];remote=owner['remote'];row=self.raw_clients[remote['worker_id']].rpc(dict(op='metadata',replica_id=remote['replica_id']));_metadata(row,subtree,owner['inventory_sha256'])
        self.health()
    def prepare_sources(self,replacements):
        self.check_sources();self.ledger=TransportLedger(self.output,self.parent);self.clients={w:TrackedClient(c,self.ledger) for w,c in self.raw_clients.items()}
        slots={s['subtree_id']:s for s in self.parent['tree']['subtrees']}
        pairs={(source,self.assignments[sid]) for sid,item in replacements.items() for source in item['from']}
        for source,destination in sorted(pairs):
            self.health();owner=self.parent['owners'][source];remote=owner['remote']
            if remote['worker_id']==destination:continue
            client=self.clients[remote['worker_id']];exported=client.rpc(dict(op='export',replica_id=remote['replica_id'],expected_inventory_sha256=owner['inventory_sha256']));archive=self.output/(source+'-to-'+destination+'.tar');client.download(exported['export_id'],exported['receipt'],archive)
            if archive.stat().st_size!=exported['receipt']['bytes'] or file_hash(archive)!=exported['receipt']['sha256']:raise ValueError('source transfer wire digest differs')
            if content_hash(exported['receipt']['files'])!=owner['inventory_sha256']:raise ValueError('source transfer inventory differs')
            replica='input-'+uuid.uuid4().hex;result=self.clients[destination].upload(replica,archive,exported['receipt'],slots[source]);_metadata(result,slots[source],owner['inventory_sha256'])
        self.builder=RemoteBuilder(self.clients,self.assignments,health=self.health,attempt=self);self.check_sources()
    def reserve(self,worker,replica,sid,generation,directory):
        self.ledger.start(worker,'build-reservation',replica_id=replica,subtree_id=sid,generation=generation,local_mirror=str(directory),binding=self.binding())
    def __call__(self,*args,**kwargs):
        if self.builder is None:raise ValueError('remote source preparation required')
        result=self.builder(*args,**kwargs)
        for row in self.ledger.state['operations']:
            if row['kind']=='build-reservation' and row.get('subtree_id')==result['subtree_id'] and row['status']=='started':self.ledger.finish(row)
        return result
    def owner(self,subtree):
        built=self.builder.built[subtree['subtree_id']];client=self.clients[built['worker_id']];owner=_owner(Path(subtree['snapshot']['path']).parent,subtree,built['worker_id'],built['replica_id'],client);_metadata(built['metadata'],subtree,owner['inventory_sha256']);return owner
    def ready(self,record):
        if self.readiness_started:raise ValueError('remote readiness attempt is single-use')
        self.readiness_started=True
        self.check_sources();self.generation=TrackedGeneration(record,self.clients,self.ledger);self.health()
    def verify(self,record):
        if self.generation is None or not _same(self.generation.record,record):raise ValueError('transport candidate differs')
        self.check_sources();self.generation.verify();self.health()
    def close(self):
        if self.generation is not None:self.generation.close()
        if self.ledger is not None:
            pending=[row['operation_id'] for row in self.ledger.state['operations'] if row['status']!='completed']
            self.ledger.state['cleanup']=dict(status='unreconciled' if pending else 'reconciled',pending_operations=pending);self.ledger.save()
            if pending:raise ValueError('remote private operations remain unreconciled')


def prepare_transferred_placement(delegate,collection_id,clients,assignments,output,binary,record_auditor):
    """Copy exact replicas, then prepare one complete physical-placement proposal."""
    context=copy.deepcopy(delegate.capture(collection_id));_capture_health(delegate,context);view=current_view(context);parent=load_json(checked(view['registered_bundle'],32*1024**2))['deployment'];validate_record(parent)
    if not 1<=len(clients)<=2 or len(parent['owners'])>8 or parent['tree']['source']['row_count']>128 or parent['tree']['dimension']>16:raise ValueError('tiny transfer placement admission exceeded')
    if not integer(context['parent']['collection']['version'],2**31-2) or not integer(parent['fence'],2**64-2,1):raise ValueError('transfer placement version/fence exhausted')
    if set(assignments)!=set(parent['owners']) or any(w not in clients for w in assignments.values()):raise ValueError('complete verified transfer assignment required')
    if any(o.get('remote') for o in parent['owners'].values()):raise ValueError('initial transfer placement requires local captured sources')
    job=context['compaction'];c=context['parent']['collection']
    if [job['pulled_log_offset'],job['target_total_records'],job['target_logical_size_bytes']]!=[c['log_position'],c['total_records_post_compaction'],c['size_bytes_post_compaction']]:raise ValueError('transfer placement must preserve log/count/size')
    output=Path(output).resolve()
    protected=[Path(o['checkpoint_dir']).resolve() for o in parent['owners'].values()]+[Path(parent['canonical_vectors']['path']).resolve(),Path(view['registered_bundle']['path']).resolve(),Path(view['metadata']['namespace_root']['path']).resolve()]
    if any(output==p or output in p.parents or (p.is_dir() and p in output.parents) for p in protected):raise ValueError('transfer output overlaps retained parent')
    output.mkdir(parents=True,exist_ok=False);ledger=TransportLedger(output/'transport',parent);tracked={w:TrackedClient(client,ledger) for w,client in clients.items()};generation=None
    _atomic_write(output/'preparation.json',canonical_bytes(dict(status='preparing-private-transfer',parent=view['registered_bundle'])))
    try:
        owners={}
        for subtree in parent['tree']['subtrees']:
            sid=subtree['subtree_id'];source=parent['owners'][sid];_capture_health(delegate,context)
            if content_hash(inventory(source['checkpoint_dir']))!=source['inventory_sha256']:raise ValueError('local transfer source changed')
            archive=output/(sid+'.tar');receipt=export_checkpoint(source['checkpoint_dir'],archive);worker=assignments[sid];replica='placement-'+uuid.uuid4().hex;metadata=tracked[worker].upload(replica,archive,receipt,subtree);_metadata(metadata,subtree,source['inventory_sha256']);mirror=output/('mirror-'+sid);tracked[worker].mirror(replica,source['inventory_sha256'],mirror,output/(sid+'-mirror.tar'));owners[sid]=_owner(mirror,subtree,worker,replica,tracked[worker])
        record=make_record(copy.deepcopy(parent['tree']),owners,parent,'placement');validate_record(record);generation=TrackedGeneration(record,tracked,ledger)
        bundle=dict(format_version=1,operation='verified-transfer-placement',collection_parent=context['parent'],compaction=context['compaction'],deployment=record,hierarchy_metadata=copy.deepcopy(view['metadata']));path=output/'bundle.json';_atomic_write(path,canonical_bytes(bundle));reference=artifact(path);proposal,candidate=_proposal(context,reference);target=_candidate_context(context,candidate);current_view(target);record_auditor(target,view['metadata']['namespace_root'],output/'prepare-audit');generation.verify();_capture_health(delegate,context)
        prepared=dict(context=context,descriptor=reference,proposal=proposal,candidate_state=candidate,proposal_sha256=content_hash(proposal),expected_current_bundle=view['registered_bundle'],journal_path=str(output/'submission.json'),attempt_id=None,transport_parent=parent)
        _atomic_write(output/'prepared.json',canonical_bytes(prepared));return prepared,generation,ledger
    except BaseException as error:
        _atomic_write(output/'preparation.json',canonical_bytes(dict(status='failed-private-transfer',error=repr(error))))
        if generation is not None:
            try:generation.close()
            except BaseException as cleanup_error:error.add_note(repr(cleanup_error))
        raise


def submit_transferred_placement(delegate,prepared,generation,record_auditor):
    def health(delegate,prepared):
        context=prepared['context'];_capture_health(delegate,context);before=prepared['transport_parent'];bundle=load_json(checked(prepared['descriptor'],32*1024**2));after=bundle['deployment'];validate_record(after)
        if not _same(bundle['collection_parent'],context['parent']) or not _same(bundle['compaction'],context['compaction']) or not _same(current_view(context)['registered_bundle'],prepared['expected_current_bundle']):raise ValueError('transfer placement captured context differs')
        if not _same(bundle['hierarchy_metadata'],current_view(context)['metadata']) or not _same(after['tree'],before['tree']) or not _same(after['canonical_vectors'],before['canonical_vectors']) or after['parent_sha256']!=content_hash(before) or after['fence']!=before['fence']+1:raise ValueError('transfer placement changes logical content')
        if not _same(generation.record,after):raise ValueError('transfer generation differs')
        if any(after['owners'][sid]['inventory_sha256']!=before['owners'][sid]['inventory_sha256'] or after['owners'][sid]['snapshot_sha256']!=before['owners'][sid]['snapshot_sha256'] for sid in before['owners']):raise ValueError('transfer placement source inventory differs')
        proposal,candidate=_proposal(context,prepared['descriptor'])
        if not _same(proposal,prepared['proposal']) or content_hash(proposal)!=prepared['proposal_sha256'] or not _same(candidate,prepared['candidate_state']):raise ValueError('transfer complete proposal differs')
        generation.verify();target=_candidate_context(context,candidate);view=current_view(target);record_auditor(target,view['metadata']['namespace_root'],Path(prepared['journal_path']).parent/('submit-audit-'+uuid.uuid4().hex));generation.verify();_capture_health(delegate,context)
    def once(delegate,prepared):
        import os
        from .dynamic_runtime import durable_namespace
        marker=Path(prepared['journal_path']).parent/'validation-started.json'
        if marker.exists():raise ValueError('transfer placement validation already consumed')
        with marker.open('xb') as stream:
            stream.write(canonical_bytes(dict(proposal_sha256=prepared['proposal_sha256'])));stream.flush();os.fsync(stream.fileno())
        durable_namespace(marker.parent)
        try:return health(delegate,prepared)
        except BaseException as error:
            try:_atomic_write(marker.parent/'preparation.json',canonical_bytes(dict(status='failed-submit-validation',error=repr(error))))
            except BaseException as journal_error:error.add_note(repr(journal_error))
            raise
    return _submit_namespace_proposal(delegate,prepared,once)


def reconcile_transferred_placement(delegate,prepared):return reconcile_namespace_placement(delegate,prepared)
