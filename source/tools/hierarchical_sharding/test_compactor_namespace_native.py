"""Actual record/materializer and saved vector leaves; local fake authority only."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import unittest
import uuid
from .compactor_namespace import ExportedNamespaceAuthority,NamespacePreparation,SavedNamespaceProducer,current_view
from .compactor_registration import CompactorRegistration,HIERARCHY_BUNDLE
from .dynamic_deployment import load_deployment,validate_record
from .dynamic_runtime import inventory
from .test_compactor_registration import captured_authority,CapturedAuthority
from .tree_manifest import artifact,canonical_bytes,content_hash,checked_artifact


def prepare_fixture(root,pointer):
    root=Path(root).resolve();root.mkdir(exist_ok=False);parent=load_deployment(pointer);validate_record(parent)
    if parent['tree']['source']['row_count']!=16 or parent['tree']['dimension']!=4:raise ValueError('reviewed retained16x4 parent required')
    descriptor=root/'vector-parent.json';descriptor.write_bytes(canonical_bytes(parent));delegate=captured_authority(parent,artifact(descriptor));context=delegate.context
    context['parent']['collection']['log_position']=20;context['compaction']['pulled_log_offset']=22
    for i,row in enumerate(delegate.source):row['log_offset']=21+i
    delegate.source[0]['embedding'][0]=-0.0
    record=next(s for s in context['parent']['segments'] if s['scope']=='RECORD');record.update(type='urn:chroma:segment/record/blockfile',file_paths={})
    # Independent identity assignments exist before the record insertion and
    # proposed mapping. They bind exact canonical vector row identities.
    source=root/'shared-identities.json';source.write_bytes(canonical_bytes([dict(global_id=i,user_id='parent-'+str(i)) for i in range(16)]))
    c=context['parent']['collection'];rows=[dict(log_offset=r['log_offset'],user_id=r['user_id'],embedding_bits=list(struct.unpack('<IIII',struct.pack('<ffff',*r['embedding'])))) for r in delegate.source]
    data=dict(collection_id=c['collection_id'],record_segment_id=record['segment_id'],tenant=c['tenant_id'],database=c['database_name'],database_id=str(uuid.uuid4()),version=c['version'],log_position=c['log_position'],parent_deployment_sha256=content_hash(parent),ownership_generation=parent['fence'],canonical=parent['canonical_vectors'],identity_source=artifact(source),owner_maps=[s['local_to_global'] for s in sorted(parent['tree']['subtrees'],key=lambda s:s['subtree_id'])],rows=rows)
    input_path=root/'record-input.json';input_path.write_bytes(canonical_bytes(data));context_path=root/'context-before-records.json';context_path.write_bytes(canonical_bytes(context))
    (root/'setup.json').write_bytes(canonical_bytes(dict(parent_pointer=artifact(pointer),parent_vector=artifact(descriptor),context=artifact(context_path),record_input=artifact(input_path),parent_inventory={sid:inventory(o['checkpoint_dir']) for sid,o in parent['owners'].items()})))


@unittest.skipUnless(os.environ.get('SPANN_NAMESPACE_PIPELINE'),'reviewed actual export fixture required')
class NativeNamespaceRegistration(unittest.TestCase):
    def test_actual_record_and_vector_outputs_share_one_registration(self):
        root=Path(os.environ['SPANN_NAMESPACE_PIPELINE']).resolve();setup=json.loads((root/'setup.json').read_text());parent=json.loads(checked_artifact(setup['parent_vector'],'.').read_text());context=json.loads(checked_artifact(setup['context'],'.').read_text())
        exported=root/'record-export';export=artifact(exported/'export.json');receipt=artifact(exported/'export.receipt.json');proof=json.loads(Path(receipt['path']).read_text());runtime=proof['runtime_binary']
        if runtime['sha256']!=os.environ['SPANN_NAMESPACE_RUNTIME_SHA']:raise ValueError('reviewed exact runtime binary required')
        value=json.loads(Path(export['path']).read_text());record_id=json.loads(checked_artifact(value['parent_namespace'],'.').read_text())['record_segment_id']
        record=next(s for s in context['parent']['segments'] if s['segment_id']==record_id);record['file_paths']=copy.deepcopy(value['parent_record_files']);context['record_collection_binding']=copy.deepcopy(value['parent_collection_binding'])
        ns=json.loads(checked_artifact(value['parent_namespace'],'.').read_text())
        construction=root/'parent-construction.json';construction.write_bytes(canonical_bytes(dict(namespace_root=value['parent_namespace'],construction_parent_deployment_sha256=ns['construction_parent_deployment_sha256'],construction_parent_ownership_generation=ns['construction_parent_ownership_generation'],parent_namespace_sha256=ns['parent_namespace_sha256'])))
        genesis=dict(deployment=parent,hierarchy_metadata=dict(record_segment_id=record_id,namespace_root=value['parent_namespace'],construction_receipt=artifact(construction)),collection_parent={'collection':dict(context['parent']['collection'],version=2)},compaction={'pulled_log_offset':20})
        bundle=root/'parent-bundle.json';bundle.write_bytes(canonical_bytes(genesis));vector=next(s for s in context['parent']['segments'] if s['scope']=='VECTOR');vector['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(artifact(bundle)).decode().strip()]
        delegate=CapturedAuthority(context,[]);delegate.source_logs=lambda _:(_ for _ in ()).throw(AssertionError('manual source substitution forbidden'))
        # A real filesystem error after record outputs and private vector
        # outputs exist must leave no eligible bundle or registration attempt.
        failed_delegate=CapturedAuthority(copy.deepcopy(context),[])
        failed_source=ExportedNamespaceAuthority(failed_delegate,export,receipt,runtime)
        blocked=root/'blocked-namespace';blocked.mkdir()
        failed_producer=SavedNamespaceProducer(failed_source,blocked)
        failed_authority=NamespacePreparation(failed_source,failed_producer)
        failed=CompactorRegistration(failed_authority,context['parent']['collection']['collection_id'],os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],root/'failed-private-attempt')
        failed_task=str(uuid.uuid4());failed_spec=failed.task_spec()
        with self.assertRaises(FileExistsError):failed.prepare(failed_task,failed_spec)
        self.assertEqual(failed.phase,'poisoned-private-attempt');self.assertIsNone(failed.reply);self.assertEqual(failed_delegate.starts,0)
        self.assertFalse((root/'failed-private-attempt/hierarchy-bundle.json').exists())
        self.assertTrue((root/'failed-private-attempt/private-writers/write-plan.stdout').exists())
        with self.assertRaisesRegex(ValueError,'fresh immutable-parent'):failed.prepare(failed_task,failed_spec)
        source=ExportedNamespaceAuthority(delegate,export,receipt,runtime);producer=SavedNamespaceProducer(source,root/'namespace-candidate');authority=NamespacePreparation(source,producer)
        session=CompactorRegistration(authority,context['parent']['collection']['collection_id'],os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],root/'private-attempt')
        spec=session.task_spec();reply=session.prepare(str(uuid.uuid4()),spec);saved=json.loads(checked_artifact(reply['descriptor'],'.').read_text());validate_record(saved['deployment'])
        self.assertEqual([(r['record_offset_id'],r['global_id']) for r in value['captured']['rows']],[(17,16),(18,17)])
        self.assertEqual(saved['source_rows'][0]['embedding_bits'][0],0x80000000)
        expected_files=value['candidate_record_files'];proposal_files=next(s['file_paths'] for s in session.proposal['segment_flush_info'] if s['segment_id']==record_id)
        self.assertEqual(proposal_files,expected_files);self.assertNotEqual(proposal_files,value['parent_record_files'])
        self.assertEqual(saved['hierarchy_metadata']['namespace_root'],producer.saved['metadata']['namespace_root'])
        for segment in context['parent']['segments']:
            actual=next(s['file_paths'] for s in session.proposal['segment_flush_info'] if s['segment_id']==segment['segment_id'])
            if segment['segment_id']!=record_id:
                for key,paths in segment['file_paths'].items():
                    if key!=HIERARCHY_BUNDLE:self.assertEqual(actual[key],paths)
        # Actual full record audit repeats immediately before the sole attempt.
        self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1)
        delegate.state=copy.deepcopy(session.candidate_state);self.assertEqual(session.reconcile()['status'],'committed')
        next_context=copy.deepcopy(context);next_context['parent']=copy.deepcopy(session.candidate_state)
        next_context['record_collection_binding']=json.loads(checked_artifact(saved['hierarchy_metadata']['namespace_root'],'.').read_text())['collection']
        self.assertEqual(current_view(next_context)['current_parent']['next_global_id'],18)
        responses=[]
        for route in reply['routes']:
            sid=route['subtree_id'];subtree=next(s for s in saved['deployment']['tree']['subtrees'] if s['subtree_id']==sid)
            request=dict(op='score_selected_leaves',generation=subtree['generation'],request_id='namespace-saved-leaf',query=[0]*4,nodes=[route['leaf_id']],candidate_budget=128)
            run=subprocess.run([os.environ['SPANN_DYNAMIC_BINARY'],'read','--checkpoint-dir',saved['deployment']['owners'][sid]['checkpoint_dir'],'--generation',str(subtree['generation'])],input=json.dumps(request)+'\n',capture_output=True,text=True,timeout=5);self.assertEqual(run.returncode,0,run.stderr);response=json.loads(run.stdout)
            self.assertEqual(sum(h['id']==route['id'] and h['local_id']==route['local_id'] for h in response['candidates']),1);responses.append(dict(route=route,request=request,response=response))
        for sid,owner in parent['owners'].items():self.assertEqual(inventory(owner['checkpoint_dir']),setup['parent_inventory'][sid])
        checked_artifact(setup['parent_pointer'],'.');self.assertFalse((root/'current.json').exists())
        (root/'saved-leaves.json').write_bytes(canonical_bytes(responses));(root/'receipt.json').write_bytes(canonical_bytes(dict(scope='actual-local-record-materializer/vector-writer/fake-registration',export=export,export_proof=receipt,bundle=reply['descriptor'],proposal=session.proposal,namespace=saved['hierarchy_metadata'],new_current_view=current_view(next_context),saved_leaf_responses=2,record_audit_invocations=producer.serial,actual_sidecar_directory_failure_poisoned=True,no_second_pointer=True)))
