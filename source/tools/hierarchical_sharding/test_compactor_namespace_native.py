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
        if os.environ.get('SPANN_NAMESPACE_SKIP_SIDECAR')!='1':
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
        (root/'saved-leaves.json').write_bytes(canonical_bytes(responses));(root/'receipt.json').write_bytes(canonical_bytes(dict(scope='actual-local-record-materializer/vector-writer/fake-registration',export=export,export_proof=receipt,bundle=reply['descriptor'],proposal=session.proposal,namespace=saved['hierarchy_metadata'],new_current_view=current_view(next_context),saved_leaf_responses=2,record_audit_invocations=producer.serial,actual_sidecar_directory_failure_poisoned=os.environ.get('SPANN_NAMESPACE_SKIP_SIDECAR')!='1',no_second_pointer=True)))


@unittest.skipUnless(os.environ.get('SPANN_NAMESPACE_REPEATED'),'reviewed two-append current fixture required')
class NativeRepeatedNamespaceRegistration(unittest.TestCase):
    def test_current_bundle_after_placement_drives_second_actual_append(self):
        from unittest.mock import patch
        from .compactor_namespace import prepare_namespace_placement,submit_namespace_placement,reconcile_namespace_placement,prepare_current_namespace_export
        root=Path(os.environ['SPANN_NAMESPACE_REPEATED']).resolve();first=root/'first'
        # Execute the first necessary append with the new admitted binary. The
        # unchanged sidecar-error fixture is not repeated by this sequence.
        with patch.dict(os.environ,{'SPANN_NAMESPACE_PIPELINE':str(first),'SPANN_NAMESPACE_SKIP_SIDECAR':'1'}):
            NativeNamespaceRegistration().test_actual_record_and_vector_outputs_share_one_registration()
        first_receipt=json.loads((first/'receipt.json').read_text());setup=json.loads((first/'setup.json').read_text());initial=json.loads(checked_artifact(setup['context'],'.').read_text());proposal=first_receipt['proposal']
        state=copy.deepcopy(initial['parent']);state['collection'].update(version=proposal['collection_version']+1,log_position=proposal['log_position'],total_records_post_compaction=proposal['total_records_post_compaction'],size_bytes_post_compaction=proposal['size_bytes_post_compaction'])
        files={s['segment_id']:s['file_paths'] for s in proposal['segment_flush_info']}
        for segment in state['segments']:segment['file_paths']=copy.deepcopy(files[segment['segment_id']])
        first_bundle=json.loads(checked_artifact(first_receipt['bundle'],'.').read_text());old_namespace=first_bundle['hierarchy_metadata']['namespace_root'];old_root=json.loads(checked_artifact(old_namespace,'.').read_text());old_rows=json.loads(checked_artifact(old_root['mapping'],'.').read_text())
        old_vectors={sid:inventory(o['checkpoint_dir']) for sid,o in first_bundle['deployment']['owners'].items()}
        context=copy.deepcopy(initial);context['parent']=state;context['record_collection_binding']=copy.deepcopy(old_root['collection']);context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=state['collection']['log_position'],target_total_records=18,target_logical_size_bytes=state['collection']['size_bytes_post_compaction'])
        placement_delegate=CapturedAuthority(context,[]);assignments={sid:('compactor-b' if i==0 else o['worker_id']) for i,(sid,o) in enumerate(sorted(first_bundle['deployment']['owners'].items()))}
        placement=prepare_namespace_placement(placement_delegate,state['collection']['collection_id'],assignments,root/'placement')
        # Delayed publication of a stale proposal fails before its first RPC.
        stale_delegate=CapturedAuthority(copy.deepcopy(context),[]);stale_delegate.state['collection']['version']+=1
        with self.assertRaisesRegex(ValueError,'stale'):submit_namespace_placement(stale_delegate,copy.deepcopy(placement))
        self.assertEqual(stale_delegate.starts,0)
        self.assertEqual(submit_namespace_placement(placement_delegate,placement)['status'],'pending-or-unknown')
        self.assertEqual(submit_namespace_placement(placement_delegate,placement)['status'],'pending-or-unknown');self.assertEqual(placement_delegate.starts,1)
        placement_delegate.state=copy.deepcopy(placement['candidate_state']);self.assertEqual(reconcile_namespace_placement(placement_delegate,placement)['status'],'committed')
        moved=json.loads(checked_artifact(placement['descriptor'],'.').read_text());self.assertEqual(moved['hierarchy_metadata'],first_bundle['hierarchy_metadata']);self.assertEqual(moved['deployment']['tree'],first_bundle['deployment']['tree'])
        for segment in state['segments']:
            if segment['scope']=='RECORD':self.assertEqual(next(s['file_paths'] for s in placement['proposal']['segment_flush_info'] if s['segment_id']==segment['segment_id']),segment['file_paths'])
        self.assertEqual(placement['proposal']['log_position'],state['collection']['log_position']);self.assertEqual(placement['proposal']['total_records_post_compaction'],18);self.assertEqual(placement['proposal']['size_bytes_post_compaction'],state['collection']['size_bytes_post_compaction'])
        next_context=copy.deepcopy(context);next_context['parent']=copy.deepcopy(placement['candidate_state']);next_context['record_collection_binding']['wire']['version']+=1
        next_context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=24,target_total_records=20,target_logical_size_bytes=320)
        delegate=CapturedAuthority(next_context,[]);collection=state['collection']['collection_id'];runtime=first_receipt['export_proof'];first_proof=json.loads(checked_artifact(runtime,'.').read_text());runtime=first_proof['runtime_binary'];first_export=json.loads(checked_artifact(first_receipt['export'],'.').read_text())
        new_rows=[dict(log_offset=23+i,user_id='second-'+str(i),embedding_bits=list(struct.unpack('<IIII',struct.pack('<ffff',*vector)))) for i,vector in enumerate([[.25,-0.,0.,1.],[23.,-0.,1.,0.]])]
        with self.assertRaisesRegex(ValueError,'stale'):prepare_current_namespace_export(delegate,collection,first_receipt['bundle'],new_rows,first_export['record_store'],root/'stale-current')
        self.assertFalse((root/'stale-current').exists())
        actual_input=prepare_current_namespace_export(delegate,collection,placement['descriptor'],new_rows,first_export['record_store'],root/'current-input')
        env=os.environ.copy();env.update(SPANN_NAMESPACE_CURRENT_INPUT=actual_input['path'],SPANN_NAMESPACE_CURRENT_OUTPUT=str(root/'second-export'))
        run=subprocess.run([str(checked_artifact(runtime,'.')),'--ignored','--exact','export_current_namespace_append','--nocapture'],env=env,capture_output=True,timeout=30)
        (root/'second-export.stdout').write_bytes(run.stdout);(root/'second-export.stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode())
        export=artifact(root/'second-export/export.json');proof=artifact(root/'second-export/export.receipt.json');actual=json.loads(Path(export['path']).read_text());self.assertEqual([(r['record_offset_id'],r['global_id']) for r in actual['captured']['rows']],[(19,18),(20,19)]);self.assertEqual(actual['registered_bundle'],placement['descriptor']);self.assertEqual(actual['parent_namespace'],old_namespace)
        for key,value in first_export['record_inventory'].items():self.assertEqual(actual['record_inventory'][key],value)
        source=ExportedNamespaceAuthority(delegate,export,proof,runtime);producer=SavedNamespaceProducer(source,root/'second-namespace');authority=NamespacePreparation(source,producer)
        session=CompactorRegistration(authority,collection,os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],root/'second-private-attempt');reply=session.prepare(str(uuid.uuid4()),session.task_spec())
        final=json.loads(checked_artifact(reply['descriptor'],'.').read_text());validate_record(final['deployment']);new_root=json.loads(checked_artifact(final['hierarchy_metadata']['namespace_root'],'.').read_text());new_mapping=json.loads(checked_artifact(new_root['mapping'],'.').read_text());self.assertEqual(new_mapping[:18],old_rows);self.assertEqual(new_root['parent_namespace_sha256'],old_namespace['sha256']);self.assertNotEqual(new_root['construction_parent_ownership_generation'],old_root['construction_parent_ownership_generation'])
        # A separately observed advancement blocks this exact saved proposal;
        # the main authority is not rolled backward to continue afterward.
        stale=CapturedAuthority(copy.deepcopy(next_context),[]);stale.state['collection']['version']+=1;stale_source=ExportedNamespaceAuthority(stale,export,proof,runtime)
        stale_session=copy.copy(session);stale_session.authority=NamespacePreparation(stale_source,SavedNamespaceProducer(stale_source,root/'unused-stale-namespace'))
        with self.assertRaisesRegex(ValueError,'parent changed'):stale_session.submit()
        self.assertEqual(stale.starts,0);self.assertIsNone(stale_session.attempt)
        self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1)
        delegate.state=copy.deepcopy(session.candidate_state);self.assertEqual(session.reconcile()['status'],'committed')
        responses=[]
        for route in reply['routes']:
            sid=route['subtree_id'];subtree=next(s for s in final['deployment']['tree']['subtrees'] if s['subtree_id']==sid);request=dict(op='score_selected_leaves',generation=subtree['generation'],request_id='repeated-namespace-leaf',query=[0]*4,nodes=[route['leaf_id']],candidate_budget=128)
            result=subprocess.run([os.environ['SPANN_DYNAMIC_BINARY'],'read','--checkpoint-dir',final['deployment']['owners'][sid]['checkpoint_dir'],'--generation',str(subtree['generation'])],input=json.dumps(request)+'\n',capture_output=True,text=True,timeout=5);self.assertEqual(result.returncode,0,result.stderr);response=json.loads(result.stdout);self.assertEqual(sum(h['id']==route['id'] and h['local_id']==route['local_id'] for h in response['candidates']),1);responses.append(dict(route=route,request=request,response=response))
        for sid,owner in first_bundle['deployment']['owners'].items():self.assertEqual(inventory(owner['checkpoint_dir']),old_vectors[sid])
        checked_artifact(old_namespace,'.');self.assertFalse((root/'current.json').exists())
        (root/'second-saved-leaves.json').write_bytes(canonical_bytes(responses));(root/'receipt.json').write_bytes(canonical_bytes(dict(scope='two actual local appends with logical placement completeproposal/fakeauthority',first_receipt=artifact(first/'receipt.json'),placement=placement,second_input=actual_input,second_export=export,second_export_proof=proof,final_bundle=reply['descriptor'],final_proposal=session.proposal,namespace_parent_exact=True,old_record_inventory_unchanged=True,old_vector_inventory_unchanged=True,stale_capture_before_allocation=True,stale_submission_before_rpc=True,no_second_pointer=True)))
