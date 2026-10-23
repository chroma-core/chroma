"""Small same-authority topology contract; actual native fixture is opt-in."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
import uuid
from unittest.mock import patch
from .compactor_namespace import _submit_namespace_proposal,reconcile_namespace_placement,current_view
from .compactor_topology import prepare_namespace_topology,submit_namespace_topology,reconcile_namespace_topology
from .dynamic_topology import split_request,merge_request,reassign_request,owned_ids
from .dynamic_runtime import inventory
from .tree_manifest import artifact,canonical_bytes,checked_artifact,file_hash
from .test_compactor_registration import CapturedAuthority


class SubmitOnceContract(unittest.TestCase):
    def test_shared_helper_preserves_unknown_and_one_rpc(self):
        with tempfile.TemporaryDirectory() as directory:
            parent={'collection':{'collection_id':'c','version':1}};candidate={'collection':{'collection_id':'c','version':2}}
            class Authority:
                state=copy.deepcopy(parent);starts=0
                def observe(self,_):return copy.deepcopy(self.state)
                def start_registration(self,*args):self.starts+=1;raise TimeoutError('ack lost')
            authority=Authority();prepared=dict(context={'parent':parent},candidate_state=candidate,proposal={},proposal_sha256='a'*64,journal_path=str(Path(directory)/'submission.json'),attempt_id=None);checks=[]
            def health(*args):checks.append(True)
            self.assertEqual(_submit_namespace_proposal(authority,prepared,health)['status'],'pending-or-unknown');self.assertEqual(authority.starts,1)
            self.assertEqual(_submit_namespace_proposal(authority,prepared,health)['status'],'pending-or-unknown');self.assertEqual(authority.starts,1);self.assertEqual(len(checks),1)
            authority.state={'collection':{'collection_id':'c','version':3}};self.assertEqual(reconcile_namespace_placement(authority,prepared)['status'],'pending-or-unknown')
            authority.state=candidate;self.assertEqual(reconcile_namespace_placement(authority,prepared)['status'],'committed')


    def test_failed_submit_audit_latches_before_rpc(self):
        from .tree_manifest import content_hash
        from .compactor_topology import _topology_health
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);journal=root/'preparation.json';journal.write_text(json.dumps({'status':'prepared-private-topology'}));bundle=root/'bundle.json';bundle.write_text(json.dumps({'hierarchy_metadata':{'namespace_root':{}}}))
            parent={'collection':{'collection_id':'c','version':1}};proposal={};prepared=dict(context={'parent':parent},preparation_journal=str(journal),expected_current_bundle={},descriptor=artifact(bundle),replacements={},proposal=proposal,proposal_sha256=content_hash(proposal),candidate_state={},journal_path=str(root/'submission.json'),attempt_id=None)
            class Authority:
                starts=0
                def start_registration(self,*args):self.starts+=1
                def observe(self,*args):return parent
            authority=Authority()
            with patch('tools.hierarchical_sharding.compactor_topology._capture_health'),patch('tools.hierarchical_sharding.compactor_topology.current_view',return_value={'registered_bundle':{}}),patch('tools.hierarchical_sharding.compactor_topology._verify_candidate',return_value=(proposal,{},{})):
                def fail(*args):raise OSError('audit read failed')
                with self.assertRaisesRegex(OSError,'audit read failed'):submit_namespace_topology(authority,prepared,fail)
                self.assertEqual(json.loads(journal.read_text())['status'],'failed-submit-validation');self.assertEqual(authority.starts,0);self.assertIsNone(prepared['attempt_id']);self.assertFalse((root/'submission.json').exists())
                with self.assertRaisesRegex(ValueError,'not eligible'):submit_namespace_topology(authority,prepared,fail)
                self.assertEqual(authority.starts,0)


def retained_context(first):
    receipt=json.loads((first/'receipt.json').read_text());setup=json.loads((first/'setup.json').read_text());context=json.loads(checked_artifact(setup['context'],'.').read_text());proposal=receipt['proposal']
    context['parent']['collection'].update(version=proposal['collection_version']+1,log_position=proposal['log_position'],total_records_post_compaction=proposal['total_records_post_compaction'],size_bytes_post_compaction=proposal['size_bytes_post_compaction'])
    files={s['segment_id']:s['file_paths'] for s in proposal['segment_flush_info']}
    for segment in context['parent']['segments']:segment['file_paths']=copy.deepcopy(files[segment['segment_id']])
    bundle=json.loads(checked_artifact(receipt['bundle'],'.').read_text());namespace=json.loads(checked_artifact(bundle['hierarchy_metadata']['namespace_root'],'.').read_text());context['record_collection_binding']=copy.deepcopy(namespace['collection']);context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=proposal['log_position'],target_total_records=18,target_logical_size_bytes=proposal['size_bytes_post_compaction'])
    export=json.loads(checked_artifact(receipt['export'],'.').read_text());return context,export['record_store']


@unittest.skipUnless(os.environ.get('SPANN_TOPOLOGY_RUNTIME'),'reviewed topology binary/input required')
class NativeNamespaceTopology(unittest.TestCase):
    def test_split_merge_reassign_share_collection_authority(self):
        root=Path(os.environ['SPANN_TOPOLOGY_OUTPUT']).resolve();root.mkdir();context,store=retained_context(Path(os.environ['SPANN_TOPOLOGY_PARENT']));binary=Path(os.environ['SPANN_TOPOLOGY_RUNTIME']);self.assertEqual(file_hash(binary),os.environ['SPANN_TOPOLOGY_RUNTIME_SHA']);reader=os.environ['SPANN_DYNAMIC_BINARY']
        record_before=inventory(store);base=current_view(context);identity={key:base['namespace'][key] for key in ('mapping','identity_source','canonical_vectors','record_files')};audits=[]
        def auditor(target,namespace,directory):
            directory=Path(directory);directory.mkdir();input_path=directory/'input.json';output=directory/'result.json';input_path.write_bytes(canonical_bytes(dict(capture=target,namespace=namespace,store=store,output=str(output))))
            env=dict(os.environ,SPANN_NAMESPACE_VIEW_INPUT=str(input_path));run=subprocess.run([str(binary),'--ignored','--exact','audit_current_namespace_view','--nocapture'],env=env,capture_output=True,timeout=10);(directory/'stdout').write_bytes(run.stdout);(directory/'stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode());report=json.loads(output.read_text());self.assertEqual(report['namespace'],namespace);expected_root=json.loads(checked_artifact(namespace,'.').read_text());self.assertEqual(report['rows'],json.loads(checked_artifact(expected_root['mapping'],'.').read_text()));self.assertEqual(report['record_files'],expected_root['record_files']);self.assertEqual(report['collection_binding'],target['record_collection_binding']);expected_vector=next(s for s in target['parent']['segments'] if s['segment_id']==target['vector_segment_id']);self.assertEqual(report['current_bundle'],json.loads(expected_vector['file_paths']['hierarchical_spann_bundle_v1'][0]));self.assertTrue(report['all_three_record_maps_and_vector_values_verified']);audits.append(artifact(output));return artifact(output)
        authority=CapturedAuthority(copy.deepcopy(context),[]);record=json.loads(checked_artifact(base['registered_bundle'],'.').read_text())['deployment'];sid=sorted(record['owners'])[0];ids=list(owned_ids(next(s for s in record['tree']['subtrees'] if s['subtree_id']==sid)));request=split_request(record,sid,ids[:len(ids)//2],'split-a','split-b')
        # A native-ready private topology is still ineligible when its record
        # audit fails; the failed directory cannot be reused for another try.
        stale_capture=CapturedAuthority(copy.deepcopy(context),[]);stale_capture.state['collection']['version']+=1
        with self.assertRaisesRegex(ValueError,'stale'):prepare_namespace_topology(stale_capture,context['parent']['collection']['collection_id'],request,root/'stale-capture',reader,auditor,timeout=10)
        self.assertFalse((root/'stale-capture').exists())
        failed=root/'failed-audit'
        def fail(*args):raise OSError('injected record audit failure')
        with self.assertRaisesRegex(OSError,'injected'):prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],request,failed,reader,fail,timeout=10)
        self.assertEqual(json.loads((failed/'preparation.json').read_text())['status'],'failed-private-topology');self.assertFalse((failed/'prepared.json').exists());self.assertEqual(authority.starts,0);self.assertEqual(authority.state,context['parent']);self.assertTrue((failed/'vectors/tree.json').exists())
        with self.assertRaises(FileExistsError):prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],request,failed,reader,auditor,timeout=10)
        results=[]
        for name in ('split','merge','reassign'):
            view=current_view(context);record=json.loads(checked_artifact(view['registered_bundle'],'.').read_text())['deployment']
            if name=='split':replacement=request
            elif name=='merge':replacement=merge_request(record,['split-a','split-b'],'merged')
            else:
                destination=next(s for s in record['owners'] if s!='merged');source=next(s for s in record['tree']['subtrees'] if s['subtree_id']=='merged');replacement=reassign_request(record,'merged',destination,[next(owned_ids(source))])
            authority=CapturedAuthority(copy.deepcopy(context),[])
            prepared=prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],replacement,root/name,reader,auditor,timeout=10)
            candidate_bundle=json.loads(checked_artifact(prepared['descriptor'],'.').read_text());ns=json.loads(checked_artifact(candidate_bundle['hierarchy_metadata']['namespace_root'],'.').read_text())
            for key,value in identity.items():self.assertEqual(ns[key],value)
            self.assertEqual(ns['parent_namespace_sha256'],view['metadata']['namespace_root']['sha256'])
            if name=='split':
                # Each failed eligibility check consumes its own private
                # candidate. The successful attempt is freshly prepared.
                stale=CapturedAuthority(copy.deepcopy(context),[]);stale.state['collection']['version']+=1
                with self.assertRaisesRegex(ValueError,'stale'):submit_namespace_topology(stale,prepared,auditor)
                self.assertEqual(stale.starts,0);self.assertEqual(json.loads(Path(prepared['preparation_journal']).read_text())['status'],'failed-submit-validation')
                advanced=CapturedAuthority(copy.deepcopy(context),[])
                advance_prepared=prepare_namespace_topology(advanced,context['parent']['collection']['collection_id'],replacement,root/'advance-during-audit',reader,auditor,timeout=10)
                def audit_then_advance(*args):
                    result=auditor(*args);advanced.state['collection']['version']+=1;return result
                with self.assertRaisesRegex(ValueError,'stale'):submit_namespace_topology(advanced,advance_prepared,audit_then_advance)
                self.assertEqual(advanced.starts,0)
                prepared=prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],replacement,root/'split-fresh',reader,auditor,timeout=10)
            self.assertEqual(submit_namespace_topology(authority,prepared,auditor)['status'],'pending-or-unknown');self.assertEqual(submit_namespace_topology(authority,prepared,auditor)['status'],'pending-or-unknown');self.assertEqual(authority.starts,1)
            authority.state=copy.deepcopy(prepared['candidate_state']);self.assertEqual(reconcile_namespace_topology(authority,prepared)['status'],'committed')
            # Full selected-ID reranking checks the actual new snapshots against
            # the unchanged canonical vectors without an approximate policy test.
            canonical=checked_artifact(ns['canonical_vectors'],'.').read_bytes();hits=[];raw=[];query=[0.,0.,0.,0.]
            for slot in candidate_bundle['deployment']['tree']['subtrees']:
                ids=list(owned_ids(slot));request_score=dict(op='rerank_selected',generation=slot['generation'],request_id='topology-all-ids',query=query,local_ids=list(range(len(ids))))
                run=subprocess.run([reader,'read','--checkpoint-dir',candidate_bundle['deployment']['owners'][slot['subtree_id']]['checkpoint_dir'],'--generation',str(slot['generation'])],input=json.dumps(request_score)+'\n',capture_output=True,text=True,timeout=5);self.assertEqual(run.returncode,0,run.stderr);response=json.loads(run.stdout);raw.append(dict(request=request_score,response=response))
                for hit in response['results']:hits.append((hit['id'],hit['distance']))
            self.assertEqual(sorted(i for i,_ in hits),list(range(18)))
            for i,distance in hits:
                vector=struct.unpack_from('<ffff',canonical,i*16);self.assertAlmostEqual(distance,sum(v*v for v in vector),places=4)
            (root/name/'rerank-responses.json').write_bytes(canonical_bytes(raw));results.append(dict(operation=name,prepared=artifact(Path(prepared['preparation_journal']).parent/'prepared.json'),bundle=prepared['descriptor'],proposal=prepared['proposal']))
            context=copy.deepcopy(context);context['parent']=copy.deepcopy(prepared['candidate_state']);context['record_collection_binding']['wire']['version']+=1;context['compaction']['job_id']=str(uuid.uuid4());context['compaction']['session_id']=str(uuid.uuid4());current_view(context)
        self.assertEqual(inventory(store),record_before);self.assertFalse((root/'current.json').exists());(root/'receipt.json').write_bytes(canonical_bytes(dict(operations=results,audits=audits,unchanged_identity=identity,record_inventory=record_before,failed_private_artifacts=str(failed),no_second_pointer=True,scope='local same-worker lower trees plus fake collection authority')))
