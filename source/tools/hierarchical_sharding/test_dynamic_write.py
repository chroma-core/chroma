"""Tiny real selected-leaf persistence and complete publication fixtures."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from . import dynamic_write
from .dynamic_deployment import make_record,publish_deployment,content_hash,load_deployment
from .dynamic_runtime import build_lower,seal_owner,probe_reader,DeploymentReader
from .tree_manifest import artifact,canonical_bytes,routing_boundary_hash,file_hash


def make_parent(root,binary):
    root=Path(root).resolve();root.mkdir(parents=True,exist_ok=True)
    vectors=[(float(i+1),1.,0.,0.) for i in range(16)]
    corpus=root/'canonical.f32';corpus.write_bytes(b''.join(struct.pack('<ffff',*v) for v in vectors))
    model=dict(construction_epoch='global-write-tiny-v1',dimension=4,metric='l2',centroids=[[4.5,1,0,0],[12.5,1,0,0]])
    model_path=root/'model.json';model_path.write_bytes(canonical_bytes(model))
    tree=dict(format_version=1,tree_generation=1,construction_epoch=model['construction_epoch'],dimension=4,metric='l2',source=dict(source_id='global-write-tiny',row_count=16),routing_boundary_scheme='centroid-content-v1',model=artifact(model_path),subtrees=[],upper_root=dict(kind='centroid_root',children=[]))
    owners={}
    for slot in range(2):
        sid='s'+str(slot);ids=list(range(slot*8,(slot+1)*8));vp=root/(sid+'.f32');mp=root/(sid+'.u64le')
        vp.write_bytes(b''.join(struct.pack('<ffff',*vectors[i]) for i in ids));mp.write_bytes(b''.join(struct.pack('<Q',i) for i in ids))
        subtree=build_lower(binary,vp,mp,root/('parent-'+sid),sid,1,4,'l2',tree['source']['source_id'],routing_boundary_hash(tree,model,slot),construction=dict(branching_factor=2,split_threshold=4,merge_threshold=1))
        subtree['centroid_index']=slot;tree['subtrees'].append(subtree);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=slot));owners[sid]=seal_owner(root/('parent-'+sid),subtree,'local')
    parent=make_record(tree,owners,canonical_vectors=artifact(corpus));pointer=root/'current.json'
    probe=lambda sid,owner,subtree:probe_reader(binary,sid,owner,subtree)
    publish_deployment(pointer,parent,None,probe)
    return parent,pointer,vectors,probe


@contextmanager
def evidence_case(name):
    destination=os.environ.get('SPANN_WRITE_EVIDENCE')
    if destination:
        root=Path(destination).resolve()/name;root.mkdir(parents=True,exist_ok=False)
        yield root
    else:
        with tempfile.TemporaryDirectory() as temp:yield Path(temp).resolve()


class PlanValidation(unittest.TestCase):
    def test_exact_disjoint_prefix_and_route_owner_are_required(self):
        tree=dict(source=dict(row_count=2));maps={'a':[0],'b':[1]}
        plan=dict(status='saved-private-global-writes',durable=True,parent_sha256='parent',append_sha256='append',owners=[dict(subtree_id='a',ids=[0]),dict(subtree_id='b',ids=[1,2])],routes=[dict(id=2,subtree_id='b',local_id=1,leaf_id=4,distance=.1)])
        self.assertEqual(dynamic_write.validate_plan(plan,tree,maps,'parent','append',3)['b'],[1,2])
        for mutate in [lambda p:p['owners'][0]['ids'].append(2),lambda p:p['routes'][0].update(subtree_id='a'),lambda p:p.update(parent_sha256='stale'),lambda p:p['routes'][0].update(distance=True),lambda p:p['owners'][1].update(ids=[2,1])]:
            invalid=copy.deepcopy(plan);mutate(invalid)
            with self.assertRaises(ValueError):dynamic_write.validate_plan(invalid,tree,maps,'parent','append',3)


@unittest.skipUnless(os.environ.get('SPANN_WRITE_PLANNER_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted planner and reader binary required')
class RealGlobalPublication(unittest.TestCase):
    @unittest.skipIf(hasattr(os,'geteuid') and os.geteuid()==0,'permission fault requires an unprivileged writer')
    def test_flush_io_failure_discards_private_writer_and_fresh_parent_retry_succeeds(self):
        from .dynamic_runtime import inventory
        planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];reader_binary=os.environ['SPANN_DYNAMIC_BINARY']
        with evidence_case('flush-io-failure') as root:
            parent,pointer,vectors,probe=make_parent(root,reader_binary)
            original_stores={sid:inventory(owner['checkpoint_dir']) for sid,owner in parent['owners'].items()}
            original_pointer=pointer.read_bytes()
            corpus=root/'appended.f32';corpus.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+struct.pack('<ffff',.5,1,0,0))
            output=root/'failed-private';original=subprocess.Popen;faults=[]
            def deny_private_block_writes(argv,*args,**kwargs):
                if len(argv)>1 and argv[1]=='compact-global-writes':
                    spec=json.loads(Path(argv[argv.index('--spec')+1]).read_text())
                    data=Path(spec['owners'][0]['checkpoint_dir'])/'data'
                    for directory in [data]+sorted(p for p in data.rglob('*') if p.is_dir()):
                        directory.chmod(0o500);faults.append(str(directory))
                return original(argv,*args,**kwargs)
            with patch('tools.hierarchical_sharding.dynamic_write.subprocess.Popen',side_effect=deny_private_block_writes):
                with self.assertRaisesRegex(ValueError,'planning failed'):
                    dynamic_write.publish_global_append(pointer,parent,planner,reader_binary,corpus,output,probe,max_rows=128)
            stderr=(output/'write-plan.stderr').read_text()
            self.assertIn('selected writer flush',stderr)
            self.assertEqual(pointer.read_bytes(),original_pointer)
            for sid,owner in parent['owners'].items():self.assertEqual(inventory(owner['checkpoint_dir']),original_stores[sid])
            self.assertFalse((output/'index-s0/manifest-2.json').exists())
            journal=json.loads(Path(str(output)+'.transaction.json').read_text());self.assertEqual(journal['status'],'failed_unpublished')
            retry=root/'fresh-parent-retry'
            dynamic_write.publish_global_append(pointer,parent,planner,reader_binary,corpus,retry,probe,max_rows=128)
            current=DeploymentReader(pointer,reader_binary)
            try:self.assertEqual(set(current.query([0,0,0,0],17,beam_width=None,candidate_budget=17)['ids']),set(range(17)))
            finally:current.close()
            (root/'fault-receipt.json').write_text(json.dumps(dict(fault='filesystem_permission_denied_during_actual_blockfile_flush',directories=faults,parent_stores_unchanged=True,parent_pointer_unchanged_on_failure=True,fresh_parent_retry_published=True,failed_journal=journal),indent=2)+'\n')

    def test_selected_leaf_survives_atomic_saved_publication_and_old_snapshot(self):
        planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];reader_binary=os.environ['SPANN_DYNAMIC_BINARY']
        with evidence_case('successful-publication') as root:
            parent,pointer,vectors,probe=make_parent(root,reader_binary)
            old=DeploymentReader(pointer,reader_binary);before=old.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
            try:
                new_vectors=vectors+[(.5,1.,0.,0.),(17.,1.,0.,0.)]
                corpus=root/'appended.f32';corpus.write_bytes(b''.join(struct.pack('<ffff',*v) for v in new_vectors))
                output=root/'private'
                dynamic_write.publish_global_append(pointer,parent,planner,reader_binary,corpus,output,probe,max_rows=128)
                current=DeploymentReader(pointer,reader_binary)
                try:
                    result=current.query([0,0,0,0],18,beam_width=None,candidate_budget=18)
                    expected=sorted(range(18),key=lambda i:(sum(v*v for v in new_vectors[i]),i))
                    self.assertEqual(result['ids'],expected)
                    self.assertEqual(old.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids'],before['ids'])
                    plan=json.loads((output/'write-plan.stdout').read_text())
                    memberships=[]
                    for saved in plan['saved']:
                        self.assertTrue(saved['all_selected_leaf_memberships_verified'])
                        self.assertEqual(saved['selected_leaf_memberships'],[row for row in plan['routes'] if row['subtree_id']==saved['subtree_id']])
                        for route in saved['selected_leaf_memberships']:
                            request=dict(op='score_selected_leaves',generation=saved['generation'],request_id='published-leaf',query=[0,0,0,0],nodes=[route['leaf_id']],candidate_budget=128)
                            response=subprocess.run([reader_binary,'read','--checkpoint-dir',str(output/('index-'+saved['subtree_id'])),'--generation',str(saved['generation'])],input=json.dumps(request)+'\n',capture_output=True,text=True,timeout=10)
                            self.assertEqual(response.returncode,0,response.stderr)
                            reply=json.loads(response.stdout)
                            self.assertTrue(any(hit['id']==route['id'] and hit['local_id']==route['local_id'] for hit in reply['candidates']))
                            memberships.append(dict(request=request,response=reply))
                    (root/'published-leaf-memberships.json').write_text(json.dumps(memberships,indent=2)+'\n')
                finally:current.close()
            finally:old.close()

    def test_partial_owner_failure_blocks_cas_and_same_private_attempt_retry(self):
        import subprocess
        planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];reader_binary=os.environ['SPANN_DYNAMIC_BINARY']
        with evidence_case('partial-owner-failure') as root:
            parent,pointer,vectors,probe=make_parent(root,reader_binary);before=pointer.read_bytes()
            corpus=root/'appended.f32';corpus.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+struct.pack('<ffff',.5,1,0,0)+struct.pack('<ffff',17,1,0,0))
            output=root/'private';original=subprocess.Popen
            def orphan_target(argv,*args,**kwargs):
                if len(argv)>1 and argv[1]=='compact-global-writes':
                    spec=json.loads(Path(argv[argv.index('--spec')+1]).read_text())
                    (Path(spec['owners'][1]['checkpoint_dir'])/'manifest-2.json').write_bytes(b'orphan-generation')
                return original(argv,*args,**kwargs)
            with patch('tools.hierarchical_sharding.dynamic_write.subprocess.Popen',side_effect=orphan_target):
                with self.assertRaisesRegex(ValueError,'planning failed'):
                    dynamic_write.publish_global_append(pointer,parent,planner,reader_binary,corpus,output,probe,max_rows=128)
            self.assertEqual(pointer.read_bytes(),before)
            self.assertTrue((output/'index-s0/manifest-2.json').exists())
            journal=Path(str(output)+'.transaction.json');self.assertEqual(json.loads(journal.read_text())['status'],'failed_unpublished');self.assertEqual(json.loads(journal.read_text())['parameters']['private_output'],str(output))
            with self.assertRaises(FileExistsError):dynamic_write.publish_global_append(pointer,parent,planner,reader_binary,corpus,output,probe,max_rows=128)

    def test_stale_parent_cannot_publish_forked_selected_leaf_snapshots(self):
        with evidence_case('stale-parent') as root:
            planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];reader_binary=os.environ['SPANN_DYNAMIC_BINARY']
            parent,pointer,vectors,probe=make_parent(root,reader_binary)
            corpus=root/'appended.f32';corpus.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+struct.pack('<ffff',.5,1,0,0))
            candidate=dynamic_write.prepare_global_append(planner,reader_binary,parent,corpus,root/'private',max_rows=128)
            winner=make_record(parent['tree'],parent['owners'],parent)
            publish_deployment(pointer,winner,content_hash(parent),probe);before=pointer.read_bytes()
            with self.assertRaisesRegex(ValueError,'compare-and-swap'):
                publish_deployment(pointer,candidate,content_hash(parent),probe)
            self.assertEqual(pointer.read_bytes(),before)


@unittest.skipUnless(os.environ.get('SPANN_WRITE_PLANNER_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'local compactor and admitted parent builder required')
class DirectPrivateCompaction(unittest.TestCase):
    def test_raw_fork_preserves_selected_leaf_and_does_not_publish(self):
        import shutil,subprocess,time
        with evidence_case('direct-private-fork') as root:
            planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];reader_binary=os.environ['SPANN_DYNAMIC_BINARY']
            parent,pointer,vectors,probe=make_parent(root/'parent',reader_binary);head=pointer.read_bytes()
            from .dynamic_deployment import resolved_tree
            from .tree_manifest import load_model
            tree=resolved_tree(parent);model=load_model(tree);owners=[]
            append=root/'append.f32';append.write_bytes(struct.pack('<ffff',.5,1,0,0)+struct.pack('<ffff',17,1,0,0))
            canonical=root/'canonical.f32';canonical.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+append.read_bytes())
            for subtree in tree['subtrees']:
                sid=subtree['subtree_id'];clone=root/('clone-'+sid);shutil.copytree(Path(subtree['snapshot']['path']).parent,clone)
                (clone/'global-write-private.json').write_bytes(canonical_bytes(dict(parent_sha256=content_hash(parent),subtree_id=sid,manifest_sha256=subtree['snapshot']['sha256'])))
                owners.append(dict(subtree_id=sid,checkpoint_dir=str(clone),generation=subtree['generation'],manifest_sha256=subtree['snapshot']['sha256'],centroid=model['centroids'][subtree['centroid_index']]))
            spec=dict(parent_sha256=content_hash(parent),source_id=tree['source']['source_id'],dimension=4,metric='l2',old_count=16,canonical_vectors=str(canonical),canonical_sha256=file_hash(canonical),append_vectors=str(append),append_sha256=file_hash(append),owners=owners,max_rows=128)
            path=root/'write-plan-spec.json';path.write_bytes(canonical_bytes(spec));started=time.monotonic()
            with (root/'stdout').open('xb') as out,(root/'stderr').open('xb') as err:
                result=subprocess.run([planner,'compact-global-writes','--spec',str(path)],stdout=out,stderr=err,timeout=30)
            self.assertEqual(result.returncode,0,(root/'stderr').read_text())
            saved=json.loads((root/'stdout').read_text());self.assertTrue(saved['durable']);self.assertEqual(pointer.read_bytes(),head)
            self.assertEqual(len(saved['saved']),2);proof=[]
            for row in saved['saved']:
                self.assertTrue(row['all_selected_leaf_memberships_verified']);self.assertEqual(row['exact_vector_rows_verified'],9)
                for route in row['selected_leaf_memberships']:
                    query=dict(op='score_selected_leaves',generation=row['generation'],request_id='independent-membership',query=[0,0,0,0],nodes=[route['leaf_id']],candidate_budget=128)
                    reply=subprocess.run([reader_binary,'read','--checkpoint-dir',str(root/('clone-'+row['subtree_id'])),'--generation',str(row['generation'])],input=json.dumps(query)+'\n',capture_output=True,text=True,timeout=10)
                    self.assertEqual(reply.returncode,0,reply.stderr);response=json.loads(reply.stdout)
                    self.assertTrue(any(hit['id']==route['id'] and hit['local_id']==route['local_id'] for hit in response['candidates']))
                    proof.append(dict(request=query,response=response))
            (root/'independent-membership.json').write_text(json.dumps(proof,indent=2)+'\n')
            (root/'receipt.json').write_text(json.dumps(dict(status='passed',compactor_sha256=file_hash(planner),parent_builder_sha256=file_hash(reader_binary),elapsed_s=time.monotonic()-started,parent_pointer_unchanged=True,rows=18,selected_leaf_memberships=len(proof)),indent=2)+'\n')
