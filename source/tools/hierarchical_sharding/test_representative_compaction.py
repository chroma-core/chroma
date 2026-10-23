"""One bounded multi-block compaction fixture; no production authority claim."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import time
import unittest
import uuid
from .tree_manifest import artifact,canonical_bytes,checked_artifact,content_hash,routing_boundary_hash
from .dynamic_runtime import build_lower,seal_owner,inventory
from .dynamic_deployment import make_record,validate_record
from .compactor_registration import CompactorRegistration,HIERARCHY_BUNDLE
from .compactor_namespace import (ExportedNamespaceAuthority,NamespacePreparation,
                                 SavedNamespaceProducer,current_view,prepare_current_namespace_export)
from .test_compactor_registration import captured_authority,CapturedAuthority

ROWS=96
DIM=32
START=time.monotonic()


def check_budget():
    root=os.environ.get('SPANN_REPRESENTATIVE_ROOT')
    if root:
        files=[p for p in Path(root).rglob('*') if p.is_file()]
        if len(files)>4096 or sum(p.stat().st_size for p in files)>128*1024*1024:
            raise ValueError('observed fixture evidence budget exceeded')
    if time.monotonic()-START>300:raise ValueError('observed fixture work window exceeded')


def run_native(binary,arguments,output,env=None,timeout=30,input=None):
    check_budget()
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    run=subprocess.run([str(binary)]+arguments,input=input,capture_output=True,
                       env=env,timeout=timeout)
    output.with_suffix('.stdout').write_bytes(run.stdout)
    output.with_suffix('.stderr').write_bytes(run.stderr)
    check_budget()
    if run.returncode:raise ValueError('native fixture command failed: '+run.stderr.decode(errors='replace'))
    return run


def prepare_representative(root,binary):
    """Prepare exactly 96 source rows, with 48 rows per owner and fixed identity."""
    root=Path(root).resolve();root.mkdir(exist_ok=False)
    vectors=[[float(i),1.]+[0.]*(DIM-2) for i in range(ROWS)]
    canonical=root/'canonical.f32';canonical.write_bytes(b''.join(struct.pack('<'+'f'*DIM,*v) for v in vectors))
    # The first committed boundary is nearest to the first append, while its
    # actual leaves contain only extremes. The second owner's middle values
    # provide the better leaf. The native oracle must establish the crossing.
    centers=[[47.4,1.]+[0.]*(DIM-2),[47.5,1.]+[0.]*(DIM-2)]
    model=dict(construction_epoch='representative-96x32-v1',dimension=DIM,metric='l2',centroids=centers)
    model_path=root/'model.json';model_path.write_bytes(canonical_bytes(model))
    tree=dict(format_version=1,tree_generation=1,construction_epoch=model['construction_epoch'],dimension=DIM,metric='l2',source=dict(source_id='representative-96x32',row_count=ROWS),routing_boundary_scheme='centroid-content-v1',model=artifact(model_path),subtrees=[],upper_root=dict(kind='centroid_root',children=[]))
    owners={};groups={'a':list(range(24))+list(range(72,96)),'b':list(range(24,72))}
    for index,(sid,ids) in enumerate(groups.items()):
        values=root/(sid+'.f32');mapping=root/(sid+'.u64le')
        values.write_bytes(b''.join(struct.pack('<'+'f'*DIM,*vectors[i]) for i in ids));mapping.write_bytes(b''.join(struct.pack('<Q',i) for i in ids))
        slot=build_lower(binary,values,mapping,root/('parent-'+sid),sid,1,DIM,'l2',tree['source']['source_id'],routing_boundary_hash(tree,model,index),timeout=30,construction=dict(branching_factor=2,split_threshold=8,merge_threshold=2),fixture_block_target_bytes=1024)
        slot['centroid_index']=index;tree['subtrees'].append(slot);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=index));owners[sid]=seal_owner(root/('parent-'+sid),slot,'local')
    from . import ownership_pages,ownership_trie
    pages=ownership_pages.create(groups,root/'pages',page_rows=8)
    trie=ownership_trie.create(groups,root/'trie',page_rows=8,fanout=4)
    parent=make_record(tree,owners,canonical_vectors=artifact(canonical),ownership_pages=pages,ownership_trie=trie)
    descriptor=root/'parent.json';descriptor.write_bytes(canonical_bytes(parent))
    delegate=captured_authority(parent,artifact(descriptor));context=delegate.context
    context['parent']['collection'].update(log_position=96,size_bytes_post_compaction=96*DIM*4)
    context['compaction'].update(pulled_log_offset=98,target_total_records=98,target_logical_size_bytes=98*DIM*4)
    record=next(s for s in context['parent']['segments'] if s['scope']=='RECORD');record.update(type='urn:chroma:segment/record/blockfile',file_paths={})
    rows=[dict(log_offset=97+i,user_id='new-'+str(i),embedding_bits=list(struct.unpack('<'+'I'*DIM,struct.pack('<'+'f'*DIM,*v)))) for i,v in enumerate([[47.4,1.,-0.]+[0.]*(DIM-3),[48.,4097.,-0.]+[0.]*(DIM-3)])]
    identity=root/'identities.json';identity.write_bytes(canonical_bytes([dict(global_id=i,user_id='parent-'+str(i)) for i in range(ROWS)]))
    source=dict(collection_id=context['parent']['collection']['collection_id'],record_segment_id=record['segment_id'],tenant='tenant',database='database',database_id=str(uuid.uuid4()),version=3,log_position=96,parent_deployment_sha256=content_hash(parent),ownership_generation=parent['fence'],canonical=parent['canonical_vectors'],identity_source=artifact(identity),owner_maps=[s['local_to_global'] for s in tree['subtrees']],rows=rows)
    input_path=root/'record-input.json';input_path.write_bytes(canonical_bytes(source))
    (root/'setup.json').write_bytes(canonical_bytes(dict(parent=artifact(descriptor),context=context,record_input=artifact(input_path),parent_inventory={sid:inventory(o['checkpoint_dir']) for sid,o in owners.items()})))
    return parent


def block_inventory(record,binary,output):
    """Require actual current vector roots to reference multiple blocks."""
    rows=[]
    for slot in record['tree']['subtrees']:
        sid=slot['subtree_id'];directory=Path(record['owners'][sid]['checkpoint_dir'])
        result=run_native(binary,['--checkpoint-dir',str(directory),'--expected-current-sha256',artifact(directory/'manifest.json')['sha256'],'--max-root-bytes','1048576','--max-root-references','256','--max-inventory-entries','1024','--max-manifests','16'],Path(output)/sid,timeout=10)
        report=json.loads(result.stdout);snapshot=json.loads(checked_artifact(slot['snapshot'],'.').read_bytes());root_id=snapshot['block_ids']['vector_data_id'];root=report['root_objects'][root_id]
        if len(root['block_ids'])<2:raise ValueError('insufficient block diversity; stop without growing fixture')
        rows.append(dict(owner=sid,root_id=root_id,root=root,raw=artifact((Path(output)/sid).with_suffix('.stdout'))))
    return rows


@unittest.skipUnless(os.environ.get('SPANN_REPRESENTATIVE_ROOT'),'reviewed source and exact native epochs required')
class RepresentativeCompaction(unittest.TestCase):
    def test_actual_records_selected_leaves_topology_and_private_failure(self):
        from .subtree_transport import PinnedReader
        from .dynamic_runtime import descriptor
        root=Path(os.environ['SPANN_REPRESENTATIVE_ROOT']).resolve();setup=json.loads((root/'setup.json').read_bytes());parent=json.loads(checked_artifact(setup['parent'],'.').read_bytes())
        pinned=[];before=[];cleanup=[]
        def query(handle,slot):
            result=handle.call(dict(subtree_id=slot['subtree_id'],snapshot_sha256=slot['snapshot']['sha256'],payload=dict(op='rerank_selected',request_id='old-generation',query=[0.]*DIM,local_ids=list(range(slot['vector_count'])))),timeout=5)['result']
            return sorted((v['id'],v['distance']) for v in result['results'])
        try:
            for slot in parent['tree']['subtrees']:
                handle=PinnedReader(descriptor(os.environ['SPANN_DYNAMIC_BINARY'],parent['owners'][slot['subtree_id']]['checkpoint_dir'],slot),5);pinned.append((handle,slot));before.append(query(handle,slot))
            self.exercise()
            after=[query(handle,slot) for handle,slot in pinned];self.assertEqual(after,before)
            (root/'old-pinned-results.json').write_bytes(canonical_bytes(dict(before=before,after=after)))
        finally:
            for handle,slot in pinned:
                try:handle.close();cleanup.append(dict(owner=slot['subtree_id'],returncode=handle.reader.process.returncode))
                except BaseException as error:cleanup.append(dict(owner=slot['subtree_id'],error=repr(error)))
            (root/'cleanup.json').write_bytes(canonical_bytes(cleanup))
        self.assertEqual(len(cleanup),2);self.assertTrue(all(type(row.get('returncode')) is int and 'error' not in row for row in cleanup))

    def exercise(self):
        root=Path(os.environ['SPANN_REPRESENTATIVE_ROOT']).resolve()
        setup=json.loads((root/'setup.json').read_bytes());parent=json.loads(checked_artifact(setup['parent'],'.').read_bytes());context=copy.deepcopy(setup['context'])
        reader=os.environ['SPANN_DYNAMIC_BINARY'];planner=os.environ['SPANN_WRITE_PLANNER_BINARY'];runtime=artifact(Path(os.environ['SPANN_NAMESPACE_RUNTIME']));audit_binary=os.environ['SPANN_STORAGE_AUDIT']
        initial_blocks=block_inventory(parent,audit_binary,root/'initial-blocks')
        exported=root/'record-export';value=json.loads((exported/'export.json').read_bytes());proof=artifact(exported/'export.receipt.json');export=artifact(exported/'export.json')
        record_id=json.loads(checked_artifact(value['parent_namespace'],'.').read_bytes())['record_segment_id']
        record=next(s for s in context['parent']['segments'] if s['segment_id']==record_id);record['file_paths']=value['parent_record_files'];context['record_collection_binding']=value['parent_collection_binding']
        ns=json.loads(checked_artifact(value['parent_namespace'],'.').read_bytes());construction=root/'construction.json';construction.write_bytes(canonical_bytes(dict(namespace_root=value['parent_namespace'],construction_parent_deployment_sha256=ns['construction_parent_deployment_sha256'],construction_parent_ownership_generation=ns['construction_parent_ownership_generation'],parent_namespace_sha256=None)))
        genesis=dict(deployment=parent,hierarchy_metadata=dict(record_segment_id=record_id,namespace_root=value['parent_namespace'],construction_receipt=artifact(construction)),collection_parent={'collection':dict(context['parent']['collection'],version=2)},compaction={'pulled_log_offset':96})
        bundle=root/'parent-bundle.json';bundle.write_bytes(canonical_bytes(genesis));vector=next(s for s in context['parent']['segments'] if s['scope']=='VECTOR');vector['file_paths'][HIERARCHY_BUNDLE]=[canonical_bytes(artifact(bundle)).decode().strip()]
        delegate=CapturedAuthority(context,[])
        source=ExportedNamespaceAuthority(delegate,export,proof,runtime);producer=SavedNamespaceProducer(source,root/'namespace');authority=NamespacePreparation(source,producer)
        session=CompactorRegistration(authority,context['parent']['collection']['collection_id'],planner,reader,root/'append');reply=session.prepare(str(uuid.uuid4()),session.task_spec())
        saved=json.loads(checked_artifact(reply['descriptor'],'.').read_bytes());validate_record(saved['deployment']);self.assertEqual(saved['deployment']['tree']['source']['row_count'],98)
        old_rows=json.loads(checked_artifact(ns['mapping'],'.').read_bytes());new_ns=json.loads(checked_artifact(saved['hierarchy_metadata']['namespace_root'],'.').read_bytes());new_rows=json.loads(checked_artifact(new_ns['mapping'],'.').read_bytes());self.assertEqual(new_rows[:96],old_rows)
        self.assertEqual(checked_artifact(saved['deployment']['canonical_vectors'],'.').read_bytes()[:96*DIM*4],checked_artifact(parent['canonical_vectors'],'.').read_bytes())
        # The global native traversal must choose the middle owner despite the
        # first row being nearest to the committed extreme-owner boundary.
        self.assertEqual(reply['routes'][0]['id'],96);self.assertEqual(reply['routes'][0]['subtree_id'],'b')
        self.assertEqual([r['id'] for r in reply['routes']],[96,97])
        memberships=[]
        for route in reply['routes']:
            sid=route['subtree_id'];slot=next(s for s in saved['deployment']['tree']['subtrees'] if s['subtree_id']==sid)
            request=dict(op='score_selected_leaves',generation=slot['generation'],request_id='representative-membership',query=[0.]*DIM,nodes=[route['leaf_id']],candidate_budget=128)
            run=run_native(reader,['read','--checkpoint-dir',saved['deployment']['owners'][sid]['checkpoint_dir'],'--generation',str(slot['generation'])],root/('membership-'+str(route['id'])),input=canonical_bytes(request),timeout=5);response=json.loads(run.stdout)
            self.assertEqual(sum(v['id']==route['id'] and v['local_id']==route['local_id'] for v in response['candidates']),1);memberships.append(dict(route=route,response=response))
        after_blocks=block_inventory(saved['deployment'],audit_binary,root/'saved-blocks');self.exact_rerank(saved['deployment'],reader,root/'append-rerank')
        self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1);delegate.state=copy.deepcopy(session.candidate_state);self.assertEqual(session.reconcile()['status'],'committed')
        next_context=copy.deepcopy(context);next_context['parent']=copy.deepcopy(session.candidate_state);next_context['record_collection_binding']=json.loads(checked_artifact(saved['hierarchy_metadata']['namespace_root'],'.').read_bytes())['collection']
        next_context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=98,target_total_records=98,target_logical_size_bytes=98*DIM*4)
        topology=self.topology(root/'topology',next_context,value['record_store'],reader,runtime)
        fault=self.record_failure(root/'record-failure',next_context,value['record_store'],runtime)
        for sid,owner in parent['owners'].items():self.assertEqual(inventory(owner['checkpoint_dir']),setup['parent_inventory'][sid])
        self.assertFalse((root/'current.json').exists())
        (root/'receipt.json').write_bytes(canonical_bytes(dict(initial_blocks=initial_blocks,saved_blocks=after_blocks,memberships=memberships,append_bundle=reply['descriptor'],proposal=session.proposal,record_audits=producer.serial,topology=topology,record_failure=fault,no_second_pointer=True,maximum_rows=100,scope='local multi-owner saved state with fake collection authority')))

    def topology(self,root,context,store,reader,runtime):
        from .compactor_topology import prepare_namespace_topology,submit_namespace_topology,reconcile_namespace_topology
        from .dynamic_topology import split_request,merge_request,reassign_request,owned_ids
        root.mkdir();audits=[];results=[]
        def auditor(target,namespace,directory):
            directory=Path(directory);directory.mkdir();input_path=directory/'input.json';output=directory/'result.json';input_path.write_bytes(canonical_bytes(dict(capture=target,namespace=namespace,store=store,output=str(output))))
            run_native(runtime['path'],['--ignored','--exact','audit_current_namespace_view','--nocapture'],directory/'native',env=dict(os.environ,SPANN_NAMESPACE_VIEW_INPUT=str(input_path)),timeout=10)
            report=json.loads(output.read_bytes());self.assertEqual(report['namespace'],namespace);self.assertEqual(report['collection_binding'],target['record_collection_binding']);self.assertTrue(report['all_three_record_maps_and_vector_values_verified']);audits.append(artifact(output));return artifact(output)
        for name in ('split','merge','reassign'):
            view=current_view(context);parent=json.loads(checked_artifact(view['registered_bundle'],'.').read_bytes())['deployment']
            if name=='split':
                sid=sorted(parent['owners'])[0];ids=list(owned_ids(next(s for s in parent['tree']['subtrees'] if s['subtree_id']==sid)));request=split_request(parent,sid,ids[:len(ids)//2],'split-a','split-b')
            elif name=='merge':request=merge_request(parent,['split-a','split-b'],'merged')
            else:request=reassign_request(parent,'merged',next(s for s in parent['owners'] if s!='merged'),[next(owned_ids(next(s for s in parent['tree']['subtrees'] if s['subtree_id']=='merged')))])
            delegate=CapturedAuthority(copy.deepcopy(context),[]);prepared=prepare_namespace_topology(delegate,context['parent']['collection']['collection_id'],request,root/name,reader,auditor,timeout=30)
            self.assertEqual(submit_namespace_topology(delegate,prepared,auditor)['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1);delegate.state=copy.deepcopy(prepared['candidate_state']);self.assertEqual(reconcile_namespace_topology(delegate,prepared)['status'],'committed')
            candidate=json.loads(checked_artifact(prepared['descriptor'],'.').read_bytes());self.exact_rerank(candidate['deployment'],reader,root/(name+'-rerank'))
            before_ns=view['namespace'];after_ns=json.loads(checked_artifact(candidate['hierarchy_metadata']['namespace_root'],'.').read_bytes())
            for key in ('mapping','identity_source','canonical_vectors','record_files'):self.assertEqual(after_ns[key],before_ns[key])
            results.append(dict(operation=name,bundle=prepared['descriptor'],proposal=prepared['proposal']))
            context=copy.deepcopy(context);context['parent']=copy.deepcopy(prepared['candidate_state']);context['record_collection_binding']['wire']['version']+=1;context['compaction']['job_id']=str(uuid.uuid4());context['compaction']['session_id']=str(uuid.uuid4())
        return dict(results=results,audits=audits)

    def exact_rerank(self,record,reader,output):
        from .dynamic_topology import owned_ids
        canonical=checked_artifact(record['canonical_vectors'],'.').read_bytes();hits=[]
        for slot in record['tree']['subtrees']:
            request=dict(op='rerank_selected',generation=slot['generation'],request_id='representative-exact',query=[0.]*DIM,local_ids=list(range(slot['vector_count'])))
            run=run_native(reader,['read','--checkpoint-dir',record['owners'][slot['subtree_id']]['checkpoint_dir'],'--generation',str(slot['generation'])],output/slot['subtree_id'],input=canonical_bytes(request),timeout=5)
            hits+=json.loads(run.stdout)['results']
        self.assertEqual(sorted(v['id'] for v in hits),list(range(record['tree']['source']['row_count'])))
        for hit in hits:
            vector=struct.unpack_from('<'+'f'*DIM,canonical,hit['id']*DIM*4);self.assertAlmostEqual(hit['distance'],sum(v*v for v in vector),delta=max(.01,abs(hit['distance'])*1e-6))

    def record_failure(self,root,context,store,runtime):
        root.mkdir();delegate=CapturedAuthority(copy.deepcopy(context),[]);collection=context['parent']['collection']['collection_id'];view=current_view(context)
        rows=[dict(log_offset=99+i,user_id='after-fault-'+str(i),embedding_bits=list(struct.unpack('<'+'I'*DIM,struct.pack('<'+'f'*DIM,*([float(i),1.]+[0.]*(DIM-2)))))) for i in range(2)]
        source=prepare_current_namespace_export(delegate,collection,view['registered_bundle'],rows,store,root/'input',fixture_parent_rows=98);before=inventory(store);state=canonical_bytes(delegate.state)
        env=dict(os.environ,SPANN_NAMESPACE_CURRENT_INPUT=source['path'],SPANN_NAMESPACE_CURRENT_OUTPUT=str(root/'failed'),SPANN_NAMESPACE_FAIL_RECORD_ROOT='1')
        run_native(runtime['path'],['--ignored','--exact','export_current_representative_append','--nocapture'],root/'failed-run',env=env)
        fault=json.loads((root/'failed/flush-failure.json').read_bytes());self.assertEqual(fault['status'],'failed-private-record-flush');self.assertEqual(len(fault['written_roots']),3);self.assertFalse((root/'failed/export.json').exists());self.assertEqual(delegate.starts,0);self.assertEqual(canonical_bytes(delegate.state),state)
        env.pop('SPANN_NAMESPACE_FAIL_RECORD_ROOT');env['SPANN_NAMESPACE_CURRENT_OUTPUT']=str(root/'fresh-parent-recovery')
        run_native(runtime['path'],['--ignored','--exact','export_current_representative_append','--nocapture'],root/'recovery-run',env=env)
        recovery=json.loads((root/'fresh-parent-recovery/export.json').read_bytes());self.assertEqual([r['global_id'] for r in recovery['captured']['rows']],[98,99]);self.assertEqual(recovery['registered_bundle'],view['registered_bundle'])
        after=inventory(store)
        for name,value in before.items():self.assertEqual(after[name],value)
        self.assertTrue(Path(fault['blocked_path']).is_dir());self.assertEqual(delegate.starts,0)
        return dict(fault=artifact(root/'failed/flush-failure.json'),recovery=artifact(root/'fresh-parent-recovery/export.json'),registration_calls=0,no_complete_bundle=True)
