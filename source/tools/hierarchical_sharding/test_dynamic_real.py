"""Actual Rust-backed deployment moves and cross-boundary transactions.

Run with SPANN_DYNAMIC_BINARY pointing to an immutable tested worker binary.
The suite creates only temporary local stores and never uses benchmark stores.
"""
import copy
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from .tree_manifest import artifact,canonical_bytes,routing_boundary_hash,content_hash
from .dynamic_runtime import build_lower,seal_owner,probe_reader,prepare_migration,DeploymentReader
from .dynamic_deployment import make_record,publish_deployment,load_deployment
from .dynamic_topology import rewrite_topology,split_request,merge_request,reassign_request

def publish_in_process(pointer,record,parent_hash,binary,barrier,queue):
    try:
        barrier.wait(timeout=20)
        receipt=publish_deployment(pointer,record,parent_hash,lambda sid,owner,subtree:probe_reader(binary,sid,owner,subtree))
        queue.put(('published',receipt))
    except Exception as error:queue.put(('failed',type(error).__name__+': '+str(error)))

BINARY=os.environ.get('SPANN_DYNAMIC_BINARY')

@unittest.skipUnless(BINARY,'SPANN_DYNAMIC_BINARY is required for real Rust integration')
class RealTransactions(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.vectors=[(float(i+1),float((i%5)+1)) for i in range(48)]
        corpus=self.root/'corpus.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in self.vectors))
        model=dict(construction_epoch='dynamic-real-v1',dimension=2,metric='l2',centroids=[[8.5,3],[24.5,3],[40.5,3]])
        mp=self.root/'model.json';mp.write_bytes(canonical_bytes(model))
        tree=dict(format_version=1,tree_generation=1,construction_epoch=model['construction_epoch'],dimension=2,metric='l2',source=dict(source_id='dynamic-real-corpus',row_count=48),routing_boundary_scheme='centroid-content-v1',model=artifact(mp),subtrees=[],upper_root=dict(kind='centroid_root',children=[]))
        owners={}
        for slot in range(3):
            sid='s'+str(slot);ids=list(range(slot*16,(slot+1)*16));vp=self.root/(sid+'.f32');mapping=self.root/(sid+'.u64le')
            vp.write_bytes(b''.join(struct.pack('<ff',*self.vectors[i]) for i in ids));mapping.write_bytes(b''.join(struct.pack('<Q',i) for i in ids))
            subtree=build_lower(BINARY,vp,mapping,self.root/('index-'+sid),sid,1,2,'l2','dynamic-real-corpus',routing_boundary_hash(tree,model,slot),construction=dict(branching_factor=2,split_threshold=8,merge_threshold=2));subtree['centroid_index']=slot
            tree['subtrees'].append(subtree);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=slot));owners[sid]=seal_owner(self.root/('index-'+sid),subtree,'worker-a')
        self.pointer=self.root/'current.json';self.probe=lambda sid,owner,subtree:probe_reader(BINARY,sid,owner,subtree)
        self.record=make_record(tree,owners,canonical_vectors=artifact(corpus));publish_deployment(self.pointer,self.record,None,self.probe)
        self.old=DeploymentReader(self.pointer,BINARY)
    def tearDown(self):
        self.old.close();self.temp.cleanup()
    def exact(self,reader,vectors=None):
        vectors=self.vectors if vectors is None else vectors
        traces=[]
        for query in ([0.,0.],[17.,2.],[45.,4.]):
            result=reader.query(query,len(vectors),beam_width=None,candidate_budget=len(vectors))
            expected=sorted(range(len(vectors)),key=lambda i:(sum((a-b)**2 for a,b in zip(query,vectors[i])),i))
            self.assertEqual(result['ids'],expected)
            self.assertEqual(result['distances'],[sum((a-b)**2 for a,b in zip(query,vectors[i])) for i in expected])
            traces.append((result['ids'],result['distances'],result['levels'],result['selected_candidates']))
        return traces
    def test_migration_is_fenced_and_keeps_pinned_old_readers(self):
        before=self.exact(self.old);assignments={sid:('worker-b' if sid=='s0' else 'worker-a') for sid in self.record['owners']}
        moved=prepare_migration(self.record,assignments,{'s0':self.root/'worker-b'/'s0'},64*1024**2)
        self.assertEqual(content_hash(moved['tree']),content_hash(self.record['tree']))
        publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        current=DeploymentReader(self.pointer,BINARY)
        try:self.assertEqual(before,self.exact(current));self.assertEqual(before,self.exact(self.old))
        finally:current.close()
        with self.assertRaisesRegex(ValueError,'compare-and-swap'):
            publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.assertEqual(load_deployment(self.pointer),moved)
    def test_split_merge_reassign_publish_all_owners_and_preserve_unchanged_snapshot(self):
        from .dynamic_runtime import GenerationServing
        self.exact(self.old);parent=self.record;servers=[];old_http=[]
        changes=[split_request(parent['tree'],'s0',range(8),'left','right')]
        try:
            for epoch in range(3):
                if epoch==1:changes.append(merge_request(parent['tree'],['left','right'],'merged'))
                if epoch==2:changes.append(reassign_request(parent['tree'],'merged','s1',[0,1,2]))
                untouched=next(s for s in parent['tree']['subtrees'] if s['subtree_id']=='s2')
                tree=rewrite_topology(BINARY,parent,parent['canonical_vectors'],changes[epoch],self.root/('rewrite-'+str(epoch)))
                self.assertEqual(next(s for s in tree['subtrees'] if s['subtree_id']=='s2')['snapshot'],untouched['snapshot'])
                owners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-b' if s['subtree_id']!='s2' else 'worker-a') for s in tree['subtrees']}
                serving=GenerationServing(make_record(tree,owners,parent,'topology'),BINARY);servers.append(serving);record=serving.record
                publish_deployment(self.pointer,record,content_hash(parent),self.probe)
                current=DeploymentReader(self.pointer,BINARY);remote=DeploymentReader(self.pointer,BINARY,backend=serving.backend());old_http.append(remote)
                try:
                    self.assertEqual(self.exact(current),self.exact(remote));self.exact(self.old)
                    for pinned in old_http:self.exact(pinned)
                    for width in (1,2,4):
                        a=current.query([17.,2.],4,beam_width=width,candidate_budget=8);b=remote.query([17.,2.],4,beam_width=width,candidate_budget=8)
                        for field in ('ids','distances','levels','selected_candidates','candidate_contributions','global_candidate_count'):self.assertEqual(a[field],b[field])
                        self.assertLessEqual(a['global_candidate_count'],8);self.assertTrue(all(level['selected_count']<=width for level in a['levels']))
                finally:current.close()
                parent=record
        finally:
            for reader in old_http:reader.close()
            for serving in servers:serving.close()
    def test_generation_specific_http_pins_and_same_generation_budget_parity(self):
        from .dynamic_runtime import GenerationServing
        serving=GenerationServing(make_record(self.record['tree'],self.record['owners'],self.record),BINARY)
        try:
            parent=serving.record;publish_deployment(self.pointer,parent,content_hash(self.record),self.probe)
            old_http=DeploymentReader(self.pointer,BINARY,backend=serving.backend())
            try:
                expected=self.exact(old_http)
                moved=prepare_migration(parent,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'http-copy'},64*1024**2)
                new_serving=GenerationServing(moved,BINARY)
                try:
                    publish_deployment(self.pointer,new_serving.record,content_hash(parent),self.probe)
                    new_http=DeploymentReader(self.pointer,BINARY,backend=new_serving.backend());direct=DeploymentReader(self.pointer,BINARY)
                    try:
                        self.assertEqual(expected,self.exact(old_http));self.assertEqual(expected,self.exact(new_http))
                        for width in (1,2,4):
                            a=direct.query([17.,2.],4,beam_width=width,candidate_budget=8)
                            b=new_http.query([17.,2.],4,beam_width=width,candidate_budget=8)
                            for field in ('ids','distances','levels','selected_candidates','candidate_contributions','global_candidate_count'):self.assertEqual(a[field],b[field])
                            self.assertLessEqual(a['global_candidate_count'],8)
                            self.assertTrue(all(level['selected_count']<=width for level in a['levels']))
                    finally:new_http.close();direct.close()
                finally:new_serving.close()
            finally:old_http.close()
        finally:serving.close()

    def test_two_prepared_owners_have_one_cas_winner_even_without_tree_change(self):
        import concurrent.futures
        candidates=[]
        for worker in ('b','c'):
            candidates.append(prepare_migration(self.record,{'s0':'worker-'+worker,'s1':'worker-a','s2':'worker-a'},{'s0':self.root/('race-'+worker)},64*1024**2))
        def attempt(record):
            try:return publish_deployment(self.pointer,record,content_hash(self.record),self.probe)
            except ValueError as error:return str(error)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:outcomes=list(pool.map(attempt,candidates))
        self.assertEqual(sum(isinstance(x,dict) for x in outcomes),1)
        self.assertEqual(sum(isinstance(x,str) and 'compare-and-swap' in x for x in outcomes),1)
        self.exact(self.old)

    def test_append_after_boundary_split_keeps_old_pins_and_snapshot_namespaces(self):
        from .dynamic_topology import append_canonical
        from .dynamic_runtime import GenerationServing
        tree=rewrite_topology(BINARY,self.record,self.record['canonical_vectors'],split_request(self.record,'s0',range(8),'left','right'),self.root/'before-append')
        owners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in tree['subtrees']}
        parent=make_record(tree,owners,self.record,'topology');publish_deployment(self.pointer,parent,content_hash(self.record),self.probe)
        pinned=DeploymentReader(self.pointer,BINARY);servers=[];readers=[];references=[]
        try:
            extended=list(self.vectors)
            for step in range(2):
                extended=extended+[(float(step+1),1.),(float(step+2),2.)];corpus=self.root/('extended-'+str(step)+'.f32');corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in extended))
                newtree=append_canonical(BINARY,parent,artifact(corpus),self.root/('append-'+str(step)))
                newowners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in newtree['subtrees']}
                serving=GenerationServing(make_record(newtree,newowners,parent,'compaction',artifact(corpus)),BINARY);servers.append(serving);record=serving.record
                publish_deployment(self.pointer,record,content_hash(parent),self.probe)
                direct=DeploymentReader(self.pointer,BINARY);remote=DeploymentReader(self.pointer,BINARY,backend=serving.backend());readers.append(remote);references.append(extended)
                try:self.assertEqual(self.exact(direct,extended),self.exact(remote,extended));self.exact(pinned);self.exact(self.old)
                finally:direct.close()
                for prior,expected in zip(readers,references):self.exact(prior,expected)
                if step==0:
                    previous={s['subtree_id']:s for s in tree['subtrees']};self.assertEqual(sum(s['snapshot']==previous[s['subtree_id']]['snapshot'] for s in newtree['subtrees']),3)
                parent=record
            before=self.pointer.read_bytes();bad=bytearray(corpus.read_bytes());bad[0]^=1;corrupt=self.root/'corrupt.f32';corrupt.write_bytes(bad+struct.pack('<ff',9.,9.))
            with self.assertRaisesRegex(ValueError,'existing vector bytes'):append_canonical(BINARY,parent,artifact(corrupt),self.root/'bad-append')
            self.assertEqual(self.pointer.read_bytes(),before)
        finally:
            for reader in readers:reader.close()
            for serving in servers:serving.close()
            pinned.close()

    def test_failed_build_and_stale_transaction_journals_retain_parent_head(self):
        from .dynamic_deployment import run_transaction
        before=self.pointer.read_bytes();journal=self.root/'failed-build.json'
        def fail(_parent):raise RuntimeError('injected lower build failure')
        with self.assertRaisesRegex(RuntimeError,'lower build'):run_transaction(self.pointer,self.record,fail,self.probe,journal)
        self.assertEqual(json.loads(journal.read_text())['status'],'failed_unpublished');self.assertEqual(before,self.pointer.read_bytes())
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'journal-move'},64*1024**2)
        run_transaction(self.pointer,self.record,lambda _parent:moved,self.probe,self.root/'winner.json')
        journal=self.root/'stale.json'
        with self.assertRaisesRegex(ValueError,'compare-and-swap'):run_transaction(self.pointer,self.record,lambda _parent:make_record(self.record['tree'],self.record['owners'],self.record),self.probe,journal)
        self.assertEqual(json.loads(journal.read_text())['status'],'failed_unpublished');self.assertEqual(load_deployment(self.pointer),moved);self.exact(self.old)

    def test_migrated_reader_resolves_map_and_manifest_at_destination(self):
        from .dynamic_topology import append_canonical
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'independent-copy'},64*1024**2)
        publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.old.close();source=self.root/'index-s0';offline=self.root/'source-s0-offline';source.rename(offline)
        try:
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current)
            finally:current.close()
            extended=self.vectors+[(1.,1.),(2.,2.)];corpus=self.root/'offline-append.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in extended))
            appended=append_canonical(BINARY,moved,artifact(corpus),self.root/'offline-append')
            record=make_record(appended,{s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-b') for s in appended['subtrees']},moved,'compaction',artifact(corpus))
            publish_deployment(self.pointer,record,content_hash(moved),self.probe)
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current,extended)
            finally:current.close()
            tree=rewrite_topology(BINARY,record,record['canonical_vectors'],split_request(record,'s0',range(8),'offline-left','offline-right'),self.root/'offline-split')
            next_record=make_record(tree,{s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-b') for s in tree['subtrees']},record,'topology')
            publish_deployment(self.pointer,next_record,content_hash(record),self.probe)
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current,extended)
            finally:current.close()
        finally:offline.rename(source)

    def test_snapshot_audit_and_rebuild_resource_admission_are_mandatory(self):
        before=self.pointer.read_bytes()
        for ids in ([0,1],list(range(16))):
            with self.assertRaisesRegex(ValueError,'loses live IDs|row/byte'):
                rewrite_topology(BINARY,self.record,self.record['canonical_vectors'],{'replacement':{'from':['s0'],'ids':ids}},self.root/'not-built',max_rebuild_rows=8 if len(ids)==16 else 100000)
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'audit-copy'},64*1024**2)
        private=self.root/'audit-copy'/'manifest-1.json';snapshot=json.loads(private.read_text());snapshot['integrity_audit']['coverage']['unique_live_rows']-=1;private.write_bytes(canonical_bytes(snapshot))
        moved['tree']=copy.deepcopy(moved['tree']);subtree=next(s for s in moved['tree']['subtrees'] if s['subtree_id']=='s0');subtree['snapshot']=artifact(private);moved['tree']['tree_generation']=2;moved['operation']='topology';moved['tree_sha256']=content_hash(moved['tree']);moved['owners']['s0']=seal_owner(private.parent,subtree,'worker-b')
        with self.assertRaisesRegex(ValueError,'ownership audit'):publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.assertEqual(before,self.pointer.read_bytes());self.exact(self.old)

    def test_unready_http_destination_cannot_be_published(self):
        before=self.pointer.read_bytes();owners=copy.deepcopy(self.record['owners'])
        owners['s0'].update(endpoint='http://127.0.0.1:9',serving_tree_sha256=content_hash(self.record['tree']))
        proposal=make_record(self.record['tree'],owners,self.record)
        with self.assertRaisesRegex(ValueError,'readiness failed'):publish_deployment(self.pointer,proposal,content_hash(self.record),self.probe)
        self.assertEqual(before,self.pointer.read_bytes());self.exact(self.old)

    def test_process_race_between_prepared_topology_and_placement_has_one_winner(self):
        import multiprocessing
        tree=rewrite_topology(BINARY,self.record,self.record['canonical_vectors'],split_request(self.record,'s0',range(8),'left','right'),self.root/'process-topology')
        topology=make_record(tree,{s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in tree['subtrees']},self.record,'topology')
        migration=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'process-migration'},64*1024**2)
        context=multiprocessing.get_context('spawn');queue=context.Queue();barrier=context.Barrier(2)
        children=[context.Process(target=publish_in_process,args=(self.pointer,record,content_hash(self.record),BINARY,barrier,queue)) for record in (topology,migration)]
        try:
            for child in children:child.start()
            for child in children:child.join(timeout=30);self.assertEqual(child.exitcode,0)
            outcomes=[queue.get(timeout=2) for _ in children]
            self.assertEqual(sum(row[0]=='published' for row in outcomes),1);self.assertEqual(sum(row[0]=='failed' and 'compare-and-swap' in row[1] for row in outcomes),1)
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current);self.exact(self.old)
            finally:current.close()
        finally:
            for child in children:
                if child.is_alive():child.terminate();child.join(timeout=2)
            queue.close();queue.join_thread()

    def test_journal_reconciliation_preserves_receipt_and_descendant_commit(self):
        from unittest.mock import patch
        from . import dynamic_deployment as deployment
        real_publish=deployment.publish_deployment
        proposal=make_record(self.record['tree'],self.record['owners'],self.record)
        def publish_then_descendant(pointer,record,parent_hash,probe):
            real_publish(pointer,record,parent_hash,probe)
            descendant=make_record(record['tree'],record['owners'],record)
            real_publish(pointer,descendant,content_hash(record),probe)
            raise OSError('injected exception after commit and descendant')
        journal=self.root/'descendant-reconcile.json'
        with patch.object(deployment,'publish_deployment',side_effect=publish_then_descendant):
            with self.assertRaisesRegex(OSError,'after commit'):deployment.run_transaction(self.pointer,self.record,lambda _parent:proposal,self.probe,journal)
        self.assertEqual(json.loads(journal.read_text())['status'],'published_reconciliation_required')
        parent=load_deployment(self.pointer);proposal=make_record(parent['tree'],parent['owners'],parent);real_write=deployment._atomic_write;injected=[False]
        def journal_failure(path,data):
            if Path(path)==self.root/'receipt-reconcile.json' and json.loads(data).get('status')=='published' and not injected[0]:
                injected[0]=True;descendant=make_record(proposal['tree'],proposal['owners'],proposal);real_publish(self.pointer,descendant,content_hash(proposal),self.probe);raise OSError('injected journal failure')
            return real_write(path,data)
        journal=self.root/'receipt-reconcile.json'
        with patch.object(deployment,'_atomic_write',side_effect=journal_failure):
            with self.assertRaisesRegex(OSError,'journal failure'):deployment.run_transaction(self.pointer,parent,lambda _parent:proposal,self.probe,journal)
        state=json.loads(journal.read_text());self.assertEqual(state['status'],'published_reconciliation_required');self.assertEqual(state['publication']['deployment_sha256'],content_hash(proposal));self.exact(self.old)

    def test_targeted_lazy_vector_block_loss_is_rejected_before_cas(self):
        import uuid
        from .dynamic_runtime import descriptor
        from .subtree_transport import PinnedReader,TreeError
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'lazy-copy'},64*1024**2)
        subtree=next(s for s in moved['tree']['subtrees'] if s['subtree_id']=='s0');directory=self.root/'lazy-copy';manifest=json.loads((directory/'manifest-1.json').read_text())
        root_bytes=(directory/'data'/'sparse_index'/manifest['block_ids']['vector_data_id']).read_bytes()
        blocks=[p for p in (directory/'data'/'block').iterdir() if uuid.UUID(p.name).bytes in root_bytes or p.name.encode() in root_bytes]
        self.assertTrue(blocks,'must identify actual vector-root referenced raw blocks')
        for block in blocks:block.unlink()
        reader=PinnedReader(descriptor(BINARY,directory,subtree),30)
        try:
            item=dict(subtree_id='s0',snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root'));reader.call(item)
            item['payload']=dict(op='rerank_selected',query=[0.,0.],local_ids=list(range(16)))
            with self.assertRaises(TreeError):reader.call(item)
        finally:reader.close()
        before=self.pointer.read_bytes()
        with self.assertRaises(ValueError):publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.assertEqual(before,self.pointer.read_bytes());self.exact(self.old)

    def test_canonical_mismatch_unverified_producer_and_legacy_snapshot_reject(self):
        from .dynamic_deployment import validate_record
        from .dynamic_content import PROOF_NAME
        before=self.pointer.read_bytes();changed=self.root/'different-corpus.f32';data=bytearray(Path(self.record['canonical_vectors']['path']).read_bytes());data[:4]=struct.pack('<f',99.);changed.write_bytes(data)
        proposal=make_record(self.record['tree'],self.record['owners'],canonical_vectors=artifact(changed))
        with self.assertRaisesRegex(ValueError,'canonical vector values'):publish_deployment(self.root/'bad-initial.json',proposal,None,self.probe)
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'legacy-copy'},64*1024**2)
        proof=self.root/'legacy-copy'/PROOF_NAME;original=proof.read_bytes();proof.unlink()
        with self.assertRaisesRegex(ValueError,'provenance is required'):validate_record(moved)
        proof.write_bytes(original);moved['accepted_build_producers'].append('0'*64);moved['accepted_build_producers'].sort()
        with self.assertRaisesRegex(ValueError,'caller cannot declare'):publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.assertEqual(before,self.pointer.read_bytes());self.exact(self.old)

    def test_relative_artifacts_are_rejected_even_with_a_valid_external_base(self):
        before=self.pointer.read_bytes()
        for field in ('model','canonical','checkpoint'):
            proposal=make_record(self.record['tree'],self.record['owners'],self.record)
            proposal=copy.deepcopy(proposal)
            if field=='model':proposal['tree']['model']['path']='model.json';proposal['tree_sha256']=content_hash(proposal['tree'])
            elif field=='canonical':proposal['canonical_vectors']['path']='corpus.f32'
            else:proposal['owners']['s0']['checkpoint_dir']='index-s0'
            with self.assertRaisesRegex(ValueError,'absolute'):publish_deployment(self.pointer,proposal,content_hash(self.record),self.probe,base=self.root)
        self.assertEqual(before,self.pointer.read_bytes())

    def test_fresh_control_ancestry_failure_and_post_pointer_exception_reconcile(self):
        from unittest.mock import patch,Mock
        from . import dynamic_runtime as runtime,dynamic_deployment as deployment
        original_namespace=runtime.durable_namespace;fresh=self.root/'new-state'/'publications'/'tenant'/'current.json';probe=Mock(side_effect=AssertionError('probe must not run after control durability failure'))
        def fail_control(path):
            if Path(path)==fresh.parent:raise OSError('injected publication ancestor fsync')
            return original_namespace(path)
        with patch.object(runtime,'durable_namespace',side_effect=fail_control):
            with self.assertRaisesRegex(OSError,'ancestor'):publish_deployment(fresh,self.record,None,probe)
        self.assertFalse(fresh.exists());probe.assert_not_called()
        journal=self.root/'other-state'/'journal'/'attempt.json'
        with patch.object(runtime,'durable_namespace',side_effect=OSError('injected journal ancestor fsync')):
            with self.assertRaisesRegex(OSError,'journal ancestor'):deployment.run_transaction(self.pointer,self.record,lambda p:make_record(p['tree'],p['owners'],p),self.probe,journal)
        self.assertFalse(journal.exists())
        original_write=deployment._atomic_write;injected=[False];proposal=make_record(self.record['tree'],self.record['owners'],self.record)
        def after_pointer(path,data):
            original_write(path,data)
            if Path(path)==self.pointer and not injected[0]:injected[0]=True;raise OSError('injected exception after pointer replacement')
        journal=self.root/'post-pointer.json'
        with patch.object(deployment,'_atomic_write',side_effect=after_pointer):
            with self.assertRaisesRegex(OSError,'pointer replacement'):deployment.run_transaction(self.pointer,self.record,lambda p:proposal,self.probe,journal)
        self.assertEqual(json.loads(journal.read_text())['status'],'published_reconciliation_required');self.assertEqual(load_deployment(self.pointer),proposal);self.exact(self.old)

    def test_failed_readiness_missing_data_and_invalid_ownership_leave_head_unchanged(self):
        before=self.pointer.read_bytes()
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'bad-copy'},64*1024**2)
        victim=next(p for p in (self.root/'bad-copy').rglob('*') if p.is_file() and p.name not in ('manifest.json','manifest-1.json'))
        victim.unlink()
        with self.assertRaises(ValueError):publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.assertEqual(self.pointer.read_bytes(),before)
        bad={'bad':{'from':['s0'],'ids':[0,1,1]}}
        with self.assertRaises(ValueError):rewrite_topology(BINARY,self.record,self.record['canonical_vectors'],bad,self.root/'invalid')
        self.assertEqual(self.pointer.read_bytes(),before);self.exact(self.old)

    def test_compaction_uses_destination_maps_when_preparation_stores_are_offline(self):
        from .dynamic_topology import append_canonical
        from .dynamic_runtime import copy_checkpoint
        extended=self.vectors+[(1.,1.),(2.,2.)]
        corpus=self.root/'candidate-corpus.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in extended))
        tree=append_canonical(BINARY,self.record,artifact(corpus),self.root/'candidate-preparation')
        owners={};offline=[]
        try:
            for subtree in tree['subtrees']:
                source=Path(subtree['snapshot']['path']).parent
                destination=self.root/'candidate-destination'/subtree['subtree_id']
                copy_checkpoint(source,destination,64*1024**2)
                owners[subtree['subtree_id']]=seal_owner(destination,subtree,'worker-b')
                if self.root/'candidate-preparation' in source.parents:
                    hidden=source.with_name(source.name+'-offline');source.rename(hidden);offline.append((hidden,source))
            record=make_record(tree,owners,self.record,'compaction',artifact(corpus))
            publish_deployment(self.pointer,record,content_hash(self.record),self.probe)
            reader=DeploymentReader(self.pointer,BINARY)
            try:self.exact(reader,extended)
            finally:reader.close()
        finally:
            for hidden,source in offline:hidden.rename(source)
