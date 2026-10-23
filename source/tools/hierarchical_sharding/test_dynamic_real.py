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
        tree=rewrite_topology(BINARY,self.record,self.record['canonical_vectors'],split_request(self.record['tree'],'s0',range(8),'left','right'),self.root/'before-append')
        owners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in tree['subtrees']}
        parent=make_record(tree,owners,self.record,'topology');publish_deployment(self.pointer,parent,content_hash(self.record),self.probe)
        pinned=DeploymentReader(self.pointer,BINARY)
        try:
            extended=self.vectors+[(1.,1.),(2.,2.)];corpus=self.root/'extended.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in extended))
            newtree=append_canonical(BINARY,parent,artifact(corpus),self.root/'append')
            newowners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in newtree['subtrees']}
            record=make_record(newtree,newowners,parent,'compaction',artifact(corpus));publish_deployment(self.pointer,record,content_hash(parent),self.probe)
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current,extended);self.exact(pinned);self.exact(self.old)
            finally:current.close()
            previous={s['subtree_id']:s for s in tree['subtrees']}
            self.assertEqual(sum(s['snapshot']==previous[s['subtree_id']]['snapshot'] for s in newtree['subtrees']),3)
            before=self.pointer.read_bytes();bad=bytearray(corpus.read_bytes());bad[0]^=1;corrupt=self.root/'corrupt.f32';corrupt.write_bytes(bad)
            with self.assertRaisesRegex(ValueError,'existing vector bytes'):append_canonical(BINARY,parent,artifact(corrupt),self.root/'bad-append')
            self.assertEqual(self.pointer.read_bytes(),before)
        finally:pinned.close()

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
        moved=prepare_migration(self.record,{'s0':'worker-b','s1':'worker-a','s2':'worker-a'},{'s0':self.root/'independent-copy'},64*1024**2)
        publish_deployment(self.pointer,moved,content_hash(self.record),self.probe)
        self.old.close()
        source=self.root/'index-s0';offline=self.root/'source-s0-offline';source.rename(offline)
        try:
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current)
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
