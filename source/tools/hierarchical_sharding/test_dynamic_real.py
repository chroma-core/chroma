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
            subtree=build_lower(BINARY,vp,mapping,self.root/('index-'+sid),sid,1,2,'l2','dynamic-real-corpus',routing_boundary_hash(tree,model,slot));subtree['centroid_index']=slot
            tree['subtrees'].append(subtree);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=slot));owners[sid]=seal_owner(self.root/('index-'+sid),subtree,'worker-a')
        self.pointer=self.root/'current.json';self.probe=lambda sid,owner,subtree:probe_reader(BINARY,sid,owner,subtree)
        self.record=make_record(tree,owners,canonical_vectors=artifact(corpus));publish_deployment(self.pointer,self.record,None,self.probe)
        self.old=DeploymentReader(self.pointer,BINARY)
    def tearDown(self):
        self.old.close();self.temp.cleanup()
    def exact(self,reader):
        traces=[]
        for query in ([0.,0.],[17.,2.],[45.,4.]):
            result=reader.query(query,48,beam_width=None,candidate_budget=48)
            expected=sorted(range(48),key=lambda i:(sum((a-b)**2 for a,b in zip(query,self.vectors[i])),i))
            self.assertEqual(result['ids'],expected)
            self.assertEqual(result['distances'],[sum((a-b)**2 for a,b in zip(query,self.vectors[i])) for i in expected])
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
        self.exact(self.old);parent=self.record
        changes=[split_request(parent['tree'],'s0',range(8),'left','right')]
        for epoch in range(3):
            if epoch==1:changes.append(merge_request(parent['tree'],['left','right'],'merged'))
            if epoch==2:changes.append(reassign_request(parent['tree'],'merged','s1',[0,1,2]))
            untouched=next(s for s in parent['tree']['subtrees'] if s['subtree_id']=='s2')
            tree=rewrite_topology(BINARY,parent,parent['canonical_vectors'],changes[epoch],self.root/('rewrite-'+str(epoch)))
            self.assertEqual(next(s for s in tree['subtrees'] if s['subtree_id']=='s2')['snapshot'],untouched['snapshot'])
            owners={s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-b' if s['subtree_id']!='s2' else 'worker-a') for s in tree['subtrees']}
            record=make_record(tree,owners,parent,'topology')
            publish_deployment(self.pointer,record,content_hash(parent),self.probe)
            current=DeploymentReader(self.pointer,BINARY)
            try:self.exact(current);self.exact(self.old)
            finally:current.close()
            parent=record
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
