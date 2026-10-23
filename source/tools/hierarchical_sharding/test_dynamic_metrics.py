"""Real IP/cosine transactions bind normalization and reject zero routing."""
import json
import math
import os
from pathlib import Path
import struct
import tempfile
import unittest
from .tree_manifest import artifact,canonical_bytes,routing_boundary_hash,content_hash
from .dynamic_runtime import build_lower,seal_owner,probe_reader,DeploymentReader,GenerationServing
from .dynamic_deployment import make_record,publish_deployment
from .dynamic_topology import rewrite_topology,merge_request,reassign_request,append_canonical

BINARY=os.environ.get('SPANN_DYNAMIC_BINARY')

@unittest.skipUnless(BINARY,'SPANN_DYNAMIC_BINARY is required for real Rust integration')
class MetricTransactions(unittest.TestCase):
    def test_ip_and_cosine_normalization_and_cancellation_admission(self):
        for metric in ('ip','cosine'):
            with self.subTest(metric=metric),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary);vectors=[(2.,0.),(0.,3.),(-2.,0.),(0.,-3.)];corpus=root/'vectors.f32';corpus.write_bytes(b''.join(struct.pack('<ff',*v) for v in vectors))
                model=dict(construction_epoch='dynamic-metric-v1',dimension=2,metric=metric,centroids=[[1.,1.5],[-1.,-1.5]])
                model_path=root/'model.json';model_path.write_bytes(canonical_bytes(model))
                tree=dict(format_version=1,tree_generation=1,construction_epoch=model['construction_epoch'],dimension=2,metric=metric,source=dict(source_id='metric-'+metric,row_count=4),routing_boundary_scheme='centroid-content-v1',model=artifact(model_path),subtrees=[],upper_root=dict(kind='centroid_root',children=[]));owners={}
                for index,sid in enumerate(('a','b')):
                    vp=root/(sid+'.f32');mp=root/(sid+'.u64le');ids=list(range(2*index,2*index+2));vp.write_bytes(b''.join(struct.pack('<ff',*vectors[i]) for i in ids));mp.write_bytes(b''.join(struct.pack('<Q',i) for i in ids))
                    subtree=build_lower(BINARY,vp,mp,root/('index-'+sid),sid,1,2,metric,'metric-'+metric,routing_boundary_hash(tree,model,index),construction=dict(branching_factor=2,split_threshold=8,merge_threshold=2));subtree['centroid_index']=index;tree['subtrees'].append(subtree);tree['upper_root']['children'].append(dict(subtree_id=sid,centroid_index=index));owners[sid]=seal_owner(root/('index-'+sid),subtree,'worker-a')
                record=make_record(tree,owners,canonical_vectors=artifact(corpus));pointer=root/'current.json';probe=lambda sid,owner,subtree:probe_reader(BINARY,sid,owner,subtree)
                publish_deployment(pointer,record,None,probe);old=DeploymentReader(pointer,BINARY)
                def check(reader):
                    for query in ([1.,0.],[0.,1.],[-1.,0.]):
                        def score(i):
                            v=vectors[i];norm=math.sqrt(sum(x*x for x in v)) if metric=='cosine' else 1
                            return 1-sum(a*b/norm for a,b in zip(query,v))
                        expected=sorted(range(4),key=lambda i:(score(i),i));result=reader.query(query,4,beam_width=None,candidate_budget=4)
                        self.assertEqual(result['ids'],expected);self.assertEqual(result['distances'],[score(i) for i in expected])
                serving=None;current=None
                try:
                    check(old);before=pointer.read_bytes()
                    if metric=='cosine':
                        with self.assertRaisesRegex(ValueError,'centroid cancels'):rewrite_topology(BINARY,record,record['canonical_vectors'],merge_request(record,['a','b'],'merged'),root/'zero-cosine')
                        self.assertEqual(pointer.read_bytes(),before);change=reassign_request(record,'a','b',[0])
                    else:change=merge_request(record,['a','b'],'merged')
                    changed=rewrite_topology(BINARY,record,record['canonical_vectors'],change,root/'valid-change')
                    proposal=make_record(changed,{s['subtree_id']:seal_owner(Path(s['snapshot']['path']).parent,s,'worker-a') for s in changed['subtrees']},record,'topology')
                    serving=GenerationServing(proposal,BINARY);publish_deployment(pointer,serving.record,content_hash(record),probe)
                    current=DeploymentReader(pointer,BINARY,backend=serving.backend());check(current);check(old)
                    self.assertEqual(serving.record['vector_semantics']['normalization'],'worker-f64-l2-to-f32' if metric=='cosine' else 'identity')
                    bad=root/'bad-append.f32';bad.write_bytes(corpus.read_bytes()+struct.pack('<ff',float('nan'),1.))
                    with self.assertRaisesRegex(ValueError,'nonfinite'):append_canonical(BINARY,serving.record,artifact(bad),root/'bad-append')
                finally:
                    if current:current.close()
                    if serving:serving.close()
                    old.close()
