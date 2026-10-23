"""Algorithm contracts only; native saved-state audits run in the retained replay."""
import copy
import unittest
from unittest.mock import patch
from .poc_incremental_validation import FixedInputSession,payload
from .tree_manifest import content_hash

class FixedInputComposition(unittest.TestCase):
    def test_append_then_slot_reordered_split_composes_exact_ownership(self):
        def slot(sid,count,centroid):
            return dict(subtree_id=sid,generation=1,vector_count=count,snapshot=dict(path=sid,sha256=sid),local_to_global=dict(path=sid),root_ref=dict(id=1),routing_boundary_hash=sid,ownership_revision=sid,centroid_index=centroid)
        def record(owners,rows,fence,parent=None):
            tree=dict(source=dict(source_id='fixed',row_count=rows),dimension=1,metric='l2',construction_epoch='fixed',model={},subtrees=owners,upper_root=dict(children=[dict(subtree_id=s['subtree_id'],centroid_index=s['centroid_index']) for s in owners]))
            return dict(tree=tree,tree_sha256=content_hash(tree),fence=fence,parent_sha256=None if parent is None else content_hash(parent),owners={s['subtree_id']:{} for s in owners})
        a=slot('a',2,0);b=slot('b',2,1);parent=record([a,b],4,1)
        appended_b=slot('b',4,1);appended_b['generation']=2;append=record([a,appended_b],6,2,parent)
        b_shift=copy.deepcopy(appended_b);b_shift['centroid_index']=0
        left=slot('left',1,1);right=slot('right',1,2);split=record([b_shift,left,right],6,3,append)
        calls=[];audits=[]
        def maps(s):
            calls.append(s['subtree_id']);return dict(a=[0,3],b=[1,2] if s['vector_count']==2 else [1,2,4,5],left=[0],right=[3])[s['subtree_id']]
        def audit(sid,slot,owner,ids):audits.append((sid,ids));return dict(saved_ids=ids)
        def model(tree):return dict(centroids=[[10],[20]] if len(tree['subtrees'])==2 else [[20],[10],[30]])
        with patch('tools.hierarchical_sharding.poc_incremental_validation.mapped',side_effect=maps),patch('tools.hierarchical_sharding.poc_incremental_validation.load_model',side_effect=model),patch('tools.hierarchical_sharding.poc_incremental_validation.routing_boundary_hash',side_effect=lambda tree,model,index:tree['subtrees'][index]['routing_boundary_hash']):
            session=FixedInputSession(parent,dict(tree_sha256=parent['tree_sha256'],vector_count=4))
            first=session.transition(append,'append',['b'],audit);self.assertEqual(calls,['b','b']);self.assertEqual(first['unchanged_owners'],['a'])
            calls.clear();second=session.transition(split,'topology',['a'],audit)
            self.assertEqual(calls,['a','left','right']);self.assertEqual(second['unchanged_owners'],['b']);self.assertEqual(second['vector_count'],6)
        self.assertEqual(audits,[('b',[1,2,4,5]),('left',[0]),('right',[3])])
