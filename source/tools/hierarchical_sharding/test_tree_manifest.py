import concurrent.futures
import copy
import json
from pathlib import Path
import struct
import tempfile
import unittest
from .tree_manifest import artifact, assemble_tree, canonical_bytes, content_hash, load_published, publish, validate_tree, validate_placement


def fixture(root,generation=1,mappings=((0,2),(1,3)),model=None):
    root.mkdir(parents=True)
    if model is None:model=dict(construction_epoch='epoch',dimension=2,metric='l2',centroids=[[-1,0],[1,0]])
    (root/'model.json').write_bytes(canonical_bytes(model));md=artifact(root/'model.json',root)
    subtrees=[]
    for slot,ids in enumerate(mappings):
        mapfile=root/f'{slot}.u64';mapfile.write_bytes(struct.pack('<'+'Q'*len(ids),*ids));m=artifact(mapfile,root)
        boundary=content_hash(dict(model_sha256=md['sha256'],centroid_index=slot))
        ref=dict(kind='leaf',id=slot)
        snapshot=dict(generation=generation,source_id='src',shard_id=str(slot),dimension=2,metric='l2',vector_count=len(ids),block_ids={'leaf_node_id':'block-'+str(slot)},root_ref=ref,id_mapping=dict(m,count=len(ids)),routing_boundary_hash=boundary)
        path=root/f'{slot}.json';path.write_bytes(canonical_bytes(snapshot))
        subtrees.append(dict(subtree_id=str(slot),centroid_index=slot,generation=generation,root_ref=ref,vector_count=len(ids),local_to_global=m,snapshot=artifact(path,root),routing_boundary_hash=boundary))
    return dict(format_version=1,tree_generation=generation,construction_epoch='epoch',dimension=2,metric='l2',source=dict(source_id='src',row_count=sum(map(len,mappings))),model=md,upper_root=dict(kind='centroid_root',children=[dict(subtree_id=str(i),centroid_index=i) for i in range(2)]),subtrees=subtrees)


class ManifestTests(unittest.TestCase):
    def test_digests_coverage_and_placement_are_enforced(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);tree=fixture(root/'base')
            report=validate_tree(tree,root/'base')
            self.assertEqual(report['vector_count'],4)
            self.assertFalse(report['lower_block_contents_verified'])
            placement=dict(format_version=1,tree_sha256=content_hash(tree),placement_epoch=1,owners={'0':dict(owner_id='host0',endpoint='http://a:1'),'1':dict(owner_id='host0',endpoint='http://a:1')})
            validate_placement(placement,tree)
            bad=copy.deepcopy(placement);bad['tree_sha256']='bad'
            with self.assertRaises(ValueError):validate_placement(bad,tree)
            (root/'base/0.u64').write_bytes(struct.pack('<QQ',0,0))
            with self.assertRaisesRegex(ValueError,'digest'):validate_tree(tree,root/'base')
            duplicate=fixture(root/'duplicate',mappings=((0,2),(1,2)))
            with self.assertRaisesRegex(ValueError,'duplicate global'):validate_tree(duplicate,root/'duplicate')
            missing=fixture(root/'missing',mappings=((0,1),(2,7)))
            with self.assertRaises(ValueError):validate_tree(missing,root/'missing')

    def test_assembly_uses_persisted_map_and_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);tree=fixture(root/'base')
            prepared=dict(construction_epoch=tree['construction_epoch'],dimension=2,metric='l2',source_id='src',global_row_count=4,model=tree['model'],upper_root=tree['upper_root'],subtrees=tree['subtrees'])
            path=root/'base/input.json';path.write_bytes(canonical_bytes(prepared))
            actual=assemble_tree(path,[root/'base/0.json',root/'base/1.json'],1,root/'base')
            self.assertEqual(actual,tree)
            prepared['subtrees'][0]['local_to_global']['sha256']='bad'
            path.write_bytes(canonical_bytes(prepared))
            with self.assertRaisesRegex(ValueError,'persisted map'):assemble_tree(path,[root/'base/0.json',root/'base/1.json'],1,root/'base')

    def test_locked_cas_append_and_boundary_immutability(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);one=fixture(root/'one');pointer=root/'published/current.json'
            first=publish(pointer,one,base=root/'one')
            loaded,base=load_published(pointer)
            self.assertEqual(loaded,one);self.assertEqual(base,(root/'one').resolve())
            with self.assertRaises(ValueError):publish(pointer,one,base=root/'one')
            two=fixture(root/'two',2,((0,2,4),(1,3)))
            # The unchanged owner keeps generation 1 and the exact snapshot hash.
            (root/'two/1.json').write_bytes((root/'one/1.json').read_bytes())
            two['subtrees'][1]['generation']=1
            two['subtrees'][1]['snapshot']=artifact(root/'two/1.json',root/'two')
            self.assertEqual(two['subtrees'][1]['snapshot']['sha256'],one['subtrees'][1]['snapshot']['sha256'])
            # Identical frozen centroid bytes can move to another bundle path.
            (root/'two/copied').mkdir()
            (root/'two/copied/model.json').write_bytes((root/'two/model.json').read_bytes())
            two['model']['path']='copied/model.json'
            def contender():
                try:return publish(pointer,two,first['tree_sha256'],root/'two')['tree_generation']
                except ValueError:return 'conflict'
            with concurrent.futures.ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:contender(),range(2)))
            self.assertCountEqual(results,[2,'conflict'])
            current=json.loads(pointer.read_text())
            self.assertEqual(validate_tree(one,root/'one')['vector_count'],4)
            pinned=json.loads((pointer.parent/first['manifest']).read_text())
            self.assertEqual(pinned,one)
            changed=fixture(root/'changed',3,((0,2,4,6),(1,3,5,7)),model=dict(construction_epoch='epoch',dimension=2,metric='l2',centroids=[[-5,0],[5,0]]))
            with self.assertRaisesRegex(ValueError,'immutable'):publish(pointer,changed,current['tree_sha256'],root/'changed')
            reassigned=fixture(root/'reassign',3,((1,2,4,6),(0,3,5,7)))
            with self.assertRaisesRegex(ValueError,'prefix'):publish(pointer,reassigned,current['tree_sha256'],root/'reassign')
            self.assertEqual(json.loads(pointer.read_text()),current)

if __name__=='__main__':unittest.main()
