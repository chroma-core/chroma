import json
from pathlib import Path
import tempfile
import unittest
import numpy as np
from .spatial_prepare import prepare, distances
from .tree_manifest import checked_artifact


class SpatialPrepareTests(unittest.TestCase):
    def test_frozen_append_and_original_global_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            x=np.array([[-3,0],[3,0],[-2,1],[2,1],[-4,-1],[4,-1]],dtype='<f4')
            x.tofile(root/'base.f32')
            a=prepare(root/'base.f32',root/'a',2,2,'epoch','l2','source',sample_size=6)
            ownership={}
            for subtree in a['subtrees']:
                ids=np.fromfile(checked_artifact(subtree['local_to_global'],root/'a'),dtype='<u8')
                vectors=np.fromfile(checked_artifact(subtree['vectors'],root/'a'),dtype='<f4').reshape(-1,2)
                np.testing.assert_array_equal(vectors,x[ids])
                for ident in ids:ownership[int(ident)]=subtree['subtree_id']
            self.assertEqual(set(ownership),set(range(6)))
            self.assertEqual(ownership[0],ownership[2])
            self.assertNotEqual(ownership[0],ownership[1])
            y=np.array([[5,2],[-5,2]],dtype='<f4');y.tofile(root/'append.f32')
            b=prepare(root/'append.f32',root/'b',2,2,'epoch','l2','source',previous=root/'a/input.json',start_id=6)
            self.assertEqual(a['model']['sha256'],b['model']['sha256'])
            mapped=[]
            for old,new in zip(a['subtrees'],b['subtrees']):
                before=checked_artifact(old['local_to_global'],root/'a').read_bytes()
                after=checked_artifact(new['local_to_global'],root/'b').read_bytes()
                self.assertTrue(after.startswith(before)); mapped.extend(np.frombuffer(after,dtype='<u8').tolist())
                self.assertEqual(new['local_start'],old['vector_count'])
            self.assertEqual(sorted(mapped),list(range(8)))
            (root/'changed-model.json').write_text('{}')
            with self.assertRaises(ValueError):prepare(root/'append.f32',root/'bad',2,2,'epoch','l2','source',previous=root/'a/input.json',model_path=root/'changed-model.json',start_id=6)
            with self.assertRaises(ValueError):prepare(root/'append.f32',root/'bad2',2,2,'epoch','l2','source',previous=root/'a/input.json',start_id=5)

    def test_reproducible_model_and_cosine_routing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);np.array([[2,0],[3,1],[-2,0],[-3,-1]],dtype='<f4').tofile(root/'x')
            a=prepare(root/'x',root/'a',2,2,'epoch','cosine','src',seed=42,sample_size=4)
            b=prepare(root/'x',root/'b',2,2,'epoch','cosine','src',seed=42,sample_size=4)
            self.assertEqual(a['model']['sha256'],b['model']['sha256'])
            np.testing.assert_allclose(distances([[1,0]],[[2,0],[-2,0]],'cosine'),[[0,2]])
            with self.assertRaises(ValueError):distances([[0,0]],[[1,0]],'cosine')

if __name__=='__main__':unittest.main()
