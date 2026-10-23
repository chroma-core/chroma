import tempfile
import unittest
from pathlib import Path
import numpy as np
from .build import partition
from .prepare_subset import prepare


class SubsetTests(unittest.TestCase):
    def test_prefix_append_ids_and_dense_reconstruction(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); x=np.arange(60,dtype='<f4').reshape(20,3); x.tofile(root/'x.f32')
            partition(root/'x.f32',root/'parent',3,2,'test')
            result=prepare(root/'parent',root/'subset',10,4)
            np.testing.assert_array_equal(np.fromfile(root/'subset/base.f32',dtype='<f4').reshape(-1,3),x[:10])
            np.testing.assert_array_equal(np.fromfile(root/'subset/append.f32',dtype='<f4').reshape(-1,3),x[10:14])
            np.testing.assert_array_equal(np.fromfile(root/'subset/append/shard-1.f32',dtype='<f4').reshape(-1,3),x[11:14:2])
            self.assertEqual([o['global_id_start'] for o in result['outputs']], [0,1,10,11])
            with self.assertRaises(ValueError): prepare(root/'parent',root/'bad',11,4)

if __name__ == '__main__': unittest.main()
