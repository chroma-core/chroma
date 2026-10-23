"""Old-key identity and new-object accounting are separate from copy volume."""
import unittest
from .measure_shared_store import store_delta

class StoreAccounting(unittest.TestCase):
    def test_new_uuid_objects_separate_from_sidecars_and_old_head(self):
        before={'data/block/old':dict(bytes=10,sha256='a'),'manifest.json':dict(bytes=5,sha256='b')}
        after=dict(before,**{'data/block/new':dict(bytes=7,sha256='c'),'data/sparse_index/new':dict(bytes=3,sha256='d'),'generation-2/construction-input.f32':dict(bytes=12,sha256='e')})
        delta=store_delta(before,after,True);self.assertEqual(delta['new_uuid_object_bytes'],10);self.assertEqual(delta['new_uuid_object_count'],2);self.assertEqual(delta['modified_old_nonobjects'],{})
    def test_shared_all_old_paths_and_both_modes_old_objects_remain_exact(self):
        before={'data/block/old':dict(bytes=10,sha256='a'),'manifest.json':dict(bytes=5,sha256='b')}
        mutated=dict(before,**{'manifest.json':dict(bytes=5,sha256='other')})
        with self.assertRaises(ValueError):store_delta(before,mutated,True)
        self.assertIn('manifest.json',store_delta(before,mutated,False)['modified_old_nonobjects'])
        for shared in (False,True):
            with self.assertRaises(ValueError):store_delta(before,dict(before,**{'data/block/old':dict(bytes=10,sha256='other')}),shared)
            with self.assertRaises(ValueError):store_delta(before,{'manifest.json':before['manifest.json']},shared)
