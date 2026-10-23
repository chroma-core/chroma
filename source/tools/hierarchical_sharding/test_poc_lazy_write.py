"""Actual copy-byte accounting; complete writer semantics use the native fixture."""
import struct
import tempfile
import unittest
from pathlib import Path
from .run_poc_lazy_write import baseline_copies

class EagerCopyAccounting(unittest.TestCase):
    def test_two_canonical_materializations_and_owner_copies_are_counted(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);old=root/'old';grown=root/'grown'
            old.write_bytes(struct.pack('<ff',1,2));grown.write_bytes(struct.pack('<ffff',1,2,3,4))
            owners={}
            for sid,size in [('a',5),('b',7)]:
                path=root/sid;path.mkdir();(path/'value').write_bytes(b'x'*size);owners[sid]=dict(checkpoint_dir=str(path))
            parent=dict(tree=dict(dimension=1,source=dict(row_count=2)),canonical_vectors=dict(path=str(old)),owners=owners)
            result=baseline_copies(parent,grown,root/'baseline')
            self.assertEqual(result['canonical_materialization_bytes'],32)
            self.assertEqual(result['append_file_bytes'],8)
            self.assertEqual(result['canonical_returned_bytes'],40)
            self.assertEqual(result['owners'],dict(a=dict(copied_bytes=5,copied_files=1),b=dict(copied_bytes=7,copied_files=1)))
            self.assertEqual((root/'baseline/private-writers/canonical.f32').read_bytes(),grown.read_bytes())
            self.assertEqual(old.read_bytes(),struct.pack('<ff',1,2))
