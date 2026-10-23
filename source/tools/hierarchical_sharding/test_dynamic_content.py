"""Bounded normalization and execution-provenance digest checks."""
from pathlib import Path
import struct
import tempfile
import unittest
from .dynamic_content import normalized_row,mapped_digest

class ContentTests(unittest.TestCase):
    def test_non_axis_cosine_normalization_has_known_fp32_rounding(self):
        raw=struct.pack('<ff',3.,4.)
        self.assertEqual(normalized_row(raw,2,'cosine'),struct.pack('<II',0x3f19999a,0x3f4ccccd))
        self.assertEqual(normalized_row(raw,2,'ip'),raw)
        with self.assertRaises(ValueError):normalized_row(struct.pack('<ff',0.,0.),2,'cosine')
    def test_digest_rejects_changed_values_and_preserves_global_id_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);canonical=root/'canonical';subset=root/'subset';mapping=root/'map'
            canonical.write_bytes(struct.pack('<ffff',3.,4.,5.,12.));subset.write_bytes(struct.pack('<ffff',5.,12.,3.,4.));mapping.write_bytes(struct.pack('<QQ',1,0))
            self.assertEqual(mapped_digest(subset,mapping,2,'cosine'),mapped_digest(canonical,mapping,2,'cosine',canonical=True))
            canonical.write_bytes(struct.pack('<ffff',3.,5.,5.,12.))
            self.assertNotEqual(mapped_digest(subset,mapping,2,'cosine'),mapped_digest(canonical,mapping,2,'cosine',canonical=True))

    def test_producer_pin_covers_all_executed_admission_modules(self):
        from unittest.mock import patch
        from .dynamic_runtime import producer_bundle_sha256
        seen=[]
        def hashed(path):
            seen.append(Path(path).name)
            return '0'*64
        with patch('tools.hierarchical_sharding.dynamic_runtime.file_hash',side_effect=hashed):producer_bundle_sha256()
        self.assertEqual(set(seen),{'dynamic_runtime.py','dynamic_content.py','tree_manifest.py','subtree_transport.py'})
