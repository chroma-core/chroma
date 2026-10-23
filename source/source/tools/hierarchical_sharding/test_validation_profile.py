"""Counterexamples precede any incremental-admission shortcut."""
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from .validation_profile import ValidationIO,profile_full_record

class CounterTests(unittest.TestCase):
    def test_counts_actual_returned_bytes_not_file_size(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'data';p.write_bytes(b'abcdef')
            with ValidationIO() as io:
                with p.open('rb') as f:self.assertEqual(f.read(2),b'ab');self.assertEqual(f.read(),b'cdef')
                self.assertEqual(p.read_bytes(),b'abcdef')
            self.assertEqual(io.report()['total_read_bytes'],12);self.assertEqual(io.report()['total_opens'],2)

@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native fixture builder required')
class RealAudit(unittest.TestCase):
    def test_full_audit_cost_and_same_size_mutation_invalidates_prior_observation(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .dynamic_deployment import validate_record
        from .tree_manifest import checked_artifact
        with evidence_case('validation-invalidation') as root:
            parent,_,_,_=make_parent(root,os.environ['SPANN_DYNAMIC_BINARY'],page_rows=4,trie_fanout=4)
            first=profile_full_record(parent);second=profile_full_record(parent)
            self.assertGreater(first['validation_io']['total_read_bytes'],parent['canonical_vectors']['bytes'])
            self.assertGreater(first['shared_object_durability_io']['total_fsyncs'],0)
            vectors=Path(parent['canonical_vectors']['path']);before=vectors.stat();raw=vectors.read_bytes();vectors.write_bytes(struct.pack('<f',123.)+raw[4:]);os.utime(vectors,ns=(before.st_atime_ns,before.st_mtime_ns))
            self.assertEqual(vectors.stat().st_size,before.st_size);self.assertEqual(vectors.stat().st_mtime_ns,before.st_mtime_ns)
            with self.assertRaises(ValueError):checked_artifact(parent['canonical_vectors'],'.')
            with self.assertRaises(ValueError):validate_record(parent)
            (root/'cost-and-invalidation.json').write_text(json.dumps(dict(first=first,second=second,same_size_and_mtime=True,prior_audit_does_not_authorize_reuse=True,after_mutation='full oracle rejects'),indent=2)+'\n')
