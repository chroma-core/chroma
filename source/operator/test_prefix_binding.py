import hashlib
from pathlib import Path
import tempfile
import time
import unittest

from remote_stages import bind_input


class PrefixBindingTests(unittest.TestCase):
    def test_prefix_is_derived_from_hash_bound_full_object(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'vectors'
            data = bytes(range(64))
            path.write_bytes(data)
            spec = {'rows': 2, 'dimension': 3, 'vectors': {'path': str(path), 'bytes': 64,
                    'sha256': hashlib.sha256(data).hexdigest()}}
            result = bind_input(spec, time.time() + 1000)
            self.assertEqual(result['prefix_bytes'], 24)
            self.assertEqual(result['prefix_sha256'], hashlib.sha256(data[:24]).hexdigest())
            path.write_bytes(data[:-1] + b'x')
            with self.assertRaises(ValueError):
                bind_input(spec, time.time() + 1000)

    def test_deadline_reserve_rejects_before_binding(self):
        with self.assertRaises(TimeoutError):
            bind_input({'rows': 100000, 'dimension': 768, 'vectors': {'path': __file__}}, time.time())
