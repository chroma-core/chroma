import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('project', Path(__file__).with_name('project_rounds.py'))
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)

class ProjectionTest(unittest.TestCase):
    def test_variable_rounds_delayed_owner_and_prefixes(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp); labels = [0, 0, 0, 1, 1, 0, 2, 2, 1]
            (p/'vectors').write_bytes(struct.pack('<9f', *range(9)))
            (p/'labels').write_bytes(struct.pack('<9I', *labels))
            r = module.project(p/'vectors', p/'labels', p/'out', 1, 3, 3)
            expected = {owner: [] for owner in range(3)}
            for job in r['jobs']:
                owner = job['owner']; start = job['global_round_start']
                new = [i for i in range(start, min(start+3, 9)) if labels[i] == owner]
                self.assertEqual(job['prior_rows'], len(expected[owner]))
                self.assertEqual(job['resume'], bool(expected[owner]))
                expected[owner] += new
                raw = Path(job['id_map']).read_bytes()
                self.assertEqual(list(struct.unpack('<'+'Q'*(len(raw)//8), raw)), expected[owner])
                self.assertEqual(list(struct.unpack('<'+'f'*len(new), Path(job['vectors']).read_bytes())), new)
                self.assertEqual(job['checkpoint_rows'], len(new))
            self.assertEqual(sorted(i for ids in expected.values() for i in ids), list(range(9)))
            self.assertEqual([j['generation'] for j in r['jobs'] if j['owner']==2], [3])
    def test_invalid_owner_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp); (p/'v').write_bytes(struct.pack('<f', 1)); (p/'l').write_bytes(struct.pack('<I', 2))
            with self.assertRaises(ValueError): module.project(p/'v', p/'l', p/'o', 1, 1, 1)

if __name__ == '__main__': unittest.main()
