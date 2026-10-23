import tempfile
import unittest
from pathlib import Path
import numpy as np
from .benchmark import exact_shard
from .exact_blocked import exact_blocked


class BlockedExactTests(unittest.TestCase):
    def test_matches_independent_oracle_all_metrics_and_ties(self):
        rng = np.random.default_rng(42)
        x = rng.normal(size=(39, 7)).astype('<f4')
        x[12] = x[0]
        queries = [dict(query_id=str(i), vector=v.tolist()) for i, v in enumerate(x[:5])]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'x.f32'
            x.tofile(path)
            for metric in ('l2', 'ip', 'cosine'):
                oracle = exact_shard(path, queries, 7, 10, metric, 3, 4)
                actual = exact_blocked(path, queries, 7, 10, metric, 3, 4, 8, 2)
                for a, b in zip(oracle, actual):
                    self.assertEqual(a['ids'], b['ids'])
                    np.testing.assert_allclose(a['distances'], b['distances'], atol=1e-13)

    def test_rejects_invalid_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'x.f32'
            np.array([[0., 0.]], dtype='<f4').tofile(path)
            with self.assertRaises(ValueError):
                exact_blocked(path, [dict(vector=[1., 0.], query_id='q')], 2, metric='cosine')
            path.write_bytes(b'123')
            with self.assertRaises(ValueError):
                exact_blocked(path, [], 2)

if __name__ == '__main__':
    unittest.main()
