import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
from .prepare_real import prepare, REVISION


class PrepareTests(unittest.TestCase):
    def test_pinned_download_disjoint_queries_hashes_normalization(self):
        vectors = [[float(i+1)]+[1.]*1023 for i in range(8)]
        class Column:
            def to_pylist(self):
                return vectors
        class Batch:
            def column(self, index):
                return Column()
        class Parquet:
            def __init__(self, path):
                pass
            def iter_batches(self, **kwargs):
                self.kwargs = kwargs
                return [Batch()]
        parquet = types.ModuleType('pyarrow.parquet')
        parquet.ParquetFile = Parquet
        arrow = types.ModuleType('pyarrow')
        arrow.parquet = parquet
        urls = []
        def download(url, path):
            urls.append(url)
            Path(path).write_bytes(b'fixture')
        with tempfile.TemporaryDirectory() as tmp, patch.dict('sys.modules', {'pyarrow': arrow, 'pyarrow.parquet': parquet}), patch('urllib.request.urlretrieve', download):
            out = Path(tmp)/'run'
            meta = prepare(out, 4, 2, 2, True)
            self.assertIn(REVISION, urls[0])
            x = np.fromfile(out/'corpus.f32', dtype='<f4').reshape(4, 1024)
            np.testing.assert_allclose(np.linalg.norm(x, axis=1), 1, atol=1e-6)
            q = [json.loads(line) for name in ('queries.calibration.jsonl', 'queries.test.jsonl') for line in (out/name).read_text().splitlines()]
            self.assertEqual({r['source_row'] for r in q}, {4, 5, 6, 7})
            self.assertEqual(meta['outputs']['corpus.f32']['bytes'], 4*1024*4)
            with self.assertRaises(ValueError):
                prepare(out, 4, 2, 2)

if __name__ == '__main__':
    unittest.main()
