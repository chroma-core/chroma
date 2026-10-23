import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

s = importlib.util.spec_from_file_location('pair', Path(__file__).with_name('run_pair.py'))
pair = importlib.util.module_from_spec(s)
s.loader.exec_module(pair)

class PairTests(unittest.TestCase):
    def test_policy_changes_only_window_and_compute(self):
        original = {'limits': {'fresh_window_seconds': 1800, 'compute_usd': 2.4192, 'science_seconds': 300}, 'thread_environment': {'RAYON_NUM_THREADS': '32'}}
        expected = json.loads(json.dumps(original))
        expected['limits'].update(fresh_window_seconds=2400, compute_usd=2.688)
        self.assertEqual(pair.paired_policy(original), expected)
        self.assertEqual(original['limits']['fresh_window_seconds'], 1800)
        with self.assertRaises(ValueError):
            pair.paired_policy({'limits': {'fresh_window_seconds': 1200, 'compute_usd': 2.4192}})

    def test_baseline_provenance_and_source_are_separate(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            old, batch, baseline = root/'old', root/'batch', root/'baseline'
            for c, content in [(old, b'baseline'), (batch, b'batched')]:
                (c/'source').mkdir(parents=True)
                (c/'source/member').write_bytes(content)
                (c/'source-manifest.json').write_text(json.dumps({'files': {'member': {'bytes': len(content), 'sha256': pair.digest(c/'source/member')}}, 'symlinks': {}}))
            (old/'build').mkdir()
            (old/'build/native').write_bytes(b'native')
            receipt = {'binary': str(old/'build/native'), 'binary_sha256': pair.digest(old/'build/native'), 'source_manifest_sha256': pair.digest(old/'source-manifest.json'), 'profile': {'opt_level': '3', 'debuginfo': 0}}
            raw = json.dumps(receipt).encode()
            (old/'build/result.json').write_bytes(raw)
            (batch/'operator').mkdir()
            for name in ['supervise.py', 'SPEC-TEMPLATE.json']:
                (batch/name).write_text('{}')
            (batch/'PLAN-TEMPLATE.json').write_text(json.dumps({'limits': {'fresh_window_seconds': 1800, 'compute_usd': 2.4192}}))
            proof, host = root/'proof.json', root/'host.json'
            proof.write_text('{}');host.write_text('{}')
            with patch.multiple(pair, BASE=old, BASE_BINARY_SHA=pair.digest(old/'build/native'), BASE_MANIFEST_SHA=pair.digest(old/'source-manifest.json'), BATCH_MANIFEST_SHA=pair.digest(batch/'source-manifest.json'), BASE_RECEIPT_SHA=pair.digest(old/'build/result.json')):
                pair.prepare(batch, baseline, proof, host, time.time()+2400)
            self.assertEqual(json.loads((baseline/'PLAN-TEMPLATE.json').read_text())['source_archive'], {'sha256': 'bdee8c4ad3ff8ebe683ca02d16b9a57a9f38330eb6441de563802f03918fcfe1', 'bytes': 2364530})
            self.assertEqual((old/'build/result.json').read_bytes(), raw)
            self.assertEqual((baseline/'build/result.json').read_bytes(), raw)
            self.assertEqual((baseline/'source/member').read_bytes(), b'baseline')
            self.assertEqual((batch/'source/member').read_bytes(), b'batched')
            self.assertNotEqual(json.loads((baseline/'PLAN-TEMPLATE.json').read_text())['source_manifest_sha256'], json.loads((batch/'PLAN-TEMPLATE.json').read_text())['source_manifest_sha256'])

    def test_mismatch_and_insufficient_time_fail_before_baseline_creation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            base = root/'base'
            with self.assertRaises(TimeoutError):
                pair.prepare(root/'batch', base, root/'proof', root/'host', time.time()+1000)
            self.assertFalse(base.exists())
            p = root/'member';p.write_bytes(b'x')
            m = root/'manifest';m.write_text('{}')
            with self.assertRaises(ValueError):
                pair.check_source(root, m, '0'*64, time.time()+2400)

if __name__ == '__main__':
    unittest.main()
