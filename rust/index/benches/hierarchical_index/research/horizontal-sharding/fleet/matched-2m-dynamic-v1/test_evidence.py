import importlib.util
from pathlib import Path
import tempfile
import time
import json
import hashlib
import unittest
import tarfile

spec = importlib.util.spec_from_file_location('collector', Path(__file__).with_name('collect-evidence.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class EvidenceTests(unittest.TestCase):
    def test_small_results_and_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'result.json').write_text('{}')
            records = module.collect({'root':tmp,'deadline_unix':time.time()+30,'files':['result.json','missing.json']},root/'out.tar')
            self.assertEqual(records[1]['status'], 'missing')
            with tarfile.open(root/'out.tar') as archive:
                self.assertEqual(archive.getnames(), ['result.json','collection-receipt.json'])

    def test_reject_store_escape_and_large_file(self):
        for name in ['store/manifest.json','../escape.json','vectors.f32','large.json']:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)
                with (root/'large.json').open('wb') as stream:
                    stream.truncate(module.MAX_FILE+1)
                with self.assertRaises(ValueError):
                    module.collect({'root':tmp,'deadline_unix':time.time()+30,'files':[name]},root/'out.tar')

    def test_exact_metadata_exceptions_and_archive_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            names = sorted(module.METADATA_EXCEPTIONS)
            for name in names:
                path = root/name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{}')
            out = root/'out.tar'
            module.collect({'root':tmp,'deadline_unix':time.time()+30,'files':names},out)
            receipt = json.loads((root/'out.tar.receipt.json').read_text())
            self.assertEqual(receipt['archive']['bytes'], out.stat().st_size)
            self.assertEqual(receipt['archive']['sha256'], hashlib.sha256(out.read_bytes()).hexdigest())
            self.assertEqual(receipt['missing'], [])

    def test_expired(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TimeoutError):
                module.collect({'root':tmp,'deadline_unix':0,'files':['result.json']},Path(tmp)/'out.tar')

if __name__ == '__main__':
    unittest.main()
