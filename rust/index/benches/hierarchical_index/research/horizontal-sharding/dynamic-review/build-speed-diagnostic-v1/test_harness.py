"""No real corpus/native/cloud; check malformed admission stops before output."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import time
import json
import sys
HERE=Path(__file__).resolve().parent
loader=importlib.util.spec_from_file_location('tiny_harness',HERE/'run-diagnostic.py');m=importlib.util.module_from_spec(loader);loader.loader.exec_module(m)
class Harness(unittest.TestCase):
    def test_budget_and_extent_fail_before_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=dict(deadline_unix=time.time()+120,threads=2,rows=8192,dimension=768,balance_every=4096,arm_seconds=30,total_seconds=120,output=str(Path(tmp)/'output'))
            for key,value in [('deadline_unix',0),('threads',True),('rows',2000000),('total_seconds',121),('arm_seconds',31)]:
                with self.subTest(key=key),self.assertRaises(ValueError):m.run(dict(base,**{key:value}))
            self.assertFalse(Path(base['output']).exists())
    def test_actual_descriptor_hash_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'native';f.write_bytes(b'not executed')
            good=m.descriptor(f);self.assertEqual(m.verify(good),f.resolve())
            with self.assertRaises(ValueError):m.verify(dict(good,sha256='0'*64))
    def test_fake_subprocess_success_and_failed_cell_stops_sequence(self):
        for failure in [False,True]:
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp).resolve();source=root/'FAKE-sparse-extent.f32'
                with source.open('wb') as f:f.truncate(6144000000)
                binary=root/'FAKE-native';binary.write_text('#!'+sys.executable+"\nimport json,sys\nfrom pathlib import Path\ns=json.loads(Path(sys.argv[3]).read_text())\n"+("raise SystemExit(7)\n" if failure else "p=Path(s['output']);p.mkdir();v=dict(status='passed',spec=s,spec_sha256=__import__('hashlib').sha256(Path(sys.argv[3]).read_bytes()).hexdigest(),census=dict(unique_live_rows=8192,duplicate_current_entries=0));(p/'result.json').write_text(json.dumps(v));print(json.dumps(v))\n"));binary.chmod(0o755)
                vectors=dict(path=str(source),bytes=6144000000,sha256='0'*64)
                receipt=root/'FAKE-receipt.json';receipt.write_text(json.dumps(dict(status='complete',canonical=vectors,rows=2000000,dimension=768,expected_bytes=6144000000)))
                spec=dict(binary=m.descriptor(binary),restore_receipt=m.descriptor(receipt),vectors=vectors,output=str(root/'output'),deadline_unix=time.time()+120,threads=2,rows=8192,dimension=768,balance_every=4096,arm_seconds=30,total_seconds=120,config=dict(FAKE=True))
                result=m.run(spec)
                self.assertEqual(result['status'],'failed' if failure else 'passed')
                self.assertEqual(len(result['results']),0 if failure else 5)
                if failure:self.assertFalse((root/'output/parallel-serial-spec.json').exists())
if __name__=='__main__':unittest.main(verbosity=2)
