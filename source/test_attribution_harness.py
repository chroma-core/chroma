import importlib.util
from pathlib import Path
import tempfile
import unittest
import json
import sys
import time
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE))
loader=importlib.util.spec_from_file_location('pair',HERE/'run-attribution.py');m=importlib.util.module_from_spec(loader);loader.loader.exec_module(m)
class Harness(unittest.TestCase):
    def test_fake_pair_policy_binding_command_accounting_and_store_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();source=root/'FAKE-sparse.f32'
            with source.open('wb') as f:f.truncate(6144000000)
            vectors=dict(path=str(source),bytes=6144000000,sha256='0'*64)
            receipt=root/'FAKE-receipt.json';receipt.write_text(json.dumps(dict(status='complete',canonical=vectors,rows=2000000,dimension=768,expected_bytes=6144000000)))
            binary=root/'FAKE-native';binary.write_text('#!'+sys.executable+"\nimport json,sys,hashlib\nfrom pathlib import Path\np=Path(sys.argv[3]);s=json.loads(p.read_text());o=Path(s['output']);o.mkdir();(o/'store').mkdir();f=(o/'store'/'FAKE-sparse-object');f.open('wb').truncate(30*1024**2);lazy=s['lazy_disabled_evaluation_trace'];v=dict(spec=s,spec_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),census=dict(unique_live_rows=8192,duplicate_current_entries=0,unreachable_nodes=0),cuts=[dict(trace_attribution=dict(lazy=lazy,trace_enabled=False,evaluation_events=10,elided_reference_checks=10 if lazy else 0,retained_reference_checks=0 if lazy else 10),owner_counters={'0':dict(actor_commands=20 if lazy else 30)})]);(o/'result.json').write_text(json.dumps(v));print(json.dumps(v))\n");binary.chmod(0o755)
            spec=dict(binary=m.descriptor(binary),restore_receipt=m.descriptor(receipt),vectors=vectors,output=str(root/'output'),rows=8192,dimension=768,balance_every=4096,threads=2,arm_seconds=30,total_seconds=60,deadline_unix=time.time()+60,config=dict(FAKE=True))
            result=m.run(spec);self.assertEqual(result['status'],'passed',result);self.assertEqual(result['retained_store_bytes'],60*1024**2);self.assertLess(result['metadata_bytes'],50*1024**2);self.assertEqual([r['policy'] for r in result['results']],['baseline','lazy'])
    def test_wrong_window_rejected_before_children(self):
        for total in [61,120]:
            with self.assertRaises(ValueError):m.run(dict(deadline_unix=time.time()+60,rows=8192,dimension=768,balance_every=4096,threads=2,arm_seconds=30,total_seconds=total))
if __name__=='__main__':unittest.main(verbosity=2)
