import importlib.util
from pathlib import Path
import tempfile
import unittest
import json
import sys
import time
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE))
loader=importlib.util.spec_from_file_location('one',HERE/'run-lazy-before-scrub.py');m=importlib.util.module_from_spec(loader);loader.loader.exec_module(m)
class Harness(unittest.TestCase):
    def test_fake_single_arm_echo_and_accounting(self):
        for valid in [True,False]:
            with self.subTest(valid=valid),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp).resolve();source=root/'FAKE-sparse.f32'
                with source.open('wb') as f:f.truncate(6144000000)
                vectors=dict(path=str(source),bytes=6144000000,sha256='0'*64);receipt=root/'FAKE-receipt.json';receipt.write_text(json.dumps(dict(status='complete',canonical=vectors,rows=2000000,dimension=768,expected_bytes=6144000000)))
                binary=root/'FAKE-native';binary.write_text('#!'+sys.executable+"\nimport json,sys,hashlib\nfrom pathlib import Path\np=Path(sys.argv[3]);s=json.loads(p.read_text());assert s['profile_before_scrub'] and s['lazy_disabled_evaluation_trace'] and s['lazy_disabled_before_scrub_trace'];o=Path(s['output']);o.mkdir();c=dict(trace_preparations=2,physical_requests=2,physical_rows=10,reference_queries=0,reference_successes=0,batch_validation_calls=2,batch_validated_rows=10,batch_validation_successes="+('2' if valid else '1')+",trace_preparation_ns=100,semantic_scrub_calls=2,semantic_live_rows=9,semantic_scrub_ns=20);v=dict(spec=s,spec_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),census=dict(unique_live_rows=8192,duplicate_current_entries=0,unreachable_nodes=0),cuts=[dict(before_scrub_profile=dict(enabled=True,lazy_trace=True,trace_enabled=False,cumulative=c),trace_attribution=dict(lazy=True,trace_enabled=False,evaluation_events=10,elided_reference_checks=10,retained_reference_checks=0),owner_counters={'0':dict(actor_commands=30)})]);(o/'result.json').write_text(json.dumps(v));print(json.dumps(v))\n");binary.chmod(0o755)
                spec=dict(binary=m.descriptor(binary),restore_receipt=m.descriptor(receipt),vectors=vectors,output=str(root/'output'),rows=8192,dimension=768,balance_every=4096,threads=2,arm_seconds=30,total_seconds=30,deadline_unix=time.time()+30,config=dict(FAKE=True))
                result=m.run(spec);self.assertEqual(result['status'],'passed' if valid else 'failed',result)
                self.assertEqual(len(result['results']),1 if valid else 0)
    def test_expanded_window_rejected(self):
        with self.assertRaises(ValueError):m.run(dict(deadline_unix=time.time()+60,rows=8192,dimension=768,balance_every=4096,threads=2,arm_seconds=30,total_seconds=60))
if __name__=='__main__':unittest.main(verbosity=2)
