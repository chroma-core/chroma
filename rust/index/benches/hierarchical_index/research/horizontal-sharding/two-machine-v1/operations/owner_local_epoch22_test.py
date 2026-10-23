"""Validate the retained-binary3M pair without starting processes or cloud resources."""
import argparse,importlib.util,json,time,unittest
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent
s=importlib.util.spec_from_file_location('spec21',P/'make-3m-spec-epoch22.py');g=importlib.util.module_from_spec(s);s.loader.exec_module(g)
class Tests(unittest.TestCase):
 def args(self,arm='3m-distributed',mode='run'):
  now=int(time.time());return now,argparse.Namespace(arm=arm,mode=mode,window_id='epoch22-test',base=g.BASE,job_deadline=now+590,window_deadline=now+1800,worker_ip='172.31.65.187',peer_ip='172.31.73.6')
 def registry(self,values):
  raw=Path.read_text;binary=Path.read_bytes
  return patch.object(Path,'read_text',lambda p,*a,**kw:json.dumps(values) if p.name=='runtime-binaries-epoch22.json' else raw(p,*a,**kw)),patch.object(Path,'read_bytes',lambda p:json.dumps(values).encode() if p.name=='runtime-binaries-epoch22.json' else binary(p))
 def test_three_spec_roles(self):
  p,b=self.registry({'two_machine_spann':'a'*64,'two_machine_reader':'b'*64})
  with p,b:
   for arm,mode in [('3m-distributed','run'),('3m-distributed','serve')]:
    now,a=self.args(arm,mode);v=g.generate(a,now);d=v['args'];self.assertEqual(d['--rows'],3000000);self.assertEqual(d['--workers'],32);self.assertNotIn('profile_gate',v);self.assertEqual(v['reader_sha256'],{'two_machine_spann':'a'*64,'two_machine_reader':'b'*64}['two_machine_reader']);self.assertEqual(v['workload_timeout_seconds'],600)
    if arm=='3m-reference':self.assertEqual(d['--input'],g.INPUT);self.assertEqual(v['minimum_free_bytes'],12*1024**3);self.assertEqual(d['--initial-batch-rows'],100000);self.assertEqual(d['--later-batch-rows'],1000000)
    else:self.assertEqual(d['--input-descriptor'],g.DESCRIPTOR);self.assertEqual(d['--owner-budget'],2160000);self.assertEqual(d['--first-owner-budget'],0);self.assertEqual(d['--initial-batch'],100000);self.assertEqual(d['--later-batch'],1000000)
 def test_missing_registry(self):
  if (P/'runtime-binaries-epoch22.json').exists():self.skipTest('Actual verified registry exists')
  now,a=self.args()
  with self.assertRaises(FileNotFoundError):g.generate(a,now)
 def test_changed_binary_rejected(self):
  changed=dict({'two_machine_spann':'a'*64,'two_machine_reader':'b'*64});changed['two_machine_reader']='invalid';p,b=self.registry(changed)
  with p,b,self.assertRaises(AssertionError):now,a=self.args();g.generate(a,now)
 def test_reference_serve_rejected(self):
  now,a=self.args('3m-reference','serve')
  with self.assertRaises(AssertionError):g.generate(a,now)
 def test_job_deadline_bound(self):
  now,a=self.args();a.job_deadline=now+591
  with self.assertRaises(AssertionError):g.generate(a,now)
if __name__=='__main__':unittest.main()
