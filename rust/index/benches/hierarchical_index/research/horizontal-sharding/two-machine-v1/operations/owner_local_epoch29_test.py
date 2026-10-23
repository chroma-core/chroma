import argparse,importlib.util,json,time,unittest
from pathlib import Path
from unittest.mock import patch
P=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('generator',P/'make-3m-spec-epoch29.py');g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)
class Tests(unittest.TestCase):
 def generate(self,mode='run'):
  now=int(time.time());args=argparse.Namespace(arm='3m-distributed',mode=mode,window_id='epoch29-test',base=g.BASE,job_deadline=now+1190,window_deadline=now+2700,worker_ip='172.31.65.187',peer_ip='172.31.73.6');return g.generate(args,now),now
 def setUp(self):
  raw=Path.read_text;data=Path.read_bytes;values={'two_machine_spann':'a'*64,'two_machine_reader':'b'*64}
  self.a=patch.object(Path,'read_text',lambda p,*a,**k:json.dumps(values) if p.name=='runtime-binaries-epoch29.json' else raw(p,*a,**k));self.b=patch.object(Path,'read_bytes',lambda p:json.dumps(values).encode() if p.name=='runtime-binaries-epoch29.json' else data(p));self.a.start();self.b.start()
 def tearDown(self):self.a.stop();self.b.stop()
 def test_roles_and_repeat_flag(self):
  for mode in ['run','serve']:
   value,now=self.generate(mode);cmd,_=g.remote.validate(value,now);self.assertEqual(value['args']['--rows'],3000000);self.assertEqual(value['args']['--workers'],32);self.assertEqual('--save-unchanged-generation' in cmd,mode=='run')
   if mode=='run':i=cmd.index('--save-unchanged-generation');self.assertTrue(i+1==len(cmd) or cmd[i+1].startswith('--'))
 def test_missing_config_hash_rejected(self):
  value,now=self.generate();del value['storage_config_sha256']
  with self.assertRaises(ValueError):g.remote.validate(value,now)
 def test_unbound_path_rejected(self):
  value,now=self.generate();value['args']['--storage-config']='/tmp/elsewhere'
  with self.assertRaises(ValueError):g.remote.validate(value,now)
 def test_peer_repeat_rejected(self):
  value,now=self.generate('serve');value['args']['--save-unchanged-generation']=1
  with self.assertRaises(ValueError):g.remote.validate(value,now)
 def test_window_bound(self):
  value,now=self.generate();value['deadline_unix']=value['window_deadline_unix']
  with self.assertRaises(ValueError):g.remote.validate(value,now)
if __name__=='__main__':unittest.main()
