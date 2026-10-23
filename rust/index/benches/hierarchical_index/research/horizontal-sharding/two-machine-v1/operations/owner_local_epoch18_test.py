"""Check the fixed candidate admission and the observed terminal-process race."""
import argparse,importlib.util,json,time,unittest
from pathlib import Path
from unittest.mock import patch,Mock
P=Path(__file__).resolve().parent
def load(name,file):
 s=importlib.util.spec_from_file_location(name,P/file);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
j=load('job15','remote-job-epoch15.py');g=load('spec15','make-500k-spec-epoch18.py')
class Tests(unittest.TestCase):
 def test_specs(self):
  now=int(time.time());read=Path.read_text;read_bytes=Path.read_bytes
  def fixture(p,*a,**kw):
   if p.name=='runtime-binaries-epoch18.json':return json.dumps({'two_machine_spann':'a'*64,'two_machine_reader':'b'*64})
   return read(p,*a,**kw)
  with patch.object(Path,'read_text',fixture),patch.object(Path,'read_bytes',lambda p:fixture(p).encode() if p.name=='runtime-binaries-epoch18.json' else read_bytes(p)):
   for mode in ('run','serve'):
    v=g.generate(argparse.Namespace(arm='500k-distributed',mode=mode,window_id='epoch18-test',base=g.BASE,job_deadline=now+590,window_deadline=now+1800,worker_ip='172.31.65.187',peer_ip='172.31.73.6'),now)
    self.assertNotIn('profile_gate',v);self.assertEqual(v['args']['--rows'],500000);self.assertEqual(v['args']['--workers'],32);self.assertEqual(v['args']['--owner-budget'],500000);self.assertEqual(v['args']['--input-descriptor'],'/home/ubuntu/spann-two-machine-v1/epoch14/input/500k.json')
    self.assertEqual(v['binary_sha256'],'a'*64);self.assertIn('/epoch18/build/',v['binary'])
 def test_missing_binaries_fail_closed(self):
  if (P/'runtime-binaries-epoch18.json').exists():self.skipTest('Build bindings already supplied')
  now=int(time.time())
  with self.assertRaises(FileNotFoundError):g.generate(argparse.Namespace(arm='500k-distributed',mode='run',window_id='epoch18-test',base=g.BASE,job_deadline=now+590,window_deadline=now+1800),now)
 def group_fixture(self,states,pgid=7):
  stat=Mock();stat.read_text.side_effect=['(worker) '+s+' '+' '.join(['0']*18)+' 123' for s in states]
  env=Mock();env.read_bytes.side_effect=PermissionError('exit race')
  entry=Mock();entry.name='123';entry.__truediv__=Mock(side_effect=lambda key:stat if key=='stat' else env)
  proc=Mock();proc.iterdir.return_value=[entry]
  return patch.object(j,'Path',return_value=proc),patch.object(j.os,'getpgid',return_value=pgid),env
 def test_terminal_environ_race_skips(self):
  p,o,_=self.group_fixture(['S','Z'])
  with p,o:self.assertEqual(j.owned_group(7,'test'),[])
 def test_unreadable_live_group_fails_closed(self):
  p,o,_=self.group_fixture(['S','S'])
  with p,o,self.assertRaises(PermissionError):j.owned_group(7,'test')
 def test_unrelated_group_never_reads_environment(self):
  p,o,env=self.group_fixture([],8)
  with p,o:self.assertEqual(j.owned_group(7,'test'),[])
  env.read_bytes.assert_not_called()
if __name__=='__main__':unittest.main()
