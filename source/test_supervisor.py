import importlib.util,pathlib,tempfile,time,sys,json,unittest
p=pathlib.Path(__file__).with_name('supervise.py');s=importlib.util.spec_from_file_location('supervisor',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Guards(unittest.TestCase):
 def test_null_binding_rejected(self):
  with self.assertRaisesRegex(ValueError,'Unfilled execution gate'):m.admit(m.load(p.with_name('PLAN-TEMPLATE.json')))
 def test_expired_creates_no_output(self):
  with tempfile.TemporaryDirectory()as t:
   receipt=pathlib.Path(t)/'receipt'
   with self.assertRaises(TimeoutError):m.run([sys.executable,'-c','pass'],receipt,t,time.time()-1,10**9,10**9,10**6,t)
   self.assertFalse(receipt.exists())
 def test_child_reaped_and_receipt_serialized(self):
  with tempfile.TemporaryDirectory()as t:
   receipt=pathlib.Path(t)/'receipt';r=m.run([sys.executable,'-c','print("ok")'],receipt,pathlib.Path(t)/'output',time.time()+3,10**9,10**9,10**6,t)
   self.assertEqual(r['exit_code'],0);self.assertIsNone(r['guard']);self.assertEqual(m.load(receipt/'process.json')['exit_code'],0)
 def test_deadline_kills_and_reaps(self):
  with tempfile.TemporaryDirectory()as t:
   r=m.run([sys.executable,'-c','import time;time.sleep(10)'],pathlib.Path(t)/'receipt',pathlib.Path(t)/'output',time.time()+.3,10**9,10**9,10**6,t)
   self.assertEqual(r['guard'],'deadline');self.assertLess(r['seconds'],3)
if __name__=='__main__':unittest.main()

class RepairGuards(unittest.TestCase):
 def fixture(self,t):
  p=m.load(pathlib.Path(__file__).with_name('PLAN-TEMPLATE.json'));r=pathlib.Path(t).resolve()
  p.update(data_mount=str(r),data_output_root=str(r/'data'),control_root=str(r/'control'),source_root=str(r/'source'),receipt_dir=str(r/'control'/'run'))
  return p,dict(output=str(r/'data'/'run'),vectors=dict(path=str(r/'retained'/'vectors')))
 def test_paths_and_immutable_policy(self):
  import copy
  with tempfile.TemporaryDirectory()as t:
   p,s=self.fixture(t);self.assertTrue(m.policy_and_paths(p,s))
   for key in ['science_seconds','shutdown_reserve_seconds','rss_kill_bytes']:
    q=copy.deepcopy(p);q['limits'][key]+=1
    with self.assertRaises(ValueError):m.policy_and_paths(q,s)
   q=copy.deepcopy(p);q['thread_environment']['RAYON_NUM_THREADS']='8'
   with self.assertRaises(ValueError):m.policy_and_paths(q,s)
   for output in ['relative',str(pathlib.Path(t).resolve()/'retained'/'run'),p['receipt_dir'],str(pathlib.Path(t).resolve()/'data')]:
    with self.assertRaises(ValueError):m.policy_and_paths(p,dict(s,output=output))
   (pathlib.Path(t)/'data').symlink_to(pathlib.Path(t)/'retained')
   with self.assertRaises(ValueError):m.policy_and_paths(p,s)
 def test_real_term_and_leader_exit_kill_descendant(self):
  import subprocess,os,signal
  for terminate in [False,True]:
   with tempfile.TemporaryDirectory()as t:
    r=pathlib.Path(t);marker=r/'escaped';ready=r/'ready'
    grandchild='import time,pathlib;time.sleep(1);pathlib.Path('+repr(str(marker))+').write_text("escaped")'
    leader='import subprocess,sys,pathlib,time;subprocess.Popen([sys.executable,"-c",'+repr(grandchild)+']);pathlib.Path('+repr(str(ready))+').write_text("ready");'+('time.sleep(10)'if terminate else '')
    wrapper='import importlib.util;z=importlib.util.spec_from_file_location("s",'+repr(str(p))+');m=importlib.util.module_from_spec(z);z.loader.exec_module(m);m.run('+repr([sys.executable,'-c',leader])+','+repr(str(r/'receipt'))+','+repr(str(r/'output'))+',__import__("time").time()+5,10**12,10**12,10**7,'+repr(t)+')'
    proc=subprocess.Popen([sys.executable,'-c',wrapper],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    until=time.monotonic()+3
    while not ready.exists()and proc.poll()is None and time.monotonic()<until:time.sleep(.01)
    self.assertTrue(ready.exists())
    if terminate:proc.send_signal(signal.SIGTERM)
    stdout,stderr=proc.communicate(timeout=5)
    if not terminate:self.assertEqual(proc.returncode,0,stderr)
    time.sleep(1.1);self.assertFalse(marker.exists(),'Descendant escaped owned process group cleanup')
