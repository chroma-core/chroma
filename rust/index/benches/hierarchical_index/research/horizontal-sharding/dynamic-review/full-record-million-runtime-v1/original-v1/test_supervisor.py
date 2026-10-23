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
