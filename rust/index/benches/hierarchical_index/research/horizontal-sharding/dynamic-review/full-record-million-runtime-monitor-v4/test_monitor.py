import unittest,tempfile,pathlib,sys,time,subprocess,json,importlib.util,os
spec=importlib.util.spec_from_file_location('monitor',pathlib.Path(__file__).with_name('supervise.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class MonitorTests(unittest.TestCase):
 def run_case(self,root,seconds=2,**kw):
  return m.run([sys.executable,'-c','import time;time.sleep('+str(seconds)+')'],root/'receipt',root/'output',time.time()+kw.pop('deadline',3),10**12,10**9,10**7,root,**kw)
 def test_blocked_scan_cannot_block_deadline_and_cleanup(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t);scans=[]
   def factory(paths):
    scan=m.OutputScan(paths,command=[sys.executable,'-c','import time;time.sleep(20)']);scans.append(scan);return scan
   r=self.run_case(root,deadline=.2,scan_timeout=.3,scan_factory=factory)
   self.assertEqual(r['guard'],'deadline');self.assertLess(r['seconds'],2)
   self.assertFalse(r['terminal_accounting_complete']);self.assertTrue(all(s.child.poll()is not None and s.closed for s in scans))
 def test_scan_timeout_fails_closed(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t)
   def factory(paths):return m.OutputScan(paths,command=[sys.executable,'-c','import time;time.sleep(20)'])
   r=self.run_case(root,scan_timeout=.1,scan_factory=factory)
   self.assertEqual(r['guard'],'output accounting timeout');self.assertNotEqual(r['exit_code'],0)
 def test_completion_relative_no_overlap_and_terminal_bytes(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t);output=root/'output';output.mkdir();(output/'data').write_bytes(b'x'*123);scans=[]
   def factory(paths):
    self.assertTrue(all(s.closed for s in scans),'overlapping scanners')
    cmd=[sys.executable,'-c','import time,json;time.sleep(.12);print(json.dumps('+repr([123] if len(paths)==1 else [123,0])+'))']
    scan=m.OutputScan(paths,cmd);scans.append(scan);return scan
   r=self.run_case(root,seconds=.8,scan_interval=.15,scan_timeout=1,scan_factory=factory)
   self.assertIsNone(r['guard']);self.assertTrue(r['terminal_accounting_complete']);self.assertEqual(r['output_bytes'],123)
   live=scans[:-1];self.assertGreaterEqual(len(live),2)
   for first,second in zip(live,live[1:]):self.assertGreaterEqual(second.started-first.started,.27)
   self.assertTrue(all(s.closed for s in scans))
 def test_real_terminal_size_and_output_cap(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t);output=root/'output';output.mkdir();(output/'data').write_bytes(b'x'*123)
   r=m.run([sys.executable,'-c','pass'],root/'receipt',output,time.time()+3,10**12,100,10**7,root)
   self.assertIn(r['guard'],['output','terminal output']);self.assertTrue(r['terminal_accounting_complete']);self.assertEqual(r['output_bytes'],123)
 def test_real_term_cleans_scanner_group(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t);pidfile=root/'scanner.pid'
   script='import importlib.util,pathlib,sys,time;z=importlib.util.spec_from_file_location("m",'+repr(str(pathlib.Path(m.__file__).resolve()))+');m=importlib.util.module_from_spec(z);z.loader.exec_module(m)\ndef factory(paths):\n s=m.OutputScan(paths,[sys.executable,"-c","import time;time.sleep(20)"]);pathlib.Path('+repr(str(pidfile))+').write_text(str(s.child.pid));return s\nm.run([sys.executable,"-c","import time;time.sleep(20)"],'+repr(str(root/'receipt'))+','+repr(str(root/'output'))+',time.time()+10,10**12,10**9,10**7,'+repr(t)+',scan_factory=factory)'
   child=subprocess.Popen([sys.executable,'-c',script],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
   try:
    until=time.monotonic()+3
    while not pidfile.exists()and time.monotonic()<until:time.sleep(.01)
    self.assertTrue(pidfile.exists());pid=int(pidfile.read_text());child.terminate();child.wait(timeout=3)
    with self.assertRaises(ProcessLookupError):os.kill(pid,0)
   finally:
    if child.poll()is None:child.kill();child.wait()
if __name__=='__main__':unittest.main()
