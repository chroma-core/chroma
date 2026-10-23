import unittest,tempfile,pathlib,hashlib,os,json
from build_validity_benchmark import refresh,require_recompiled
class ValidityTests(unittest.TestCase):
 def manifest(self,root):return dict(files={p.relative_to(root).as_posix():dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in root.rglob('*')if p.is_file()},symlinks={})
 def test_changed_inputs_and_entry_refreshed_without_byte_change(self):
  with tempfile.TemporaryDirectory()as t:
   r=pathlib.Path(t);(r/'benches').mkdir();(r/'benches/main.rs').write_text('entry');(r/'benches/value.rs').write_text('old');baseline=self.manifest(r);(r/'benches/value.rs').write_text('new');current=self.manifest(r)
   for p in r.rglob('*.rs'):os.utime(p,ns=(1700000000000000000,1700000000000000000))
   result=refresh(r,current,baseline,entry='benches/main.rs',prefix='benches/')
   self.assertEqual(result['changed_source_files'],['benches/value.rs']);self.assertEqual(self.manifest(r),current);self.assertTrue(all(row['after_mtime_ns']>row['before_mtime_ns']for row in result['refreshed']))
 def test_tamper_and_outside_scope_reject_before_touch(self):
  with tempfile.TemporaryDirectory()as t:
   r=pathlib.Path(t);(r/'benches').mkdir();entry=r/'benches/main.rs';entry.write_text('entry');other=r/'lib.rs';other.write_text('old');baseline=self.manifest(r);other.write_text('new');current=self.manifest(r);before=entry.stat().st_mtime_ns
   with self.assertRaisesRegex(ValueError,'outside benchmark'):refresh(r,current,baseline,entry='benches/main.rs',prefix='benches/')
   self.assertEqual(entry.stat().st_mtime_ns,before);other.write_text('old');current=self.manifest(r);entry.write_text('tampered')
   with self.assertRaisesRegex(ValueError,'Source bytes differ'):refresh(r,current,baseline,entry='benches/main.rs',prefix='benches/')
 def test_fresh_or_absent_artifact_rejects(self):
  for v in [{},{'fresh':True},{'fresh':None}]:
   with self.assertRaises(ValueError):require_recompiled(v)
  v={'fresh':False};self.assertIs(require_recompiled(v),v)
if __name__=='__main__':unittest.main()
