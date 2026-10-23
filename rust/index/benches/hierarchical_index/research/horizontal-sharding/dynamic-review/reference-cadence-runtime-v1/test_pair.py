import importlib.util,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
s=importlib.util.spec_from_file_location('cadence',Path(__file__).with_name('run_pair.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class CadenceTests(unittest.TestCase):
 def setUp(self):self.original=json.loads(Path(__file__).with_name('retained-spec.json').read_text())
 def test_only_cadence_diff_with_identical_split_admission(self):
  short=m.science_spec(self.original,4096);long=m.science_spec(self.original,100000)
  self.assertEqual([k for k in short if short[k]!=long[k]],['balance_every'])
  self.assertEqual(short['split_rows'],100000);self.assertEqual(long['split_working_bytes'],68719476736)
  self.assertEqual(short['staging_rows'],8192);self.assertEqual(short['staging_working_bytes'],134217728)
  self.assertEqual(self.original['split_rows'],8192)
 def test_unreviewed_settings_reject(self):
  for delta in [{'threads':4},{'single_owner_local_reassignment':True},{'decoded_vector_cache':None},{'split_rows':100000},{'balance_every':100000}]:
   with self.assertRaises(ValueError):m.science_spec(dict(self.original,**delta),4096)
  with self.assertRaises(ValueError):m.science_spec(self.original,8192)
 def test_pending_readiness_rejects(self):
  with tempfile.TemporaryDirectory()as d:
   pending=Path(d)/'pending.json';pending.write_text(json.dumps({'accepted':False}))
   with patch.object(m,'READINESS',pending):
    with self.assertRaisesRegex(ValueError,'not admitted'):m.readiness()
 def test_runtime_hash_mismatch_rejects_before_copy(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);(root/'marker').write_text('changed')
   with patch.object(m,'RUNTIME',root),patch.object(m,'RUNTIME_HASHES',{'marker':'0'*64}):
    with self.assertRaisesRegex(ValueError,'runtime differs'):m.verify_retained(9999999999)
if __name__=='__main__':unittest.main()
