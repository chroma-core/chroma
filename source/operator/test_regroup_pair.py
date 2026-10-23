import json,unittest
from pathlib import Path
import run_pair as m
class RegroupPair(unittest.TestCase):
 def base(self):return json.loads(Path(m.__file__).with_name('paired-spec.json').read_text())
 def test_exact_only_flag(self):
  a=m.arm_spec(self.base(),False);b=m.arm_spec(self.base(),True)
  self.assertEqual({k for k in a if a[k]!=b[k]},{'stage_regroup'});self.assertIsNone(a['stage_regroup']);self.assertEqual(b['stage_regroup'],{'max_rows':8192,'delta_working_bytes':134217728})
  self.assertIs(a['prepared_leaf_split'],False);self.assertEqual(a['threads'],32);self.assertEqual(a['balance_every'],4096)
 def test_bad_common_settings(self):
  a=self.base();a['prepared_leaf_split']=True
  with self.assertRaises(ValueError):m.arm_spec(a,True)
 def test_boolean_required(self):
  with self.assertRaises(ValueError):m.arm_spec(self.base(),1)
 def test_ready(self):self.assertEqual(len(m.configuration_ready()['source_manifest_sha256']),64)
if __name__=='__main__':unittest.main()
