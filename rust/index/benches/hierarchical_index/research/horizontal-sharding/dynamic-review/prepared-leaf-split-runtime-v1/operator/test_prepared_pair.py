import json,unittest
from pathlib import Path
import run_pair as m
class PreparedPair(unittest.TestCase):
 def test_exact_only_flag(self):
  base=json.loads(Path(m.__file__).with_name('paired-spec.json').read_text());off=m.arm_spec(base,False);on=m.arm_spec(base,True)
  self.assertEqual({k for k in off if off[k]!=on[k]},{'prepared_leaf_split'});self.assertEqual(off['balance_every'],4096);self.assertEqual(off['split_working_bytes'],68719476736);self.assertEqual(off['threads'],32)
 def test_invalid_common(self):
  base=json.loads(Path(m.__file__).with_name('paired-spec.json').read_text());base['threads']=4
  with self.assertRaises(ValueError):m.arm_spec(base,True)
 def test_boolean_required(self):
  with self.assertRaises(ValueError):m.arm_spec({},1)
 def test_source_contract(self):self.assertEqual(len(m.configuration_ready()['source_manifest_sha256']),64)
if __name__=='__main__':unittest.main()
