import importlib.util,json,unittest
from pathlib import Path
p=Path(__file__).with_name('run-pair.py');s=importlib.util.spec_from_file_location('pair',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_native_report_shape_and_negative_gates(self):
  x=json.loads((p.parent.parent/'reference-parallel-owner-adapter-v1/local-native/owner/result.json').read_text())['owner_report'];m.shape(x)
  for field,value in [('placement_moves',0),('last_reference_subtree_assignments',[[1],[],[],[]]),('peak_distinct_subtree_workers',1),('schedule_sample_overflow',True),('pending_balance',1),('central_current_versions',1)]:
   y=dict(x);y[field]=value
   with self.assertRaises(ValueError):m.shape(y)
 def test_fixed_pair_template(self):
  x=json.loads(p.with_name('runtime-spec.template.json').read_text());self.assertEqual((x['rows'],x['dimension'],x['threads'],x['balance_every'],x['total_seconds']),(8192,768,4,4096,60));self.assertEqual(x['config']['write_navigation'],'fp');self.assertIs(x['config']['fp_npa'],True);self.assertIsNone(x['deadline_unix']);self.assertIsNone(x['binary']['sha256'])
if __name__=='__main__':unittest.main()
