import importlib.util,json,unittest
from pathlib import Path
p=Path(__file__).with_name('run-pair.py');s=importlib.util.spec_from_file_location('pair',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_three_native_reports(self):
  values={a:json.loads((p.parent/'local-native'/a/'result.json').read_text()) for a in ['reference','owner1','owner4']}
  common=None
  for arm,v in values.items():
   self.assertFalse(v['spec']['predicate_batching']);self.assertTrue(v['spec']['construction_observation'])
   self.assertEqual(v['all_id_exact_fp_distance_bits'],values['reference']['all_id_exact_fp_distance_bits'])
   for phase in v['phases']:
    before=phase['counts_before'];add=phase['counts_after_add'];balance=phase['counts_after_balance'];self.assertGreater(balance['current_version_requests'],0)
    for k in before:self.assertGreaterEqual(add[k],before[k]);self.assertGreaterEqual(balance[k],add[k])
   if arm!='reference':
    report=v['owner_report'];m.shape(report,movement=arm=='owner4');self.assertEqual(report['message_timing']['queued'],0)
    self.assertEqual(set(report['message_timing']['operations']),{'other','current','version_mutation','posting','predicate','registration'})
    self.assertGreater(report['message_timing']['operations']['predicate']['calls'],0)
  self.assertEqual(sum(x['placement_moves'] for x in values['owner1']['phases']),0)
 def test_movement_and_region_negative_gates(self):
  report=json.loads((p.parent/'local-native/owner1/result.json').read_text())['owner_report']
  with self.assertRaises(ValueError):m.shape(report,movement=True)
  for k,v in [('placement_moves',1),('last_reference_subtree_assignments',[[1],[]]),('peak_distinct_subtree_workers',1),('schedule_sample_overflow',True)]:
   x=dict(report);x[k]=v
   with self.assertRaises(ValueError):m.shape(x,movement=False)
 def test_fixed_runtime_budget(self):
  x=json.loads(p.with_name('runtime-spec.template.json').read_text());self.assertEqual((x['rows'],x['dimension'],x['threads'],x['balance_every'],x['arm_seconds'],x['total_seconds']),(8192,768,4,4096,30,90));self.assertIsNone(x['deadline_unix']);self.assertIsNone(x['binary']['sha256'])
if __name__=='__main__':unittest.main()
