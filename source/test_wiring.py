import importlib.util,json,unittest,subprocess,tempfile
from pathlib import Path
p=Path(__file__).with_name('run-pair.py');s=importlib.util.spec_from_file_location('pair',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_policy_reports_shape_and_saved_parity(self):
  values=[]
  for arm in ['legacy','batched']:
   x=json.loads((p.parent/'local-native'/arm/'result.json').read_text());r=x['owner_report'];m.shape(r,movement=True);self.assertEqual(r['batched_scrub_validation'],arm=='batched');self.assertFalse(x['spec']['predicate_batching']);self.assertFalse(x['spec']['direct_current_reads']);self.assertFalse(x['spec']['skip_placement']);self.assertEqual(x['spec']['owner_slots'],4)
   self.assertEqual(r['direct_current_reads'],0);self.assertEqual(r['scrub_validation_request_rows']>0,arm=='batched');a=m.validation_accounting(r);self.assertGreaterEqual(a['individual_current_callbacks'],0);values.append(x)
  self.assertEqual(values[0]['all_id_exact_fp_distance_bits'],values[1]['all_id_exact_fp_distance_bits'])
  r=dict(values[1]['owner_report']);r['placement_moves']=0
  with self.assertRaises(ValueError):m.shape(r,movement=True)
 def test_logical_requested_bytes_exclude_missing_directory_rows(self):
  r={'scrub_validation_owner_calls':5,'scrub_validation_rows':130,'scrub_validation_request_rows':129,'message_timing':{'operations':{'current':{'calls':11}}}}
  a=m.validation_accounting(r);self.assertEqual((a['logical_request_id_bytes'],a['logical_response_version_value_upper_bound_bytes'],a['individual_current_callbacks']),(516,129,6))
  r['scrub_validation_request_rows']=131
  with self.assertRaises(ValueError):m.validation_accounting(r)
 def test_budget(self):
  x=json.loads(p.with_name('runtime-spec.template.json').read_text());self.assertEqual((x['rows'],x['threads'],x['owner_slots'],x['arm_seconds'],x['total_seconds']),(8192,4,4,30,60));self.assertIsNone(x['deadline_unix']);self.assertIsNone(x['binary']['sha256'])
 def test_native_invalid_policy_rejects_before_input(self):
  spec=json.loads((p.parent/'local-native/legacy-spec.json').read_text());spec['vectors']['path']='/NONEXISTENT-MUST-NOT-OPEN'
  with tempfile.TemporaryDirectory() as td:
   for policy,treatment,error in [(1,'owner','invalid type'),(True,'reference','batched scrub policy requires owner treatment')]:
    spec.update(batched_scrub_validation=policy,treatment=treatment);f=Path(td)/'spec.json';f.write_text(json.dumps(spec));r=subprocess.run([str(p.parent/'local-native-binary'),'reference-owner-diagnostic','--spec',str(f)],capture_output=True,timeout=5);self.assertNotEqual(r.returncode,0);self.assertIn(error,r.stderr.decode());self.assertNotIn('No such file',r.stderr.decode())
if __name__=='__main__':unittest.main()
