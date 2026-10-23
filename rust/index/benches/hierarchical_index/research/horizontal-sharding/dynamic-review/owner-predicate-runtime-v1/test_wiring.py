import importlib.util,json,unittest,subprocess,tempfile
from pathlib import Path
p=Path(__file__).with_name('run-pair.py');s=importlib.util.spec_from_file_location('pair',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_native_policy_work_and_shape(self):
  for arm in ['disabled','enabled']:
   x=json.loads((p.parent/'local-native'/arm/'result.json').read_text());m.shape(x['owner_report']);self.assertEqual(x['spec']['predicate_batching'],arm=='enabled');self.assertEqual(x['owner_report']['predicate_batching'],arm=='enabled');self.assertGreater(x['owner_report']['live_version_requests'],0)
  x=json.loads((p.parent/'local-native/enabled/result.json').read_text())['owner_report']
  for field,value in [('placement_moves',0),('last_reference_subtree_assignments',[[1],[],[],[]]),('peak_distinct_subtree_workers',1),('schedule_sample_overflow',True),('pending_balance',1),('central_current_versions',1)]:
   y=dict(x);y[field]=value
   with self.assertRaises(ValueError):m.shape(y)
 def test_fixed_template(self):
  x=json.loads(p.with_name('runtime-spec.template.json').read_text());self.assertEqual((x['rows'],x['dimension'],x['threads'],x['balance_every'],x['total_seconds']),(8192,768,4,4096,60));self.assertEqual(x['config']['write_navigation'],'fp');self.assertIs(x['config']['fp_npa'],True);self.assertIsNone(x['deadline_unix']);self.assertIsNone(x['binary']['sha256'])
 def test_native_rejects_wrong_type_and_reference_policy_before_input(self):
  spec=json.loads((p.parent/'local-native/disabled-spec.json').read_text());spec['vectors']['path']='/NONEXISTENT-MUST-NOT-OPEN'
  with tempfile.TemporaryDirectory() as td:
   for policy,treatment,expected in [(1,'owner','invalid type'),(True,'reference','predicate policy requires owner treatment')]:
    spec.update(predicate_batching=policy,treatment=treatment);f=Path(td)/'spec.json';f.write_text(json.dumps(spec));r=subprocess.run([str(p.parent/'local-native-binary'),'reference-owner-diagnostic','--spec',str(f)],capture_output=True,timeout=5);self.assertNotEqual(r.returncode,0);self.assertIn(expected,r.stderr.decode());self.assertNotIn('No such file',r.stderr.decode())
if __name__=='__main__':unittest.main()
