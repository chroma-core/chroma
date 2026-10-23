import importlib.util,json,unittest,subprocess,tempfile
from pathlib import Path
p=Path(__file__).with_name('run-pair.py');s=importlib.util.spec_from_file_location('pair',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Wiring(unittest.TestCase):
 def test_actual_policy_reports_and_gates(self):
  values=[]
  for arm in ['message','direct']:
   x=json.loads((p.parent/'local-native'/arm/'result.json').read_text());r=x['owner_report'];m.shape(r,movement=False);self.assertEqual(r['direct_current_policy'],arm=='direct');self.assertFalse(x['spec']['predicate_batching']);self.assertTrue(x['spec']['skip_placement']);self.assertEqual(x['spec']['owner_slots'],1)
   self.assertEqual(r['message_timing']['operations']['current']['calls']>0,arm=='message');self.assertEqual(r['direct_current_reads']>0,arm=='direct');values.append(x)
  self.assertEqual(values[0]['all_id_exact_fp_distance_bits'],values[1]['all_id_exact_fp_distance_bits'])
  r=dict(values[1]['owner_report']);r['placement_moves']=1
  with self.assertRaises(ValueError):m.shape(r,movement=False)
 def test_budget(self):
  x=json.loads(p.with_name('runtime-spec.template.json').read_text());self.assertEqual((x['rows'],x['threads'],x['owner_slots'],x['arm_seconds'],x['total_seconds']),(8192,4,1,30,60));self.assertIsNone(x['deadline_unix']);self.assertIsNone(x['binary']['sha256'])
 def test_native_rejects_invalid_policy_before_input(self):
  spec=json.loads((p.parent/'local-native/message-spec.json').read_text());spec['vectors']['path']='/NONEXISTENT-MUST-NOT-OPEN'
  with tempfile.TemporaryDirectory() as td:
   for policy,treatment,error in [(1,'owner','invalid type'),(True,'reference','direct current policy requires owner treatment')]:
    spec.update(direct_current_reads=policy,treatment=treatment);f=Path(td)/'spec.json';f.write_text(json.dumps(spec));r=subprocess.run([str(p.parent/'local-native-binary'),'reference-owner-diagnostic','--spec',str(f)],capture_output=True,timeout=5);self.assertNotEqual(r.returncode,0);self.assertIn(error,r.stderr.decode());self.assertNotIn('No such file',r.stderr.decode())
if __name__=='__main__':unittest.main()
