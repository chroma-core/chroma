"""Real module admission with metadata-only local fixtures; no fake helper exports."""
import ast,copy,importlib.util,json,pathlib,struct,tempfile,unittest
from harness_common import descriptor
P=pathlib.Path(__file__).with_name('run-quality-code4.py');S=importlib.util.spec_from_file_location('repair',P);m=importlib.util.module_from_spec(S);S.loader.exec_module(m)
class Admission(unittest.TestCase):
 def test_actual_modules_and_three_arm_snapshot_admission(self):
  tree=ast.parse(P.read_text());references={n.attr for n in ast.walk(tree) if isinstance(n,ast.Attribute) and isinstance(n.value,ast.Name) and n.value.id=='previous'}
  self.assertEqual(references,{'CACHE','cell','child','input_snapshot','inventory','load_split','verify_package','write_final_result'})
  for name in references:
   value=getattr(m.previous,name)
   self.assertTrue(isinstance(value,dict) if name=='CACHE' else callable(value),name)
  root=pathlib.Path(__file__).parents[3];fleet=root/'fleet/matched-pruning-16k-v1/runtime-evidence/runtime-attempt1'
  with tempfile.TemporaryDirectory() as td:
   tmp=pathlib.Path(td);out=tmp/'output';out.mkdir();spec={'snapshots':{}}
   # These are copies of retained manifests and synthetic dense ID bytes only.
   # No vector/block payload is created, and this test makes no native-census claim.
   for arm in m.ARMS:
    store=tmp/arm;store.mkdir();manifest=json.loads((fleet/arm/'store/manifest.json').read_text());mp=store/manifest['id_mapping']['path'];mp.write_bytes(struct.pack('<16384Q',*range(16384)));path=store/'manifest.json';path.write_text(json.dumps(manifest));spec['snapshots'][arm]=dict(manifest=descriptor(path),mapping=descriptor(mp),roots=manifest['block_ids'],census=manifest['integrity_audit'])
   saved=copy.deepcopy(spec);snapshots,censuses,before=m.admit_snapshots(spec,out,lambda cap,end:cap)
   self.assertEqual(set(snapshots),set(m.ARMS));self.assertEqual(set(censuses),set(m.ARMS));self.assertEqual(json.loads((out/'snapshot-before.json').read_text()),before)
   for arm in m.ARMS:
    self.assertEqual(snapshots[arm]['vector_count'],16384);self.assertEqual(set(before[arm]['files']),{'manifest.json','id-map-1.u64le'});self.assertEqual(descriptor(saved['snapshots'][arm]['manifest']['path']),saved['snapshots'][arm]['manifest'])
   bad=tmp/'bad-output';bad.mkdir();mp=pathlib.Path(saved['snapshots']['dynamic1']['mapping']['path']);data=bytearray(mp.read_bytes());data[0]^=1;mp.write_bytes(data)
   with self.assertRaisesRegex(ValueError,'descriptor mismatch'):m.admit_snapshots(saved,bad,lambda cap,end:cap)
   self.assertFalse((bad/'snapshot-before.json').exists())
if __name__=='__main__':unittest.main()
