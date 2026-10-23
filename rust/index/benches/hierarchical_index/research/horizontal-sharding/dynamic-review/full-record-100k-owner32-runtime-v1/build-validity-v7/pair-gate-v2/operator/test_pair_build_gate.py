import unittest,tempfile,pathlib,json,sys,time
from unittest.mock import patch
import run_pair
class PairGateTests(unittest.TestCase):
 def test_stale_or_absent_freshness_blocks_both_arms_before_parent(self):
  for field in [True,None]:
   with self.subTest(fresh=field),tempfile.TemporaryDirectory()as t:
    root=pathlib.Path(t);batched=root/'batched';(batched/'build').mkdir(parents=True);(batched/'build/result.json').write_text(json.dumps({'bench_artifact_fresh':field}));calls=[]
    class Stage:
     def build(self,*args):calls.append('build')
     def fill_and_run(self,*args):calls.append('science');raise AssertionError('Science forbidden')
    argv=['pair','--batched',str(batched),'--baseline',str(root/'baseline'),'--science-parent',str(root/'science'),'--proof',str(root/'proof'),'--host',str(root/'host'),'--deadline',str(time.time()+2000)]
    with patch.object(sys,'argv',argv),patch.object(run_pair,'prepare'),patch.object(run_pair,'module',return_value=Stage()):
     with self.assertRaisesRegex(ValueError,'freshness invalid'):run_pair.main()
    self.assertEqual(calls,['build']);self.assertFalse((root/'science').exists());self.assertEqual(json.loads((batched/'paired-result.json').read_text())['arms'],[])
 def test_valid_new_build_binds_raw_artifact_and_refresh(self):
  with tempfile.TemporaryDirectory()as t:
   root=pathlib.Path(t);b=root/'build';b.mkdir();native=b/'native';native.write_bytes(b'new-test-binary');refresh={'changed_source_files':['rust/index/benches/hierarchical_index/writer/reference_owner_adapter.rs']};(b/'source-refresh.json').write_text(json.dumps(refresh));(b/'result.json').write_text(json.dumps(dict(bench_artifact_fresh=False,source_manifest_sha256=run_pair.BATCH_MANIFEST_SHA,binary=str(native),binary_sha256=run_pair.digest(native),source_refresh=refresh)));artifact={'reason':'compiler-artifact','target':{'name':'hierarchical_spann_shard'},'executable':str(native),'fresh':False,'profile':{'opt_level':'3','debuginfo':0}};(b/'stdout.jsonl').write_text(json.dumps(artifact)+'\n')
   class Stage:
    def validate_artifact(self,values):
     self.asserted=values;assert len(values)==1 and values[0]['fresh']is False
   stage=Stage();r=run_pair.validate_pair_build(root,stage);self.assertEqual(r['status'],'passed-before-either-science-arm');self.assertTrue((root/'paired-build-validity.json').exists())
if __name__=='__main__':unittest.main()
