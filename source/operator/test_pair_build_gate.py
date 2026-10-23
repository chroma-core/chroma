import json,tempfile,time,unittest,sys
from pathlib import Path
from unittest.mock import patch
import run_pair
class GateTests(unittest.TestCase):
 def test_only_reassignment_diff_and_common_settings(self):
  original={'rows':100000,'dimension':768,'balance_every':4096,'owner_eligibility_prefix':False,'owner_slots':1,'immutable_handle_batching':False,'single_owner_local_scrub':True,'single_owner_local_reassignment':False,'decoded_vector_cache':{'payload_bytes_per_owner':402653184,'aggregate_payload_bytes':402653184},'full_records':{'queue_cache_pages_per_home':4},'threads':32}
  off=run_pair.arm_spec(original,False);on=run_pair.arm_spec(original,True)
  self.assertEqual(off,original);self.assertEqual([k for k in off if off[k]!=on[k]],['single_owner_local_reassignment'])
  self.assertTrue(on['single_owner_local_reassignment']);self.assertFalse(original['single_owner_local_reassignment'])
  for delta in [{'single_owner_local_scrub':False},{'owner_eligibility_prefix':True},{'immutable_handle_batching':True},{'owner_slots':4},{'decoded_vector_cache':None},{'single_owner_local_reassignment':True},{'threads':4},{'rows':1000000}]:
   with self.assertRaises(ValueError):run_pair.arm_spec(dict(original,**delta),True)
 def test_unfilled_source_binding_rejects(self):
  with tempfile.TemporaryDirectory()as d:
   config=Path(d)/'config';config.write_text(json.dumps({'source_manifest_sha256':None,'changed_source_files':None}))
   with patch.object(run_pair,'CONFIG',config):
    with self.assertRaisesRegex(ValueError,'unfilled'):run_pair.configuration_ready()
 def test_stale_build_blocks_both_arms(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d).resolve();b=root/'build';(b/'build').mkdir(parents=True);(b/'build/result.json').write_text(json.dumps({'bench_artifact_fresh':True}));(b/'reassignment-source.json').write_text(json.dumps({'source_manifest_sha256':'f'*64,'changed_source_files':['a']}));host=root/'host';proof=root/'proof';deadline=time.time()+1800
   host.write_text(json.dumps({'deadline_unix':deadline}));proof.write_text(host.read_text())
   class Stage:
    def build(self,*a):pass
   args=['pair','--build-control',str(b),'--off-control',str(root/'off'),'--on-control',str(root/'on'),'--science-parent',str(root/'science'),'--proof',str(proof),'--host',str(host),'--deadline',str(deadline)]
   with patch.object(sys,'argv',args),patch.object(run_pair,'module',return_value=Stage()),patch.object(run_pair,'CONFIG',b/'reassignment-source.json'):
    with self.assertRaisesRegex(ValueError,'freshness'):run_pair.main()
   self.assertFalse((root/'science').exists());self.assertEqual(json.loads((b/'paired-result.json').read_text())['arms'],[])
 def test_exact_reviewed_refresh_and_raw_binary(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);b=root/'build';b.mkdir();native=b/'native';native.write_bytes(b'new');changed=['a','b','c','d'];config=root/'config';config.write_text(json.dumps({'source_manifest_sha256':'expected','changed_source_files':changed,'old_binary_sha256':['old']}));refresh={'changed_source_files':changed};(b/'source-refresh.json').write_text(json.dumps(refresh));receipt={'bench_artifact_fresh':False,'source_manifest_sha256':'expected','binary':str(native),'binary_sha256':run_pair.digest(native),'source_refresh':refresh};(b/'result.json').write_text(json.dumps(receipt));(b/'stdout.jsonl').write_text(json.dumps({'reason':'compiler-artifact','target':{'name':'hierarchical_spann_shard'},'executable':str(native),'fresh':False})+'\n')
   class Stage:
    def validate_artifact(self,values):assert len(values)==1 and values[0]['fresh']is False
   with patch.object(run_pair,'CONFIG',config):
    run_pair.validate_pair_build(root,Stage())
    refresh['changed_source_files']=['a'];receipt['source_refresh']=refresh;(b/'source-refresh.json').write_text(json.dumps(refresh));(b/'result.json').write_text(json.dumps(receipt))
    with self.assertRaisesRegex(ValueError,'frozen-source'):run_pair.validate_pair_build(root,Stage())
if __name__=='__main__':unittest.main()
