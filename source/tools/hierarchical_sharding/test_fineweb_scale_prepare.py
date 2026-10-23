import copy,json,tempfile,unittest
from pathlib import Path
import numpy as np
from .fineweb_stream import prepare
from .fineweb_parallel import ingest as real_ingest
from .fineweb_scale_prepare import prepare as scale_prepare
from .tree_manifest import content_hash
from unittest.mock import patch
import time
from .fineweb_scale_prepare import audit_membership,MINIMUM_STAGE_ESTIMATES
from .test_fineweb_stream import fixture
from .tree_manifest import artifact,checked_artifact
class FullMembershipTests(unittest.TestCase):
 def test_complete_verified_workflow_on_bounded_fixture_without_network(self):
  values,manifest,opener=fixture()
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);source=root/'manifest';source.write_text(json.dumps(manifest));plan=root/'plan';plan.write_text(json.dumps(dict(plan=dict(status='passed',count=21,manifest_sha256=content_hash(manifest)))))
   pools={}
   for pool in ('calibration','test'):
    path=root/(pool+'.jsonl');path.write_text(''.join(json.dumps(dict(query_id=pool+str(i),vector=values[0].tolist()))+'\n' for i in range(100)));pools[pool]=dict(artifact(path),count=100)
   job=dict(owned_root=str(root),reports=str(root/'reports'),source_manifest=artifact(source),source_manifest_sha256=content_hash(manifest),source_plan=artifact(plan),dataset=manifest['dataset'],revision=manifest['revision'],count=21,metric='ip',stop_epoch=time.time()+21600,query_pools=pools,stage_estimates_s=dict(MINIMUM_STAGE_ESTIMATES),resources=dict(minimum_start_free_allocated_bytes=0,minimum_free_allocated_bytes=0,max_new_logical_output_bytes=1024**3),ingest=dict(output=str(root/'ingest'),workers=2,chunk_rows=5,batch_rows=4,retries=0,max_source_bytes=1024**3,max_output_bytes=1024**3,max_wall_s=1200),spatial=dict(output=str(root/'spatial'),children=3,sample_size=12,seed=1729,iterations=3,batch_rows=4,construction_epoch='bounded-fixture'))
   path=root/'job';path.write_text(json.dumps(job))
   def actual_ingest(*args,**kwargs):return real_ingest(*args,**kwargs,open_source=opener)
   # Production admission is separately tested; the complete data/audit/reference
   # workflow runs here on an in-memory Parquet fixture instead of100M rows.
   with patch('tools.hierarchical_sharding.fineweb_scale_prepare.validate_scale_job'),patch('tools.hierarchical_sharding.fineweb_scale_prepare.ingest',side_effect=actual_ingest),patch('builtins.print'):
    report=scale_prepare(path)
   self.assertEqual(report['status'],'passed');self.assertTrue(any(s['stage']=='independent-full100m-reference-check' for s in report['stages']))
   self.assertEqual(len((root/'reports/attempt-0000/calibration.exact.jsonl').read_text().splitlines()),100)

 def test_complete_membership_and_original_value_audit_catches_rehashed_corruption(self):
  _,manifest,opener=fixture()
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)/'prepared';p=prepare(manifest,root,21,3,'epoch','source',chunk_rows=5,batch_rows=4,sample_size=12,open_source=opener);model=json.loads((root/'model.json').read_text())
   audit_membership(p['source']['chunks'],p['subtrees'],root,model,21)
   owner=next(x for x in p['subtrees'] if x['vector_count']);path=checked_artifact(owner['vectors'],root);old=path.read_bytes();x=np.frombuffer(old,dtype='<f4').copy();x[0]+=1;path.write_bytes(x.tobytes());owner['vectors']=artifact(path,root)
   with self.assertRaises(AssertionError):audit_membership(p['source']['chunks'],p['subtrees'],root,model,21)
   path.write_bytes(old);owner['vectors']=artifact(path,root)
   missing=copy.deepcopy(p['subtrees']);missing.remove(next(x for x in missing if x['vector_count']))
   with self.assertRaises(AssertionError):audit_membership(p['source']['chunks'],missing,root,model,21)
if __name__=='__main__':unittest.main()
