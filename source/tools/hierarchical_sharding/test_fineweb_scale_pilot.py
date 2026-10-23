import json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from .fineweb_scale_pilot import pilot
from .tree_manifest import artifact
class PilotTests(unittest.TestCase):
 def fixture(self,root):
  plan=root/'plan';plan.write_text(json.dumps(dict(plan=dict(files=[dict(row_count=n,parquet_rows=n) for n in (1090356,1066143,30720,1094506)]))))
  manifest=root/'manifest';manifest.write_text('{}');job=root/'job';job.write_text(json.dumps(dict(owned_root=str(root),source_plan=artifact(plan),source_manifest=artifact(manifest),stop_epoch=time.time()+21600,stage_estimates_s={'remaining':18710})));return job
 def test_two_file_baseline_precedes_four_and_canonical_mismatch_fails(self):
  for mismatch in (False,True):
   with tempfile.TemporaryDirectory() as d:
    root=Path(d);job=self.fixture(root);calls=[]
    def ingest(*args,**kwargs):
     calls.append(kwargs['workers']);return dict(chunks=[dict(start_id=0,row_count=3281725,vectors=dict(bytes=10,sha256='different' if mismatch and kwargs['workers']==4 else 'same',path='attempt-'+str(kwargs['workers'])),uuids=dict(bytes=20,sha256='uuids',path='other-attempt'))],source_request_bytes_charged=100)
    with patch('tools.hierarchical_sharding.fineweb_scale_pilot.validate_scale_job'),patch('tools.hierarchical_sharding.fineweb_scale_pilot.ingest',side_effect=ingest):
     if mismatch:
      with self.assertRaisesRegex(ValueError,'canonical'):pilot(job)
     else:self.assertFalse(pilot(job)['configuration_change_admitted'])
    self.assertEqual(calls,[2,4])
 def test_missing_read_gate_prevents_both_source_arms(self):
  with tempfile.TemporaryDirectory() as d:
   job=self.fixture(Path(d))
   with patch('tools.hierarchical_sharding.fineweb_scale_pilot.validate_scale_job',side_effect=ValueError('gate')),patch('tools.hierarchical_sharding.fineweb_scale_pilot.ingest') as read:
    with self.assertRaises(ValueError):pilot(job)
    read.assert_not_called()
if __name__=='__main__':unittest.main()
