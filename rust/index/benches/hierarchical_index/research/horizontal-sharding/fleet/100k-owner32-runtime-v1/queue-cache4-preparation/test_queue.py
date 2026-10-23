import importlib.util,json
from pathlib import Path
import unittest
s=importlib.util.spec_from_file_location('queue_run',Path(__file__).with_name('run_queue.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class QueueTests(unittest.TestCase):
 def test_only_one_scientific_field_changes(self):
  original={'rows':100000,'threads':32,'owner_slots':1,'output':'old','full_records':{'queue_cache_pages_per_home':1,'other':16}}
  expected=json.loads(json.dumps(original));expected['full_records']['queue_cache_pages_per_home']=4
  self.assertEqual(m.science_spec(original),expected);self.assertEqual(original['full_records']['queue_cache_pages_per_home'],1)
  with self.assertRaises(ValueError):m.science_spec(expected)
 def test_only_window_compute_policy_changes(self):
  original={'limits':{'fresh_window_seconds':2400,'compute_usd':2.688,'science_seconds':300,'shutdown_reserve_seconds':180,'remaining_collection_grace_seconds':120},'source_manifest_sha256':m.MANIFEST_SHA}
  expected=json.loads(json.dumps(original));expected['limits'].update(fresh_window_seconds=900,compute_usd=1.008)
  self.assertEqual(m.operational_plan(original),expected)
  with self.assertRaises(ValueError):m.operational_plan(expected)
if __name__=='__main__':unittest.main()
