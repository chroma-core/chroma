import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from .fineweb_scale_pipeline import run
class PipelineTests(unittest.TestCase):
 def test_preparation_or_archive_failure_never_produces_passed_handoff(self):
  for failure in ('prepare','archive'):
   with self.subTest(failure=failure),tempfile.TemporaryDirectory() as d:
    root=Path(d);job=root/'job';job.write_text(json.dumps(dict(owned_root=str(root))))
    with patch('tools.hierarchical_sharding.fineweb_scale_pipeline.prepare',side_effect=ValueError('bad membership') if failure=='prepare' else None,return_value=dict(status='passed')),patch('tools.hierarchical_sharding.fineweb_scale_pipeline.archive_and_restore',side_effect=ValueError('restore mismatch')) as archive:
     with self.assertRaises(ValueError):run(job,store_factory=lambda _:object())
     if failure=='prepare':archive.assert_not_called()
    self.assertEqual(json.loads((root/'pipeline.json').read_text())['status'],'failed')
    with self.assertRaises(FileExistsError):run(job,store_factory=lambda _:object())
if __name__=='__main__':unittest.main()
