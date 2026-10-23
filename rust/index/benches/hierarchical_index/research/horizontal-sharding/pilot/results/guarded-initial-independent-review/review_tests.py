"""Independent publication-boundary checks; run with the worktree on PYTHONPATH."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from tools.hierarchical_sharding.test_subtree_guarded_build import GuardedInitialBuildTests
from tools.hierarchical_sharding.subtree_guarded_build import construct,jobs_for
from tools.hierarchical_sharding.build import command
class ReviewTests(unittest.TestCase):
 def test_real_argv_keeps_periodic_checkpoint_and_final_generation(self):
  f=GuardedInitialBuildTests();jobs,_=jobs_for(f.prepared(),f.config());argv=command(dict(jobs[0],host='local'))
  for flag,value in [('--checkpoint-rows','10000'),('--generation','1'),('--row-count','23001')]:self.assertEqual(argv[argv.index(flag)+1],value)
 def failure(self,stage):
  f=GuardedInitialBuildTests()
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);p=f.prepared();p['subtrees'][0].update(vectors={'bytes':4,'sha256':'v'},local_to_global={'bytes':8,'sha256':'m'});c=f.config();b=d/'batch';b.write_text('{}');c.update(binary='/binary',binary_sha256='b',measured_sha256='m',guarded_batch_config=str(b),host_order=['worker']);pp=d/'input';pp.write_text(json.dumps(p));cp=d/'config';cp.write_text(json.dumps(c))
   with patch('tools.hierarchical_sharding.subtree_guarded_build.ssh',return_value='[]'),patch('tools.hierarchical_sharding.subtree_guarded_build.run_guarded_jobs',return_value={'complete':True}),patch('tools.hierarchical_sharding.subtree_guarded_build.inspect_parallel',side_effect=RuntimeError('bad reopen') if stage=='inspect' else None,return_value=[]),patch('tools.hierarchical_sharding.subtree_guarded_build.assemble_tree',side_effect=ValueError('bad coverage')),patch('tools.hierarchical_sharding.subtree_guarded_build.publish_checked') as pub:
    with self.assertRaises((RuntimeError,ValueError)):construct(pp,cp,d/'case')
    pub.assert_not_called()
   self.assertFalse((d/'case/current.json').exists())
 def test_failed_reopen_blocks_cas(self):self.failure('inspect')
 def test_failed_coverage_blocks_cas(self):self.failure('coverage')
if __name__=='__main__':unittest.main()
