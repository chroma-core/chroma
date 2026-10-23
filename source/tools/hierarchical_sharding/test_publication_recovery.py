import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from .publication_recovery import load_completed,recover
from .subtree_guarded_build import jobs_for

class RecoveryTests(unittest.TestCase):
 def fixture(self,root):
  old=root/'failed';(old/'build/host-batch').mkdir(parents=True)
  prepared=dict(start_id=0,input_row_count=1,global_row_count=1,source_id='s',dimension=1,metric='ip',subtrees=[dict(subtree_id='a',local_start=0,append_row_count=1,vector_count=1,routing_boundary_hash='b')])
  config=dict(entries={'a':dict(host='h',index='/owned/index',vectors='/owned/x',id_map='/owned/map')},measured_binary='/measured',binary='/binary',cpu_affinity='0',jobs_per_host=1,checkpoint_rows=10000,host_order=['h'])
  jobs,_=jobs_for(prepared,config);entry=dict(host='h',run_id='run',job_shard_ids=['a']);terminal=dict(run_id='run',status='passed',reconciled=True,guard_report=dict(status='passed',outcomes=[dict(index=0,status='ok',returncode=0)]))
  data={'config.json':config,'jobs.json':jobs,'build/host-batch/config.json':dict(hosts=[entry]),'build/host-batch/report.json':dict(complete=True,hosts=dict(h=terminal))}
  for path,value in data.items():(old/path).write_text(json.dumps(value))
  (old/'journal.jsonl').write_text(json.dumps(dict(phase='failed'))+'\n');source=root/'prepared.json';source.write_text(json.dumps(prepared));return old,source,prepared
 def test_rejects_unreconciled_and_already_published_original(self):
  with tempfile.TemporaryDirectory() as d:
   old,_,prepared=self.fixture(Path(d));p=old/'build/host-batch/report.json';saved=p.read_text();r=json.loads(saved);r['hosts']['h']['reconciled']=False;p.write_text(json.dumps(r))
   with self.assertRaisesRegex(ValueError,'reconciled'):load_completed(old,prepared)
   p.write_text(saved);(old/'current.json').write_text('{}')
   with self.assertRaisesRegex(ValueError,'already published'):load_completed(old,prepared)
 def test_read_only_recovery_and_failure_publication_gates(self):
  for failure in (None,'warning','reopen','terminal'):
   with self.subTest(failure=failure),tempfile.TemporaryDirectory() as d:
    root=Path(d);old,source,_=self.fixture(root);before={str(p.relative_to(old)):p.read_bytes() for p in old.rglob('*') if p.is_file()}
    def collect(jobs,batch,output,mask,concurrency,runner):
     self.assertTrue(runner()['complete']);output.mkdir();(output/'host-0-read-terminal.json').write_text(json.dumps(dict(runner()['hosts']['h'],run_id='different') if failure=='terminal' else runner()['hosts']['h']));(output/'0.raw-log-metadata.json').write_text(json.dumps({k:dict(classification_occurrences=dict(balance_nonconvergence=int(failure=='warning'))) for k in ('stdout','stderr')}));return dict(complete=True)
    with patch('tools.hierarchical_sharding.publication_recovery.run_guarded_jobs',side_effect=collect),patch('tools.hierarchical_sharding.publication_recovery.inspect_parallel',side_effect=ValueError('reopen') if failure=='reopen' else None,return_value=[]),patch('tools.hierarchical_sharding.publication_recovery.assemble_tree',return_value={}),patch('tools.hierarchical_sharding.publication_recovery.publish_checked',return_value={}) as publication:
     if failure:
      with self.assertRaises(ValueError):recover(old,source,root/'new')
      publication.assert_not_called()
     else:
      result=recover(old,source,root/'new');self.assertFalse(result['writer_rerun']);publication.assert_called_once()
    self.assertEqual(before,{str(p.relative_to(old)):p.read_bytes() for p in old.rglob('*') if p.is_file()})
if __name__=='__main__':unittest.main()
