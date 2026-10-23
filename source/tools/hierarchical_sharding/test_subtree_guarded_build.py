import copy,hashlib,tempfile,unittest,json
from unittest.mock import patch
from pathlib import Path
from .subtree_guarded_build import jobs_for,remote_preflight,construct

class GuardedInitialBuildTests(unittest.TestCase):
 def prepared(self):
  return dict(start_id=0,input_row_count=23001,global_row_count=23001,source_id='public-prefix',dimension=3,metric='ip',subtrees=[dict(subtree_id='a',local_start=0,append_row_count=23001,vector_count=23001,routing_boundary_hash='frozen')])
 def config(self):
  return dict(entries={'a':dict(host='worker',index='/owned/index/a',vectors='/input/a.f32',id_map='/input/a.u64le')},measured_binary='/worker',cpu_affinity='0-3',jobs_per_host=4,checkpoint_rows=10000)
 def test_periodic_flushes_preserve_final_snapshot_identity(self):
  jobs,entries=jobs_for(self.prepared(),self.config());j=jobs[0]
  self.assertEqual((j['row_count'],j['checkpoint_rows'],j['generation']),(23001,10000,1));self.assertEqual(entries['a']['generation'],1);self.assertFalse(j.get('resume',False));self.assertEqual(j['boundary_hash'],'frozen')
 def test_initial_build_rejects_append_or_missing_coverage(self):
  for field,value in (('start_id',1),('global_row_count',23002)):
   p=self.prepared();p[field]=value
   with self.assertRaises(ValueError):jobs_for(p,self.config())
  p=self.prepared();p['subtrees'][0]['local_start']=1
  with self.assertRaises(ValueError):jobs_for(p,self.config())
 def test_preflight_rejects_existing_output_and_wrong_map(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);binary=d/'binary';binary.write_bytes(b'compiled');vectors=d/'vectors';vectors.write_bytes(b'1234');mapping=d/'map';mapping.write_bytes(b'12345678');index=d/'index'
   desc=lambda p:dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
   row=dict(index=str(index),vectors=str(vectors),id_map=str(mapping),subtree_id='a',vectors_artifact=desc(vectors),id_map_artifact=desc(mapping));sha=desc(binary)['sha256']
   self.assertEqual(remote_preflight([row],binary,sha,binary,sha)[0]['input_bytes'],12)
   mapping.write_bytes(b'87654321')
   with self.assertRaisesRegex(ValueError,'id_map'):remote_preflight([row],binary,sha,binary,sha)
   mapping.write_bytes(b'12345678');index.mkdir()
   with self.assertRaisesRegex(ValueError,'fresh absent'):remote_preflight([row],binary,sha,binary,sha)
 def test_preflight_rejects_different_binary(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'binary';p.write_bytes(b'other')
   with self.assertRaisesRegex(ValueError,'binary'):remote_preflight([],p,'0'*64,p,'0'*64)

 def test_failed_host_batch_cannot_verify_or_publish(self):
  with tempfile.TemporaryDirectory() as d:
   d=Path(d);p=self.prepared();p['subtrees'][0].update(vectors={'bytes':4,'sha256':'v'},local_to_global={'bytes':8,'sha256':'m'});c=self.config();batch=d/'batch.json';batch.write_text('{}');c.update(binary='/binary',binary_sha256='b',measured_sha256='w',guarded_batch_config=str(batch),host_order=['worker']);prepared=d/'input.json';prepared.write_text(json.dumps(p));config=d/'config.json';config.write_text(json.dumps(c));case=d/'case'
   with patch('tools.hierarchical_sharding.subtree_guarded_build.ssh',return_value='[]'),patch('tools.hierarchical_sharding.subtree_guarded_build.run_guarded_jobs',return_value={'complete':False}),patch('tools.hierarchical_sharding.subtree_guarded_build.inspect_parallel') as inspect,patch('tools.hierarchical_sharding.subtree_guarded_build.publish_checked') as publish:
    with self.assertRaisesRegex(RuntimeError,'did not reconcile'):construct(prepared,config,case)
    inspect.assert_not_called();publish.assert_not_called()
   self.assertFalse((case/'current.json').exists());self.assertEqual(json.loads((case/'journal.jsonl').read_text().splitlines()[-1])['phase'],'failed')

if __name__=='__main__':unittest.main()
