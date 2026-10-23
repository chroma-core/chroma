"""Exercise concrete admission without infrastructure or corpus reads."""
import copy,json,tempfile,time,unittest
from pathlib import Path
from .fineweb_scale_runtime import validate_scale_job
from .tree_manifest import artifact
from . import test_fineweb_scale_job as scale_fixture
from .fineweb_prepare import DATASET,REVISION
class RuntimeTests(unittest.TestCase):
 def fixture(self,root):
  gate=root/'gate.json';gate.write_text(json.dumps(dict(status='passed',corpus_rows=10000000,allow_100m_preparation=True)))
  query=root/'queries';query.write_text('fixture admission artifact')
  desc=artifact(query);now=time.time();owned=root/'owned';owned.mkdir()
  job=dict(stage_estimates_s={'ingestion':6800,'whole-prefix-model':60,'routing':1200,'full-ID-and-bitwise-routing-audit':1600,'whole-prefix-sample-and-UUID-spotcheck':250,'exact-calibration':1100,'exact-test':1100,'independent-full100m-reference-check':600,'archive-and-restore':6000},dataset=DATASET,revision=REVISION,dimension=768,count=100000000,metric='ip',started_epoch=now,stop_epoch=now+21600,owned_root=str(owned),reports=str(owned/'reports'),read_gate=artifact(gate),query_pools={p:dict(desc,count=100) for p in ('calibration','test')},producer_sources=[artifact(Path(__file__).with_name('fineweb_scale_prepare.py'))],ingest=dict(output=str(owned/'ingest'),workers=2,chunk_rows=100000,batch_rows=2048,retries=1,max_source_bytes=384*1024**3,max_output_bytes=512*1024**3,max_wall_s=19800),spatial=dict(output=str(owned/'spatial'),children=128,sample_size=65536,batch_rows=4096),resources=dict(hard_timeout_s=19800,max_new_logical_output_bytes=2*1024**4,max_process_rss_bytes=32*1024**3),archive=dict(output=str(owned/'archive'),restore=str(owned/'restore'),scopes=[str(owned/k) for k in ('ingest','spatial','reports')],concurrency=4,part_bytes=64*1024**2,maximum_part_count=10000))
  return job,scale_fixture.ScalePlanTests().fixture()
 def test_quality_clock_exact_source_and_complete_scope_are_required(self):
  with tempfile.TemporaryDirectory() as d:
   job,plan=self.fixture(Path(d));r=validate_scale_job(job,plan);self.assertEqual(r['source_files'],100);self.assertFalse(r['launch_allowed'])
   for change in ('deadline','scope','archive','readgate','query','source','projection'):
    j=copy.deepcopy(job)
    if change=='deadline':j['stop_epoch']+=1
    elif change=='scope':j['archive']['scopes'].remove(j['reports'])
    elif change=='archive':j['archive']['part_bytes']=1024
    elif change=='readgate':j['read_gate']['sha256']='0'*64
    elif change=='query':j['query_pools']['test']['count']=99
    elif change=='source':j['producer_sources'][0]['sha256']='0'*64
    else:j['stage_estimates_s']['archive-and-restore']=1
    with self.subTest(change=change),self.assertRaises(ValueError):validate_scale_job(j,plan)
 def test_failed_read_capacity_receipt_blocks_before_bulk(self):
  with tempfile.TemporaryDirectory() as d:
   job,plan=self.fixture(Path(d));p=Path(job['read_gate']['path']);p.write_text(json.dumps(dict(status='failed',corpus_rows=10000000,allow_100m_preparation=False)));job['read_gate']=artifact(p)
   with self.assertRaisesRegex(ValueError,'read/capacity'):validate_scale_job(job,plan)
if __name__=='__main__':unittest.main()
