import copy,unittest
from .fineweb_scale_job import validate_100m_plan
from .fineweb_prepare import DATASET,REVISION
class ScalePlanTests(unittest.TestCase):
 def fixture(self):
  files=[]
  for i in range(100):
   name=f'data/{i:04d}.parquet'
   files.append(dict(file=dict(path=name,bytes=1000000000,publisher_sha256='a'*64,url=f'https://huggingface.co/datasets/{DATASET}/resolve/{REVISION}/{name}'),row_count=1000000,parquet_rows=1000000,start_id=i*1000000,row_groups=[dict(row_group=j,source_row_start=j*250000,row_count=250000,columns=[dict(path='dense_embedding.list.element',compressed_bytes=128*1024**2),dict(path='id',compressed_bytes=1024**2)]) for j in range(4)]))
  return dict(status='passed',dataset=DATASET,revision=REVISION,count=100000000,metadata_payload_bytes=6553600,wall_s=100,projected_compressed_column_bytes=100*4*129*1024**2,files=files)

 def test_more_than32_files_preserves_dense_prefix_and_no_launch(self):
  result=validate_100m_plan(self.fixture());self.assertEqual(result['source_files'],100);self.assertFalse(result['launch_allowed']);self.assertEqual(result['ingest_concurrency'],2)
 def test_source_gap_early_truncation_and_caps_reject(self):
  for mutation in ('gap','truncation','files','metadata','wall','request'):
   p=self.fixture()
   if mutation=='gap':p['files'][1]['start_id']+=1
   elif mutation=='truncation':p['files'][0]['parquet_rows']+=1
   elif mutation=='files':p['files']*=6
   elif mutation=='metadata':p['metadata_payload_bytes']=129*1024**2
   elif mutation=='wall':p['wall_s']=301
   else:
    for f in p['files']:
     for group in f['row_groups']:
      for column in group['columns']:column['compressed_bytes']=256*1024**2
    p['projected_compressed_column_bytes']=100*4*512*1024**2
   with self.subTest(mutation=mutation),self.assertRaises(ValueError):validate_100m_plan(p)
if __name__=='__main__':unittest.main()
