import copy,unittest
from .fineweb_job import validate_job
from .fineweb_prepare import DATASET,REVISION,DIMENSION

class JobTests(unittest.TestCase):
    def setUp(self):
        self.job=dict(dataset=DATASET,revision=REVISION,dimension=DIMENSION,count=10000000,metric='ip',projected_compressed_column_bytes=14800596084,seed_compressed_column_bytes=3190918351,additional_projected_compressed_column_bytes=11609677733,source_plan='/mnt/spann/plan.json',ingest=dict(workers=2,chunk_rows=100000,batch_rows=2048,max_source_bytes=32*1024**3,max_output_bytes=96*1024**3,max_wall_s=3600,retries=1,output='/mnt/spann/ingest',seed_previous='/mnt/spann/seed/ingest.json'),spatial=dict(children=128,sample_size=65536,iterations=12,batch_rows=4096,output='/mnt/spann/prepared'),resources=dict(hard_timeout_s=7200,max_new_logical_output_bytes=192*1024**3,minimum_free_allocated_bytes=512*1024**3,minimum_start_free_allocated_bytes=704*1024**3,max_process_rss_bytes=16*1024**3,monitor_interval_s=1))
    def test_valid(self):
        self.assertEqual(validate_job(self.job)['network_bytes'],0)
    def test_caps_and_paths_fail_closed(self):
        for group,key,value in [('resources','minimum_start_free_allocated_bytes',512*1024**3),('ingest','max_source_bytes',1024),('ingest','workers',True),('spatial','output','../escape'),('spatial','sample_size',1),('spatial','max_wall_s',1800)]:
            job=copy.deepcopy(self.job);job[group][key]=value
            with self.assertRaises(ValueError):validate_job(job)
