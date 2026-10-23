"""Isolated Arrow primitive decoder epoch: same real1M values and IDs."""
import json,resource,sys,time
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.fineweb_parallel import ingest
from tools.hierarchical_sharding.tree_manifest import artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');out=root/'fineweb-columnar1m-v2';reports=root/'fineweb-columnar1m-reports-v2';reports.mkdir(exist_ok=False)
start=time.monotonic();report=dict(status='running',decoder_epoch='arrow_primitive_f16_v2',caps=dict(source_bytes=2*1024**3,output_bytes=8*1024**3,wall_s=600,external_timeout_s=900))
try:
 result=ingest(json.loads((root/'fineweb-source-manifest-v2.json').read_text()),out,1000000,workers=1,chunk_rows=100000,batch_rows=2048,max_source_bytes=2*1024**3,max_output_bytes=8*1024**3,max_wall_s=600,retries=0)
 previous=json.loads((root/'fineweb-stream1m-128-v1/checkpoint.json').read_text())['chunks'];assert len(result['chunks'])==len(previous)==10
 for old,new in zip(previous,result['chunks']):
  assert old['start_id']==new['start_id'] and old['row_count']==new['row_count']
  for key in ['vectors','uuids']:assert old[key]['sha256']==new[key]['sha256']
 receipt=json.loads((out/'files/000000/complete.json').read_text());audit=json.loads((out/'files/000000/try-0/source-ranges.jsonl').read_text())
 report.update(status='passed',rows=1000000,all_vector_and_uuid_chunk_hashes_equal=True,ingest_wall_s=result['wall_s'],source_request_bytes_charged=result['source_request_bytes_charged'],owned_output_bytes=result['owned_output_bytes'],file_wall_s=receipt['file_wall_s'],emit_and_chunk_hash_s=receipt['emit_and_chunk_hash_s'],decode_timings=audit['timings'],request_wall_including_range_hash_s=sum(r['request_wall_s'] for r in audit['ranges']),requests=len(audit['ranges']),complete=artifact(out/'complete.json'))
except Exception as error:report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
finally:
 usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-start,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss);_atomic_write(reports/'run.json',canonical_bytes(report));print(json.dumps(report),flush=True)
