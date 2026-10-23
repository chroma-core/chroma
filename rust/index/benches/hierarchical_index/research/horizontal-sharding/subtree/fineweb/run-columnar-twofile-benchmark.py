"""Matched full two-file ingestion task graph at concurrency1 and2."""
import json,resource,sys,time
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.fineweb_parallel import ingest
from tools.hierarchical_sharding.tree_manifest import artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');reports=root/'fineweb-columnar-twofile-reports-v2';reports.mkdir(exist_ok=False)
plan=json.loads((root/'fineweb-parallel1m1-reports-v1/verified-row-group-plan.json').read_text());count=sum(x['rows'] for x in plan);assert count==2156499
manifest=json.loads((root/'fineweb-source-manifest-v2.json').read_text());start=time.monotonic();report=dict(status='running',rows=count,source_files=[x['file'] for x in plan],cases=[],caps_per_case=dict(source_bytes=4*1024**3,output_bytes=16*1024**3,internal_wall_s=600),scope='same pinned two-file prefix, no fault injection, repeated-window uncertainty not measured')
_atomic_write(reports/'run.json',canonical_bytes(report));results=[]
try:
 for workers in [1,2]:
  began=time.monotonic();before=resource.getrusage(resource.RUSAGE_SELF);out=root/f'fineweb-columnar-twofile-c{workers}-v2'
  result=ingest(manifest,out,count,workers=workers,chunk_rows=100000,batch_rows=2048,max_source_bytes=4*1024**3,max_output_bytes=16*1024**3,max_wall_s=600,retries=0)
  after=resource.getrusage(resource.RUSAGE_SELF);files=[]
  for receipt in sorted(out.glob('files/*/complete.json')):
   data=json.loads(receipt.read_text());audit=json.loads(next(receipt.parent.glob('try-*/source-ranges.jsonl')).read_text());files.append(dict(source=data['source']['file']['path'],rows=data['source']['row_count'],file_wall_s=data['file_wall_s'],emit_and_chunk_hash_s=data['emit_and_chunk_hash_s'],decode_timings=audit['timings'],request_wall_including_range_hash_s=sum(x['request_wall_s'] for x in audit['ranges'])))
  case=dict(workers=workers,wall_s=time.monotonic()-began,cpu_user_s=after.ru_utime-before.ru_utime,cpu_system_s=after.ru_stime-before.ru_stime,process_lifetime_max_rss_kib=after.ru_maxrss,source_request_bytes_charged=result['source_request_bytes_charged'],owned_output_bytes=result['owned_output_bytes'],files=files,completion=artifact(out/'complete.json'))
  results.append(result);report['cases'].append(case);_atomic_write(reports/'run.json',canonical_bytes(report));print(json.dumps(case),flush=True)
 assert len(results[0]['chunks'])==len(results[1]['chunks'])
 for a,b in zip(results[0]['chunks'],results[1]['chunks']):
  assert a['start_id']==b['start_id'] and a['row_count']==b['row_count']
  for key in ['vectors','uuids']:assert a[key]['sha256']==b[key]['sha256']
 report.update(status='passed',all_vector_uuid_chunk_hashes_equal=True,concurrency2_speedup=report['cases'][0]['wall_s']/report['cases'][1]['wall_s'])
except Exception as error:report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
finally:
 report['wall_s']=time.monotonic()-start;_atomic_write(reports/'run.json',canonical_bytes(report));print(json.dumps(dict(status=report['status'],wall_s=report['wall_s'])),flush=True)
