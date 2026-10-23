"""Run in the explicitly isolated worker preparation slot, under hard timeout."""
import json,os,resource,sys,time
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import numpy as np
from tools.hierarchical_sharding.fineweb_stream import prepare
from tools.hierarchical_sharding.exact_blocked import exact_chunks
from tools.hierarchical_sharding.benchmark import read_jsonl,write_jsonl
from tools.hierarchical_sharding.tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');out=root/'fineweb-stream1m-128-v1';reports=root/'fineweb-stream1m-reports-v1';reports.mkdir(exist_ok=True)
source=root/'fineweb-source-manifest-v2.json';started=time.monotonic();report=dict(status='running',stages=[],source_manifest=artifact(source),caps=dict(source_payload_bytes=4*1024**3,preparation_stage_wall_s=1200,external_hard_timeout_s=1800),threads={k:os.environ.get(k) for k in ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS']})
def save():_atomic_write(reports/'run.json',canonical_bytes(report))
def stage(name,began,**values):
 row=dict(stage=name,wall_s=time.monotonic()-began,**values);report['stages'].append(row);save();print(json.dumps(row),flush=True)
save()
try:
 began=time.monotonic();prepared=prepare(json.loads(source.read_text()),out,1000000,128,'fineweb1m-128slots-v1','Qdrant/FineWeb-10B@3aea0b8d5f98741e343d07f067ce0f49dca1ccd2:lex-prefix1m:preserved-f16-to-f32',chunk_rows=100000,batch_rows=2048,sample_size=32768,iterations=12,resume=out.exists(),max_source_bytes=4*1024**3,max_wall_s=1200)
 attempt=sorted(out.glob('attempt-*.json'))[-1];stage('streaming-preparation',began,rows=1000000,source_bytes=json.loads(attempt.read_text())['source_bytes_fetched'],input=artifact(out/'input.json'),owner_counts=[s['vector_count'] for s in prepared['subtrees']])
 began=time.monotonic();state=json.loads((out/'checkpoint.json').read_text());assert state['chunks'][0]['vectors']['sha256']=='5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c'
 seen=np.zeros(1000000,dtype=np.bool_)
 for slot in prepared['subtrees']:
  ids=np.fromfile(checked_artifact(slot['local_to_global'],out),dtype='<u8');assert len(ids)==slot['vector_count'] and len(np.unique(ids))==len(ids);assert np.all(ids<1000000);assert not seen[ids].any();seen[ids]=True
 assert seen.all();stage('full-ownership-audit',began,unique_global_ids=int(seen.sum()),first100k_matches_independently_validated_prefix=True,min_owner=min(s['vector_count'] for s in prepared['subtrees']),max_owner=max(s['vector_count'] for s in prepared['subtrees']))
 chunks=[(checked_artifact(c['vectors'],out),c['start_id'],c['row_count']) for c in state['chunks']]
 for pool in ['calibration','test']:
  began=time.monotonic();queries=root/'fineweb-pilot-queries'/(pool+'.jsonl');rows=exact_chunks(chunks,read_jsonl(queries),768,100,'ip');dest=reports/(pool+'.exact.jsonl');write_jsonl(dest,rows)
  stage('exact-'+pool,began,corpus_rows=1000000,queries=len(rows),engine='numpy_cpu_blas_float64_chunked',metric='one_minus_inner_product',tie_policy='distance_then_global_id',queries_artifact=artifact(queries),output=artifact(dest))
 report['status']='passed'
except Exception as e:
 report.update(status='failed',error_type=type(e).__name__,error=str(e));raise
finally:
 usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-started,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss);save();print(json.dumps(dict(status=report['status'],wall_s=report['wall_s'],report=str(reports/'run.json'))),flush=True)
