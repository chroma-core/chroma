"""Prepare a canonical100k delta using the already-frozen real1M boundary."""
import hashlib,json,resource,sys,time
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import numpy as np
from tools.hierarchical_sharding.spatial_prepare import prepare
from tools.hierarchical_sharding.exact_blocked import exact_blocked
from tools.hierarchical_sharding.benchmark import read_jsonl,write_jsonl
from tools.hierarchical_sharding.tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');reports=root/'fineweb-append100k-reports-v1';reports.mkdir(exist_ok=False);start=time.monotonic();report=dict(status='running',stages=[],source_ids=[1000000,1100000],caps=dict(network_bytes=0,additional_output_bytes=1024**3,hard_timeout_s=300))
def save():_atomic_write(reports/'run.json',canonical_bytes(report))
def stage(name,began,**values):
 row=dict(stage=name,wall_s=time.monotonic()-began,**values);report['stages'].append(row);save();print(json.dumps(row),flush=True)
def copy_delta(base,chunks,output):
 digest=hashlib.sha256();rows=0
 for chunk in chunks:
  first=max(1000000,chunk['start_id']);last=min(1100000,chunk['start_id']+chunk['row_count'])
  if first>=last:continue
  path=checked_artifact(chunk['vectors'],base)
  with path.open('rb') as source:
   source.seek((first-chunk['start_id'])*768*4);remaining=(last-first)*768*4
   while remaining:
    raw=source.read(min(8*1024**2,remaining));assert raw;digest.update(raw)
    if output is not None:output.write(raw)
    remaining-=len(raw)
  rows+=last-first
 assert rows==100000;return digest.hexdigest()
save()
try:
 began=time.monotonic();canonical=root/'fineweb-columnar-twofile-c2-v2';ingested=json.loads((canonical/'ingest.json').read_text());assert ingested['global_row_count']==2156499
 delta=root/'fineweb-delta100k-v1.f32'
 with delta.open('xb') as output:delta_hash=copy_delta(canonical,ingested['chunks'],output)
 verified=root/'fineweb-parallel1m1-v1';old_ingested=json.loads((verified/'ingest.json').read_text());assert delta_hash==copy_delta(verified,old_ingested['chunks'],None)
 stage('canonical-delta-extraction',began,rows=100000,source_prefix=artifact(canonical/'complete.json'),delta=artifact(delta),matches_verified1m1_suffix=True)
 began=time.monotonic();previous=root/'fineweb-stream1m-128-v1/input.json';old=json.loads(previous.read_text());out=root/'fineweb-append100k-128-v1'
 prepared=prepare(delta,out,768,children=128,construction_epoch=old['construction_epoch'],metric='ip',source_id=old['source_id'],previous=previous,start_id=1000000,block_rows=4096)
 assert prepared['model']['sha256']==old['model']['sha256'];seen=np.zeros(1100000,dtype=np.bool_);values=np.memmap(delta,dtype='<f4',mode='r',shape=(100000,768));affected=[]
 for before,after in zip(old['subtrees'],prepared['subtrees']):
  assert before['routing_boundary_hash']==after['routing_boundary_hash'];old_ids=np.fromfile(checked_artifact(before['local_to_global'],previous.parent),dtype='<u8');ids=np.fromfile(checked_artifact(after['local_to_global'],out),dtype='<u8')
  np.testing.assert_array_equal(ids[:len(old_ids)],old_ids);assert len(np.unique(ids))==len(ids) and np.all(ids<1100000) and not seen[ids].any();seen[ids]=True
  added=ids[len(old_ids):];assert len(added)==after['append_row_count'] and np.all(added>=1000000)
  actual=np.fromfile(checked_artifact(after['vectors'],out),dtype='<f4').reshape(-1,768);np.testing.assert_array_equal(actual.view('<u4'),values[added-1000000].view('<u4'))
  if len(added):affected.append(after['subtree_id'])
 assert seen.all() and sum(s['append_row_count'] for s in prepared['subtrees'])==100000
 stage('frozen-routing-and-full-ID-audit',began,global_ids=int(seen.sum()),affected_owners=len(affected),unaffected_owners=128-len(affected),old_map_prefixes_unchanged=True,model_unchanged=True,input=artifact(out/'input.json'),append_counts={s['subtree_id']:s['append_row_count'] for s in prepared['subtrees']})
 for pool in ['calibration','test']:
  began=time.monotonic();queries=root/'fineweb-pilot-queries'/(pool+'.jsonl');base_refs=root/'fineweb-stream1m-reports-v1'/(pool+'.exact.jsonl')
  previous_report=json.loads((root/'fineweb-stream1m-reports-v1/run.json').read_text());previous_stage=next(x for x in previous_report['stages'] if x['stage']=='exact-'+pool)
  assert artifact(base_refs)['sha256']==previous_stage['output']['sha256'] and artifact(queries)['sha256']==previous_stage['queries_artifact']['sha256']
  query_rows=read_jsonl(queries);base=read_jsonl(base_refs);new=exact_blocked(delta,query_rows,768,100,'ip',id_offset=1000000);rows=[]
  assert len(query_rows)==len(base)==len(new)==100 and len({q['query_id'] for q in query_rows})==100
  for a,b in zip(base,new):
   assert a['query_id']==b['query_id'];pairs=sorted(zip(a['distances']+b['distances'],a['ids']+b['ids']))[:100];rows.append(dict(query_id=a['query_id'],ids=[i for d,i in pairs],distances=[d for d,i in pairs]))
  dest=reports/(pool+'.exact1m1.jsonl');write_jsonl(dest,rows);stage('exact1m1-'+pool,began,queries=len(rows),corpus_rows=1100000,base1m_reference=artifact(base_refs),scored_delta=artifact(delta),output=artifact(dest),method='merge independently scored full1M and delta100k exact top100, distance/globalID tie order')
 report['owned_output_bytes']=delta.stat().st_size+sum(p.stat().st_size for folder in [out,reports] for p in folder.rglob('*') if p.is_file())
 assert report['owned_output_bytes']<1024**3
 report['status']='passed'
except Exception as error:report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
finally:
 usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-start,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss);save();print(json.dumps(dict(status=report['status'],wall_s=report['wall_s'])),flush=True)
