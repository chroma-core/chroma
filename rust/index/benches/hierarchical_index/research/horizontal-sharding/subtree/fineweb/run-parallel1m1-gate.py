"""Isolated real two-file ingestion/recovery gate; no index or performance claim."""
import hashlib,json,random,resource,sys,time
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import numpy as np
import pyarrow.parquet as pq
from tools.hierarchical_sharding.fineweb_prepare import RangeReader
from tools.hierarchical_sharding.fineweb_parallel import ingest
from tools.hierarchical_sharding.fineweb_stream import _source_batches
from tools.hierarchical_sharding.tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');out=root/'fineweb-parallel1m1-v1';reports=root/'fineweb-parallel1m1-reports-v1';reports.mkdir(exist_ok=False)
manifest=json.loads((root/'fineweb-source-manifest-v2.json').read_text());start=time.monotonic();report=dict(status='running',stages=[],purpose='real multi-file canonical ingest and recovery validation, not index timing',hard_timeout_s=1800)
def save():_atomic_write(reports/'run.json',canonical_bytes(report))
def stage(name,began,**values):
 row=dict(stage=name,wall_s=time.monotonic()-began,**values);report['stages'].append(row);save();print(json.dumps(row),flush=True)
def injected(index,retry):
 if index==1:raise OSError('intentional second-file pre-read fault for resume validation')
save()
try:
 began=time.monotonic();metadata_budget=dict(max_bytes=64*1024**2,bytes_read=0,deadline=time.monotonic()+90);metadata=[]
 for entry in manifest['files'][:2]:
  with RangeReader(entry['url'],entry['bytes'],budget=metadata_budget) as source:
   parquet=pq.ParquetFile(source);groups=[];offset=0
   for i in range(parquet.num_row_groups):
    group=parquet.metadata.row_group(i);columns=[group.column(j) for j in range(group.num_columns) if group.column(j).path_in_schema.split('.')[0] in ('dense_embedding','id')]
    groups.append(dict(start=offset,rows=group.num_rows,compressed_bytes=sum(c.total_compressed_size for c in columns),columns=len(columns)));offset+=group.num_rows
   metadata.append(dict(file=entry['path'],rows=parquet.metadata.num_rows,groups=groups,ranges=source.ranges))
 assert metadata[0]['rows']==1090356
 second_rows=1100000-metadata[0]['rows']
 def needed(file,start,rows):
  selected=[g for g in file['groups'] if g['start']<start+rows and g['start']+g['rows']>start]
  return sum(g['compressed_bytes']+g['columns']*8*1024**2 for g in selected)
 # Include full intersecting compressed columns, read slack per column, footer
 # allowance, and one complete retry for the resumed second-file job.
 second_needed=needed(metadata[1],0,second_rows);tail_needed=needed(metadata[0],1000000,90356)+second_needed
 resume_cap=((2*second_needed+64*1024**2+1024**2-1)//(1024**2))*1024**2
 tail_cap=((tail_needed+64*1024**2+1024**2-1)//(1024**2))*1024**2
 assert resume_cap<=1024**3 and tail_cap<=1024**3
 assert needed(metadata[0],0,metadata[0]['rows'])+64*1024**2<=3*1024**3
 report['caps']=dict(metadata_bytes=64*1024**2,initial_bytes=3*1024**3,resume_bytes=resume_cap,serial_tail_bytes=tail_cap,output_bytes=12*1024**3)
 _atomic_write(reports/'verified-row-group-plan.json',canonical_bytes(metadata))
 stage('verified-metadata-and-caps',began,source_bytes=metadata_budget['bytes_read'],resume_cap=resume_cap,serial_tail_cap=tail_cap)
 began=time.monotonic()
 try:
  ingest(manifest,out,1100000,workers=2,chunk_rows=100000,batch_rows=2048,max_source_bytes=3*1024**3,max_output_bytes=12*1024**3,max_wall_s=900,retries=0,before_file=injected)
  raise AssertionError('expected injected fault did not occur')
 except OSError as error:
  assert 'intentional second-file' in str(error)
  assert (out/'files/000000/complete.json').exists() and not (out/'complete.json').exists()
  first=json.loads((out/'files/000000/complete.json').read_text());assert first['source']['parquet_rows']==1090356
  stage('injected-fault-ingest',began,status='expected_injected_fault',first_file_complete=True,source_request_bytes_charged=json.loads((out/'attempt-0000.json').read_text())['source_request_bytes_charged'])
 began=time.monotonic();result=ingest(manifest,out,1100000,workers=2,chunk_rows=100000,batch_rows=2048,max_source_bytes=resume_cap,max_output_bytes=12*1024**3,max_wall_s=180,retries=1,resume=True)
 assert json.loads((out/'files/000000/complete.json').read_text())==first
 stage('verified-resume',began,rows=result['global_row_count'],source_request_bytes_charged=result['source_request_bytes_charged'],owned_output_bytes=result['owned_output_bytes'],complete=artifact(out/'complete.json'))
 began=time.monotonic();serial=root/'fineweb-stream1m-128-v1';previous=json.loads((serial/'checkpoint.json').read_text())['chunks'];chunks=result['chunks']
 assert len(chunks)>=12
 for old,new in zip(previous,chunks[:10]):
  assert old['start_id']==new['start_id'] and old['row_count']==new['row_count']==100000
  for key in ['vectors','uuids']:assert old[key]['sha256']==new[key]['sha256']
 # Read only the new100k suffix through a separate serial source cursor.
 budget=dict(max_bytes=tail_cap,bytes_read=0,deadline=time.monotonic()+180);tail=[];uuids=[];remaining=100000
 with (reports/'serial-tail-source-ranges.jsonl').open('w') as audit:
  iterator=_source_batches(manifest,dict(file_index=0,row_offset=1000000),budget,2048,None,audit)
  try:
   for x,ids,_,_ in iterator:
    take=min(len(x),remaining);tail.append(x[:take].copy());uuids.extend(ids[:take]);remaining-=take
    if not remaining:break
  finally:iterator.close()
 assert remaining==0;expected=np.concatenate(tail);actual=np.concatenate([np.fromfile(checked_artifact(c['vectors'],out),dtype='<f4').reshape(-1,768) for c in chunks[10:]])
 np.testing.assert_array_equal(actual,expected)
 actual_ids=[];actual_uuids=[]
 for c in chunks:
  records=[json.loads(x) for x in checked_artifact(c['uuids'],out).read_text().splitlines()]
  actual_ids.extend(r['global_id'] for r in records)
  if c['start_id']>=1000000:actual_uuids.extend(r['source_uuid'] for r in records)
 assert actual_ids==list(range(1100000)) and actual_uuids==uuids
 # The same global random sample must read byte-identical rows regardless of
 # source-file concurrency or chunk boundaries.
 indices=sorted(random.Random(1729).sample(range(1100000),32768));sample_a=hashlib.sha256();sample_b=hashlib.sha256();handles={}
 def row_at(base,entries,index):
  entry=next(c for c in entries if c['start_id']<=index<c['start_id']+c['row_count']);path=checked_paths[(str(base),entry['start_id'])]
  stream=handles.setdefault(str(path),path.open('rb')) if str(path) not in handles else handles[str(path)];stream.seek((index-entry['start_id'])*768*4);raw=stream.read(768*4);assert len(raw)==768*4;return raw
 checked_paths={(str(base),c['start_id']):checked_artifact(c['vectors'],base) for base,entries in [(out,chunks),(serial,previous)] for c in entries}
 try:
  for index in indices:
   sample_a.update(row_at(out,chunks,index));sample_b.update(row_at(serial,previous,index) if index<1000000 else expected[index-1000000].tobytes())
 finally:
  for stream in handles.values():stream.close()
 assert sample_a.hexdigest()==sample_b.hexdigest()
 stage('canonical-values-uuid-and-sample-validation',began,first1m_matches_serial_hashes=True,serial_tail_rows=100000,tail_source_bytes=budget['bytes_read'],total_unique_global_ids=len(actual_ids),sample_rows=len(indices),sample_sha256=sample_a.hexdigest(),no_normalization=True)
 report['status']='passed'
except Exception as e:
 report.update(status='failed',error_type=type(e).__name__,error=str(e));raise
finally:
 usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-start,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss);save();print(json.dumps(dict(status=report['status'],wall_s=report['wall_s'])),flush=True)
