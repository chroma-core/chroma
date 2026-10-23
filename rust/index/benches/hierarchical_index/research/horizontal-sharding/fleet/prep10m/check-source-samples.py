"""Bounded independent source-value/UUID spotcheck, after prep timings finish."""
import json,sys,time,resource
from pathlib import Path
sys.path.insert(0,'/mnt/spann/source')
import numpy as np
import pyarrow.parquet as pq
from tools.hierarchical_sharding.fineweb_prepare import RangeReader
from tools.hierarchical_sharding.tree_manifest import checked_artifact,artifact,canonical_bytes,_atomic_write
root=Path('/mnt/spann');out=Path('/var/lib/spann-prep-evidence/source-sample-check.json')
if out.exists():raise FileExistsError(out)
started=time.monotonic();budget=dict(max_bytes=1024**3,bytes_read=0,deadline=started+180);report=dict(status='running',checks=[],caps=dict(source_bytes=1024**3,wall_s=180))
try:
 ingest=root/'fineweb-columnar10m-v2';receipt=json.loads((ingest/'ingest.json').read_text());plan=json.loads((root/'fineweb-10m-source-plan-v1.json').read_text());chunks=receipt['chunks'];starts=np.array([c['start_id'] for c in chunks])
 for slot in (0,len(plan['files'])//2,len(plan['files'])-1):
  selected=plan['files'][slot];entry=selected['file'];first=selected['start_id'];count=min(16,selected['row_count']);index=int(np.searchsorted(starts,first,side='right')-1);chunk=chunks[index]
  with RangeReader(entry['url'],entry['bytes'],budget=budget) as source:
   parquet=pq.ParquetFile(source)
   if parquet.metadata.num_rows!=selected['parquet_rows'] or source.etag!=selected['etag']:raise ValueError('source identity or rows changed')
   # This independent path materializes Python scalars only AFTER slicing16rows.
   records=parquet.read_row_group(0,columns=['dense_embedding','id'],use_threads=False).slice(0,count).to_pylist()
   values=np.asarray([r['dense_embedding'] for r in records],dtype=np.float16).astype('<f4')
   with checked_artifact(chunk['vectors'],ingest).open('rb') as stream:
    stream.seek((first-chunk['start_id'])*768*4);actual=np.frombuffer(stream.read(count*768*4),dtype='<f4').reshape(count,768)
   np.testing.assert_array_equal(actual.view('<u4'),values.view('<u4'))
   uuids=[]
   with checked_artifact(chunk['uuids'],ingest).open() as stream:
    for line in stream:
     row=json.loads(line)
     if first<=row['global_id']<first+count:uuids.append(row['source_uuid'])
     if len(uuids)==count:break
   assert uuids==[r['id'] for r in records]
   report['checks'].append(dict(file=entry['path'],start_id=first,rows=count,bitwise_vectors_match=True,UUIDs_match=True,source_etag=source.etag,ranges=source.ranges))
 report.update(status='passed',rows=sum(r['rows'] for r in report['checks']),method='three public source files, independent sliced Python-scalar float16 decoder versus prepared float32 bytes and UUIDs')
except Exception as error:report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
finally:
 usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-started,source_bytes=budget['bytes_read'],cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss);_atomic_write(out,canonical_bytes(report))
