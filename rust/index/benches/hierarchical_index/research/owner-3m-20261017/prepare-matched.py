import json,pathlib,hashlib,time,shutil
import pyarrow.parquet as pq
import numpy as np
r=pathlib.Path('/home/ubuntu/owner3m');ref=pathlib.Path('/home/ubuntu/reference3m');assert shutil.disk_usage(r).free>30*1024**3
manifest=json.loads((r/'source-manifest.json').read_text())
out=r/'wikipedia3m.f32';assert not out.exists();h=hashlib.sha256();count=0;t=time.monotonic()
with out.open('xb') as f:
 for shard in manifest['shards']:
  path=pathlib.Path(shard['path']);sh=hashlib.sha256()
  with path.open('rb') as inp:
   for chunk in iter(lambda:inp.read(1048576),b''):sh.update(chunk)
  assert sh.hexdigest()==shard['sha256']
  for batch in pq.ParquetFile(path).iter_batches(batch_size=8192,columns=['emb']):
   arr=batch.column(0);assert not arr.null_count;assert np.all(np.diff(np.asarray(arr.offsets))==1024)
   a=np.asarray(arr.values.to_numpy(zero_copy_only=False),dtype='<f4').reshape(-1,1024);assert np.isfinite(a).all();data=a.tobytes();f.write(data);h.update(data);count+=len(a)
assert count==3000000 and h.hexdigest()=='dbbff5086b32f0be34c21870d6edb42478f13ee46d9d42aec189b07c4a9cb022'
gt=ref/'ground-truth/wikipedia_1m_3m_ground_truth.parquet';assert hashlib.sha256(gt.read_bytes()).hexdigest()=='21f7a8627ac2c0e79bf5f0d61d7fc5477a646262cdab5fa67468bc8e90478d91'
ids=json.loads((ref/'ground-truth/wikipedia_1m_3m_ground_truth.json').read_text())['query_ids'];rows=[x for x in pq.read_table(gt).to_pylist() if x['max_vector_id']==3000000];assert len(rows)==100
(r/'queries.json').write_text(json.dumps([dict(id=i,vector=x['query_vector'],neighbors=x['neighbors_l2']) for i,x in zip(ids,rows)]))
(r/'input-binding.json').write_text(json.dumps(dict(rows=count,bytes=out.stat().st_size,sha256=h.hexdigest(),conversion_seconds=time.monotonic()-t,gt_sha256=hashlib.sha256(gt.read_bytes()).hexdigest()),indent=2)+'\n')
print('Retained input converted and hash matched')
