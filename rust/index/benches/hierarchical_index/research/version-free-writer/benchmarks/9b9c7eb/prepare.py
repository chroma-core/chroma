import hashlib,json,shutil
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
r=Path('/mnt/msmarco/version-free-writer-7927-9b9c7eb'); records=[]
for dataset,repo,folder,rows in [('wikipedia','CohereLabs--wikipedia-2023-11-embed-multilingual-v3','en',100000),('msmarco','CohereLabs--msmarco-v2-embed-multilingual-v3','corpus',1000000)]:
 old=Path('/home/ubuntu/benchmark-data'); meta=json.loads((old/f'{dataset}_1m_10m_ground_truth.json').read_text()); gt=old/f'{dataset}_1m_10m_ground_truth.parquet'
 assert hashlib.sha256(gt.read_bytes()).hexdigest()==meta['parquet_sha256']
 d=pq.read_table(gt).to_pydict(); assert d['max_vector_id']==[1000000]*100+[10000000]*100
 q=np.asarray(d['query_vector'][:100],dtype=np.float32); assert q.shape==(100,1024); assert np.array_equal(q,np.asarray(d['query_vector'][100:],dtype=np.float32))
 for i,ns in enumerate(d['neighbors_l2']): assert len(ns)==len(set(ns))==100 and min(ns)>=0 and max(ns)<d['max_vector_id'][i]
 cache=Path('/mnt/data/huggingface/hub')/('datasets--'+repo); rev=(cache/'refs/main').read_text().strip(); snap=cache/'snapshots'/rev
 sources=[]
 ids=np.array(meta['query_ids']); seen=np.zeros(100,dtype=bool)
 for n in range(1000000//rows):
  path=snap/folder/f'{n:04}.parquet'; original=Path(meta['source_paths'][n]); assert path.exists() and original.exists()
  def digest(p):
   h=hashlib.sha256()
   with p.open('rb') as f:
    for chunk in iter(lambda:f.read(8*1024*1024),b''):h.update(chunk)
   return h.hexdigest()
  h=digest(path); assert h==digest(original), (path,original)
  offset=n*rows
  for batch in pq.ParquetFile(path).iter_batches(batch_size=10000,columns=['emb']):
   vals=np.asarray(batch.column(0).values.to_numpy(),dtype=np.float32).reshape(-1,1024)
   mask=(ids>=offset)&(ids<offset+len(vals)); assert np.array_equal(q[mask],vals[ids[mask]-offset]); seen|=mask; offset+=len(vals)
  assert offset==(n+1)*rows
  sources.append({'path':str(path),'sha256':h,'original':str(original)})
 assert seen.all()
 out=r/'artifacts'/f'{dataset}-gt.parquet'; shutil.copyfile(gt,out)
 shutil.copyfile(old/f'{dataset}_1m_10m_ground_truth.json',r/'artifacts'/f'{dataset}-gt-provenance.json')
 records.append({'dataset':dataset,'gt_sha256':meta['parquet_sha256'],'prefix':1000000,'query_count':100,'dimension':1024,'metric':'squared L2','sources':sources,'hf_revision':rev,'query_vectors_match_source_bits':True})
 print('Verified',dataset,flush=True)
(r/'artifacts/gt-verification.json').write_text(json.dumps(records,indent=2)+'\n')
