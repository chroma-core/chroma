import hashlib,json,time,fcntl
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
r=Path('/mnt/msmarco/version-free-followup-20261020'); a=r/'artifacts'; a.mkdir(parents=True,exist_ok=True)
lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
start=time.time(); old=Path('/home/ubuntu/benchmark-data'); meta=json.loads((old/'wikipedia_1m_10m_ground_truth.json').read_text()); gt=old/'wikipedia_1m_10m_ground_truth.parquet'
assert hashlib.sha256(gt.read_bytes()).hexdigest()==meta['parquet_sha256']
t=pq.read_table(gt); rows=t.to_pylist()[:100]; q=np.asarray([x['query_vector'] for x in rows],dtype=np.float64); assert q.shape==(100,1024)
cache=Path('/mnt/data/huggingface/hub/datasets--CohereLabs--wikipedia-2023-11-embed-multilingual-v3'); rev=(cache/'refs/main').read_text().strip(); snap=cache/'snapshots'/rev
verification=json.loads(Path('/mnt/msmarco/version-free-writer-7927-9b9c7eb/artifacts/gt-verification-10m.json').read_text())
print('Prior source verification loaded',type(verification).__name__,flush=True)
bd=np.full((100,100),np.inf); bi=np.zeros((100,100),dtype=np.uint32); qn=(q*q).sum(axis=1); offset=0;sources=[]
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for c in iter(lambda:f.read(8*1024*1024),b''):h.update(c)
 return h.hexdigest()
for shard in range(30):
 p=snap/'en'/f'{shard:04}.parquet'; original=Path(meta['source_paths'][shard]); h=digest(p);assert h==digest(original)
 sources.append({'path':str(p),'sha256':h})
 for batch in pq.ParquetFile(p).iter_batches(batch_size=10000,columns=['emb']):
  x=np.asarray(batch.column(0).values.to_numpy(),dtype=np.float64).reshape(-1,1024)
  d=qn[:,None]+(x*x).sum(axis=1)[None,:]-2*(q@x.T)
  ix=np.argpartition(d,99,axis=1)[:,:100]; ds=np.take_along_axis(d,ix,axis=1); ids=ix.astype(np.uint32)+offset
  ds=np.concatenate([bd,ds],axis=1); ids=np.concatenate([bi,ids],axis=1); keep=np.argpartition(ds,99,axis=1)[:,:100];bd=np.take_along_axis(ds,keep,axis=1);bi=np.take_along_axis(ids,keep,axis=1);offset+=len(x)
 print('Verified/scanned',offset,flush=True)
assert offset==3000000
ix=np.lexsort((bi,bd),axis=1);bi=np.take_along_axis(bi,ix,axis=1)
existing=pq.read_table('/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet').to_pylist(); existing=[x for x in existing if x['max_vector_id']==3000000]
assert np.array_equal(np.asarray([x['query_vector'] for x in existing],dtype=np.float64),q)
matching=sum(set(x['neighbors_l2'])==set(bi[i].tolist()) for i,x in enumerate(existing))
for i,x in enumerate(existing):x['neighbors_l2']=bi[i].tolist()
out=a/'wikipedia-gt.parquet';pq.write_table(pa.Table.from_pylist(rows+existing,schema=t.schema),out)
(a/'gt-verification.json').write_text(json.dumps({'boundaries':[1000000,3000000],'query_ids':meta['query_ids'],'dimension':1024,'query_count':100,'metric':'squared L2','method':'1M verified prior exact file; 3M exhaustive float64 L2 scan, 10000-row batches','existing_3m_neighbor_sets_matching':matching,'sources':sources,'hf_revision':rev,'gt_sha256':digest(out),'prep_seconds':time.time()-start},indent=2)+'\n')
print('Complete; existing matching sets',matching,flush=True)
