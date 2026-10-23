"""SYNTHETIC normal-vector pilot, independent held-out queries, never real corpus evidence."""
import argparse,pathlib,json,hashlib,time
import numpy as np
p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--count',type=int,required=True);p.add_argument('--dimension',type=int,default=1024);p.add_argument('--queries',type=int,default=40);p.add_argument('--shards',type=int,default=2);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=False);start=time.monotonic();rng=np.random.default_rng(1729)
with (out/'corpus.f32').open('wb') as f:
 for offset in range(0,a.count,4096):rng.standard_normal((min(4096,a.count-offset),a.dimension),dtype=np.float32).astype('<f4').tofile(f)
queries=rng.standard_normal((a.queries,a.dimension),dtype=np.float32)
for label,selection in [('calibration',queries[:a.queries//2]),('test',queries[a.queries//2:]),('all',queries)]:
 with (out/(label+'.jsonl')).open('w') as f:
  for n,q in enumerate(selection):f.write(json.dumps({'query_id':label+'-'+str(n),'vector':q.tolist()})+'\n')
source=np.memmap(out/'corpus.f32',dtype='<f4',mode='r',shape=(a.count,a.dimension))
for shard in range(a.shards):
 with (out/f'shard-{shard}.f32').open('wb') as f:
  for offset in range(0,a.count,4096*a.shards):source[offset+shard:min(a.count,offset+4096*a.shards):a.shards].tofile(f)
def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for chunk in iter(lambda:f.read(8*1024*1024),b''):h.update(chunk)
 return h.hexdigest()
metadata={'label':'SYNTHETIC','distribution':'independent standard normal float32','numpy_version':np.__version__,'seed':1729,'count':a.count,'dimension':a.dimension,'shards':a.shards,'query_count':a.queries,'metric':'squared_l2','heldout_queries':True,'seconds':time.monotonic()-start,'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha(p)} for p in out.iterdir()}}
(out/'preparation.json').write_text(json.dumps(metadata,indent=2)+'\n');print(json.dumps(metadata))
