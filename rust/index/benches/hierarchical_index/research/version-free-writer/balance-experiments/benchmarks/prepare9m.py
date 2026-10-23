import json,hashlib,fcntl
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
r=Path('/mnt/msmarco/balance-experiments-20261020');a=r/'artifacts'
lock=open('/mnt/data/chroma-benchmark.lock','w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
source=Path('/home/ubuntu/.cache/wikipedia_en/ground_truth.parquet');t=pq.read_table(source);rows=[x for x in t.to_pylist() if x['max_vector_id']==9000000];gt=pq.read_table(a/'wikipedia-gt.parquet');fixed=gt.to_pylist()[:100]
assert len(rows)==100 and [x['query_vector'] for x in rows]==[x['query_vector'] for x in fixed]
assert all(len(x['neighbors_l2'])==100 and max(x['neighbors_l2'])<9000000 for x in rows)
(a/'gt9-source-manifest.json').write_text(json.dumps({'source':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'rows':100,'neighbors':100,'queries_equal_verified_1m_3m':True,'independently_recomputed_9m':False},indent=2))
pq.write_table(pa.Table.from_pylist(gt.to_pylist()+rows,schema=t.schema),a/'wikipedia-gt.parquet')
(a/'gt9-combined.sha256').write_text(hashlib.sha256((a/'wikipedia-gt.parquet').read_bytes()).hexdigest())
