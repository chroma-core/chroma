import hashlib,json
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
r=Path('/home/ubuntu/reference3m')
old=r/'historical-ground-truth.parquet'
assert hashlib.sha256(old.read_bytes()).hexdigest()=='2c220ca2066768f9be447c185a6788988a716f2c7db1d65f17a8249735d15d12'
a=pq.read_table(old).to_pylist();a=[x for x in a if x['max_vector_id']<=3000000]
b=pq.read_table(r/'ground-truth/wikipedia_1m_3m_ground_truth.parquet').to_pylist()
assert len(a)==len(b)==300
query_mismatch=0;neighbor_mismatch={k:0 for k in ['neighbors_l2','neighbors_ip','neighbors_cosine']}
for x,y in zip(a,b):
 assert x['max_vector_id']==y['max_vector_id']
 query_mismatch+=int(not np.array_equal(x['query_vector'],y['query_vector']))
 for k in neighbor_mismatch:
  assert len(y[k])==100 and len(set(y[k]))==100
  assert min(y[k])>=0 and max(y[k])<y['max_vector_id']
  neighbor_mismatch[k]+=int(set(x[k])!=set(y[k]))
result=dict(rows=300,query_mismatches=query_mismatch,neighbor_set_mismatch_rows=neighbor_mismatch,scope='same three corpus prefixes and historical100 queries; exhaustivefloat64 new computation, not independent algorithm')
(r/'evidence/ground-truth-crosscheck.json').write_text(json.dumps(result,indent=2)+'\n')
assert query_mismatch==0 and neighbor_mismatch['neighbors_l2']==0,result
print(json.dumps(result))
