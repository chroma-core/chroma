import json, os
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
root=Path('/home/ubuntu/benchmark-data')
info=json.loads((root/'wikipedia_1m_10m_ground_truth.json').read_text())
sources=json.loads((root/'wikipedia_10m_sources.json').read_text())['shards']
assert len(info['source_paths']) == len(sources) == 100
for old,new in zip(info['source_paths'],sources):
    assert os.path.basename(os.path.realpath(old)) == os.path.basename(os.path.realpath(new['path']))
    assert os.path.getsize(old) == os.path.getsize(new['path'])
assert sum(s['rows'] for s in sources) == 10_000_000
queries=pq.read_table(root/'wikipedia_1m_10m_ground_truth.parquet',columns=['query_vector']).to_pydict()['query_vector'][:100]
for query_id,query in zip(info['query_ids'][:5], queries[:5]):
    shard=sources[query_id//100_000]
    values=pq.read_table(shard['path'],columns=['emb']).column(0)
    assert np.array_equal(np.asarray(values[query_id%100_000].as_py(), dtype=np.float32), np.asarray(query,dtype=np.float32))
print('All100 source shard blob hashes, file sizes, and shard order match exactGTsource. Totalrows=10M. Five sampled current-source query vectors equal ground-truth query vectors after float32 conversion.')
