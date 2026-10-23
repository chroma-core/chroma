#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq

r = Path('/home/ubuntu/benchmark-data/wikipedia-40m-current')
old_sources = json.loads(Path('/home/ubuntu/benchmark-data/wikipedia_10m_sources.json').read_text())['shards']
new_sources = json.loads((r / 'wikipedia_40m_sources.json').read_text())['shards']
assert len(new_sources) == 400
assert sum(s['rows'] for s in new_sources) == 40_000_000
for old, new in zip(old_sources, new_sources):
    for field in ['filename', 'rows', 'bytes', 'start_id', 'end_id']:
        assert old[field] == new[field], (field, old, new)
    assert Path(old['path']).resolve() == Path(new['path']).resolve()
new_gt = pq.read_table(r / 'wikipedia_1m_40m_ground_truth.parquet').to_pylist()
old_gt = pq.read_table(r / 'previous-ground-truth.parquet').to_pylist()
assert len(new_gt) == 4000
old_by_key = {}
for row in old_gt:
    key = (row['max_vector_id'], tuple(row['query_vector']))
    old_by_key[key] = row
for row in new_gt:
    boundary = row['max_vector_id']
    assert boundary in range(1_000_000, 40_000_001, 1_000_000)
    assert len(row['query_vector']) == 1024
    assert np.isfinite(row['query_vector']).all()
    for metric in ['l2', 'ip', 'cosine']:
        ids = row[f'neighbors_{metric}']
        assert len(ids) == 100 and len(set(ids)) == 100
        assert all(0 <= i < boundary for i in ids)
    key = (boundary, tuple(row['query_vector']))
    if key in old_by_key:
        for metric in ['l2', 'ip', 'cosine']:
            assert set(row[f'neighbors_{metric}']) == set(old_by_key[key][f'neighbors_{metric}'])
counts = {}
for row in new_gt:
    counts[row['max_vector_id']] = counts.get(row['max_vector_id'], 0) + 1
assert len(counts) == 40 and all(c == 100 for c in counts.values())
print('Verified 400 ordered source shards and 40M rows; first 10M source identities match existing cache.')
print('Verified 100 queries and exact top-100 IDs at each of 40 prefix boundaries; overlapping ground truth matches existing results.')
print('Ground truth SHA256:', hashlib.sha256((r / 'wikipedia_1m_40m_ground_truth.parquet').read_bytes()).hexdigest())
