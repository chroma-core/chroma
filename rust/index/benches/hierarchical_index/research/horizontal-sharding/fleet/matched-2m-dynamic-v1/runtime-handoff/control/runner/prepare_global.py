"""Prepare a common exact reference without spatial partitioning or vector copies."""
import json
from pathlib import Path
import time

from artifact_bindings import descriptor, freeze_prepared, verify_descriptor


def prepare(vectors, queries, output, deadline_unix, rows=2000000, dimension=768):
    vector_path = verify_descriptor(vectors)
    query_path = verify_descriptor(queries)
    if vectors['bytes'] != rows * dimension * 4:
        raise ValueError('canonical corpus extent mismatch')
    if time.time() >= deadline_unix:
        raise ValueError('expired preparation deadline')
    selected = [json.loads(line) for line in query_path.read_text().splitlines() if line.strip()]
    if len(selected) != 16 or len({q['query_id'] for q in selected}) != 16:
        raise ValueError('sixteen distinct frozen queries required')
    # Imported only after the launcher verifies the bound implementation package.
    from tools.hierarchical_sharding.exact_blocked import exact_blocked
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    reference = exact_blocked(vector_path, selected, dimension, 100, 'ip', corpus_block=8192, query_block=16)
    with (output/'reference.jsonl').open('x') as stream:
        for row in reference:
            stream.write(json.dumps(row)+'\n')
    verify_descriptor(vectors)
    verify_descriptor(queries)
    if time.time() >= deadline_unix:
        raise RuntimeError('preparation exceeded fixed deadline')
    result = {'rows': rows, 'dimension': dimension, 'metric': 'ip',
              'vectors': vectors, 'queries': queries,
              'reference': descriptor(output/'reference.jsonl'),
              'static_partition': False, 'materialized_vector_copies': 0,
              'reference_engine': 'blocked float64 BLAS; distance 1-dot; global-ID ties'}
    (output/'prepared.json').write_text(json.dumps(result, indent=2)+'\n')
    freeze_prepared(output, vector_path, query_path)
    return result
