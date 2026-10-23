"""Prepare both treatments from one already-verified local corpus.

Run on the approved test machine, never as a corpus download to the Mac.
The native build and cloud lifecycle are separate operations.
"""
import json
import os
from pathlib import Path
import time
import numpy as np
from project_rounds import project, digest
from tools.hierarchical_sharding.spatial_prepare import fit_model, distances
from tools.hierarchical_sharding.exact_blocked import exact_blocked


def prepare(vectors, queries, output, expected_vectors_sha256, expected_queries_sha256,
            deadline_unix, dimension=768, rows=2000000, owners=32, round_rows=100000):
    vectors, queries, output = map(Path, (vectors, queries, output))
    if vectors.stat().st_size != rows * dimension * 4:
        raise ValueError('wrong corpus extent')
    if digest(vectors) != expected_vectors_sha256 or digest(queries) != expected_queries_sha256:
        raise ValueError('source identity mismatch')
    if time.time() >= deadline_unix:
        raise RuntimeError('expired execution deadline')
    query_rows = [json.loads(line) for line in queries.read_text().splitlines() if line.strip()]
    if len(query_rows) != 16 or len({q['query_id'] for q in query_rows}) != 16:
        raise ValueError('exactly sixteen distinct frozen queries required')
    output.mkdir(parents=True, exist_ok=False)
    model = fit_model(vectors, dimension, owners, 'matched-2m-v1', 'ip', 1729, 8192, 12)
    (output/'model.json').write_text(json.dumps(model, sort_keys=True, separators=(',', ':'))+'\n')
    source = np.memmap(vectors, dtype='<f4', mode='r', shape=(rows, dimension))
    centers = np.asarray(model['centroids'], dtype=np.float32)
    try:
        with (output/'spatial.labels').open('xb') as spatial, (output/'global.labels').open('xb') as global_labels:
            for start in range(0, rows, 4096):
                if time.time() >= deadline_unix: raise RuntimeError('fixed preparation deadline')
                x = source[start:min(start+4096, rows)]
                if not np.isfinite(x).all(): raise ValueError('nonfinite corpus')
                labels = np.argmin(distances(x, centers, 'ip'), axis=1).astype('<u4')
                spatial.write(labels.tobytes()); global_labels.write(np.zeros(len(x), dtype='<u4').tobytes())
    finally:
        source._mmap.close()
    # Outer process supervisor must enforce deadline/disk/RSS throughout these calls.
    spatial = project(vectors, output/'spatial.labels', output/'spatial', dimension, owners, round_rows)
    global_tree = project(vectors, output/'global.labels', output/'global', dimension, 1, round_rows)
    reference = exact_blocked(vectors, query_rows, dimension, 100, 'ip', corpus_block=8192, query_block=16)
    with (output/'reference.jsonl').open('x') as stream:
        for row in reference: stream.write(json.dumps(row)+'\n')
    if digest(vectors) != expected_vectors_sha256 or digest(queries) != expected_queries_sha256:
        raise ValueError('source changed during preparation')
    result = dict(rows=rows, dimension=dimension, owners=owners, round_rows=round_rows,
                  corpus_sha256=expected_vectors_sha256, queries_sha256=expected_queries_sha256,
                  model_sha256=digest(output/'model.json'), reference_sha256=digest(output/'reference.jsonl'),
                  owner_counts=spatial['owner_counts'], global_counts=global_tree['owner_counts'],
                  reference_engine='numpy float64 BLAS;1-dot; ascending distance then globalID',
                  numpy_version=np.__version__, blas_environment={k:os.environ.get(k) for k in
                      ['OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS']})
    (output/'prepared.json').write_text(json.dumps(result, indent=2)+'\n')
    from artifact_bindings import freeze_prepared
    freeze_prepared(output, vectors, queries)
    return result
