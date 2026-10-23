"""Bounded-memory CPU exact references with float64 BLAS accumulation."""
import argparse
import json
import os
import time
from pathlib import Path
import numpy as np
from .benchmark import read_jsonl, write_jsonl


def exact_blocked(path, queries, dimension, k=100, metric='l2', id_offset=0,
                  id_stride=1, corpus_block=8192, query_block=32):
    if min(dimension, k, id_stride, corpus_block, query_block) < 1 or id_offset < 0 or metric not in ('l2', 'ip', 'cosine'):
        raise ValueError('invalid reference settings')
    size = Path(path).stat().st_size
    if size % (4 * dimension):
        raise ValueError('truncated f32 row')
    n = size // (4 * dimension)
    if not n:
        raise ValueError('empty corpus')
    source = np.memmap(path, dtype='<f4', mode='r', shape=(n, dimension))
    q = np.asarray([row['vector'] for row in queries], dtype=np.float64)
    if q.shape != (len(queries), dimension) or not np.isfinite(q).all():
        raise ValueError('invalid query vectors')
    qnorm = np.einsum('ij,ij->i', q, q)
    if metric == 'cosine' and np.any(qnorm == 0):
        raise ValueError('cosine requires nonzero vectors')
    # Only top-k state survives corpus blocks. lexsort includes global IDs at
    # the selection boundary, where argpartition alone would lose tied IDs.
    best = [(np.empty(0, dtype=np.int64), np.empty(0)) for _ in queries]
    for start in range(0, n, corpus_block):
        x = np.array(source[start:start + corpus_block], dtype=np.float64)
        if not np.isfinite(x).all():
            raise ValueError('nonfinite corpus vector')
        norms = np.einsum('ij,ij->i', x, x)
        if metric == 'cosine' and np.any(norms == 0):
            raise ValueError('cosine requires nonzero vectors')
        ids = id_offset + np.arange(start, start + len(x), dtype=np.int64) * id_stride
        for qs in range(0, len(q), query_block):
            dots = q[qs:qs + query_block] @ x.T
            if metric == 'l2':
                distances = np.maximum(qnorm[qs:qs + query_block, None] + norms[None, :] - 2 * dots, 0)
            elif metric == 'ip':
                distances = 1 - dots
            else:
                distances = 1 - dots / np.sqrt(qnorm[qs:qs + query_block, None] * norms[None, :])
            for local, d in enumerate(distances):
                i = qs + local
                oldids, olddist = best[i]
                allids, alldist = np.concatenate((oldids, ids)), np.concatenate((olddist, d))
                selected = np.lexsort((allids, alldist))[:k]
                best[i] = allids[selected], alldist[selected]
    return [dict(query_id=row['query_id'], ids=ids.tolist(), distances=d.tolist())
            for row, (ids, d) in zip(queries, best)]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('vectors', 'queries', 'output'):
        p.add_argument('--' + key, required=True)
    p.add_argument('--dimension', type=int, required=True)
    p.add_argument('--metric', choices=('l2', 'ip', 'cosine'), default='cosine')
    for key, default in [('k', 100), ('id-offset', 0), ('id-stride', 1), ('corpus-block', 8192), ('query-block', 32)]:
        p.add_argument('--' + key, type=int, default=default)
    a = p.parse_args()
    started = time.perf_counter()
    rows = exact_blocked(a.vectors, read_jsonl(a.queries), a.dimension, a.k, a.metric,
                         a.id_offset, a.id_stride, a.corpus_block, a.query_block)
    write_jsonl(a.output, rows)
    Path(a.output + '.metadata.json').write_text(json.dumps(dict(
        engine='numpy_cpu_blas_float64', numpy_version=np.__version__, metric=a.metric,
        tie_policy='ascending_distance_then_global_id', wall_s=time.perf_counter()-started,
        corpus_rows=Path(a.vectors).stat().st_size//(4*a.dimension), queries=len(rows),
        settings=vars(a), blas_thread_environment={k: os.environ.get(k) for k in
        ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS')}), indent=2)+'\n')

if __name__ == '__main__':
    main()
