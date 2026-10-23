"""Download a pinned public corpus into an isolated directory and reserve query rows."""
import argparse
import hashlib
import json
import time
import urllib.request
from pathlib import Path
import numpy as np
from .benchmark import split_queries, write_jsonl

REPO = 'CohereLabs/msmarco-v2-embed-multilingual-v3'
REVISION = 'aaaddf8cd10328d4ccd5755832c2cc27773ceef6'
DIMENSION = 1024


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def prepare(output, count=1_000_000, calibration=100, test=100, normalize=False):
    import pyarrow.parquet as pq
    if min(count, calibration, test) < 1:
        raise ValueError('positive counts required')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    # Refuse to overwrite an existing run or reuse another benchmark's cache.
    if any(output.iterdir()):
        raise ValueError('output directory must be empty')
    started = time.perf_counter()
    rows_seen = 0
    queries, files = [], []
    norms_min, norms_max = float('inf'), 0.
    target = count + calibration + test
    corpus = output / 'corpus.f32'
    with open(corpus, 'wb') as writer:
        for shard in range(139):
            relative = f'corpus/{shard:04}.parquet'
            url = f'https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/{relative}'
            local = output / f'source-{shard:04}.parquet'
            urllib.request.urlretrieve(url, local)
            files.append(dict(source_path=relative, url=url, bytes=local.stat().st_size, sha256=sha256(local)))
            parquet = pq.ParquetFile(local)
            for batch in parquet.iter_batches(batch_size=4096, columns=['emb']):
                values = batch.column(0).to_pylist()
                if any(v is None or len(v) != DIMENSION for v in values):
                    raise ValueError('null or wrong-dimensional source row')
                x = np.asarray(values, dtype='<f4')
                if not np.isfinite(x).all():
                    raise ValueError('nonfinite source vector')
                norms = np.linalg.norm(x.astype(np.float64), axis=1)
                if np.any(norms == 0):
                    raise ValueError('zero source vector')
                norms_min, norms_max = min(norms_min, float(norms.min())), max(norms_max, float(norms.max()))
                if normalize:
                    x = (x.astype(np.float64) / norms[:, None]).astype('<f4')
                for vector in x:
                    if rows_seen >= target:
                        break
                    if rows_seen < count:
                        writer.write(vector.tobytes())
                    else:
                        queries.append(dict(query_id=f'source-row-{rows_seen}', source_row=rows_seen, vector=vector.tolist()))
                    rows_seen += 1
                if rows_seen >= target:
                    break
            if rows_seen >= target:
                break
    if rows_seen != target:
        raise ValueError('source exhausted')
    a, b = split_queries(queries, calibration, test, 1729)
    write_jsonl(output / 'queries.calibration.jsonl', a)
    write_jsonl(output / 'queries.test.jsonl', b)
    manifest = dict(dataset=REPO, revision=REVISION, dimension=DIMENSION,
        corpus_rows=count, corpus_source_range=[0, count], heldout_source_range=[count, target],
        source_order='ascending corpus/NNNN.parquet then parquet row order; null rows rejected',
        query_provenance='reserved corpus rows excluded from indexed corpus; not independent search-query embeddings',
        normalization='float64 L2 normalization rounded to float32' if normalize else 'none; original float32',
        source_norm_range=[norms_min, norms_max], split_seed=1729, calibration_count=calibration,
        test_count=test, source_files=files, preparation_wall_s=time.perf_counter()-started,
        outputs={p.name: dict(bytes=p.stat().st_size, sha256=sha256(p)) for p in
                 [corpus, output/'queries.calibration.jsonl', output/'queries.test.jsonl']})
    (output/'provenance.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', required=True)
    p.add_argument('--count', type=int, default=1_000_000)
    p.add_argument('--calibration', type=int, default=100)
    p.add_argument('--test', type=int, default=100)
    p.add_argument('--normalize', action='store_true')
    a = p.parse_args()
    print(json.dumps(prepare(a.output, a.count, a.calibration, a.test, a.normalize)))

if __name__ == '__main__':
    main()
