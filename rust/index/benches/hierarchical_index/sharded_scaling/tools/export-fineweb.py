"""Export a FineWeb prefix as one little-endian float32 file in canonical order.

The canonical order is the pinned source manifest's file order, then parquet
row order; global row IDs start at zero. Each 768-dimensional float16 dense
embedding converts exactly to float32, with no normalization. This matches
the existing 1M, 3M and 10M FineWeb inputs, which the run checks: the SHA-256
of the output's first `--check-rows` rows must equal `--check-sha256`.

The export reads only the `dense_embedding` column, through HTTP range reads
of the pinned immutable URLs. It first reads every needed file's parquet
footer to learn its row count, so each file's rows have a fixed offset in the
output. Files then download in parallel and write at their offsets. A file's
completion is recorded in `progress.jsonl`; a rerun skips recorded files.

Usage: python export-fineweb.py --manifest source-manifest-v2.json --rows 100000000
           --output /data/fineweb100m.f32 [--workers 24]
           [--check-rows 10000000 --check-sha256 HEX]
"""
import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import time
from pathlib import Path

import fsspec
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DIM = 768
ROW_BYTES = DIM * 4


def open_remote(url):
    fs = fsspec.filesystem('https')
    return fs.open(url, 'rb', block_size=8 * 2**20, cache_type='readahead')


def footer_rows(url):
    for attempt in range(6):
        try:
            with open_remote(url) as f:
                return pq.ParquetFile(f).metadata.num_rows
        except Exception:
            if attempt == 5:
                raise
            time.sleep(2 ** attempt)


def export_file(url, offset, rows, take, out_path):
    """Write the first `take` rows of one source file at row `offset`."""
    for attempt in range(6):
        try:
            written = 0
            with open_remote(url) as f, open(out_path, 'r+b') as out:
                pf = pq.ParquetFile(f)
                if pf.metadata.num_rows != rows:
                    raise ValueError(f'{url}: row count changed')
                field = pf.schema_arrow.field('dense_embedding').type
                if not pa.types.is_float16(field.value_type):
                    raise ValueError(f'{url}: dense_embedding is not float16')
                out.seek((offset) * ROW_BYTES)
                for batch in pf.iter_batches(batch_size=65536, columns=['dense_embedding'], use_threads=False):
                    if written >= take:
                        break
                    col = batch.column(0)
                    n = min(len(col), take - written)
                    col = col.slice(0, n)
                    if col.null_count:
                        raise ValueError(f'{url}: null embedding')
                    lengths = np.diff(col.offsets.to_numpy()) if hasattr(col, 'offsets') else None
                    if lengths is not None and not (lengths == DIM).all():
                        raise ValueError(f'{url}: embedding of wrong length')
                    values = col.flatten().to_numpy(zero_copy_only=False)
                    if values.size != n * DIM:
                        raise ValueError(f'{url}: wrong value count')
                    out.write(values.astype(np.float16, copy=False).astype('<f4').tobytes())
                    written += n
            if written != take:
                raise ValueError(f'{url}: wrote {written} of {take} rows')
            return written
        except Exception:
            if attempt == 5:
                raise
            time.sleep(2 ** attempt)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--manifest', required=True)
    p.add_argument('--rows', type=int, required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--workers', type=int, default=24)
    p.add_argument('--check-rows', type=int)
    p.add_argument('--check-sha256')
    a = p.parse_args()
    manifest = json.loads(Path(a.manifest).read_text())
    assert manifest['dimension'] == DIM and manifest['value_policy'].startswith('exact float16 to float32')
    out = Path(a.output)
    plan_path = out.with_suffix('.plan.json')
    progress_path = out.with_suffix('.progress.jsonl')
    began = time.time()
    if plan_path.exists():
        plan = json.loads(plan_path.read_text())
    else:
        plan, offset = [], 0
        files = iter(manifest['files'])
        # Footers in parallel, in batches, until the prefix is covered.
        with cf.ThreadPoolExecutor(32) as pool:
            while offset < a.rows:
                batch = [next(files) for _ in range(32)]
                for f, rows in zip(batch, pool.map(lambda f: footer_rows(f['url']), batch)):
                    if offset >= a.rows:
                        break
                    take = min(rows, a.rows - offset)
                    plan.append(dict(path=f['path'], url=f['url'], offset=offset, rows=rows, take=take))
                    offset += take
        plan_path.write_text(json.dumps(dict(manifest_revision=manifest['revision'], rows=a.rows, files=plan), indent=1))
        plan = json.loads(plan_path.read_text())
    files = plan['files']
    print(json.dumps(dict(files=len(files), footer_seconds=time.time() - began)), flush=True)
    if not out.exists():
        with open(out, 'wb') as f:
            f.truncate(a.rows * ROW_BYTES)
    done = set()
    if progress_path.exists():
        done = {json.loads(l)['path'] for l in progress_path.read_text().splitlines() if l.strip()}
    todo = [f for f in files if f['path'] not in done]
    with cf.ThreadPoolExecutor(a.workers) as pool, open(progress_path, 'a') as progress:
        futures = {pool.submit(export_file, f['url'], f['offset'], f['rows'], f['take'], out): f for f in todo}
        for fut in cf.as_completed(futures):
            f = futures[fut]
            fut.result()
            progress.write(json.dumps(dict(path=f['path'], offset=f['offset'], take=f['take'], at=time.time() - began)) + '\n')
            progress.flush()
            print(json.dumps(dict(done=f['path'], elapsed=round(time.time() - began, 1))), flush=True)
    os.sync()
    h, total = hashlib.sha256(), 0
    check = None
    with open(out, 'rb') as f:
        while True:
            b = f.read(64 * 2**20)
            if not b:
                break
            if a.check_rows and check is None and total + len(b) >= a.check_rows * ROW_BYTES:
                # The running hash at this boundary is the prefix's hash.
                cut = a.check_rows * ROW_BYTES - total
                h.update(b[:cut])
                check = h.copy().hexdigest()
                h.update(b[cut:])
            else:
                h.update(b)
            total += len(b)
    result = dict(rows=a.rows, bytes=total, sha256=h.hexdigest(), files=len(files), seconds=time.time() - began,
                  manifest_revision=plan['manifest_revision'], prefix_check_rows=a.check_rows,
                  prefix_check_sha256=check, prefix_check_passed=(check == a.check_sha256) if a.check_sha256 else None)
    out.with_suffix('.result.json').write_text(json.dumps(result, indent=1))
    print(json.dumps(result), flush=True)
    if a.check_sha256 and check != a.check_sha256:
        raise SystemExit('prefix check failed')


if __name__ == '__main__':
    main()
