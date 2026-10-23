"""Prepare the fixed 100M MS MARCO prefix without changing other dataset caches."""
import concurrent.futures
import json
import os
from pathlib import Path
import time

import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

ROOT = Path('/mnt/msmarco')
REPO = 'CohereLabs/msmarco-v2-embed-multilingual-v3'
REVISION = 'aaaddf8cd10328d4ccd5755832c2cc27773ceef6'
CACHE = ROOT / 'huggingface/hub'
REPO_CACHE = CACHE / 'datasets--CohereLabs--msmarco-v2-embed-multilingual-v3'


def fetch(i):
    name = f'corpus/{i:04d}.parquet'
    old = Path('/mnt/data/huggingface/hub') / REPO_CACHE.name / 'snapshots' / REVISION / name
    dest = REPO_CACHE / 'snapshots' / REVISION / name
    if old.exists() and not dest.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.symlink_to(old.resolve())
    for attempt in range(6):
        try:
            path = dest if dest.exists() else Path(hf_hub_download(REPO, name, repo_type='dataset', revision=REVISION, cache_dir=CACHE))
            metadata = pq.ParquetFile(path).metadata
            if metadata.num_rows != 1_000_000:
                raise ValueError(f'Unexpected row count: {name}: {metadata.num_rows}')
            print(f'Ready {name}: {metadata.num_rows:,} rows, {path.stat().st_size:,} bytes', flush=True)
            return {'filename': name, 'rows': metadata.num_rows, 'bytes': path.stat().st_size, 'start_id': i * 1_000_000, 'end_id': (i + 1) * 1_000_000, 'path': str(path)}
        except Exception:
            if attempt == 5:
                raise
            time.sleep(10 * (attempt + 1))


if __name__ == '__main__':
    CACHE.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        shards = list(pool.map(fetch, range(100)))
    refs = REPO_CACHE / 'refs'
    refs.mkdir(parents=True, exist_ok=True)
    (refs / 'main').write_text(REVISION)
    out = ROOT / 'results/msmarco_100m_sources.json'
    out.write_text(json.dumps({'repo_id': REPO, 'revision': REVISION, 'dimensions': 1024, 'prefix_vectors': 100_000_000, 'shards': shards}, indent=2) + '\n')
    print(f'100M source prefix prepared: {out}', flush=True)
