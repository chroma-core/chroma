#!/usr/bin/env python3
"""Cache the ordered 10M-vector prefix of a benchmark dataset on local NVMe."""

import argparse
import json
import time
from pathlib import Path

import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

DATASETS = {
    "msmarco": ("CohereLabs/msmarco-v2-embed-multilingual-v3", "corpus"),
    "wikipedia": ("CohereLabs/wikipedia-2023-11-embed-multilingual-v3", "en"),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=DATASETS)
    parser.add_argument("--cache-dir", type=Path, default=Path("/mnt/data/huggingface/hub"))
    parser.add_argument("--manifest-dir", type=Path, default=Path("/home/ubuntu/benchmark-data"))
    args = parser.parse_args()
    repo, subdir = DATASETS[args.dataset]
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    args.manifest_dir.mkdir(parents=True, exist_ok=True)
    shards = []
    count = 0
    started = time.time()
    for idx in range(415 if args.dataset == "wikipedia" else 139):
        filename = f"{subdir}/{idx:04d}.parquet"
        for attempt in range(5):
            try:
                path = Path(hf_hub_download(repo, filename, repo_type="dataset", cache_dir=args.cache_dir))
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                delay = 15 * (attempt + 1)
                print(f"Retry {filename} after {exc}: {delay}s", flush=True)
                time.sleep(delay)
        metadata = pq.ParquetFile(path).metadata
        rows = metadata.num_rows
        shards.append({"filename": filename, "rows": rows, "bytes": path.stat().st_size, "start_id": count, "end_id": count + rows, "path": str(path)})
        count += rows
        print(f"{args.dataset}: {filename}: {rows:,} rows; prefix {count:,}; elapsed {time.time() - started:.1f}s", flush=True)
        if count >= 40_000_000:
            break
    manifest = {"dataset": args.dataset, "repo_id": repo, "dimensions": 1024, "vector_column": "emb", "prefix_vectors": 40_000_000, "downloaded_rows": count, "shards": shards}
    path = args.manifest_dir / f"{args.dataset}_40m_sources.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote {path}", flush=True)


if __name__ == "__main__":
    main()
