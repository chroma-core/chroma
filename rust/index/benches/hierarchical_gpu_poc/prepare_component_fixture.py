#!/usr/bin/env python3
"""Build a component fixture from a public Wikipedia embedding shard."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--revision", default="main")
    args = parser.parse_args()
    repo = "CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
    filename = "en/0000.parquet"
    source = Path(hf_hub_download(repo_id=repo, filename=filename,
                                  repo_type="dataset", revision=args.revision))
    dim = 1024
    dtype = np.dtype([("id", "<u4"), ("version", "<u4"),
                      ("vector", "<f4", (dim,))])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with args.output.open("wb") as target:
        target.write(b"\0" * 32)
        for batch in pq.ParquetFile(source).iter_batches(
            batch_size=10000, columns=["emb"]
        ):
            if count >= args.count:
                break
            values = batch.column(0)
            if values.null_count or len(values.values) != len(values) * dim:
                raise ValueError("unexpected embedding shape or null values")
            take = min(len(values), args.count - count)
            vectors = values.values.to_numpy(zero_copy_only=False)
            records = np.empty(take, dtype=dtype)
            records["id"] = np.arange(count, count + take, dtype=np.uint32)
            records["version"] = 0
            records["vector"] = vectors[:take * dim].reshape(take, dim)
            target.write(records.tobytes())
            count += take
        if count != args.count:
            raise ValueError(f"source has only {count} vectors")
        target.seek(0)
        target.write(struct.pack("<8sIIQII", b"HSPNSPL1", count, dim, 0, 0, 0))
    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    print(json.dumps({"path": str(args.output), "rows": count, "dim": dim,
                      "source": f"{repo}/{filename}", "revision": args.revision,
                      "sha256": digest}))


if __name__ == "__main__":
    main()
