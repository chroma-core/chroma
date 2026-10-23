#!/usr/bin/env python3
"""Check that a regenerated file recovered the right MS MARCO queries.

    python scripts/verify_regeneration.py --sets . --regenerated regenerated/

Compares the SHA-256 of the recovered `query` column against the fingerprints
in `_checksums.json`. This is the check worth running: MS MARCO v1.1 and v2.1
reuse the same numeric query ids for different queries, so a config mix-up
returns plausible text for the wrong question and silently corrupts every
recall number computed from it. The hash catches that on the first row.

A mismatch means the join is wrong, not that your embeddings are wrong -- this
does not check vectors. See the README on reproducibility for those.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sets", default=".", help="bundle dir holding _checksums.json")
    ap.add_argument("--regenerated", required=True, help="dir written by regenerate_all.py")
    args = ap.parse_args()

    expected = json.loads((Path(args.sets) / "_checksums.json").read_text())
    out_dir = Path(args.regenerated)

    failures, checked = [], 0
    for name, want in expected.items():
        path = out_dir / f"{name}_regenerated.parquet"
        if not path.exists():
            print(f"{name:19} SKIP  (not regenerated)")
            continue

        pf = pq.ParquetFile(path)
        if "query" not in pf.schema_arrow.names:
            failures.append(f"{name}: no `query` column")
            print(f"{name:19} FAIL  no `query` column")
            continue

        h, rows = hashlib.sha256(), 0
        for batch in pf.iter_batches(batch_size=8192, columns=["query"]):
            for q in batch.column(0).to_pylist():
                h.update(q.encode())
                h.update(b"\0")
                rows += 1

        checked += 1
        if rows != want["rows"]:
            failures.append(f"{name}: {rows:,} rows, expected {want['rows']:,}")
            print(f"{name:19} FAIL  {rows:,} rows, expected {want['rows']:,}")
        elif h.hexdigest() != want["query_text_sha256"]:
            failures.append(f"{name}: query text does not match")
            print(f"{name:19} FAIL  query text mismatch -- wrong config/split, "
                  f"or rows reordered")
        else:
            print(f"{name:19} ok    {rows:,} rows")

    if failures:
        raise SystemExit(f"\n{len(failures)} set(s) failed:\n  " + "\n  ".join(failures))
    if checked == 0:
        raise SystemExit("\nnothing checked -- is --regenerated pointing at the right dir?")
    print(f"\n{checked} set(s) verified")


if __name__ == "__main__":
    main()
