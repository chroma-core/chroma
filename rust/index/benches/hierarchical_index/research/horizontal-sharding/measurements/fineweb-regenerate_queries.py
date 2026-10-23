#!/usr/bin/env python3
"""Rebuild the MS MARCO queries + their embeddings for a stripped release file.

The released files carry results only. Each row names where its query lives in
Hugging Face `microsoft/ms_marco` (`msmarco_config` / `msmarco_split` /
`msmarco_query_id`); this script fetches those queries, re-embeds them with the
models the ground truth was built with, and joins everything back to the hits.

    python scripts/regenerate_queries.py \
        --in  gt_dense_k1000.parquet \
        --out regenerated/dense_regenerated.parquet \
        --vectors dense

Vectors are reproduced with the exact conventions of the original run:

  dense   Alibaba-NLP/gte-multilingual-base via sentence-transformers, plain
          encode() (the model's own ST config ends in a Normalize module, so
          output is unit-norm), float32, no query prefix. Verified against the
          released vectors at cosine >= 0.9999999.

  sparse  mGTE's OFFICIAL sparse scheme: relu of the per-token
          AutoModelForTokenClassification logit, token-id keys, special tokens
          dropped, max-dedup, max_length=8192. Verified to reproduce the
          released sparse vectors exactly (identical token sets, cosine
          1.0000000).

          NOTE: sentence-transformers' SparseEncoder does NOT produce this
          scheme for gte -- it attaches a randomly-initialized SPLADE head.
          Neither does `nova embed`, whose sparse backend is that SparseEncoder.
          Do not substitute either one.

Only `--vectors none` avoids the torch/sentence-transformers dependency.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATASETS_SERVER = "https://datasets-server.huggingface.co/parquet"
DENSE_MODEL = "Alibaba-NLP/gte-multilingual-base"
SPARSE_MODEL = "Alibaba-NLP/gte-multilingual-base"
MAX_LENGTH = 8192


# --------------------------------------------------------------------------
# MS MARCO query text
# --------------------------------------------------------------------------
def _download(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 22):
                fh.write(chunk)
    tmp.rename(dst)


def load_query_text(
    needed: set[tuple[str, str]], cache: Path, keep_raw: bool
) -> dict[tuple[str, str, str], str]:
    """(config, split, query_id) -> query, fetching only the SPLITS needed.

    Keyed on (config, split) throughout, which matters twice:

    * Download size. the dense set needs only v2.1/test (204 MB); keying on config
      alone would pull all of v2.1 — 7 train shards plus validation, ~2.1 GB —
      to answer a question none of it can answer.
    * Cache correctness. The cache is written one file per split, so "this
      config has a cache file" does not mean "every split I need is cached".
      Interrupting a download once left later runs convinced they were done,
      failing far away with an unresolved-rows error.
    """
    lookup: dict[tuple[str, str, str], str] = {}
    todo = set(needed)

    for cfg, split in sorted(needed):
        hit = cache / f"qids_{cfg}_{split}.parquet"
        if hit.exists():
            t = pq.read_table(hit).to_pydict()
            lookup.update(
                ((cfg, split, str(i)), q) for i, q in zip(t["query_id"], t["query"])
            )
            todo.discard((cfg, split))
            print(f"  {cfg}/{split}: {len(t['query_id']):,} queries from cache")

    if todo:
        listing = requests.get(DATASETS_SERVER, params={"dataset": "microsoft/ms_marco"}, timeout=120)
        listing.raise_for_status()
        files = [
            f for f in listing.json()["parquet_files"] if (f["config"], f["split"]) in todo
        ]
        per_split: dict[tuple[str, str], dict[str, str]] = {}
        for f in files:
            raw = cache / "raw" / f"{f['config']}_{f['split']}_{f['filename']}"
            if not raw.exists():
                print(f"  downloading {f['config']}/{f['split']}/{f['filename']} "
                      f"({f['size']/1e6:.0f} MB)", flush=True)
                _download(f["url"], raw)
            t = pq.read_table(raw, columns=["query_id", "query"]).to_pydict()
            per_split.setdefault((f["config"], f["split"]), {}).update(
                (str(i), q) for i, q in zip(t["query_id"], t["query"])
            )
            if not keep_raw:
                raw.unlink()
        for (cfg, split), m in per_split.items():
            pq.write_table(
                pa.table({"query_id": list(m), "query": list(m.values())}),
                cache / f"qids_{cfg}_{split}.parquet",
                compression="zstd",
            )
            lookup.update(((cfg, split, i), q) for i, q in m.items())
            print(f"  {cfg}/{split}: {len(m):,} queries cached")
    return lookup


# --------------------------------------------------------------------------
# embedding
# --------------------------------------------------------------------------
def embed_dense(texts: list[str], batch_size: int, device: str | None):
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(DENSE_MODEL, trust_remote_code=True, device=device)
    return model.encode(texts, batch_size=batch_size, convert_to_numpy=True,
                        show_progress_bar=True).astype("float32")


def embed_sparse(texts: list[str], batch_size: int, device: str | None):
    """mGTE official sparse -- see module docstring."""
    import torch
    from transformers import AutoModelForTokenClassification, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(SPARSE_MODEL)
    model = AutoModelForTokenClassification.from_pretrained(SPARSE_MODEL, trust_remote_code=True)
    dev = device or ("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(dev).eval()
    specials = set(tok.all_special_ids)

    out = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start : start + batch_size]
        enc = tok(chunk, padding=True, truncation=True, max_length=MAX_LENGTH,
                  return_tensors="pt").to(dev)
        with torch.no_grad():
            weights = torch.relu(model(**enc).logits).squeeze(-1)
        ids_b = enc["input_ids"].tolist()
        mask_b = enc["attention_mask"].tolist()
        for ids, mask, ws in zip(ids_b, mask_b, weights.tolist()):
            acc: dict[int, float] = {}
            for tid, keep, w in zip(ids, mask, ws):
                if not keep or tid in specials or w <= 0:
                    continue
                acc[tid] = max(acc.get(tid, 0.0), w)   # max-dedup
            items = sorted(acc.items())
            out.append({"indices": [k for k, _ in items], "values": [v for _, v in items]})
        print(f"  sparse {min(start+batch_size, len(texts)):,}/{len(texts):,}",
              end="\r", file=sys.stderr, flush=True)
    print(file=sys.stderr)
    return out


# --------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="inp", required=True, help="stripped release parquet")
    ap.add_argument("--out", required=True)
    ap.add_argument("--vectors", choices=["dense", "sparse", "both", "none"], default="dense")
    ap.add_argument("--cache", default="data/msmarco_cache")
    ap.add_argument("--keep-raw", action="store_true",
                    help="keep the downloaded MS MARCO parquets (~2.1 GB) instead of "
                         "deleting them once the small id->query cache is built")
    ap.add_argument("--batch-size", type=int, default=128)
    ap.add_argument("--device", default=None, help="cuda / cpu (default: auto)")
    ap.add_argument("--limit", type=int, default=None, help="first N rows only (smoke test)")
    args = ap.parse_args()

    src = Path(args.inp)
    pf = pq.ParquetFile(src)
    names = pf.schema_arrow.names
    for required in ("msmarco_config", "msmarco_split", "msmarco_query_id"):
        if required not in names:
            raise SystemExit(f"{src.name} has no `{required}` column -- is it a stripped release file?")

    prov = pq.read_table(src, columns=["msmarco_config", "msmarco_split", "msmarco_query_id"])
    if args.limit is not None:
        prov = prov.slice(0, args.limit)
    keys = list(zip(prov.column(0).to_pylist(), prov.column(1).to_pylist(), prov.column(2).to_pylist()))
    print(f"{src.name}: {len(keys):,} rows, configs={sorted({k[0] for k in keys})}")

    cache = Path(args.cache)
    cache.mkdir(parents=True, exist_ok=True)
    lookup = load_query_text({(c, s) for c, s, _ in keys}, cache, args.keep_raw)

    missing = [k for k in keys if k not in lookup]
    if missing:
        raise SystemExit(f"{len(missing)} rows unresolved, e.g. {missing[:3]}")
    queries = [lookup[k] for k in keys]
    print(f"recovered {len(queries):,} query strings")

    dense = sparse = None
    if args.vectors in ("dense", "both"):
        print(f"embedding dense with {DENSE_MODEL}")
        dense = embed_dense(queries, args.batch_size, args.device)
    if args.vectors in ("sparse", "both"):
        print(f"embedding sparse with {SPARSE_MODEL} (official mGTE scheme)")
        sparse = embed_sparse(queries, args.batch_size, args.device)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    writer = None
    pos = 0
    try:
        for batch in pf.iter_batches(batch_size=2048):
            n = batch.num_rows
            if args.limit is not None and pos >= args.limit:
                break
            if args.limit is not None and pos + n > args.limit:
                batch = batch.slice(0, args.limit - pos)
                n = batch.num_rows
            arrays = list(batch.columns) + [pa.array(queries[pos : pos + n], pa.string())]
            out_names = list(batch.schema.names) + ["query"]
            if dense is not None:
                # float64 to match the released files' `list<double>`: an
                # exact upcast from the float32 the model produces, and the
                # only way `pa.concat_tables([original, regenerated])` works.
                arrays.append(pa.array([r.tolist() for r in dense[pos : pos + n]],
                                       pa.list_(pa.float64())))
                out_names.append("dense_embedding")
            if sparse is not None:
                # Likewise int64/double, matching the released sparse struct.
                arrays.append(pa.array(sparse[pos : pos + n],
                                       pa.struct([("indices", pa.list_(pa.int64())),
                                                  ("values", pa.list_(pa.float64()))])))
                out_names.append("sparse_embedding")
            rb = pa.RecordBatch.from_arrays(arrays, names=out_names)
            if writer is None:
                writer = pq.ParquetWriter(args.out, rb.schema, compression="zstd")
            writer.write_batch(rb)
            pos += n
    finally:
        if writer is not None:
            writer.close()
    print(f"wrote {pos:,} rows -> {args.out}")


if __name__ == "__main__":
    main()
