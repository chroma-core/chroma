---
pretty_name: Qdrant-FineWeb-10B
license: odc-by
language:
- en
size_categories:
- 10B<n<100B
---

# Qdrant-FineWeb-10B

## Overview
Qdrant-FineWeb-10B (Q-FineWeb-10B) is a 10-billion-vector retrieval benchmark derived from [FineWeb](https://huggingface.co/datasets/HuggingFaceFW/fineweb). Each document is represented with dense and sparse embeddings from [Alibaba-NLP/gte-multilingual-base](https://huggingface.co/Alibaba-NLP/gte-multilingual-base), alongside its original FineWeb payload and metadata. The benchmark also includes exact brute-force ground truth for ~120,000 MS MARCO queries.

The dataset includes:

- **10 billion dense embeddings**
- **10 billion sparse embeddings**
- **FineWeb document metadata and payloads**
- **~120,000 MS MARCO retrieval queries**
- **Exact top-1000 ground truth for dense, sparse, and filtered retrieval**


## Dataset Contents

The corpus is a 10,074,324,060-document slice of `HuggingFaceFW/fineweb`.

Each row carries the document's dense and sparse embeddings, and all eight FineWeb source fields are retained verbatim:

| Field | Type | Notes |
|---|---|---|
| `id` | `string` | FineWeb's `<urn:uuid:...>` document id |
| `text` | `string` | Full document text |
| `url` | `string` | Source URL |
| `dump` | `string` | Common Crawl dump id, e.g. `CC-MAIN-2016-44` |
| `date` | `string` | ISO-8601 crawl timestamp, e.g. `2016-10-20T19:25:59Z` |
| `language` | `string` | FineWeb language label (`en` throughout the observed shards) |
| `language_score` | `double` | fastText language-detection confidence, ≥ 0.65 |
| `token_count` | `int32` | FineWeb's GPT-2 token count |
| `dense_embedding` | `list<halffloat>` | 768-d mGTE dense vector, unit-norm |
| `sparse_embedding` | `struct<indices: list<uint32>, values: list<float>>` | mGTE sparse weights over token vocabulary, unnormalized |

### Embeddings
Each document is represented using [Alibaba-NLP/gte-multilingual-base](https://huggingface.co/Alibaba-NLP/gte-multilingual-base).

- **Dense:** 768 dimensions; Vectors are unit-norm. Compare with cosine, or equivalently dot product; do not re-normalize.
- **Sparse:** indices are mGTE token ids over the model's 250,048-token vocabulary; values are the corresponding token weights. The pair of lists is index-aligned and the vectors are not normalized - score with dot product.

### Queries and Ground Truth

The queries and the ground truth results are contained within a single file with the following format: 

| Column | Type | Meaning |
|---|---|---|
| `msmarco_config`, `msmarco_split`, `msmarco_query_id` | `string` | MS MARCO query identifier |
| `hit_scores` | `list<float>` | similarity scores in descending order |
| `hit_fineweb_ids` | `list<string>` | aligned FineWeb document UUIDs |

The filtered files append their query's filter definition after those five; the added columns are described under [Filtered Queries](#filtered-queries). To recover the query text and embeddings, see [Regenerating MS MARCO Embeddings](#regenerating-ms-marco-embeddings).

#### Queries

The benchmark contains **119,953 MS MARCO queries** divided across four query sets:

- **100,000 dense queries**
- **10,000 sparse queries**
- **4,953 text-filtered dense queries**
- **5,000 structured-filtered dense queries**

The query sets are derived from MS MARCO v1.1 and v2.1 — the dense, sparse, and structured-filter sets from v2.1 `test`, the text-filter set from v1.1 (`train` 4,119 / `validation` 492 / `test` 389). Scripts are provided to recover the original queries from MS MARCO and regenerate their embeddings. 

*Note that text-filtered set started at 5,000 queries; however, 47 of those queries matched no document and were dropped, leaving 4,953.*


#### Ground Truth

Exact top-1000 ground truth was computed with [Supernova's](https://github.com/qdrant-labs/supernova) `nova-bf` over the full 10-billion-vector corpus.

| File | Rows | Vector | Filter | MS MARCO | Query columns beyond the common five |
|---|---:|---|---|---|---|
| `gt_dense_k1000.parquet` | 100,000 | dense | none | v2.1 / test | — |
| `gt_sparse_k1000.parquet` | 10,000 | sparse | none | v2.1 / test | — |
| `gt_text_filters_k1000.parquet` | 4,953 | dense | text + domain | v1.1 / train + validation + test | `selectivity_tier`, `keyword_phrase`, `domains` |
| `gt_structured_filters_k1000.parquet` | 5,000 | dense | numeric + date + set | v2.1 / test | `selectivity_tier`, `structured_group`, `ls_gte`, `date_gte`, `date_lt`, `dump_set` |

Dense results use cosine similarity on unit-normalized vectors; sparse results use dot product. `hit_fineweb_ids` are stored as bare UUIDs rather than FineWeb's `<urn:uuid:...>` representation.

##### Filtered Queries

Text-filtered queries combine a required `keyword_phrase` with 1–9 `domains` matched by OR. They also include a `selectivity_tier` (`low`, `medium`, or `high`). Of the 4,953 retained queries, 557 have fewer than 1,000 matching documents; 47 queries with no matches were dropped.

Structured-filtered queries use combinations of language score (`ls_gte`), date range (`date_gte`, `date_lt`), and Common Crawl dump membership (`dump_set`):

| Group | Constraints |
|---|---|
| A | language score |
| B | date |
| C | dump set |
| D | date + dump set |
| E | language score + date + dump set |

Each group contains 1,000 queries, and all structured queries return 1,000 results. A bound a group does not constrain is set to `null`.


### Regenerating MS MARCO Embeddings

The provided scripts recover MS MARCO query text from the original source and regenerate the corresponding dense and sparse embeddings.

Requirements: `pyarrow`, `requests`, `torch`, `transformers`, `sentence-transformers`

All query sets:

```bash
python scripts/regenerate_all.py --sets . --out-dir regenerated/
```

A single set:

```bash
python scripts/regenerate_queries.py \
    --in gt_dense_k1000.parquet \
    --out regenerated/dense_regenerated.parquet \
    --vectors dense
```

The regenerated files preserve the original row order and add `query` plus the requested `dense_embedding` and/or `sparse_embedding` columns. MS MARCO splits are cached locally and reused; use `--device cuda` for GPU embedding.

`--limit N` is intended only for smoke tests: batching differences in `SentenceTransformer.encode()` can cause subset embeddings to differ from a full run by approximately `1e-7`.

##### Sanity Check

```bash
python scripts/verify_regeneration.py --sets . --regenerated regenerated/
```

This verifies recovered query text against `_checksums.json`; it does not compare embeddings.

##### Numerical Reproducibility

Supernova searches the float16 FineWeb embeddings using float32 queries. Although ties are resolved deterministically, floating-point arithmetic is not fully deterministic across implementations and hardware, so small numerical differences may perturb result ordering, particularly near the (k=1000) boundary.

### Intended Uses

Q-FineWeb-10B is intended for large-scale evaluation of:

- dense vector search
- sparse vector search
- filtered vector search
- hybrid retrieval
- vector database ingestion and indexing
- approximate nearest-neighbor algorithms
- distributed retrieval systems

It is also useful for work that needs a very large embedded corpus rather than a retrieval benchmark: quantization and compression studies, index-build cost and memory-footprint measurement, sharding and routing strategies, embedding-space analysis at web scale, and cost/recall trade-off curves where 10B vectors make the trade-off visible in a way 1M-vector benchmarks do not.



## Licensing

### Q-FineWeb-10B

The FineWeb corpus, embeddings, and ground-truth results are released under the ODC-BY-1.0 license.

Note that ODC-BY covers the database and its contents as assembled here; the underlying web pages remain subject to their own terms, and FineWeb's own note that use is additionally subject to the [Common Crawl Terms of Use](https://commoncrawl.org/terms-of-use) applies to this dataset as well.

### MS MARCO Queries

The ground truth was generated using MS MARCO queries, which remain subject to their original licensing terms. We do not redistribute MS MARCO query text or the derived embeddings; instead, the provided regeneration script downloads the queries directly from the original Hugging Face source and embeds them.

MS MARCO is made available by Microsoft for **non-commercial research purposes only**: "The MS MARCO datasets are intended for non-commercial research purposes only to promote advancement in the field of artificial intelligence and related areas, and is made available free of charge without extending any license or other intellectual property rights." Running the regeneration scripts places the resulting query text and vectors under those terms, independent of this dataset's ODC-BY license.

Users should review the [MS MARCO license](https://microsoft.github.io/msmarco/) before use.

## Acknowledgments
Thank you to [Vulr](https://www.vultr.com/) for partnering with us to generate the FineWeb embeddings and [HuggingFace](https://huggingface.co/) for providing a storage grant to host this dataset. Additionally, this dataset would not exist without the upstream work it is built on:

- **HuggingFaceFW** and the FineWeb authors — Guilherme Penedo, Hynek Kydlíček, Loubna Ben Allal, Anton Lozhkov, Margaret Mitchell, Colin Raffel, Leandro Von Werra, and Thomas Wolf — for releasing FineWeb under ODC-BY and documenting its construction in enough detail to build on.
- **Common Crawl**, whose crawls inform FineWeb and therefore this corpus.
- **Alibaba-NLP / Tongyi Lab** for `gte-multilingual-base` and the mGTE work, released under Apache 2.0.
- **Microsoft** for MS MARCO, the source of the benchmark queries.