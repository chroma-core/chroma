# Real datasets for the scale study

The preferred scale study uses a real web corpus and measures 100M, 250M, 500M, and 1B prefixes only after the current reader correctness and build-convergence gates pass. Bulk download and a billion-vector run wait on correctness, reference-production, and capacity gates. These are engineering gates within the authorized sandbox work.

## Preferred corpus

The [publisher's dataset card](https://huggingface.co/datasets/Qdrant/FineWeb-10B/blob/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2/README.md) describes 10,074,324,060 documents with 768-dimensional float16 dense vectors from `Alibaba-NLP/gte-multilingual-base`. Its dense vectors are unit normalized and its instructions explicitly prohibit renormalization. The [pinned file manifest](https://huggingface.co/api/datasets/Qdrant/FineWeb-10B/revision/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2) identifies 10,148 corpus parquet files. Preserve source float16 values when converting them exactly to float32. Use inner-product distance, one minus dot product, as the proposed implementation path, pending numerical agreement with the reference producer. The existing MS MARCO cosine loader renormalizes vectors and must not be reused for this dataset.

The [dense query-set manifest](https://huggingface.co/datasets/Qdrant/FineWeb-10B/blob/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2/queries/_sets.json) identifies 100,000 queries and top-1000 answers for the full corpus. A prefix is a different search universe: recompute its exact answers. Filtering full-corpus top-1000 answers cannot recover omitted prefix neighbors. Preserve a stable row-to-document UUID mapping because published answers use document UUIDs, while the pilot uses integer IDs.

## Query reproduction gate

The [regeneration script](https://huggingface.co/datasets/Qdrant/FineWeb-10B/blob/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2/queries/scripts/regenerate_queries.py) reconstructs query text from MS MARCO identifiers and applies plain sentence-transformer encoding with no query prefix. It emits float32 vectors and defaults to batches of 128. Its model call and source query lookup are not pinned to immutable revisions. The current model revision observed through the public API is `9bbca17d9273fd0d03d5725c7a4b0f6b45142062`; the current MS MARCO dataset revision is `a47ee7aae8d7d466ba15f9f0bfac3b3681087b3a`. These observations are candidate reproduction pins, not proof that the publisher used those exact versions.

Before generating queries, freeze the model and tokenizer revisions, package versions, runtime and hardware, query-source files, batching, and output hashes. Recover the exact query identities and preserve their order. The [verification script](https://huggingface.co/datasets/Qdrant/FineWeb-10B/blob/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2/queries/scripts/verify_regeneration.py) checks query-text hashes; it does not validate embedding equality. Require a numerical reference comparison before using released answers. Freeze separate calibration and test pools before tuning.

## Resource and progression gates

A billion 768-dimensional float32 vectors require 3,072,000,000,000 raw vector bytes before index metadata, source parquet, temporary files, retained generations, and replicas. Source float16 values require half that raw amount, but the current worker stores float32. This is arithmetic, not a measured storage budget. The full query-reference task for 100,000 queries and a billion vectors contains 100 trillion vector comparisons. Budget exact scoring separately and measure a smaller real reference run before requesting that compute.

Preserve exact source ordering, source/output hashes, metric, ID mapping, and normalization policy at every prefix. The source file list must be pinned; a live branch name is insufficient. Advance only when measured build convergence, peak disk, navigation memory, read correctness, and exact-reference cost fit the next point. Larger real alternatives listed in the plan remain alternatives until their manifests and query/reference compatibility are independently verified. Repeated synthetic vectors do not establish a real billion-vector result.

The dataset card identifies separate terms for the corpus and the MS MARCO-derived queries. Query text and regenerated embeddings require their own reuse review before redistributing benchmark artifacts. Preserve attribution and link the publisher's licensing section with any released results.

## Completed exporter check

A bounded 100k real FineWeb prefix exports in 43.074 seconds with 295,997,376 fetched source payload bytes. Independent Polars decoding matches every converted vector value and UUID. A cache-only fresh retry reproduces the output hashes; existing output is rejected unchanged, and resume remains unsupported. This validates corpus preparation rather than index recall. The reports and pinned source manifest are in `subtree/fineweb`; query reproduction and exact prefix references remain separate gates.
