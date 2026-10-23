# Reproducing a small FineWeb query check

Query regeneration is a separate artifact from corpus export. The completed first check recovers the publisher's query identities and text, embeds 16 recorded query rows, and verifies finite 768-dimensional output and repeatability. It establishes plumbing and numerical conventions; it does not establish subset recall or reproduce full-corpus ground truth.

## Immutable inputs

- Pin the FineWeb corpus and query-set repository to `3aea0b8d5f98741e343d07f067ce0f49dca1ccd2`. The dense query-identity and answer file is `queries/gt_dense_k1000.parquet`, 2,126,961,576 bytes, with publisher SHA-256 `847989d235ecd8c7118b64709cfe5d54ff36b82ad8c70c9de31c5347633bbd81`.
- Pin the model and tokenizer repository, `Alibaba-NLP/gte-multilingual-base`, to observed revision `9bbca17d9273fd0d03d5725c7a4b0f6b45142062`. Preserve tokenizer files, pooling/normalization modules, model configuration, and their hashes.
- Pin the separate implementation repository, `Alibaba-NLP/new-impl`, to observed revision `40ced75c3017eb27626c9d4ea981bde21a2662f4`. The model's configuration points its automatic model/configuration classes at this repository. Pinning the weights alone leaves the trusted remote implementation unpinned.
- Pin `microsoft/ms_marco` to observed revision `a47ee7aae8d7d466ba15f9f0bfac3b3681087b3a`; the dense set joins `v2.1/test-00000-of-00001.parquet` by query ID. Retrieve exact object size/hash metadata and preserve fetched-range hashes before reading it.

These are observed candidate reproduction pins. They do not establish which exact revisions the publisher uses when producing its released answers. The publisher's pinned regeneration script and text-checksum file remain the behavioral reference. The original query terms apply separately from the corpus terms.

## Recover and verify text without embedding the whole pool

Project only `msmarco_config`, `msmarco_split`, and `msmarco_query_id` from the dense identity file. Project only query IDs and query text from the pinned MS MARCO source. Use the range reader's source-byte and wall-time limits, preserve row order, and reject missing or duplicate join keys. Verify the full reconstructed query-text hash when the small projected text columns fit the preparation budget. The publisher's verification checks text, not embeddings.

Choose 16 query rows with a recorded seed for the first model check. Save the full identities, selected order, source text hashes, and query-text artifact before loading the model. A partial embedding run must remain labeled a smoke check: batching differences can affect final bits, so a 16-row batch is not proof that all 100,000 released-query embeddings are reproduced.

## Freeze the runtime before the first model call

Create an isolated CPU environment on the driver. Record its exact Python, NumPy, torch, transformers, sentence-transformers, tokenizer, and supporting package versions in a lockfile, along with the operating system, CPU model, thread limits, and model configuration. Hash that lockfile. The exporter currently tests NumPy 2.5.4, PyArrow 26.0.0, and Polars 2.0.0; those versions do not substitute for a tested model-runtime lock. The completed CPU smoke environment records Python 3.12.3, torch 2.5.1+cpu, transformers 4.48.3, sentence-transformers 3.4.1, and NumPy 2.5.4. Its complete runtime lock has SHA-256 `be2cfca52be55d744e7ddd3ce10ecfd5e69d8c1af70a711351372e53d26ae17e`.

Pass the model revision explicitly and pass the separate implementation revision through the model loader's code-revision option. Confirm that the installed sentence-transformer constructor forwards that option; if it does not, instantiate the pinned transformer/tokenizer and the pinned pooling/normalization modules directly. Preserve float32 model/query computation, plain text encoding, no query prefix, no extra normalization beyond the model's own module, and a declared batch size. The publisher defaults to batch size 128. Record the shorter smoke batch separately and rerun full-batch conventions before using released answer parity.

Run the 16 queries twice with fixed ordering and thread limits. Check shape, finite values, norm range, and per-vector hashes or explicit numeric deltas. Keep model outputs and query provenance separate from the FineWeb source vectors. Validate subset exact scoring against an independent tiny oracle before any recall result.

## CPU and GPU options

Use the existing driver for the first 16-query check while timed index work is idle. Apply explicit model-download, source-byte, wall-time, and process-memory budgets, and preserve all failed attempts. This small check does not justify reserving another machine.

If the measured CPU rate makes the selected production query pool impractical, use a dedicated sandbox GPU worker with pinned hardware, driver, CUDA, runtime, batch size, and model dtype. Compare a shared CPU/GPU sample before accepting the faster producer. A GPU batch can perturb ranking near ties; it must not silently become interchangeable with the publisher's queries. Select and price hardware from live availability after measuring model memory and throughput. No bulk embedding run should compete with the writer correctness/convergence gate.

The next query deliverables are a pinned source-identity manifest, a verified text artifact, a tested model-runtime lock, and the small query-output artifact. Production calibration/test pools and exact prefix references follow only after those checks pass.

## Primary references

The [pinned regeneration script](https://huggingface.co/datasets/Qdrant/FineWeb-10B/blob/3aea0b8d5f98741e343d07f067ce0f49dca1ccd2/queries/scripts/regenerate_queries.py) defines query encoding conventions. The [model configuration](https://huggingface.co/Alibaba-NLP/gte-multilingual-base/blob/9bbca17d9273fd0d03d5725c7a4b0f6b45142062/config.json) identifies the separate implementation repository. The [pinned remote implementation](https://huggingface.co/Alibaba-NLP/new-impl/tree/40ced75c3017eb27626c9d4ea981bde21a2662f4) and [pinned query source](https://huggingface.co/datasets/microsoft/ms_marco/tree/a47ee7aae8d7d466ba15f9f0bfac3b3681087b3a) complete the source references. The tested runtime lock and output provenance accompany the completed 16-query check.

## Completed bounded CPU check

Recovering the full dense query identity order and text fetches 4,101,532 projected source payload bytes in 33.724 seconds. The reconstructed 100,000-query text hash exactly matches the publisher's expected `3a354db0c5aa2bcf5d4563844c645d8fc23ff1780f83cce28900b7fc917a5756`. Selection uses a recorded seed and preserves the 16 selected publisher row indices.

The selected model/tokenizer files total 627,840,230 bytes, and locally computed hashes verify the publisher's large-file digests. The loaded implementation module names the pinned remote-code revision. Model setup, verification, and two encoding passes take 7.061 seconds inside the helper, or 10.95 seconds for the whole process. Peak resident memory is 2,234,068 KiB. The first 16-query encoding takes 0.141 seconds with four CPU threads. Repeated outputs are bit-identical; norms range from 0.9999999105 to 1.0000000678. The model's own normalization module is present, with no additional normalization or query prefix.

This validates a CPU query reproduction path rather than published-answer parity. The 16-query batch does not reproduce the publisher's complete batch schedule. A separate deterministic 100/100 calibration/test selection is prepared before performance tuning; its 200 embeddings and independently computed 100k-prefix inner-product references wait for the index timing slot to finish. No GPU or bulk embedding run is needed for the bounded check.

## Frozen pilot pools

The 200-row pilot selection is complete before FineWeb performance tuning. Selection seed 20261014 chooses publisher identities uniformly, and split seed 1729 assigns 100 calibration and 100 disjoint test IDs. The canonical frozen-pool manifest has SHA-256 `cb84821fab1169b58690ade3e2ab0855d9fe7205a52bd9355df84ca4a12822c8`. Its selected text and full-pool text checksum are preserved in `query-pools200`. Embedding and subset references use those exact frozen identities; they must not replace IDs after observing search performance.

The CPU environment is reproducible by installing torch 2.5.1 from the official CPU wheel index first, then installing the exact package versions in the saved runtime lock. Its installed torch version is `2.5.1+cpu`; the CPU wheel source matters because the default package index does not identify that build by itself. Model, tokenizer, and remote-code commits remain separate pins from package versions.
