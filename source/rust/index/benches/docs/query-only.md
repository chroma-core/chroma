# Query a saved hierarchical index

The benchmark can open an existing checkpoint and measure reads without loading source datasets or running insertion, balancing, commit, garbage collection, or build-summary scans. Supply the directory containing `checkpoint.json` and `data/`, plus a precomputed ground-truth Parquet file. The saved index must remain stable throughout the run; do not query a directory that another process is updating.

```sh
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- \
  --query-only \
  --save-dir /mnt/data/wikipedia-40m-baseline-20261009 \
  --ground-truth /mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet \
  --metric l2 \
  --threads 32 \
  --num-queries 100 \
  --read-beam-min 10 \
  --read-beam-max 256 \
  --read-level-min-pcts 50,5,0 \
  --recall-tau-values 0.7,1,2 \
  --recall-rerank-vectors 1,8 \
  --max-cache-bytes 1073741824
```

The checkpoint metadata supplies its vector count. Ground truth must contain query rows with exactly that count in `max_vector_id`; answers for a larger corpus are rejected. The file uses the existing dataset schema: `query_vector` is a list of float32 values, `max_vector_id` is uint64, and the selected metric's neighbor column (`neighbors_l2`, `neighbors_ip`, or `neighbors_cosine`) is a list of uint32 IDs. Each row must contain at least 100 neighbors within the saved prefix. Query dimensions must match the stored index. Choose the same metric used to build the checkpoint; the checkpoint metadata does not record it.

The existing read flags control the search sweep, reranking, query count, concurrency, and cache capacity. The default lazy recall mode runs cold and warm passes and reports recall, mean latency, search-phase timings, and estimated logical bytes. Its cold pass clears reader buffers, while provider and operating-system caches can still be warm; the byte estimates are not measured network traffic. Whole-index build diagnostics, per-level reachable recall, and the optimal-leaf baseline are skipped in query-only mode. Their columns and estimates of total navigation-plus-scan bytes are omitted or marked `-`; scan-byte estimates and actual end-to-end recall remain available.

The mode opens persisted tree metadata to obtain the shared recall diagnostics' shape, then opens the reader for the timed search sweep. It creates no checkpoint directories or files. Missing inputs, write/resume flags, and build-only diagnostics are rejected. Source-dataset selection and writer tuning are unnecessary for this mode.
