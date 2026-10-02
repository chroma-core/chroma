# Hierarchical SPANN GPU POC: measured runs

The CPU baselines ran on the same SF Compute H100 host reserved for GPU experiments. Each run used 14 insertion workers, one balancing worker, full-precision writer navigation and nearest-posting assignment, a single checkpoint, and validation before commit and after reopen. Recall used 1,000 sampled data vectors as queries and exact nearest neighbors at k=100. The manifests and full logs remain under `/home/dev/hspann-results` on the stopped node `hspann-gpu-poc-01`; the mission is [hierarchical-spann-gpu-poc](https://autoresearch.sfcompute.com/missions/hierarchical-spann-gpu-poc).

| Dataset | Vectors | Posting validation | Index build | Recall at tau=2, rerank=8 |
| --- | ---: | --- | ---: | ---: |
| Wikipedia English | 300,000 | 300,000 of 300,000 before and after commit | 36.58 s | 99.28% |
| MS MARCO v2 | 300,000 | 300,000 of 300,000 before and after commit | 36.02 s | 99.50% |
| Wikipedia English | 1,000,000 | 1,000,000 of 1,000,000 before and after commit | about 2.0 min | 98.05% |
| MS MARCO v2 | 1,000,000 | 1,000,000 of 1,000,000 before and after commit | about 2.0 min | 97.55% |

The one-million-vector reader's warm query latency was 82.8 ms for Wikipedia and 82.5 ms for MS MARCO at tau=2 and rerank=8. About 56 ms per query was spent in its distance-scoring phase. The exact-neighbor calculation took about 1.3 minutes for Wikipedia and 1.2 minutes for MS MARCO after the benchmark parallelized queries and selected only the top 100 distances.

The writer split replay used a captured Wikipedia leaf with 100,000 vectors of 1,024 dimensions. A vectorized NumPy two-means took 0.947 s; the CuPy H100 prototype took 0.898 s including transfers, a 1.05× speedup. Labels agreed for every vector, and the largest center-coordinate difference was 6.5e-7. This probe shares deterministic initial seed pairs across its CPU and GPU versions, but it does not reproduce Rust's seed selection or measure the complete writer.

The reader scoring probe used synthetic records in the same 144-byte 1-bit code layout and the Euclidean distance formula used by the reader. It scored 133,100 records in a median 1.82 ms and 178,900 records in 2.37 ms, including host-to-GPU and GPU-to-host transfers. The kernel itself took about 0.05 ms. Scores matched a CPU formula on 256 sampled records within 8e-5 absolute error. This probe excludes tree search, disk reads, and score integration; it establishes a promising scoring ceiling, not an end-to-end reader speedup.

## Real reader scoring replay

The real-data replay opens each saved one-million-vector index and captures the first exact-neighbor query's selected posting codes, per-leaf query quantization, and CPU scores. It uses tau=2 and a rerank factor of 8, then checks GPU scores and candidate sets against those exact CPU scores. The CPU column times the capture path's scoring loop on one thread; the GPU columns are medians of nine replays and include transfers, allocation, and kernel launches. The batched GPU path combines all selected leaves into one launch.

| Dataset | Selected leaves | Codes | CPU scoring | GPU per leaf | GPU batched | CPU / batched GPU | Top-800 overlap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Wikipedia English | 128 | 186,863 | 12.074 ms | 7.636 ms | 2.149 ms | 5.62× | 800 / 800 |
| MS MARCO v2 | 128 | 184,378 | 11.899 ms | 8.011 ms | 2.121 ms | 5.61× | 800 / 800 |

Both GPU paths match all top 100 and top 800 IDs on both captured queries. The largest absolute score difference from the Rust reader is 3.58e-7. The CPU capture and GPU replay use the same stored codes and quantized query values; the Python replay prepares a flat code buffer and leaf lookup outside the timed GPU section. This preparation, the Rust-to-GPU integration cost, tree navigation, posting loads, deduplication, and vector reranking still need measurement in one reader search. The 5.6× figure describes this bounded scoring replay only; it is not a full-query speedup or a recall result across all queries.

The saved fixtures are `/home/dev/hspann-results/reader-wikipedia-1m-q0.bin` and `/home/dev/hspann-results/reader-ms-marco-1m-q0.bin` on the stopped node. The matching CPU logs and GPU JSON reports are in the same directory. The one-million-vector indexes and exact-neighbor query caches are unchanged, so another replay can use the same input.

## Paired reader search

The benchmark reader can now send selected leaf codes directly to a CUDA scoring library. A CPU and GPU run opens the same saved one-million-vector index, reads the same first 100 exact-neighbor queries, uses tau=2 and rerank=8, and records the ordered top-100 IDs. The table reports average warm search latency per query, including tree navigation, scoring, candidate selection, and vector reranking. Each run first makes a cold pass that loads postings and vectors; only the second pass is used for this latency comparison. The thread count is the number of concurrent recall queries, not the writer's insertion workers.

| Dataset | Concurrent queries | CPU warm query | GPU warm query | CPU / GPU | Recall@100, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| Wikipedia English | 1 | 15.8 ms | 9.4 ms | 1.68× | 98.11% |
| Wikipedia English | 4 | 16.9 ms | 12.3 ms | 1.37× | 98.11% |
| Wikipedia English | 8 | 17.2 ms | 17.6 ms | 0.98× | 98.11% |
| Wikipedia English | 32 | 81.2 ms | 190.3 ms | 0.43× | 98.11% |
| MS MARCO v2 | 1 | 16.3 ms | 10.3 ms | 1.58× | 97.15% |
| MS MARCO v2 | 32 | 74.4 ms | 190.8 ms | 0.39× | 97.15% |

Every paired query returns the same top-100 ID set on both datasets at one and 32 concurrent queries. Wikipedia's ordered result lists also match exactly. One MS MARCO query has six rank positions in a different order, with the same 100 IDs. The intermediate four- and eight-query Wikipedia runs report the same aggregate recall, but they did not save individual result lists.

Direct transfer matters for latency. Copying about 25 MB of leaf codes into one CPU buffer cost 13–30 ms per query and made the integrated GPU reader slower than CPU even with one query. Passing pointers to the loaded leaves and keeping those leaves locked through the CUDA transfer cuts CPU preparation to about 0.3 ms; host-to-GPU copies then take about 4.2 ms and kernel plus return copy about 0.4 ms for a typical query. At 32 concurrent queries, competing transfers and GPU calls raise the distance-scoring phase to about 179 ms per query, compared with about 49–59 ms on CPU. The current CUDA bridge therefore improves isolated query latency but does not support the benchmark's usual 32-query concurrency well. GPU query batching or concurrency control needs a separate trial before considering this path for production.

The CUDA bridge is optional benchmark code, activated with `HSPANN_GPU_LIBRARY`. It allocates reusable device buffers per calling thread and loads a separately compiled library. It does not change the production reader. The paired logs and result-ID files are under `/home/dev/hspann-results/paired-*` on the parked node; source and reproduction steps are in this branch.

## Correctness limit

Two 150,000-vector checkpoints can lose hundreds of IDs from the first checkpoint during the second batch. The failure occurs with one insertion worker and one balancing worker, and eagerly loading saved postings and vectors only sometimes prevents it. The missing IDs retain version metadata and embeddings but have no posting in any leaf. The baseline wrapper therefore uses one checkpoint and rejects a run if posting validation fails. The multi-checkpoint writer path needs a separate fix before it can support a paired GPU comparison.

## Cost and artifacts

The H100 node is stopped after the paired reader runs. Current month-to-date spend is $15.86 against the user's $250 cap, with $233.98 credit available and automatic top-up off. Source, benchmark wrappers, and probes are on `codex/hierarchical-spann-gpu-poc`. Full logs, manifests, dataset hashes, and fixtures remain on the node's parked disk. Automatic approval review rejected exporting the earlier evidence directory to external object storage because the user had not specifically authorized that payload and destination.

## Next experiment

The reader path needs a concurrent-query design before GPU scoring is useful under the benchmark's usual load; batching several queries into one GPU submission is the next candidate. The split replay shows too little speedup to justify claiming a writer improvement from two-means alone. A writer comparison also needs either a correctness fix for repeated checkpoints or an explicit single-checkpoint scope.
