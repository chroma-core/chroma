# MS MARCO checkpoints for final-million benchmarks

This job prepares saved indexes with 39M and 99M real MS MARCO vectors. Each checkpoint uses metadata compatible with one-million-vector increments, so a benchmark can copy it and add the next 1M vectors to reach 40M or 100M. The build starts with 3M batches and falls back to 1M batches if available system memory falls below 24 GiB. Only the 39M and 99M source checkpoints are retained.

The isolated source archive is pinned to feature commit `b788497434f865d7080096cb484f38de4b6477e8`. The harness adds an optional ground-truth path through `CHROMA_BENCH_GT_PATH` to avoid modifying another job's query cache. It also verifies the number of valid vector IDs reachable from the persisted root and checks for missing child nodes after reopening. These harness overlays are recorded separately; no shared source checkout or feature branch changes.

The dataset is the first 100M vectors from `CohereLabs/msmarco-v2-embed-multilingual-v3` at revision `aaaddf8cd10328d4ccd5755832c2cc27773ceef6`, with 1024 dimensions. The first ten already cached shards are referenced from the first drive; additional shards are downloaded to the second drive. The source manifest records all paths and row boundaries. Exact top-100 answers are prepared for the same 100 fixed queries at 39M, 40M, 99M and 100M, with float64 accumulation for L2, inner product and cosine distance.

The writer uses full-precision navigation and reassignment, 64 add workers, 64 balancing workers, split/merge thresholds 2048/512, one replica, write tau 1.5, RNG factor 4, and a 1 GiB block cache. It clears reader block pins between balancing and commit. These are the established MS MARCO writer settings. The first large batch also uses the harness's existing early balancing schedule for a new tree.

The second NVMe drive is mounted at `/mnt/msmarco`, with filesystem UUID `b242708c-bc2b-44d0-9744-28cc8f05d731`. It has about 1.7 TiB capacity. The Wikipedia source cache and saved checkpoints remain on `/mnt/data`. Instance-local NVMe data is lost when EC2 stops; this task does not create persistent cloud storage.

The runner holds `/mnt/data/chroma-benchmark.lock` across preparation, builds, exact-answer generation and verification. Its results are in `/mnt/msmarco/results`. The saved sources are `/mnt/msmarco/checkpoint-39m` and `/mnt/msmarco/checkpoint-99m`. Verification uses independent writable copies at `/mnt/msmarco/verified-40m` and `/mnt/msmarco/verified-100m`, and checks that the source manifests remain unchanged. Snapshots share only immutable UUID block files; their metadata files are independent. No other benchmark copies are deleted by this job.

The build's administrative checkpoint number reflects 3M batches. The saved sources normalize only that number for 1M increments, retaining the original metadata alongside it. The stored total-vector count and block identifiers stay identical. A resume command computes its next input offset from the checkpoint number and batch size, so this normalization prevents skipping or repeating vectors when switching to 1M tests.

## Completed results

The complete job exits successfully on October 14, 2026, after 3 hours 51 minutes 19 seconds. Both saved sources pass the persisted-root checks at their exact expected counts, with zero missing embeddings and child nodes. Verification adds the final 1M vectors from independent copies and passes the same checks at 40M and 100M. Both source manifests remain unchanged after verification. Exact answers are saved for all four boundaries, and the benchmark lock is released.

| Verification | Add | Balance | Commit | Recall at tau 2 with 8× reranking | Warm query latency |
|---|---:|---:|---:|---:|---:|
| 39M to 40M | 14.24 s | 30.37 s | 20.65 s | 93.07% | 14.1 ms |
| 99M to 100M | 22.11 s | approximately 2.5 min | 55.76 s | 91.52% | 18.6 ms |

The build reaches 90M with 3M batches. When available memory falls below the guard threshold, the runner stops the process and resumes from the last committed boundary with 1M batches. This completes the remaining 9M successfully. The isolated script's resumed overall-throughput summary uses the full index count as its numerator; use the individual phase timings when comparing the final increments.

The two completed test outputs remain available alongside the 39M and 99M saved sources. The drive has about 550 GiB free after these verification copies. All source vectors, exact ground truth, compiler artifacts and raw logs are retained. Wikipedia files remain on the first drive, and the job deletes none of them. The exact-answer file is `msmarco_39m_100m_ground_truth.parquet`, with SHA256 `b1d07375acd7bf22c5d30671f577e01fba19703f91c8de877328376a16e0d39f`.
