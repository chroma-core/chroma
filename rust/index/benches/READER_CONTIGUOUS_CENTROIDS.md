# Contiguous reader centroids: 39M to 40M Wikipedia benchmark

Contiguous sibling codes reduce warm navigation latency by 58.5% and warm query latency by 11.8% in two 39M-to-40M repeats per revision. A separate comparison on one identical final index confirms a 54.9% navigation reduction and a 10.2% warm query reduction. All 12 corresponding rows in the fixed-index comparison preserve final recall and the displayed scanned-code counts.

## Revisions and setup

The baseline is `083ef95d6d3474f76cd703246294ae28c5f50368`; the candidate is `faaea93a67536fcc5fbf6178164b1a743d047649`. Each main run resumes a separate writable copy of `/mnt/data/wikipedia-40m-baseline-20261009-snapshots/39000000` and adds 1M real 1024-dimensional Wikipedia vectors. The order is baseline, candidate, candidate, baseline. Both binaries use Rust 1.93.1 with the same release profile and compiler flags. Runs hold the shared benchmark lock and execute sequentially on the lyon r6id.16xlarge instance, using 32 add, balance, and recall threads.

Both temporary benchmark source trees have the same ground-truth path override in the Wikipedia loader. This override selects `/mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet` without changing the shared dataset cache. Its SHA-256 is `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. There are 100 precomputed queries with exact L2 ground truth at 40M. The source checkpoints, their metadata, and this ground-truth file are preserved.

The full benchmark arguments are:

```sh
binary --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --save-dir <writable-checkpoint-copy>
```

## Main comparison

At tau 2 with eight rerank vectors per returned neighbor, the measurements average two runs per revision. Query times are per-query means across the concurrent batch.

| Metric | Baseline | Contiguous reader |
| --- | ---: | ---: |
| Warm navigation | 5.30ms | 2.20ms |
| Warm query | 29.30ms | 25.85ms |
| Final recall@100 | 95.365% | 95.365% |
| Scanned codes/query | 372,550 | 372,700 |
| Cold query | 832.55 ms | 814.35 ms |
| Reader open after indexing | 653.00 ms | 732.50 ms |
| Add | 12.76 s | 12.85 s |
| Balance | 39.64 s | 39.52 s |
| Commit | 20.85 s | 21.19 s |
| Full checkpoint wall time | 92.81 s | 93.75 s |
| Build plus final diagnostics | 151.62 s | 150.59 s |
| Whole-process peak RSS | 209.54 GiB | 209.32 GiB |

The warm query means range from 28.9–29.7 ms for the baseline and 25.2–26.5 ms for the candidate. The cold baseline ranges from 816.3–848.8 ms, which exceeds the 18.2 ms difference between variant means; these runs do not establish a reliable cold-latency improvement. Both main-run means have 95.365% final recall. Small scan-count and per-run recall differences arise from independently balanced trees, so the fixed-index comparison below checks the layout in isolation.

Full checkpoint wall time includes clearing block pins, dropping the old writer and input batch, and garbage collection. The short `Indexed ... total` line excludes some of that work. The reconciled build time also includes loading all postings and calculating final tree statistics; it excludes recall. The writer code is identical between variants, so differences in add, balance, and commit time do not demonstrate a reader optimization effect.

Reader numeric payload is reported as 18.0 MiB for the baseline and 18.1 MiB for the candidate, excluding container and allocator overhead. The candidate retains a copied child-ID array per parent while replacing individual centroid allocations and the node-wide code map with sibling buffers. Whole-process peak RSS is dominated by writing and does not measure reader-only memory.

## Fixed-index comparison

Both binaries also reopen separate copies of the exact final 40M checkpoint produced by baseline-1. The loop starts after checkpoint 40, so these runs perform no add, balance, or commit. This is one additional run per revision.

| Metric, tau 2 / rerank 8 | Baseline | Contiguous reader |
| --- | ---: | ---: |
| Warm navigation | 5.10ms | 2.30ms |
| Warm query | 28.40ms | 25.50ms |
| Final recall@100 | 95.37% | 95.37% |
| Scanned codes/query | 372,700 | 372,700 |
| Reader open | 276 ms | 272 ms |

Reader open is about 80 ms slower for the candidate after indexing, but the fixed-index opens take 276 ms and 272 ms respectively. These observations do not establish a consistent open-time regression; the process setup and allocator state differ between the two cohorts.

## All warm query rows

The rows below average the two main runs per revision.

| Tau | Rerank factor | Baseline navigation ms | Candidate navigation ms | Baseline query ms | Candidate query ms | Baseline recall % | Candidate recall % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.7 | 1 | 0.802 | 0.358 | 1.50 | 1.04 | 60.505 | 60.675 |
| 0.7 | 8 | 0.809 | 0.341 | 10.90 | 9.05 | 67.975 | 68.130 |
| 1.0 | 1 | 0.803 | 0.343 | 1.65 | 1.65 | 60.505 | 60.675 |
| 1.0 | 8 | 0.814 | 0.430 | 8.65 | 7.35 | 67.975 | 68.130 |
| 2.0 | 1 | 5.100 | 2.100 | 19.90 | 17.50 | 75.775 | 75.765 |
| 2.0 | 8 | 5.300 | 2.200 | 29.30 | 25.85 | 95.365 | 95.365 |

## Interpretation and artifacts

This optimization reduces CPU time spent navigating the tree. It does not reduce the beam width, scanned posting codes, or reranking workload. Cold query latency remains dominated by loading full-precision reranking vectors.

The CLI’s per-level beam and reachable-recall columns come from `writer.diagnose_level_recall_with_policy`, which uses one-bit centroid codes. They do not describe the four-bit reader navigation and are excluded from the reader conclusions here. The scanned-code count, final recall, and query timing columns come from actual reader searches.

Cold clears reader postings and block pins; provider and operating-system caches remain. The measurements use local NVMe storage and do not establish production network latency. There are two main repeats per revision and one fixed-index pair, using the same fixed 100 queries.

Raw CLI output, exact per-run commands, timings, binary hashes, source-preservation manifests, and build logs are saved on the node under `/mnt/data/reader-contiguous-7903/results/`. Local copies, `results.json`, the analysis script, and launch scripts are under `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/reader-contiguous-7903/`.
