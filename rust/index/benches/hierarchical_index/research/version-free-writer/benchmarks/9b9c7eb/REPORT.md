# Version-free writer benchmark results

Fresh 1M builds improve repeatably, but the 10M results do not establish a broad writer-speed improvement. The candidate preserves posting coverage and sampled stored values in the completed correctness audits. Higher peak memory and dataset-dependent scaling remain material tradeoffs.

The comparison is baseline `1f433be7969b3d28c78e463fe33a299fa0c5fee9` against candidate `9b9c7ebde93144055b1fc86ad29e7ea3b64c140b` from [PR #7927](https://github.com/chroma-core/chroma/pull/7927). The failed earlier candidate `0e7d28f` remains preserved in the parent benchmark directory and contributes no timings to these comparisons.

The timing table separates the complete instrumented build from a phase total that excludes posting validation and final tree diagnostics. The latter is calculated from mutually exclusive measured phases; it is not a separate run without instrumentation. Each 1M figure is the mean of two fresh runs in opposite variant orders. Each 10M figure is one fresh baseline/candidate pair.

| Dataset / vectors | Full build, baseline → candidate | Change | Build excluding validation and final diagnostics | Change | Peak process RSS, baseline → candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| wikipedia / 1M | 60.63 → 45.75 s | -24.5% | 57.11 → 44.57 s | -22.0% | 8.20 → 9.17 GiB |
| wikipedia / 10M | 405.83 → 393.62 s | -3.0% | 341.59 → 372.38 s | +9.0% | 62.32 → 71.18 GiB |
| msmarco / 1M | 61.70 → 49.90 s | -19.1% | 58.29 → 48.75 s | -16.4% | 8.65 → 9.41 GiB |
| msmarco / 10M | 506.24 → 486.18 s | -4.0% | 454.56 → 465.36 s | +2.4% | 62.40 → 69.59 GiB |

Negative timing changes mean less time. Peak RSS is the maximum resident memory of the whole process, including validation and recall; for repeated 1M runs the table uses the higher observed peak. The raw process wall times, phase times, and checkpoint memory measurements remain in `comparison.json`, `*.time`, and `*.log`.

Full process wall time includes the build, recall evaluation, and process startup/teardown. The values below use means for the two 1M repetitions and the single observation at 10M.

| Dataset / vectors | Baseline process wall time | Candidate process wall time |
| --- | ---: | ---: |
| wikipedia / 1M | 63.298 s | 48.590 s |
| wikipedia / 10M | 427.106 s | 416.330 s |
| msmarco / 1M | 64.095 s | 52.400 s |
| msmarco / 10M | 526.401 s | 506.726 s |

Across the paired 1M runs, candidate recall differences range from −0.19 to +0.57 percentage points for Wikipedia and −0.19 to +0.56 percentage points for MS MARCO. All four read settings are represented in each run.

The 10M phase breakdown shows where the writer spends time. Input loading, validations, memory diagnostics, cleanup, and output are separate from these phases.

| Dataset / variant | Add | Balance | Commit preparation | Commit flush | Reopen |
| --- | ---: | ---: | ---: | ---: | ---: |
| wikipedia / baseline | 57.293 s | 126.493 s | 49.882 s | 31.697 s | 0.331 s |
| wikipedia / candidate | 66.357 s | 156.028 s | 39.525 s | 28.851 s | 0.353 s |
| msmarco / baseline | 70.233 s | 129.277 s | 48.759 s | 28.891 s | 0.335 s |
| msmarco / candidate | 80.144 s | 134.372 s | 39.476 s | 29.317 s | 0.348 s |

The Wikipedia 10M balance-time increase is concentrated in one checkpoint. At checkpoint 9 the candidate spends 48.342 s balancing versus 18.663 s for the baseline. All other checkpoints combined spend 107.686 s versus 107.831 s. This single observed outlier accounts for nearly all the balance regression; the experiment does not establish whether it recurs. No extra run is added to investigate it.

Input validation adds work before parallel collection begins. The candidate scans every coordinate for finite values and checks duplicate IDs before starting its buffered collection timer. At 10M × 1024 dimensions that visits 10.24 billion float coordinates, or 40.96 GB of coordinate values. Summing total add minus the reported collection and flush sub-times leaves 7.774 s for the candidate and 0.029 s for the baseline. That 7.745 s difference is consistent with the added preflight work and accounts for about 85% of the observed 9.064 s add-time increase, but the residual also includes any other untimed overhead and rounded sub-times. It is not an isolated measurement of preflight. The relevant implementation is writer/writer.rs, preflight and add_batch_buffered.

Grouped ownership transfers also add source-list work. The Wikipedia 10M candidate records 639,132 applied batches, 3,756,754 accepted moved-vector events, and 905,128,573 scanned source entries, approximately 241 scanned entries per moved-vector event. Each destination group clones the source IDs/codes, scans the source, and rebuilds its remaining postings. These counters establish work volume, not its isolated runtime. The source-scan counter includes scans that accept no moves; the applied-batch counter does not.

Reassignment counters and worker timers change meaning between revisions. The candidate counts proposed IDs before navigation, including retries and proposals routed back to their source. The baseline increments only after its single-vector routine reaches its end, excluding several early-return cases. The candidate also omits the baseline’s navigation/register/balance sub-timers within reassignment. Therefore the raw reassignment counts and nested worker task timings are not direct like-for-like completed-work comparisons. Exclusive phase wall times remain the timing basis.

The higher memory reading has a visible contributor in the final Wikipedia checkpoint: the baseline reports 4.75M cached embeddings using 18.12 GiB, while the candidate reports 6.61M using 25.23 GiB. The same baseline checkpoint reports only 5.6 MiB of version-map memory, so the extra 7.11 GiB of cached embeddings greatly exceeds that direct memory saving. This supports greater retained embedding volume as an explanation for part of the memory increase; it does not isolate every allocation or the CPU savings from avoiding version checks.

Recall@100 measures how many of each query’s exact 100 nearest neighbors are returned. All runs use the same fixed 100 queries and prefix-matched exact squared-L2 ground truth. The 10M values below come from the warm rows; cold rows report the same recall.

| Dataset / tau / rerank | Baseline recall | Candidate recall | Difference |
| --- | ---: | ---: | ---: |
| wikipedia / tau1.50-rerank1x | 75.87% | 76.27% | +0.40 percentage points |
| wikipedia / tau1.50-rerank8x | 95.90% | 96.67% | +0.77 percentage points |
| wikipedia / tau2.00-rerank1x | 76.22% | 76.67% | +0.45 percentage points |
| wikipedia / tau2.00-rerank8x | 96.86% | 97.56% | +0.70 percentage points |
| msmarco / tau1.50-rerank1x | 80.69% | 80.45% | -0.24 percentage points |
| msmarco / tau1.50-rerank8x | 94.95% | 94.98% | +0.03 percentage points |
| msmarco / tau2.00-rerank1x | 81.17% | 80.92% | -0.25 percentage points |
| msmarco / tau2.00-rerank8x | 95.92% | 96.13% | +0.21 percentage points |

The saved-index audits check all expected IDs and count physical and live postings. Baseline entries are live only when their stored versions match authoritative metadata; stale baseline entries are allowed. Candidate entries have no versions and must occur exactly once. Each audit also checks 100 stored embedding vectors bit for bit against their source values and recomputes their posting codes using the persisted leaf centroid. Those samples are the fixed query vectors selected from the first million IDs at both scales.

| Saved index | Valid IDs | Physical postings | Live postings | Embedding/code samples |
| --- | ---: | ---: | ---: | ---: |
| msmarco-10m-baseline-1 | 10,000,000 | 10,009,861 | 10,000,000 | 100 |
| msmarco-10m-candidate-1 | 10,000,000 | 10,000,000 | 10,000,000 | 100 |
| msmarco-baseline-1 | 1,000,000 | 1,002,527 | 1,000,000 | 100 |
| msmarco-baseline-2 | 1,000,000 | 1,002,725 | 1,000,000 | 100 |
| msmarco-candidate-1 | 1,000,000 | 1,000,000 | 1,000,000 | 100 |
| msmarco-candidate-2 | 1,000,000 | 1,000,000 | 1,000,000 | 100 |
| wikipedia-10m-baseline-1 | 10,000,000 | 10,012,294 | 10,000,000 | 100 |
| wikipedia-10m-candidate-1 | 10,000,000 | 10,000,000 | 10,000,000 | 100 |
| wikipedia-baseline-1 | 1,000,000 | 1,003,617 | 1,000,000 | 100 |
| wikipedia-baseline-2 | 1,000,000 | 1,002,563 | 1,000,000 | 100 |
| wikipedia-candidate-1 | 1,000,000 | 1,000,000 | 1,000,000 | 100 |
| wikipedia-candidate-2 | 1,000,000 | 1,000,000 | 1,000,000 | 100 |

All fresh builds validate the complete inserted-ID set before every commit and after reopening every checkpoint. Candidate validation additionally rejects duplicate IDs and unreachable tree nodes. No candidate error or pending-work refusal is treated as a speedup. Claim conflicts, retries, transfer batches, moved vectors, and source entries scanned are preserved per checkpoint in `comparison.json` and raw output; claim-hold durations aggregate concurrent workers and are not wall time.

The experiment uses one idle r6id.16xlarge host with 64 logical CPUs, 32 physical cores, and approximately 495 GiB RAM. Both revisions use Rust 1.92.0, the optimized benchmark profile, and `-C target-cpu=native`. The same shared benchmark lock serializes compilation, builds, and audits. All new data and temporary/build output reside on NVMe storage. Original Wikipedia 40M snapshots and MS MARCO checkpoints are untouched.

Within each dataset and scale, both variants use identical settings: branching 100, split 2048, merge 512, full-precision write navigation and neighbor reassignment, write beam 10–16, a 1 GiB cache, cleared reader pins, and no replication flags. Wikipedia uses 32 write/balance threads, write tau 0.5, and write level taus `_,_,0.30`. MS MARCO uses 64 threads and write tau 1.5 without per-level write flags. The 1M runs have ten 100k checkpoints; the 10M runs have ten 1M checkpoints.

Read settings are tau 1.5 and 2, rerank 1 and 8, and beam minimum 10. Wikipedia uses beam maximum 256 with level minimum percentages `50,5,0`; MS MARCO uses beam maximum 128 and the standard unset level schedule. Every comparison keeps those settings fixed. GT/source verification checks the complete 1M and 10M prefixes against recorded source hashes and verifies the query values and valid neighbor ranges. Preparation is excluded from measured runs, and brute-force GT is disabled.

The measured binaries contain the exact archived revisions without benchmark overlays. Saved-data audits use separately compiled binaries whose two-file harness patches and hashes are preserved. One initial audit-harness compilation fails on boxed-error conversion; its log is preserved, and the corrected overlay completes. The measured binaries and run results are unaffected.

The successful first Wikipedia 1M baseline is reused after checking its exact binary hash, machine, inputs, and matching settings. All remaining runs are fresh independent indexes. The two 1M pairs support repeatability; each 10M comparison has only one pair and should not support a small-percentage speed claim. Validation and diagnostics can affect cache state even when their measured durations are subtracted. Embedding/code samples are not a full byte audit of all stored vectors.

Do not present this as an unconditional performance win or merge based solely on these measurements. The 1M gains are repeatable, while larger-index timing and memory need to be weighed explicitly. No merge or PR-ready change is performed.

Raw commands, SHAs, binary and source archive hashes, compiler output, environment details, GT provenance, checkpoint metadata, complete stdout/stderr, process timing, and audit results are preserved here and at `/mnt/msmarco/version-free-writer-7927-9b9c7eb/artifacts`. The full indexes and binaries remain on that machine. `manifest.json` identifies the exact revisions. The SSH ingress change is recorded in the parent directory with explicit user authorization and before/after verification.
