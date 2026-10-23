# Balancing experiment results

Finer scheduling jobs provide a large, repeatable benefit on the saved 8M-to-9M Wikipedia workload: balancing falls from a mean 46.56s to 16.40s, and insertion plus balancing plus commit/reopen falls from 62.01s to 31.92s. Batched transfers provide only a marginal result at 3M and no useful gain at 1M. Recommend the standalone scheduling candidate; retain batching as an experiment without combining it.

## Method

The reference is eea4ada6f21554adb535d1ffdea95f74cb76650b, batched transfers are f76e8c6541c363c1068d864eb6ff9835093167e1, and finer scheduling jobs are 14dc9698eb6e400ba6f677575eff7a5659deeed6. Both candidates are independent direct children of the reference. No combination or production PR change occurs in these experiments.

Each initial fresh 1M run commits every 100k vectors. All revisions use the same full-precision Wikipedia settings, 32 insertion and balancing threads, branching factor 100, split threshold 2048, merge threshold 512, writer tau 0.5, writer level taus _,_,0.30, 1GiB cache, fixed 100 queries and matching precomputed L2 neighbors. Replication flags and writer level minimum percentages are absent. Exact argument arrays and binary/archive hashes accompany raw logs.

Core indexing time sums insertion including production preflight, balance, commit preparation, flush and writer reopen. Benchmark posting validation is disabled. Input loading, benchmark diagnostic phases, output formatting, reader-pin cleanup, destruction and benchmark garbage collection are excluded from this sum. Raw full wall totals remain available. This phase sum should not be described as all production latency. Saved-index correctness audits execute separately.

Rust 1.92.0 builds use target-cpu=native on the existing 64-vCPU/32-core Intel Xeon 8375C instance with approximately 495GiB RAM. Shared locking serializes every build and benchmark. All output and build artifacts reside on NVMe. The unchanged reference binary is reused from its prior exact eea release build, whose provenance is retained.

## Initial 1M results

These are single runs. Differences of a few percent do not establish a repeatable improvement or regression.

| Measure | Reference | Batched transfers | Finer jobs |
|---|---:|---:|---:|
| Insert + balance + commit + reopen | 25.906s | 26.058s (+0.6%) | 26.794s (+3.4%) |
| Balance wall time | 16.291s | 16.318s | 17.220s |
| Structural worker wall time | 15.006s | 12.588s | 15.990s |
| Retry or joined transfer phase | 1.280s | 3.722s | 1.224s |
| Source posting entries scanned | 11.675M | 5.185M | 11.792M |
| Peak RSS | 9.168GiB | 9.095GiB | 8.858GiB |
| Recall at tau2, rerank8 | 99.53% | 99.43% | 99.56% |

Batched transfers eliminate claim conflicts during transfer application, but their queued ID counter includes all deferred proposals. The reference only queues unsuccessful immediate transfers. Raw queued-ID totals and retry counters therefore represent different work. The batched retry timer includes parallel preparation and publication wall time; it is not a serial retry cost. Summed worker durations overlap across threads and do not measure CPU consumption.

All three initial 1M saved indexes pass the separate audit. The reference and transfer candidate have no final leaf-size violations; the scheduler retains one oversized leaf containing 2175 entries under the shared eight-round size cap.

## Batched transfers at 3M

The fresh 3M pair uses three 1M checkpoints and runs candidate before reference, reversing the initial 1M order. This is one pair, not repeatable evidence of a small speedup.

| Measure | Reference | Batched transfers |
|---|---:|---:|
| Insert + balance + commit + reopen | 69.120s | 67.661s (−2.1%) |
| Balance | 36.469s | 35.646s (−2.3%) |
| Structural worker wall time | 29.927s | 23.732s |
| Retry or joined transfer phase | 6.530s | 11.900s |
| Source posting entries scanned | 36.663M | 11.811M (−67.8%) |
| Peak RSS | 23.242GiB | 23.582GiB |
| Recall at tau2, rerank8 | 98.56% | 98.60% |
| Residual oversized leaves | 0 | 2; largest 2295 entries |

The batching mechanism reduces scans and structural worker time, but its joined routing/preparation/publication phase replaces most of that saving. It changes some placements and round progression, and the final two oversized leaves are allowed by the shared eight-round size cap. The 2.1% phase-sum difference is marginal and insufficient to justify the extra transfer machinery. No more batching benchmarks run.

## Scheduling from 8M to 9M

Every replay starts from a full independent copy of the same preserved 8M snapshot at /mnt/msmarco/version-free-followup-20261020/wikipedia-diagnostic-9m-1/snapshots/8000000. The checkpoint opens successfully with all revisions, and its metadata hash remains 34f6b9e1c021d1e84666f70f29d0c033bbeea84d5324eb43be625ca0b7b83c32. No rebuild to 8M occurs. Each replay adds exactly 1M vectors and commits the ninth checkpoint.

The first pair runs reference then candidate with both outputs on /mnt/msmarco. The second pair reverses the order, running candidate then reference with both outputs on /mnt/data. Both are local NVMe volumes on the same host; the repeat uses the second volume to preserve every first-pair output with sufficient free space. Compare candidates within each pair. The cross-volume change is disclosed rather than assuming identical storage conditions across pairs. Binaries, settings, source checkpoint and query inputs remain identical across repeats.

| Measure | Pair1 reference | Pair1 finer jobs | Pair2 reference | Pair2 finer jobs |
|---|---:|---:|---:|---:|
| Insert + balance + commit + reopen | 63.254s | 32.228s (−49.0%) | 60.773s | 31.615s (−48.0%) |
| Balance | 47.856s | 16.692s (−65.1%) | 45.264s | 16.101s (−64.4%) |
| Insert including preflight | 7.264s | 7.358s | 7.262s | 7.220s |
| Commit preparation + flush | 8.078s | 8.118s | 8.206s | 8.252s |
| Retry phase | 0.368s | 1.678s | 0.421s | 1.541s |
| Peak RSS | 59.246GiB | 58.863GiB | 59.137GiB | 58.829GiB |
| Recall at tau1.5, rerank1 | 76.30% | 76.17% | 76.32% | 76.34% |
| Recall at tau1.5, rerank8 | 97.00% | 96.93% | 97.01% | 96.95% |
| Recall at tau2, rerank1 | 76.48% | 76.41% | 76.49% | 76.49% |
| Recall at tau2, rerank8 | 97.63% | 97.66% | 97.64% | 97.58% |

The scheduling candidate increases contention/retry work while keeping workers occupied, with a large net elapsed improvement. Reference rounds 3–5 take 9.60/7.18/5.71s in pair1 and 8.14/7.42/5.12s in pair2. Candidate rounds 3–5 take 1.56/1.00/0.65s and 1.53/1.26/0.62s. The candidate partition counter counts eligible leaf jobs, whereas the reference counts whole subtree roots. These counter values represent different units and cannot be interpreted as a direct reduction in work.

Process CPU sampling supports the explanation of a shorter idle tail. Within the sampled balance intervals, reference utilization averages 5.39 and 5.41 logical cores; candidate utilization averages 17.49 and 17.06. Reference samples contain 32 and 31 approximately one-second intervals below two cores, while candidate samples contain none. Sampled CPU work is approximately 249/240 CPU-seconds in the reference and 265/258 in the candidate. The candidate therefore achieves its lower latency without evidence of substantially less CPU work.

Sampling is bounded rather than exact phase accounting. The balance window ends at the timestamp of the first output line immediately after joined insertion/balancing and starts one measured balance duration earlier. Each resumed checkpoint uses one insertion/balance batch. Output latency and one-second sampling omit edge intervals: reference coverage is 46.16 of 47.86s and 44.16 of 45.26s; candidate coverage is 15.11 of 16.69s and 15.11 of 16.10s. CPU-seconds and average cores apply only to those covered intervals. They are not an estimate of recoverable elapsed time.

All four replays reach the eight-round size cap. Both implementations permit remaining size violations once transfer ownership work is complete. The final residuals are shown to make that stopping condition explicit.

| Replay | Undersized leaves | Oversized leaves | Largest leaf |
|---|---:|---:|---:|
| Reference pair1 | 1 | 3 | 2451 |
| Finer jobs pair1 | 4 | 2 | 2224 |
| Reference pair2 | 3 | 1 | 2168 |
| Finer jobs pair2 | 4 | 1 | 2213 |

The small residual counts and comparable sampled CPU work support a scheduling improvement rather than a result explained by skipping most balancing work. The scheduler still changes work ordering and defers newly created jobs to a later round. This evidence covers one saved tree with two run orders; it does not establish the gain for every dataset, checkpoint, or full fresh build. The fresh 1M scheduler run is 3.4% slower in core phases, so no general fresh-index speedup is claimed.

## Ground truth and correctness

Fresh 1M and 3M runs use the retained verified fixed-query ground truth. The 1M neighbor file has recorded prior exact-source provenance; the 3M rows pass the scoped exhaustive float64 L2 verification. The replay uses the existing 9M-prefix neighbor rows from /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet. All 100 query vectors exactly match the verified 1M/3M query set, each row has 100 L2 neighbors, and every neighbor ID is within the 9M prefix. Its source file and extracted combined file hashes are recorded in gt9-source-manifest.json and gt9-combined.sha256. The 9M neighbors are precomputed data, not independently recomputed in this experiment. No mismatched corpus prefix or newly computed queries enter the timed runs.

All nine unique saved indexes pass separate audits for complete root-reachable exact-once ID coverage, consistent posting/code lengths, resolved topology, and 100 fixed source embedding/code samples. All six 3M/9M indexes additionally pass an extended audit of 200 embeddings/codes: the 100 fixed older query vectors plus 100 vectors spread across the newest inserted million. Stored embeddings match source float bits, and posting codes match recomputation against each current leaf centroid. Audits load persisted outputs through the lazy reopen path and run outside benchmark timings. Full per-leaf sizes and audit commands/results are retained.

The maximum observed recall loss across these paired settings is 0.36 percentage points in the initial 1M batching run. Scheduler replay differences range from −0.13 to +0.03 percentage points in pair1 and −0.06 to +0.02 in pair2. These 100-query measurements show no material recall regression but do not exclude smaller changes. Complete cold/warm recall and query-latency rows remain in raw logs; reader latency is not the optimization target in this experiment.

## Recommendation and artifacts

Proceed with the standalone finer-job scheduler experiment. Its two matched replay pairs show approximately 65% lower balancing latency and 48–49% lower insertion/balance/commit/reopen time on this saved-tree workload. Keep the batched transfer implementation isolated: its marginal small-run benefit does not justify combining its additional placement and buffering logic with the demonstrated scheduler improvement.

Implementation handoffs report 189 passing test executions for batching and 173 for finer jobs, including exact-once concurrent mutation, persisted reopen, topology and code checks. Independent concurrency and storage reviewers report no blockers. Benchmark source archives and release binaries have recorded SHA256 hashes, and all performance runs disable benchmark posting validation. No implementation or GitHub changes are made by this benchmark owner.

Local evidence is /Users/chroma/Documents/ChatGPT/Hierarchical SPANN/version-free-writer/balance-experiments/benchmarks/. Remote source archives, binaries, first-pair outputs and raw evidence are /mnt/msmarco/balance-experiments-20261020; reverse-pair output copies are /mnt/data/balance-experiments-repeats-20261020, referenced through workspace symlinks. Reproduction uses build.sh, initial.sh, extended.sh, reverse9m.sh and finish.sh; every command array and environment manifest is in artifacts/. The report includes no combined candidate and no larger fresh builds.

All benchmark and audit processes have finished, and the shared benchmark lock is available. The host has approximately 486GiB available RAM, 143GiB free on /mnt/data and 51GiB free on /mnt/msmarco. Original 40M Wikipedia checkpoints, MS MARCO sources, prior evidence and the retained 8M snapshot remain intact. No cleanup of other work, SSH rule update, provisioning, PR publication or merge occurs.
