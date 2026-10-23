# Wikipedia writer follow-up

Compacting each source posting once reduces posting scans, but the current implementation increases insertion, balancing, and commit time in the measured 1M and 3M workloads. Serial retry processing accounts for nearly all of the additional balancing time at 3M. These results support fixing retry work before treating source compaction as a performance improvement.

## Revisions and method

The instrumented reference is `8581e80920036247715da0b40a4d0a4702118122`. The source-compaction candidate is `fc39a953806ef1d9cb02f1985b2fcf1228e5cdb8`. Both belong to [PR #7927](https://github.com/chroma-core/chroma/pull/7927). The reference preserves the transfer algorithm from `9b9c7ebde93144055b1fc86ad29e7ea3b64c140b`, with added timing and coordinate/ID validation split into separate passes. The primary comparison therefore holds that validation structure constant. The candidate also avoids unused quantized-query construction during full-precision navigation.

Each revision runs twice in opposite order on fresh Wikipedia indexes. The 1M builds commit every 100k vectors; the 3M builds commit every 1M vectors. Both use 32 insertion and balancing threads, full-precision navigation, and the established identical writer/read settings. Commands, source and binary hashes, environment details, process resource samples, raw output, and audit results are retained under `artifacts/`. Cargo uses Rust 1.92.0 with `-C target-cpu=native`. The shared host lock prevents overlapping builds or benchmarks.

The timed builds disable benchmark-only posting validation. Production insertion preflight remains included. The primary core-indexing total adds insertion after preflight, production preflight, balancing, commit fork, commit flush, and writer reopen. It excludes input loading, benchmark diagnostics, output formatting, reader-pin cleanup, object destruction, and benchmark garbage collection. The secondary wall total excludes all measured benchmark diagnostic phases and includes input loading and cleanup. Raw unadjusted totals remain in the artifacts. Exact L2 ground truth uses the same 100 fixed queries at each prefix. The existing 3M neighbors pass a scoped exhaustive float64 check; all 100 neighbor sets match. Ground-truth preparation is excluded from timing.

## Quick comparison

The table shows two-run means. A positive change means the candidate takes longer.

| Measure | 1M reference | 1M compaction | 3M reference | 3M compaction |
|---|---:|---:|---:|---:|
| Insert + balance + commit + reopen | 26.045 s | 27.443 s (+5.4%) | 66.753 s | 70.841 s (+6.1%) |
| Balance wall time | 16.717 s | 18.094 s | 34.267 s | 38.185 s |
| Serial retry drain | 0.975 s | 1.446 s | 3.772 s | 7.487 s |
| Worker wall time | 15.736 s | 16.642 s | 30.483 s | 30.684 s |
| Source posting entries scanned | 74.17 M | 11.55 M | 292.75 M | 36.89 M |
| Pending ID visits entering retry drains | 75,908 | 104,035 | 166,012 | 226,212 |
| Peak RSS | 9.10 GiB | 8.89 GiB | 23.32 GiB | 23.29 GiB |

The 3M source scans fall by 87.4%, while serial retry time rises by 3.715 seconds. That increase explains approximately 95% of the 3.918-second balancing regression. Worker wall time is nearly unchanged. Both retry volume and time per queued ID increase: average pending ID visits rise by 36%, while aggregate drain cost rises from approximately 22.7 to 33.1 microseconds per queued ID. Pending IDs count repeated queue visits rather than unique vectors.

The core-indexing phase difference varies across runs. The first 1M pair is 9.7% slower and the reversed pair is 1.2% slower. Both 3M pairs are slower, by 7.5% and 4.8%, and their serial retry costs remain elevated. Secondary wall totals, after subtracting posting validation, final posting/tree diagnostics, checkpoint memory/statistics diagnostics, and checkpoint output, average 44.470 versus 45.743 seconds at 1M and 85.197 versus 89.205 seconds at 3M. They include input loading and cleanup; the narrower core-indexing phases define the primary comparison.

## Serial insertion checks

The coordinate validation is the largest serial insertion check. At 1M, the reference averages 0.716 seconds of preflight: 0.674 seconds validates coordinates and 0.041 seconds checks IDs. At 3M, it averages 2.180 seconds: 1.999 seconds for coordinates and 0.181 seconds for IDs. Fresh sequential IDs require no persisted existence probes in these runs.

These measurements directly confirm a serial coordinate-scanning cost of roughly 0.67 seconds per million 1024-dimensional vectors on this host. They should not be extrapolated as an exact attribution of the earlier interleaved validation implementation, because the instrumented reference changes validation pass ordering.

## Correctness and counter interpretation

All eight saved quick-run indexes pass separate audits for complete ID coverage, exact-once physical postings, resolved child links, and absence of unreachable tree nodes. All 100 sampled embeddings per index match source bits, and their stored codes match recomputation against their leaf centroid. Final recall uses matching exact ground truth and varies by −0.20 to +0.69 percentage points across matched settings and run pairs; complete per-run recall rows are preserved in `artifacts/comparison.json`.

Transfer counters measure different work units across the change. Reference claim failure queues one target group. Candidate source-claim failure can queue every target group while incrementing the conflict count once, so similar conflicts can produce substantially more retry work. Successful transfer batches count pair applications in the reference and source compactions in the candidate. Claim-held duration is unweighted by the number of reserved leaves. These counters cannot establish a speedup or reduced contention on their own.

## Checkpoint 9 diagnosis

The earlier 10M candidate run spends 48.342 seconds balancing checkpoint 9, versus 18.663 seconds for the versioned baseline. The remaining checkpoints have almost equal aggregate balancing time. No exact predecessor snapshot survives from that run, and an unrelated older 8M fixture cannot reproduce its tree trajectory.

A fresh instrumented reference build reproduces the checkpoint 9 interval at 44.084 seconds. Serial retries contribute only 0.172 seconds; worker rounds contribute 43.895 seconds. The process samples cover most of that interval and record approximately 233 CPU-seconds, no major faults, and no process disk-read bytes. Checkpoint 8 records approximately 227 sampled CPU-seconds in only 13.741 seconds of balance wall time. CPU use falls from roughly 30 cores early in checkpoint 9 to roughly one core through much of its final 30 seconds. These samples support lost parallelism rather than a disk-read stall during the covered interval; they are sampled counters, not exact full-phase CPU accounting.

One replay from an independent full copy of the retained 8M snapshot confirms that the scheduler groups many available leaf jobs into a few indivisible subtree jobs. The scheduler selects the first tree level with at least 32 subtrees, estimates work by counting leaves outside their size limits, and assigns whole subtrees to workers. It cannot distribute leaves from one selected subtree among multiple workers.

| Replay round | Leaves needing work | Subtrees with work | Largest subtree's leaf jobs | Slowest worker | Next-slowest worker |
|---|---:|---:|---:|---:|---:|
| 3 | 56 | 14 | 16 | 8.313 s | 1.194 s |
| 4 | 38 | 9 | 27 | 8.193 s | 0.322 s |
| 5 | 39 | 7 | 32 | 6.097 s | 0.256 s |

This grouping produces a concrete scheduling bottleneck even when dozens of leaf jobs remain. Individual leaf costs also vary: in replay round 2, a worker visiting eight eligible leaves takes 6.215 seconds while another visiting fifteen takes 2.956 seconds. Finer partitions or dynamic leaf scheduling are plausible follow-ups, but these experiments do not measure their benefit. The reproduced shape strongly explains the historical checkpoint 9 outlier; it does not prove that every internal event matches the historical run.

The diagnostic uses reference `8581e809` plus recorded timing/counter overlays. It disables recall rather than using mismatched 9M ground truth. Its retained 8M snapshot and final 9M index remain separate from the speed comparisons. Automatic approval rejected using hard links for the writable replay, so the replay uses full independent file copies. No further large builds or replays are needed for this diagnosis.

The PR also changes when destination leaves are balanced. The versioned baseline calls destination balancing recursively after registration, while the version-free transfer path leaves that work to outer balancing rounds. This code difference can expose the coarse subtree scheduling tail, but the benchmark does not isolate it as the sole cause. The regression should not be attributed to version removal alone.

## Filtering superseded retries

The reviewed follow-up `eea4ada6f21554adb535d1ffdea95f74cb76650b` checks queued IDs against their source leaf before loading embeddings and navigating again. In one fresh 3M run, 130,211 of 225,452 coalesced queued IDs are superseded (57.8%); 95,241 remain current. The filter avoids navigation for the superseded IDs while adding a source-membership scan. Its implementation handoff reports 161 passing targeted tests, formatting, benchmark-driver compilation, and independent review.

| 3M measure | Instrumented reference, mean of two | Source compaction, mean of two | Compaction + membership filter, one run |
|---|---:|---:|---:|
| Insert + balance + commit + reopen | 66.753 s | 70.841 s | 68.931 s |
| Balance wall time | 34.267 s | 38.185 s | 36.972 s |
| Serial retry drain | 3.772 s | 7.487 s | 5.641 s |
| Worker wall time | 30.483 s | 30.684 s | 31.319 s |
| Peak RSS | 23.32 GiB | 23.29 GiB | 23.23 GiB |

The filter reduces retry time by 24.7% relative to the unfiltered mean. Its core indexing total is 2.7% lower than the unfiltered mean and 3.3% higher than the reference mean. This single run supports the mechanism of avoiding stale navigation, but its overall timing improvement is small relative to observed run variation. The combined candidate does not establish a net performance win, so no further runs or destination-claim experiments are performed.

The final filtered index passes the separate audit with 3,000,000 physical postings, 3,000,000 live IDs, complete root-reachable coverage, and 100 matching embedding/code samples. Its exact L2 recall is 77.12% / 98.30% at tau 1.5 with rerank 1 / 8, and 77.16% / 98.51% at tau 2 with rerank 1 / 8. The corresponding reference runs are 76.25–76.54% / 97.87–98.24% and 76.60–76.79% / 98.36–98.68%. These small-run results show no material recall loss from the filter.

## Artifacts and final state

All nine quick-run indexes pass their independent saved-index audits. The fresh diagnostic 9M index, replay 9M index, and preserved 8M snapshot also pass; the 8M metadata remains unchanged. The host has no remaining benchmark/build processes, the shared lock is released, and approximately 183 GiB remains free on the output NVMe volume. Original source caches and prior Wikipedia/MS MARCO indexes remain untouched.

Local evidence is stored in `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/version-free-writer/followup/benchmarks/`. Remote scripts, exact source archives, binaries, indexes, and evidence are stored in `/mnt/msmarco/version-free-followup-20261020`. The retained snapshot is `/mnt/msmarco/version-free-followup-20261020/wikipedia-diagnostic-9m-1/snapshots/8000000`; the replay uses separate full file copies. Machine and binary manifests, commands, raw logs, per-second process samples, audits, and comparison data are in `artifacts/`. The diagnostic overlays and `replay-partition-analysis.json` document the checkpoint 9 evidence.

The resource sampler records one-second observations, so its CPU and IO deltas cover sampled intervals rather than every instant. One reference 3M sampler encounters a permission race during process teardown after covering 88.35 of 89.37 process seconds; later samplers tolerate that race. The replay audit initially receives a duplicated `--resume` flag from its wrapper and exits at CLI parsing; the corrected audit succeeds, and the initial error log is preserved. Neither event is an index correctness failure.

No full 10M build runs in this follow-up. The only larger work is the approved reference diagnosis through 9M and one 8M-to-9M replay. No PR merge or readiness change is performed.
