## Wikipedia benchmark follow-up

The comparison measures insertion (including production preflight), balancing, commit fork/flush, and writer reopen. It excludes benchmark posting validation, input loading, diagnostics, and cleanup. Separate saved-index audits verify correctness. The reference is instrumentation-only `8581e809`; source compaction is `fc39a953`; the final retry-membership filter is `eea4ada6`. Wikipedia uses identical FP navigation and 32-thread settings. The 1M builds commit every 100k vectors; 3M builds commit every 1M.

| Core indexing time | Instrumented reference | Source compaction | Compaction + retry filter |
|---|---:|---:|---:|
| 1M, two runs in opposite order | 26.045 s | 27.443 s (+5.4%) | Not run |
| 3M | 66.753 s, two-run mean | 70.841 s, two-run mean (+6.1%) | 68.931 s, one run (+3.3%) |

Source compaction reduces scanned posting entries by 87.4% at 3M, but serial retry drain grows from 3.772 to 7.487 seconds and accounts for about 95% of the balancing regression. The membership filter finds 57.8% of coalesced queued IDs are superseded and lowers retry time to 5.641 seconds. Its overall 2.7% improvement over unfiltered compaction is a single-run result within observed timing variation; the combined candidate has not demonstrated a net performance win over the reference. The implementation handoff reports 161 passing targeted tests for the final filter.

Checkpoint 9's long balance is reproduced at 44.08 seconds and explained by coarse subtree scheduling. In the independent 8M-to-9M replay, one round has 38 leaves needing work, but one indivisible subtree contains 27 of them: its worker takes 8.193 seconds while the next slowest takes 0.322 seconds. Process samples show roughly one active CPU core during much of the tail, without sampled disk reads or major faults. The PR also defers destination balancing to outer rounds, unlike the versioned baseline's recursive destination balancing, so these costs cannot be attributed to version removal alone.

All nine quick-run indexes and the diagnostic/replay indexes pass separate saved-index audits. The final 3M filtered index contains exactly 3,000,000 physical/live postings, no missing or unreachable tree nodes, and 100 source-matching embedding/code samples. Exact L2 recall is 77.12% / 98.30% at tau 1.5, rerank 1 / 8, and 77.16% / 98.51% at tau 2. The retained 8M snapshot also passes its audit. This follow-up runs no full 10M build and makes no readiness or merge decision.

Full commands, hashes, logs, resource samples, and the report are retained locally under `version-free-writer/followup/benchmarks/` and remotely under `/mnt/msmarco/version-free-followup-20261020/`.
