# Gross version cost in the versioned writer

## Findings

Version lookups consume a substantial share of sampled CPU work in this 1M-vector build. Resolved version lookup, cache, and prefetch stacks account for 60.57 of 257.06 sampled user CPU-seconds during indexing and writer destruction, or 23.56%. Almost all of that attributed work occurs during balancing. This measures gross work in the versioned writer; it does not predict elapsed savings from deleting versions or include any replacement transfer algorithm.

Persisting changed versions takes about 0.87 seconds across ten commits. This includes gathering and sorting changed IDs, writing their version values, and clearing the changed-ID set. The shared scalar metadata container adds another 0.11–0.12 seconds for commit and flush, which is an upper bound for that part of version persistence because the container also stores other metadata.

Version update and stale-entry cleanup costs remain partly unresolved. Optimized call stacks lose enough callers that generic map operations cannot reliably be assigned to individual version writes or dirty tracking. Existing stale-entry cleanup timers report 78.91 seconds summed across worker operations, including version checks, posting loads, lock waits, and compaction. That overlapping sum is neither elapsed time nor additional cost that can be added to the lookup samples. A separate scan-versus-lookup-versus-compaction total is not supported by this profile.

## Workload and controls

The experiment uses the versioned baseline `1f433be7969b3d28c78e463fe33a299fa0c5fee9`, Wikipedia's first 1M 1024-dimensional vectors, and ten 100k checkpoints. All four fresh builds use identical full-precision write navigation and 32 worker threads. Benchmark posting validation is disabled during timed builds. Separate saved-index audits validate correctness afterward.

The marker overlay adds coarse phase and version-persistence timestamps. It adds no per-vector timers, changes no version semantics, and makes no correctness-changing ablation. CPU sampling attaches to the marker build at 49 Hz using user CPU-clock events and 16 KiB DWARF call stacks. The controls run in the order shown below.

| Run | Insert + balance + commit + reopen | Build excluding benchmark diagnostics | Full process, including final reads |
|---|---:|---:|---:|
| Unmodified baseline | 37.252 s | 55.767 s | 58.801 s |
| Marker-only control before sampling | 35.815 s | 54.379 s | 57.547 s |
| Marker build with CPU sampling | 36.784 s | 55.062 s | 58.290 s |
| Marker-only control after sampling | 41.142 s | 59.541 s | 62.549 s |

The core indexing column sums exclusive add, balance, fork, flush, and reopen phases. The secondary build column also includes input loading, writer destruction, input cleanup, reader-pin clearing, garbage collection, and outer-loop bookkeeping. It excludes posting validation, checkpoint diagnostics and metadata output, snapshot retention, final posting loads, and final tree statistics. Final read/recall work is outside the build timer. Raw exclusive phases remain in `artifacts/comparison.json`.

Sampling adds 2.70% relative to the preceding marker control and is 10.59% faster than the following marker control on core phases. The sample lies within the control range. The marker-only control range is −3.86% to +10.44% relative to the unmodified run. These runs show no separable observer slowdown, but their variation prevents a tight overhead estimate; the figures are observed comparisons, not a statistical bound. Peak RSS ranges from 8.03 to 8.19 GiB. No 3M run is needed to support the bounded 1M attribution.

## CPU attribution

The following values sum sampled event periods across threads. They are estimates of user CPU work, not critical-path elapsed seconds. A sample contributes once to the lookup union even when it contains nested version functions. Subsets must not be added to that union.

| Resolved operation | Sampled CPU-seconds | Interpretation |
|---|---:|---|
| Version lookup/cache/prefetch union | 60.571 | 23.56% of eligible sampled CPU weight. |
| Current-version lookup, inclusive subset | 40.469 | Includes resolved callees such as map lookup. |
| Paged version-cache get, inclusive subset | 19.735 | Includes page lookup, hashing, and atomic slot access when resolved. |
| Version prefetch, inclusive subset | 0.327 | Caller loss prevents treating this as the complete prefetch cost. |
| Version update/dirty tracking with exact source evidence | 0.041 | Sparse resolved evidence only; not an estimate of total update cost. |

Balance contributes 188.959 sampled CPU-seconds, of which 60.429 seconds have version lookup evidence (31.98%). Add contributes 63.224 sampled CPU-seconds, of which only 0.143 seconds have resolved lookup evidence. These proportions describe this workload and profile; missing callers can hide other version work.

The source confirms that fresh inserts update a version map and mark the ID dirty, while reassignment validates and increments versions. Version reads consult the in-memory update map, a paged cache, and persisted blocks when needed. Shared map and lock symbols without a resolved caller remain unassigned. The profile cannot separate CPU spent spinning from blocked lock time. It also excludes kernel CPU because the event uses `:u`; the process reports 56.54 system CPU-seconds for the whole run.

## Persistence elapsed time

These coarse markers bracket serial operations across all ten commits. They are nested within existing commit phases and must not be added again to core indexing time.

| Operation | Before control | Sampled run | After control |
|---|---:|---:|---:|
| Gather and sort changed versions | 0.1401 s | 0.1378 s | 0.1416 s |
| Write changed version values | 0.7264 s | 0.7150 s | 0.7243 s |
| Clear changed-version IDs | 0.0121 s | 0.0120 s | 0.0121 s |
| Version-specific subtotal | 0.8786 s | 0.8648 s | 0.8779 s |
| Shared scalar container commit | 0.0489 s | 0.0356 s | 0.0390 s |
| Shared scalar container flush | 0.0687 s | 0.0746 s | 0.0713 s |

The sampled version-specific subtotal is 2.35% of its 36.784-second core indexing time. The shared commit/flush upper bound adds 0.1102 seconds. Version bytes embedded in posting serialization, cache destruction, allocation, and shared metadata are not fully isolated by these markers.

## Stale-entry cleanup

A posting associates an ID with the version current when it enters a leaf. Cleanup scans that leaf, looks up each ID's current version, and removes mismatches from IDs, versions, and codes. Its existing timer includes loading the leaf and acquiring locks as well as the scan and removals.

The sampled build records approximately 352k cleanup calls and 344.3k removals in rounded task counters. Its existing cleanup timers sum to 78.91 worker-seconds across checkpoints. Only 0.510 sampled CPU-seconds retain a resolved cleanup caller, demonstrating that the recovered callchains cannot attribute the inclusive cleanup total reliably. That sparse sample value must not be described as cleanup's full CPU cost. Lookup work inside cleanup can be present in the 60.57-second lookup union without its cleanup caller surviving the unwind. No additional per-entry instrumentation or correctness-changing ablation is used to manufacture a split.

## Sampling recovery and completeness

The benchmark completes successfully, but the profiler's inline symbol postprocessing stalls in its address resolver after the benchmark exits. The harness times out waiting for that profiler. Only this experiment's recorder is terminated. The benchmark's zero exit status, original stdout, checkpoint, and process timing remain intact; the sampled run's result JSON is reconstructed from those files and records that provenance.

The original 249,481,968-byte perf file is preserved unchanged on the host. Its event records end exactly at EOF, but its final data-size header is unwritten. The recovery script verifies every record boundary, copies the file, and changes only the data-size field in the copy. It finds 14,968 sample records and no PERF_RECORD_LOST or PERF_RECORD_LOST_SAMPLES records. This shows no recorded lost-event notification; it does not prove that every possible sample was captured. Attaching after process start, user-only events, and recovery limitations remain relevant.

Decoding with inline resolution disabled succeeds without reported decode errors. Direct address resolution then maps 1,859 unique binary addresses to source and inline frames. Of 12,596 eligible indexing samples, 5,392 have only one recorded frame, 3,351 contain an unknown frame, and 401 have an unresolved leaf frame. These categories overlap. Tail calls, optimization, shallow or failed unwinds, and missing metadata can all remove attribution. The analysis assigns no guessed cost to those missing callers. The recovered profile represents about 305.47 total sampled user CPU-seconds across the process, compared with 315.90 user CPU-seconds reported by process accounting; phase-specific accounting is unavailable.

## Correctness and recall

All four independent saved-index audits pass. Each has exactly 1,000,000 valid IDs, physical postings, and live postings under the baseline's version semantics. All 100 sampled embeddings and quantized codes match bit-for-bit. The audit checks reachable persisted structure and ID coverage separately from timed builds.

The exact ground truth matches the first 1M IDs and the same 100 query IDs. Warm recall ranges across the four builds are 77.78–78.39% at tau 1.5/rerank 1, 98.93–99.27% at tau 1.5/rerank 8, 77.91–78.55% at tau 2/rerank 1, and 99.19–99.60% at tau 2/rerank 8. Parallel tree construction varies across runs; the overlay changes no algorithm. Raw recall rows and ground-truth provenance are retained.

## Reproduction and artifacts

The run uses the existing EC2 host with 64 logical CPUs, 32 physical cores, and approximately 495 GiB RAM. Compilation uses Rust 1.92.0 and `-C target-cpu=native`. Work remains on NVMe. The shared benchmark lock covers builds, runs, and audits. Existing datasets, checkpoints, and the retained 8M diagnostic snapshot remain untouched.

The unmodified binary SHA-256 is `156f0511621865770ec26dd02d96adbfd14f3af37ed39d6f30ae1f31a1474d71`. The marker binary SHA-256 is `4f2c80548eb6b8ca791dbfe4153d6c3c10f9f496a1159c106cbbdec8bc41f76f`. Both use baseline source plus the documented benchmark harness; `artifacts/profile-only.patch` contains the additional profiling overlay.

Local artifacts are under `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/version-free-writer/version-cost-profile/`. Remote artifacts, binaries, raw perf data, repaired perf copy, and saved indexes are under `/mnt/msmarco/version-cost-profile-20261020`. Commands, logs, process samples, source-address mapping, recovery evidence, audit output, and timing JSON are retained. `run.py`, `overlay.py`, `build.sh`, `audit.py`, `analyze_samples.py`, and `recover_perf_header.py` document the workflow. The original runner's profiler-finalization timeout is preserved as evidence.

The useful conclusion is that version lookup CPU is material and direct version persistence is under one second at 1M. This profile does not establish the total removable elapsed cost of versions. Update/dirty-tracking totals and the cleanup subcomponent split remain unresolved, and replacement transfer costs require the separate end-to-end candidate comparisons.
