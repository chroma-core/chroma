# Shared-tree sharding pilot

This report captures the first completed milestone. Current implementation continues through small local correctness tests; the task fleet is last API-confirmed stopped, and both GPU instances are terminated. Current evidence and limits are recorded in [CONTINUATION-RUNS.md](CONTINUATION-RUNS.md) and the [implementation acceptance matrix](dynamic-review/IMPLEMENTATION-ACCEPTANCE.md). The shutdown and cost figures below describe the first milestone only.

One logical search tree now distributes its lower subtrees across machines while keeping one global search budget. The pilot verifies distributed construction, consistent reads, and atomic publication after a sparse append. It establishes an implementation and a measured starting point for the larger study; it does not establish billion-vector capacity or production readiness.

## What is implemented

A frozen upper routing tree assigns each vector to an owned lower subtree. Explicit mappings preserve the original dataset IDs. A published tree manifest binds the upper model, each immutable lower snapshot, and its mapping; placement changes move the same content tree between machines. A new tree version can reuse an unchanged owner's older snapshot. Compare-and-swap publication prevents concurrent publishers from silently replacing each other.

The coordinator pools branches from all owners into one global beam, which limits the number of branches retained at each level. It then pools approximate candidates into one global reranking budget before requesting exact distances. Reads reject missing, stale, or inconsistent responses. The small real-data append test preserves the old tree during publication and exposes every new ID afterward.

Writer repairs protect structural changes, preserve versions across reopening, preserve vector codes after failed merges, and make reassignment registration safe when a target disappears. The final revision audits complete ID coverage before publication with bounded per-leaf reads and a membership bitmap. A failed audit leaves the previous published generation intact.

## Measured results

The construction experiment uses one million real MS MARCO document embeddings and 16 fixed spatial subtrees. Every successful build converges without balance-limit warnings and passes publication and coverage checks. Each owner uses one balancing thread. The same prepared task graph runs at several per-host concurrency settings.

| Concurrent owner jobs per host | One-host construction | Two-host construction |
| --- | ---: | ---: |
| 1 | 139.48 s | 75.79 s |
| 2 | 73.27 s | 46.53 s |
| 4 | 43.47 s | 26.62 s |
| 8 | 27.13 s | 22.37 s |
| 16 | 17.86 s | Not run |

The fastest measured one-host construction beats the fastest two-host construction. The one-host sixteen-job phase uses only 18.2% of 64 vCPUs and about 4.47 GiB sampled concurrent resident memory. This small task graph does not demonstrate a machine-capacity benefit from adding hosts. A failed sixteen-job SSH launch is retained separately from its successful retry. These are initial construction measurements; sparse-append correctness is verified, but append throughput and compaction scaling are not measured.

The million-vector read experiment uses the same immutable tree, a global beam of 64, and a global candidate budget of 1,000. All 100 held-out queries produce identical IDs, distances, frontiers, candidates, and reached leaves on one and two hosts. Calibration reaches 89.08% recall@100, so the selected setting is the declared highest-recall fallback after missing the 90% calibration target. Held-out recall is 90.01% on both placements; this does not retroactively satisfy the calibration gate.

| Read measurement | One host | Two hosts |
| --- | ---: | ---: |
| Mean coordinator latency, warm held-out pass | 143.1 ms | 107.0 ms |
| Throughput, one concurrent client | 7.43 queries/s | 9.20 queries/s |
| Throughput, two concurrent clients | 11.48 queries/s | 15.28 queries/s |
| Throughput, four concurrent clients | 14.72 queries/s | 20.80 queries/s |
| Throughput, eight concurrent clients | 15.47 queries/s | 24.72 queries/s |

The eight-client pilot shows about 1.60 times the throughput on two hosts. Each load point is a single 15-second warm-pool window, and the pools contain only 100 held-out document queries. These measurements do not support a p99 or saturated host-capacity claim. The one-host eight-client window uses about 2.35 of 64 CPU cores, indicating that the adapter and serialized subtree readers need profiling. The sixteen-client run fails at the adapter admission limit; its original partial counts are unavailable. A separate journaled diagnostic records 91 attempts, 86 successes, and five closed-connection errors. The harness now preserves each request outcome when another client fails.

The FineWeb correctness and quality gate uses 100,000 real vectors with their supplied float16 values preserved exactly in float32. A pinned model and query source produce disjoint 100-query calibration and test pools. Exact answers are recomputed over this subset. Calibration recall@100 is 97.56%, and held-out recall is 98.07% at beam 64 and budget 1,000. All 100 one-host and two-host traces match. All 16 lower snapshots converge and pass the final full audit. Independent source-row checks validate 300 returned inner-product distances. This gate uses audited-v3; the million-vector timing results use the separately preserved verified-v2 binary.

## Validation and artifacts

Linux validation passes all 56 Python tests without skips and ten enabled Rust release tests. Three real-index smoke checks cover the baseline worker, shared-tree traversal, and the full audit above 100,000 rows. The audit smoke verifies that failure after flushing does not advance publication. Parallel structural tests cover eight seeds, and ten repeated tree smokes pass. Independent reviews recompute recall, verify query separation and artifact hashes, compare complete traversal traces, and check audit bindings.

The isolated checkout is `/private/tmp/chroma-hierarchical-spann-sharding` on branch `codex/hierarchical-spann-sharding`; changes remain uncommitted. The complete patch, source overlay, and per-file hashes are in `artifacts/implementation-complete.patch`, `artifacts/implementation-source.tar.gz`, and `artifacts/implementation-source-manifest.json`. Raw measurements, independent reviews, and revision-specific evidence are preserved alongside this report. All three new sandbox instances are confirmed stopped after preservation; production and the existing Lyon node are unchanged. Estimated compute is approximately $16.32, excluding storage and transfer. Retained root disks cost approximately $24/month. Fleet preservation and shutdown evidence are recorded in `fleet/README.md`.

## Next scale study

The next experiment fixes 128 logical subtrees and measures suitable 8- or 16-vCPU allocations before expanding to one, two, four, and eight hosts. Storage capacity is sized separately from CPU. A separate epoch can evaluate more subtrees or parallel balancing within each owner; changing the partition count is not a placement-only scaling result.

The preferred larger corpus is pinned FineWeb, progressing through exporter and reference checks before 100M, 250M, 500M, and 1B vectors. Prepared vectors, owner copies, and mappings alone project to roughly 644 GB at 100M and 6.44 TB at 1B, before index and temporary data. The streaming loader verifies resumable chunks, preserves IDs and values, samples across the full requested prefix, and publishes complete outputs atomically. Its Linux fixture passes; no 100M or billion-vector run has been performed.

Publishable curves require a calibration setting that meets the declared recall target, larger distinct query pools, repeated measurements, controlled cache conditions, complete failure accounting, and profiling beyond the current adapter limit. Every subset needs its own exact reference answers. Full-corpus top-1000 answers cannot be filtered into a valid subset reference. The design, benchmark protocol, dataset pins, and resource gates are detailed in `DESIGN.md`, `BENCHMARK.md`, `DATASETS.md`, and `subtree/SCALE-NEXT.md`.

The current implementation keeps ownership boundaries fixed and uses dense append-only local IDs. It does not implement owner migration, cross-owner rebalancing, or production service orchestration. Retained decoded postings remain outside the block-cache budget, and structural locking includes vector loading and clustering. Those limits must be measured or addressed before claiming large-scale capacity.
