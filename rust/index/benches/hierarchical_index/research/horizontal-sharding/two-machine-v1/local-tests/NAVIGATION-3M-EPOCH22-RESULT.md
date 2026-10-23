# Navigation repair: local route churn still aborts construction

The shared-view repair passes its controlled correctness tests, but the physical 3M attempt still fails. It completes all ten intervals through 1M, adds the next million in 5.884 seconds, then aborts 10.702 seconds into balancing at 2M. Redistribution has not occurred, so this failure comes from owner-local construction. There is no completed 3M candidate, saved generation or final membership validation.

The added diagnostics distinguish this failure from the controlled centroid-publication mechanism. The failing navigation processes 702 query rows, exhausts eight attempts in 0.495514 seconds, and reports `navigation route changed: stale local image`. Both its pinned and current shared sequence are 2. The exact error names split leaf `18446744073711013099` and self-reassignment from `18446744073713207117`. The unchanged shared sequence excludes shared publication changes as this attempt's retry cause; it does not yet explain which local route changes or validation rule repeatedly reject the attempt.

## Implemented correction and validation

Each navigation attempt pins one immutable shared routing image. A publication-time topology digest distinguishes harmless centroid/revision refreshes from changed root identity, child ordering, eligibility or ownership placement. Live owned-route checks and the eight-attempt bound remain. Final failures include the last rejection reason, sequences, query count and elapsed time. See the [diagnosis](EPOCH21-NAVIGATION-FAILURE-DIAGNOSIS.md) and [independent review](EPOCH22-NAVIGATION-REVIEW.md).

The controlled actual-Cluster test reproduces the exact retry exhaustion under eight centroid-only publications and passes with one attempt after the fix. Seven focused tests cover remote-owner selection, stale/moved targets, replacement routing, malformed compatibility and finite structural churn. The applicable suite passes 213 tests with one unchanged unrelated local-writer exclusion; all three native CLI builds and the Linux benchmark/reader build pass. The complete frozen source is `source-epoch22b.tar.gz`, SHA256 `45d56253789916dbacc04a656aee18f42f3d654a0780454759925b5cde8e9af0`. The incomplete local archive22 is rejected before upload and remains retained.

These tests justify retaining the narrow compatible-publication fix. They do not establish complete construction liveness or exclusive attribution of the epoch21 physical failure. The earlier physical trace lacks per-attempt reasons, and epoch22 now exposes a separate local-route problem.

## Partial observations and next decision

The incomplete 2M balance records 272.41 CPU-seconds on owner1 and 0.72 on the empty peer. Owner1 records 54.128 clustering CPU-seconds, 722 splits and 408,149 transferred rows before aborting. These partial work counters are evidence for diagnosis, not a completed-balance throughput result or proof that all mutations finish correctly. The current valid reference21 remains the comparison baseline; it is not rerun.

The next investigation must distinguish a persistently stale routing reference from legitimate repeated local edits. Inspect the last-rejection path and reproduce the condition. If discovery is safe to defer after a split is fully installed, preserve the unfinished reassignment in the scheduler and release its claim before waiting. Verify exact membership and eventual processing under a controlled conflict; do not convert exhaustion into success, discard work, increase the retry cap blindly or keep retrying while holding protection needed by another worker. The benchmark's absolute execution limit remains independent.

The separate candidate-assembly optimization remains local-only and is excluded from this frozen run. It aims to remove extra ordering work while preserving complete remote validation and reference input order. It has no measured performance result yet. The distributed insertion payload findings from [epoch21](EPOCH21-DISTRIBUTED-INSERTION-ANALYSIS.md) remain valid for their completed insertion phase, but correctness takes priority over remote representation changes.

## Evidence, shutdown and cost

The [raw receipts](../operations/window-epoch22/3m-distributed/run-receipts.json), [partial analysis](../operations/window-epoch22/candidate-partial-summary.json), [Linux build receipt](../operations/window-epoch22/linux-build-receipts.json) and [runtime identities](../operations/runtime-binaries-epoch22.json) preserve this attempt. The partial analyzer is `summarize-partial-construction.py`; it explicitly marks the result incomplete. Input/runtime checks run independently and concurrently once per host, taking approximately 69 seconds each. No reference rerun, external profiler, sweep, automatic retry or deadline extension occurs.

Both matching supervisor cleanup receipts pass: the coordinator exits with the benchmark error and the peer stops on the operator's request. Both exact instances are API-confirmed stopped. The stop helper completes without error in this run; this does not erase earlier failures. The closed window adds 1.3260795054626464 compute once and no storage. Covered compute is 44.819599262642324083; storage remains $19.52/month and the first-month covered bound is 64.339599262642324083. This is task accounting, not a whole-account invoice.
