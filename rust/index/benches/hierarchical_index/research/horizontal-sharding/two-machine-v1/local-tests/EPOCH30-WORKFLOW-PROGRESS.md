# Completion status

The workflow is complete. The [final baseline and navigation report](EPOCH30-BASELINE-AND-NAVIGATION.md) and [transport comparison](EPOCH30-TRANSPORT-COMPARISON.md) govern the results and decisions. The implementation and attempt notes below retain intermediate binary identities; use the [completion receipt](EPOCH30-COMPLETION.json) for final artifacts.

# Checkpoint-based baseline and navigation research

The active workflow is defined in [the current plan](../../CURRENT-PLAN.md), based on [the experiment-28 investigation](EPOCH28-EXISTING-DATA-INVESTIGATION.md). Completion requires the measured baseline, one navigation optimization, and a comparison from the same 2M checkpoint.

## Current implementation

Each owner saves full-precision routing records, exact membership and posting bytes, the immutable shared routing view, ingestion history, operation counters, configuration, input digest, and next input position. Saving requires drained queues and sealed mutation admission. A fresh process checks the checkpoint digest and configuration, reconstructs prepared navigation and reservation state, retains identity counters, and opens the next mutation epoch. The checkpoint is a writer image, independent of the compressed reader format. The native restore test checks exact records, duplicate-ingestion rejection, continuing insertion, operation-ID progress, checksum rejection, and refusal to overwrite checkpoint files.

Placement chooses whole-branch moves that reduce the sum of squared owner vector counts. It refines existing internal branches when needed and preserves the existing 70% capacity ceiling and explicit insertion reserve. Indivisible leaves can leave a small residual difference. Every move preserves IDs and membership through the existing handoff protocol.

The harness samples each owner once per second and records active workers, ready and blocked queues, completed jobs, process CPU, and cumulative work scopes. Transport scopes distinguish client elapsed time, server execution, JSON encoding and decoding, connection establishment, and thread creation/start delay. Navigation scopes distinguish canonical reconstruction, reference-compatible sorting, index allocation, and gathering. Fine scopes are enabled separately from timing runs. Histograms use logarithmic microsecond buckets; they are distributions, not exact percentiles. Existing transfer counters distinguish membership probes, compaction and logical ID/code bytes copied. Inclusive scopes overlap and must not be added.

The construction-only path checks exact global membership and every posting code against the full-precision centroid and immutable vector after measured construction. A read-only navigation replay uses 256 deterministic vector queries against the restored routing view. All final timing and diagnostic runs are complete.

## Validation and retained attempts

The applicable native suite passes 270 tests, with two actual-S3 tests ignored and the previously documented unrelated parent/leaf concurrency test excluded. An initial sandboxed test invocation fails on denied localhost socket creation; the network-enabled run passes. Logs are retained alongside this report.

The Linux baseline builds in 27.34 seconds from the retained compiler cache. Its SHA-256 is `49ff6f7cb028f2ad6580c96025be419432c66556e30a728505b183fc411e0c96`. Only ten changed source files are transferred; the frozen experiment-29 source and binaries remain intact. The deployment uses the existing content-addressed compressed binary cache.

The first deployment helper rejects the new directory before transferring data. Its allowlist now names the scoped development baseline/candidate paths. The first preparation attempt cannot find the undeployed peer binary. The second preparation reaches peer readiness but the coordinator rejects a missing reader-binary argument before loading input. The next attempt supplies the existing reader binary path. These attempts are retained under `operations/development30/prepare*`.

## Verified checkpoint and frozen baseline

The checkpoint contains exactly 2,000,000 distinct vectors across both owners, with no overlap and an identical shared routing view. The two ownership counts are 999,258 and 1,000,742. Checkpoint hashes and the exact membership bitmap proof are retained in `operations/development30/checkpoint-manifest.json` and `checkpoint-membership-proof.json`. The files remain on the development machines and are reused unchanged.

The fixed 256-query navigation replay passes after direct typed serialization preserves 128-bit node IDs. The isolated baseline SHA-256 is `e79dfd8300d3220bc16348985d751c959232aba9b0f934a2934c9b250baac331`. Its source manifest is `operations/development30/baseline4-source-manifest.json`. The source directory is `/private/tmp/spann-checkpoint-research30`; it deliberately excludes concurrent saving changes found in the shared checkout. The isolated native suite passes 271 tests. Timing repetitions and detailed CPU profiles are complete.
