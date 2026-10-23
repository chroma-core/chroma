# Construction scheduler and critical-path audit

The [epoch18 result](QUEUE-SAVE-500K-RESULT.md) closes the admission/completion convoy hypothesis. A controlled test reproduces all four operation-to-queue blocking paths, and the queue-first correction removes nearly all measured admission/terminal waiting. Later balancing takes 2.067 seconds versus 9.067 previously. More concurrency increases retries and CPU; reference-style combination of compatible ready reassignment jobs is the next bounded experiment. Earlier audit anchors and readiness evidence remain source-specific.

The clearest scheduler concern is repeated work whose recorded dependency does not identify the active worker that holds its claim. This is a source-supported readiness gap, not a measured explanation of the whole performance difference. Keep the current direct-navigation comparison unchanged, then use its results to decide whether a separate scheduler experiment is justified.

## Measured evidence

The retained epoch15 result reports 8.540 seconds of balancing and 43.03 process CPU seconds after the first 100K interval. This is about 5.04 effective CPU cores across that interval, despite a reported active-job peak of 32. The peak counts admitted jobs, including jobs waiting on locks; it does not measure simultaneous useful CPU work. The retained reference reports 2.314 seconds and 33.44 CPU seconds for its corresponding tail. Independent randomized construction changes work, so these ratios do not isolate scheduler overhead.

| Rows after interval | Balance wall seconds | Local-transfer inclusive wall seconds | Local-transfer CPU seconds | Rejected reassignment groups | Reassignment rounds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 200K | 3.291 | 53.750 | 0.614 | 1,705 | 1,059 |
| 300K | 1.925 | 32.775 | 0.534 | 1,192 | 941 |
| 400K | 1.961 | 33.158 | 0.552 | 942 | 893 |
| 500K | 1.363 | 20.610 | 0.518 | 832 | 836 |

These local-transfer wall totals add overlapping operations and must not be added to phase latency. They show waiting or descheduling rather than expensive transfer CPU work. The recorded preparation and commit scopes together account for roughly half a second per tail interval; much of the outer transfer wall time lies outside those scopes. Existing region-reservation queued waits total about 24 milliseconds across the tail and do not explain seconds of delay. Evidence comes from `operations/window-epoch15/candidate-summary.json` and its original `500k-distributed/result.json`.

## Ranked questions

1. **Does a deferred job wait for the actual claim that blocked it?** The job queue excludes a deferred reassignment only while one of its blocked node IDs appears in the set of active job node IDs (`construction.rs:104`). A local transfer marks its claimed leaves with the transfer source ID (`local_transfer.rs`, claim acquisition). During a split job for node R, synchronous self or neighbor reassignment can transfer from a fresh child A or neighbor N. The active job remains keyed by R, while the recorded blocker is A or N. The queue can therefore run the retry while the actual claim remains held. The retry can repeat navigation, fail the same claim and enqueue a fresh attempt. This can amplify work; the retained counters do not identify how often this exact path occurs.
2. **Where does local-transfer waiting occur?** Before its inner preparation scope, the transfer serializes and admits its identity through the owner’s operation mutex. After its inner scopes, claim cleanup takes resident write locks and then terminalizes through that same operation mutex (`local_transfer.rs:30`, `engine.rs:324`). The queue-enqueue handlers hold the operation mutex while acquiring the job-queue mutex (`engine.rs`, Enqueue/EnqueueNeighbor/EnqueueReassign). A lock convoy is plausible. The current timing scopes cannot distinguish admission wait, cleanup wait, nested instrumentation wait or ordinary descheduling. This is a hypothesis, not an attributed cause.
3. **Can compatible deferred work avoid repeated navigation and source scans?** Each fresh reassignment attempt enters the queue separately; replay deduplication keys its attempt, not its source and ID set (`construction.rs:87`). The reference groups pending retries by source and exclusion, sorts and deduplicates their IDs before processing them (`writer/writer.rs:1196`). This is a concrete implementation difference. Coalescing must preserve exclusions, current membership checks and dependency readiness; it must not remove a required newly evaluated decision.
4. **Does the central queue create avoidable owner-affinity overhead?** The distributed balance driver creates 32 workers per owner, including the unused peer before placement. All workers share one queue; an owner scans past jobs belonging to another owner under its mutex, and all workers wake on enqueue or finish (`construction.rs:790`). This can increase scans and mutex traffic. It does not establish a throughput bottleneck by itself. Owner affinity is necessary for actual remote work; local idle peer workers cannot execute mutations on another owner simply by stealing a job.
5. **How much of the remaining tail is a necessary split dependency chain?** A split publishes replacement leaves, performs self and neighbor reassignment, then schedules the replacement leaves (`construction.rs:420`). The reference follows the same ordering (`writer/writer.rs:1490`). The first large root clustering call also limits initial available work in both implementations. Scheduling replacements earlier changes interactions and work, so it is an algorithm experiment rather than a missing reference optimization.

All source anchors refer to `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index`; line numbers identify the reviewed source and may shift with the direct-navigation change.

## Readiness regression and minimal next observation

A controlled readiness test should keep a balance job for R active while a local transfer from child A holds its posting claim. Enqueue a reassignment blocked on A and ask the scheduler for another job. The current active-ID predicate permits it because A is absent from the active-job set. A corrected design should withhold that retry until the actual producer or claim releases, while still dispatching independent work. Also test reciprocal transfers and a claim released after a node is retired. A fix needs a reliable claim-release or producer identity; substituting a different guessed node ID is insufficient.

The next navigation-only run should retain the existing observation scheme: compare process CPU and phase wall, split-kernel work, jobs, reassignment rounds and rejections, query rows, logical distance counts, and inclusive scope CPU/wall. Do not change instrumentation during that comparison. If the gap persists, run one separate diagnostic that measures local-transfer admission and cleanup wait separately and samples queue depth, ready count, active count, owner mismatch scans and dependency skips. Time-weighted active jobs and runnable jobs distinguish a narrow dependency chain from idle workers despite available work; peak active jobs alone cannot do that. Add no size or tuning sweep.

## Progress guarantees and limits

The queue is producer-aware: an empty queue ends only after active jobs finish. It scans past blocked entries rather than claiming a fixed page, and enqueue and completion notify waiting workers. The source review finds no fixed-page starvation mechanism. FIFO rotation provides access to currently eligible jobs, but the blocker mismatch means eligibility is not yet equivalent to actual claim readiness.

One reassignment call allows at most 1,024 rounds and five seconds of consecutive lack of membership progress. A deferred reassignment creates a new call, so its local budget starts again. The whole balance retains a one-million-completed-job limit and a 300-second progress deadline. These are bounded failure controls; they do not prove fair progress of each deferred source. A scheduler change must preserve these bounds, fail-closed error propagation, exact ownership, current membership checks and the staged cross-owner protocol.

This is a read-only audit. No implementation, Cargo command, cloud operation or benchmark runs as part of this investigation.

## Deterministic readiness result

The actual-callsite regression reproduces the readiness defect in 0.01 seconds. A real local transfer holds child A and its destination while the active balance job is keyed by R. A second real reassignment enqueues a retry blocked on A; the queue dispatches it before A releases its claim. The same fixture dispatches unrelated work and verifies exact membership and codes after cleanup. The expected failing assertion is preserved in `scheduler-readiness-red-01.log`. No elapsed construction impact follows from this mechanism test. A narrow claim-readiness correction is the next implementation candidate; coalescing and broad scheduling changes remain separate.

## Implemented correction awaiting elapsed-time evidence

Epoch17 passes the executing scheduled attempt through synchronous split, merge and transfer calls into resident claim metadata. The queue withholds a dependent retry until that exact active attempt finishes, including the owner response and claim cleanup. A scheduled-job wrapper replaces the existing job request; it adds no per-transfer coordinator round trip. Direct unscheduled calls retain bounded revalidation instead of inventing an active producer. The actual split/retired-source/neighbor-claim regression, reciprocal dependencies and independent work pass. The consolidated suite passes 192 applicable tests; see [the independent review](EPOCH17-READINESS-SAVE-REVIEW.md) and [validation receipt](READINESS-SAVE-VALIDATION.json). Coalescing remains separate.

The fixed 500K benchmark will count queue visits withheld by an active producer and record transfer admission, cleanup and terminalization scopes. A withheld visit is neither a unique job nor a measured waiting duration. Cleanup includes terminalization; these wall totals overlap across workers and must not be added to elapsed balance. The keep/reject decision uses correctness, repeated work, process CPU and elapsed time together.
