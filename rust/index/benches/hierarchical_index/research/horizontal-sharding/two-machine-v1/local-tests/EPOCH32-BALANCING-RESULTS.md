# Balancing: repeated comparisons and exact-state experiments

The six predeclared interleaved comparisons reproduce both the latency improvement and higher CPU consumption from coordinating centroid refreshes. Every pair finishes balancing faster with coordination and consumes more CPU. The four-block fixed-state comparison also reproduces both effects. The investigation is closed at Lyon’s request; the current results do not establish a complete causal explanation.

## Repeated end-to-end comparison

Both configurations restore the same full-precision 2M checkpoint, insert the final million vectors with 64 insertion workers, and balance with 32 workers per owner. Both use the notification-based listener and the same frozen epoch31v4 binary. Twelve runs pass exact 3M membership and posting-code validation. The predeclared six-pair order alternates coordination on/off order to reduce drift effects.

| Measurement | Coordination off, arithmetic mean | Coordination on, arithmetic mean |
|---|---:|---:|
| Balancing elapsed | 23.235 s | 17.949 s |
| Balancing CPU | 564.79 CPU-s | 676.31 CPU-s |
| Construction elapsed | 33.168 s | 27.306 s |

Mean paired balancing time falls by 5.286 seconds and CPU rises by 111.52 CPU-seconds. Approximate 95% paired t intervals are −6.473 to −4.099 seconds and +93.28 to +129.76 CPU-seconds respectively. These intervals assume independent, approximately normal paired differences with only six pairs; they do not eliminate differing tree evolution or other systematic effects. Control CPU ranges from 551.59 to 586.46 seconds, while coordinated CPU ranges from 660.12 to 699.19 seconds.

The increase is not explained simply by more completed splits. Average split count rises 0.43% and rows entering completed splits rise 0.29%. Navigation query rows rise 3.07%, owned routing distance calculations 4.48%, and proposed transfer rows 5.35%. Clustering distance calculations rise only 0.85%, while measured clustering thread CPU rises from 30.187 to 36.134 seconds, approximately 19.7%. This supports investigating higher CPU cost per operation alongside additional navigation and transfer work. It does not yet establish whether memory contention, scheduling, locality or another mechanism causes that cost.

Fine refresh and publication counters are not collected in these lightweight runs. Their zero fields are uncollected values, not evidence of zero refresh work. Separate detailed runs provide those counts. Inclusive scope times overlap and cannot be added as exclusive CPU totals.

## Exact post-insertion comparison

The retained post-insertion checkpoint contains exactly 3M disjoint vector IDs and identical shared routing views on both owners. Its owner files are 622.6 and 617.1 MB and remain on the existing machines. Every arm restores those same files, skips insertion and runs ordinary balancing, placement and exact writer validation. Four predeclared blocks compare coordination off, coordination on, and coordination with preparation under the publication turn, using the same epoch32v2 executable and notification listener.

| Measurement | Coordination off | Coordination on | Coordination plus preparation under turn |
|---|---:|---:|---:|
| Mean balancing time | 22.207 s | 18.422 s | 17.638 s |
| Mean balancing CPU | 591.76 CPU-s | 684.15 CPU-s | 703.30 CPU-s |

Coordination reduces balancing by a mean paired 3.785 seconds and increases CPU by 92.40 seconds. Approximate 95% paired t intervals are −6.446 to −1.123 seconds and +60.89 to +123.90 CPU-seconds. Starting from identical trees therefore does not remove the observed CPU increase. Balancing still changes its own subsequent work and scheduling, so this is not a fixed-operation-count experiment.

Preparing under the turn reduces balancing by a further mean paired 0.784 seconds, with an interval of −1.282 to −0.286 seconds. It increases CPU by 19.15 seconds, with an interval of +2.15 to +36.14 CPU-seconds. All four blocks favor its balancing elapsed time. The total balance-plus-placement interval includes zero improvement, so a full final-million comparison remains necessary before adoption. These four-pair intervals retain small-sample distribution assumptions.

The three lightweight instrumented whole-workload controls average 18.942 seconds balancing and 694.14 CPU-seconds, versus 17.949 seconds and 676.31 CPU-seconds in the earlier executable's six coordinated runs. They also perform more navigation work. These sequential build cohorts do not isolate disabled-instrumentation overhead from differing work or temporal variation. Candidate comparisons therefore use the same executable, and detailed instrumentation overhead is evaluated separately.

## Batched implementation and validation

The diagnostic build attributes navigation and existing operation scopes to balance, neighbor reassignment, queued reassignment or unassigned origins. Each owner retains 32 longest jobs with identities, timestamps, CPU and inclusive scope breakdowns. Remote handlers remain unassigned because request envelopes do not carry initiating-job context; per-request transport metrics provide a separate server-side view. Scheduler observations distinguish dispatch eligibility from node/producer conflicts and report condition waits and scan work. They do not equate queue length with runnable work or measure exact notification-to-dispatch delay.

The independent publication experiment obtains the existing global publication turn before snapshot preparation. Its hypothesis is reduced discarded preparation; its risk is a longer serialized section. The option preserves fresh validation, fencing, publication/broadcast ordering and failure cleanup. It remains off in the accepted implementation until measured evidence supports adoption.

The exact-state replay saves both owners immediately after insertion and before balancing jobs are queued. Restored runs skip insertion and execute the ordinary balance/placement/validation path. Checkpoints do not serialize arbitrary pending balancing queues, so this is not a mid-balance recovery claim.

The final combined native suite passes 298 tests, with three existing opt-in tests ignored and one documented unrelated concurrency test excluded. Focused transport tests, publication concurrency/error tests, exact checkpoint restoration tests, and two-process replay/ordinary-construction smoke tests pass. Source and test evidence are retained under [development32](../operations/development32). No benchmark runs overlap compilation on the same cloud machine.

## Decision and deferred work

Coordination is accepted and this investigation is closed at Lyon’s request. Six end-to-end pairs and four exact post-insertion pairs consistently show faster balancing with higher CPU consumption. The CPU increase is established for this 3M workload; its complete cause remains unresolved. Keep per-boundary coordination, the notification-based listener and the accepted saving improvements. Preparation under the global publication turn remains a separate opt-in experiment, disabled in the accepted configuration; its full-workload validation is deferred.

Revisit coordination as part of the future larger-index comparison already specified in [the current plan](../../CURRENT-PLAN.md#future-scaling-hypothesis-coordination-during-balancing). Separate index size, worker count and batch size. Measure conflicts and publication work per million inserted vectors as well as elapsed time and CPU; a smaller coordination fraction alone does not demonstrate less coordination work.

## Next target: navigation work during balancing

The coordinated detailed run records approximately 288.68 CPU-seconds in the inclusive navigation scope across both owners. Candidate processing records 82.48 CPU-seconds, reply validation 70.78, canonical ordering 39.01 and owned routing scoring 140.62. These scopes overlap and must not be summed. Detailed instrumentation changes execution costs, and this single run identifies candidates rather than establishing production proportions.

Prioritize reducing repeated candidate processing, reply validation and ordering while preserving complete results and deterministic selection. Start with the retained fixed navigation replay, then confirm any improvement with repeated complete final-million construction runs. Inspect existing CPU profiles before choosing the exact code change. This work can benefit insertion and balancing without reopening the coordination protocol.

The longest coordinated jobs in this diagnostic take approximately 2.30 seconds on owner 1 and 3.24 seconds on owner 2. The owner-1 example includes 1.22 seconds waiting for publication permission; the owner-2 example includes 2.72 seconds in client request elapsed time. These are overlapping scopes in individual jobs, not exclusive critical-path totals. Keep the publication investigation deferred; use request-level attribution to determine whether navigation, remote execution or other dependency waits explain the remaining tails.

The peer’s per-request transport counters are emitted at normal server shutdown, while the harness normally terminates that server early. This leaves a peer transport attribution gap. Persist those counters before termination in the next convenient diagnostic change. Do not infer server execution cost from client elapsed time. Scheduler eligibility samples are observations rather than time-weighted occupancy, so they do not yet justify a scheduling change.

Queued coordination comparisons are cancelled by user direction. Three detailed arms complete and are retained. Both off/on profiles finish with exit code zero and receipts from both owners retained. No additional full-workload preparation-order comparisons or worker-count sweep will start. SSH verification finds no remaining benchmark or supervisor processes on either machine; both budget timers remain active for 22:01:49 UTC. The recurring follow-up remains paused. Machines remain running under their existing budget guards.
