# Balancing follow-up: distinguish extra work from extra cost

The user authorizes the full follow-up: repeated coordination comparisons, navigation attribution, publication preparation experiments, longest-job dependencies, scheduler eligibility and transport overhead. Batch diagnostic implementation into one instrumented executable while keeping algorithm switches independent. Use the notification-based listener throughout new comparisons. Keep bounded parallel saving and the existing accepted optimizations. The recurring follow-up stays paused.

## Measurement sequence

1. Run six predeclared interleaved coordination-on/off pairs from the exact retained 2M checkpoint using the existing epoch31v4 executable. Hold insertion at 64 workers, balancing at 32 workers per owner and the notification listener constant. Alternate pair order according to the recorded sequence. Validate exact 3M membership and codes after every run. Report paired arithmetic means, spread and uncertainty; preserve all attempts.
2. In parallel, implement a single detailed diagnostic build with navigation attribution by originating work, bounded longest-job records, scheduler decision reasons, publication timing and per-request transport costs. Instrumentation is disabled in primary timing runs. Measure its overhead separately and state propagation gaps explicitly.
3. Add an independent opt-in experiment that obtains the existing global publication turn before expensive centroid-view preparation. Preserve owner validation, publication/broadcast ordering, deadlines and failure cleanup. Test realistic concurrency and error paths before physical measurements.
4. Once the shared machines are idle, build and freeze the combined diagnostic executable using the retained compiler cache, then transfer only the new binary. Validate replay and construction. Compare detailed coordination on/off runs, profile both owners separately from timing, and compare the preparation-order candidate with its same-binary control in interleaved runs.
5. Use measured scheduler and transport evidence to choose a bounded follow-up fix. Persistent connections or bounded server workers must retain framing, authentication, admission separation and deadlines. If evidence does not justify a scheduling or transport change, record the measured reason and the additional information needed instead of treating unused cores or aggregate overhead as proof.

## Interpretation

Distinguish amount of work from CPU per unit of work. Count navigation queries/candidates/distances, splits and rows split, vector reassignment, refresh attempts/retries/no-ops and publications. Inclusive scopes overlap; do not sum them as exclusive CPU. Attribute remote work only where the originating job identity is available, and label unassigned work. Record which long jobs determine the end of balancing rather than ranking only accumulated wait across concurrent jobs.

The six initial pairs address whether the CPU difference persists on average, but they still permit different tree evolution. A stronger fixed post-insertion state or fixed-operation replay may be needed to isolate cost per operation. Keep the coordination improvement while testing its CPU explanation.

## Resources and provenance

The isolated working source is `/private/tmp/spann-research32`, copied from the validated epoch31 reader source, which includes the accepted writer changes. Frozen epoch31 artifacts remain untouched. Four workers own refresh preparation, common/navigation diagnostics, scheduler diagnostics and transport diagnostics respectively. Shared native builds are serialized; cloud builds and benchmarks never overlap measured phases on the same machine.

Only the two existing sandbox research machines are in scope. Reuse input, checkpoint and caches, and keep machines running within verified guards. The initial machine guard is October 22 at 20:58:09 UTC; renew at an idle boundary when needed. New evidence is under `two-machine-v1/operations/development32`.


## Stronger balance-only comparison

Add an exact post-insertion 3M checkpoint before any balancing jobs are queued. The existing checkpoint format supports this drained boundary; it does not support arbitrary mid-balance queues. Subsequent balance-only runs restore identical nodes, postings, routing view and sequence state, skip insertion and run the ordinary balance/placement/validation path. This removes differing insertion trees as a confounder while preserving the fact that balancing scheduling can still produce different work.

Compare four predeclared interleaved blocks of three configurations from that exact state: coordination off, coordination on, and coordination on with preparation under the publication turn. Use the same executable and notification listener. Run separate detailed and sampled-CPU profile arms, and measure instrumentation overhead. Record a final whole-workload comparison if a new candidate warrants adoption.

## Retained operational failure

The second coordination-on run completes successfully, but its coordinator receipt download exceeds the original 60-second SSH timeout. Both measured execution and remote receipts remain intact. Compressed SSH and a 180-second collection deadline recover the artifacts; the study resumes in its original order without rerunning or dropping that observation. No benchmark failure is inferred from this artifact-transfer failure.


## Conditional concurrency experiment

The six end-to-end pairs show approximately 0.85% more clustering distance calculations but 19.7% more clustering CPU, alongside modest increases in navigation work. If the fixed-state comparison and profiles retain evidence of higher cost per operation under greater parallel progress, compare 24 versus 32 balancing workers per owner from the same checkpoint. Keep coordination and listener settings fixed. Use three predeclared alternating pairs (32→24, 24→32, 32→24), not an open-ended parameter sweep. This tests a specific concurrency-efficiency hypothesis; it is not automatically an adoption decision. The script is prepared but not launched until the profile evidence is reviewed.

If preparation under the publication turn establishes a useful fixed-state gain, validate it in interleaved full final-million runs before changing the accepted profile. Preserve the stronger exact-state comparison and the practical whole-workload comparison as separate evidence.

## Closeout decision, October 22

Coordination is accepted and this investigation is closed at Lyon’s request. Six end-to-end pairs and four exact post-insertion pairs consistently show faster balancing with higher CPU consumption. The CPU increase is established for this 3M workload; its complete cause remains unresolved. Keep per-boundary coordination, the notification-based listener and the accepted saving improvements. Preparation under the global publication turn remains a separate opt-in experiment, disabled in the accepted configuration; its full-workload validation is deferred.

Revisit coordination as part of the future larger-index comparison already specified in [the current plan](../../CURRENT-PLAN.md#future-scaling-hypothesis-coordination-during-balancing). Separate index size, worker count and batch size. Measure conflicts and publication work per million inserted vectors as well as elapsed time and CPU; a smaller coordination fraction alone does not demonstrate less coordination work.

Queued comparisons are cancelled by user direction. Follow the results report for the next navigation target; this plan is retained as the original experimental design, not an active execution queue.
