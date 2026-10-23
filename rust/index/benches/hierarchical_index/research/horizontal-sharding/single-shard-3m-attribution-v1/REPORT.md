# One-owner construction at three million vectors

The one-owner implementation completes this matched 3M workload close to the optimized unsharded reference: construction plus saving averages **63.81 seconds versus 61.87 seconds**, a **1.94-second / 3.14% difference**. All four runs pass complete membership and saved-state validation. Most of the elapsed-time difference occurs while balancing the first 100K vectors. The profiles identify extra resident-node access work and extra balancing navigation, but they do not causally assign every second to an implementation choice.

This supports Lyon’s expectation that a single-owner configuration can run similarly fast to the unsharded implementation on this workload. Two independently randomized builds per arm do not establish a stable 3.14% regression, general performance parity, query-quality parity or multi-machine performance. The fixed experiment is complete; further tuning and benchmarks remain stopped.

## What was measured

Both implementations receive the same 3,000,000 FineWeb vectors, in the same order, with 768 f32 components and squared L2 distance. Both use 32 insertion workers and 32 balancing workers on the same 32-physical-core Intel Xeon Platinum 8375C machine. They add 100K vectors between balances through 1M, then add 1M at a time to 2M and 3M. The fixed execution order is reference, candidate, candidate, reference. Each run starts a fresh process and index after a full input-file read; the OS cache is not flushed.

The engine implementations are unchanged by this experiment. A shared harness adds per-cycle timing and existing work counters. Both binaries use Rust 1.92.0, the same release and allocator configuration, native CPU instructions, frame pointers and debug symbols. Both collect 99 Hz user-space CPU samples. Construction uses full-precision vectors and centroids; saved readers use four-bit centroids and one-bit posting codes. Branching, split/merge thresholds, search settings, reassignment neighbors and full-batch leaf grouping match. Random clustering initialization and worker scheduling are not seed-controlled.

Construction plus saving starts before writer initialization and ends after the saved blocks are flushed. It excludes input loading, retiring the writer, reopening, validation and the query smoke check. Diagnostics between measured phases contribute only 21–22 milliseconds per run. The benchmark’s complete measured lifecycle, including loading and checking, averages 78.82 seconds for the reference and 80.54 seconds for the candidate. External process timers additionally include startup and exit.

| Run, in execution order | Add (s) | Balance (s) | Prepare saved blocks (s) | Flush (s) | Construction + save (s) |
|---|---:|---:|---:|---:|---:|
| Reference 1 | 14.051 | 30.786 | 7.048 | 10.309 | 62.215 |
| Candidate 1 | 14.665 | 32.769 | 6.949 | 10.295 | 64.700 |
| Candidate 2 | 13.853 | 31.643 | 7.008 | 10.390 | 62.916 |
| Reference 2 | 13.815 | 30.575 | 6.892 | 10.216 | 61.520 |
| Reference mean | 13.933 | 30.681 | 6.970 | 10.263 | 61.868 |
| Candidate mean | 14.259 | 32.206 | 6.979 | 10.343 | 63.808 |
| Candidate − reference | +0.326 | +1.525 | +0.009 | +0.080 | **+1.940** |

All final trees have three levels. The reference trees contain 35 internal nodes and 2,109 / 2,096 leaves; the candidate trees contain 36 / 34 internal nodes and 2,113 / 2,093 leaves. Final leaf occupancy spans 514–2,048 vectors across the four runs. The candidate executes 35 / 32 internal balancing jobs. The workload therefore reaches meaningful internal branching and internal balancing. It does not test eviction from a capacity-limited vector cache.

## Where the elapsed-time difference occurs

**The first 100K balance accounts for 1.436 seconds, or 74% of the complete 1.940-second difference.** It averages 10.064 seconds in the reference and 11.500 seconds in the candidate. CPU consumption is much closer: 22.50 versus 23.075 CPU-seconds. Dividing CPU-seconds by elapsed seconds gives 2.24 versus 2.01 effective cores. Both begin with one oversized leaf, so available work grows as dependent splits create more leaves. The shared clustering routine accounts for about 79–84% of CPU samples in this phase.

The candidate’s scheduler admits follow-up jobs during a balancing round; the reference discovers them after joined rounds. This is a concrete implementation difference that can change the sequence of available jobs. The candidate reaches a peak of 13–14 active jobs in its first balance, but that peak does not describe average occupancy. Its measured structural-publication wait is only 0.017–0.650 milliseconds in this phase, with 15–23 milliseconds of total reservation hold time. Those reservation waits cannot explain the extra 1.436 seconds. The evidence locates the gap in a phase with low effective concurrency; without individual job dependency/ready-time traces or fixed clustering decisions, it does not distinguish scheduler delay from a different random split dependency chain. Neither the job queue nor clustering is proven to cause the entire difference.

**The remaining eleven balancing cycles have nearly the same total elapsed time.** They average 20.616 seconds in the reference and 20.706 seconds in the candidate, a 0.089-second / 0.43% difference. The candidate consumes 514.005 CPU-seconds versus 471.415, about 9.0% more, in almost the same elapsed time. This is observed higher effective CPU use; it is not an isolated measurement of the benefit of immediate job scheduling. The final 2M→3M balance is 0.541 seconds slower in the candidate, while several other cycles are faster.

| Balance interval | Reference mean (s) | Candidate mean (s) | Difference (s) |
|---|---:|---:|---:|
| Initial 0→100K | 10.064 | 11.500 | +1.436 |
| Nine cycles from 100K→1M | 5.615 | 5.254 | −0.361 |
| 1M→2M | 8.194 | 8.103 | −0.091 |
| 2M→3M | 6.808 | 7.349 | +0.541 |

## What the implementation differences cost

**Resident-node handles add identifiable CPU work, but their net elapsed-time cost is not isolated.** The reference reads a node directly through a concurrent-map guard. The candidate finds a shared node handle in that map, releases the map guard and acquires the node’s own lock. The handle maintains a reference count so the node remains alive while a worker uses it. This prevents long node operations from retaining map-bucket locks and permits safe retirement, but introduces extra handle and lock operations.

The candidate’s node-handle clone accounts for 2.63% and 2.83% of insertion CPU samples in its two runs. Inclusive samples in the complete node lookup account for 6.81% and 7.38%; those include the clone and must not be added to it. The reference lacks this resident-node clone path. Assembly from the same recorded profiles confirms calls to the candidate node store and additional handle-release/lock operations in navigation. These samples support a concrete extra cost worth investigating. They do not measure cache-line contention, nor do they show that removing the entire node synchronization contract is safe.

Insertion as a whole differs by only 0.326 seconds and 2.545 CPU-seconds in the means. Navigation distance counts differ by only 0.19%. Of the elapsed difference, 0.187 seconds occurs in input/ID preflight, 0.149 in collection/navigation and −0.010 in leaf appending. The preflight source and full-batch grouping are shared. Consequently, assigning the whole insertion gap to node handles would overstate the evidence; other work, execution variation and memory layout offset or contribute to the observed difference. Both arms average about 25 effective cores during insertion.

**Balancing’s extra CPU is concentrated in finding destinations for reassigned vectors.** Candidate balancing consumes 537.08 CPU-seconds versus 493.915, about 8.7% more. It performs 12.72% more navigation distance evaluations and 2.34% more navigation calls. Inclusive navigation CPU samples increase from a mean of 26,451 to 31,238, while samples in the common clustering routine decrease slightly. The existing overlapping navigation timers likewise increase from 271.6 to 324.1 worker-seconds. These independent counters and samples support extra navigation as the main extra CPU workload, rather than a large new storage or messaging cost.

| Mean balancing work | Reference | Candidate | Difference |
|---|---:|---:|---:|
| Navigation calls | 2,359,848 | 2,415,135 | +2.34% |
| Navigation distance evaluations | 1.742 billion | 1.963 billion | +12.72% |
| Leaf-clustering input rows, including repeated splits | 5,829,459 | 5,848,747 | +0.33% |
| Neighbor vectors evaluated for reassignment | 32,305,007 | 33,119,162 | +2.52% |
| Successfully transferred vectors | 1,131,604 | 1,131,415 | −0.02% |
| Deferred transfer groups | 22,176 | 19,131 | −13.73% |
| Conflicting leaf claims | 10,774 | 8,616 | −20.03% |

The scheduler, deferred internal jobs and navigation retirement behavior can change the tree and destination choices. Candidate navigation restarts when a selected node disappears; reference navigation skips missing nodes. The candidate records 457–524 restart attempts across the build. That difference and the more granular scheduler are plausible causes of changed navigation work, but this experiment changes them together. It cannot assign the additional 221.6 million balancing distance evaluations to either one independently. Fewer transfer retries and conflicts at 3M also contradict carrying forward an explanation based on more retries from a smaller fixture.

**Structural reservations, saving and memory do not show a large new penalty here.** Candidate structural reservation waits total 0.124–0.162 worker-seconds across the entire build, with 0.597–0.605 seconds of hold time. These are measured spans, not additive wall-time costs or a measurement of every lock. Summed scheduler waiting is 413–426 worker-seconds; roughly 333–352 occurs in the first balance when most workers have no ready job. It must not be reported as hundreds of seconds of user-visible latency or proof that runnable work is being starved.

Preparing and flushing saved blocks differs by only 0.089 seconds in the means. Process peak resident memory through balancing is approximately 11.15 GiB in all four runs; the cumulative high-water mark through saving is 19.84–19.90 GiB. High-water values do not reset at each phase. No posting or embedding loads from persisted backing storage are recorded during construction; full vectors are resident and accessed through the common vector store. Those zero load counters do not mean zero memory access or zero save I/O. No remote requests, per-vector ownership-directory lookups or ownership redistribution occur on this one-owner path.

## Decision and next useful experiment

Keep the implemented design as the measured baseline. This run does not justify discarding per-node synchronization or the immediate-job scheduler. It identifies a small end-to-end difference, observable node-handle CPU work and a larger balancing CPU-work difference that higher effective concurrency largely offsets after the first balance.

If Lyon resumes optimization, the most useful first experiment is to trace the initial balance’s job readiness, start/end times, split sizes and parent-child dependencies, alongside clustering iteration counts. Use the same implementation and fixed clustering choices where feasible to distinguish scheduling delay from different split chains. The initial 100K condition is enough for that specific question; another automatic 3M sweep is unnecessary. Next, isolate resident-node handle acquisition during insertion into an established tree, preserving the node-lifetime and concurrency guarantees. Treat the altered destination-search work and navigation retry policy as a separate fixed-decision experiment. No further experiment is started by this report.

## Evidence, reproduction and limits

[The execution plan](PLAN.md), [implementation differences](IMPLEMENTATION-DIFFERENCES.md), [source/harness review](review.md) and [operational correction review](review-epoch2-delta.md) record the protocol and source mechanisms. [Raw per-run results, events and phase profiles](operations/window-epoch2/evidence/) support the tables. [Analysis](analysis.json) and [profile analysis](profile-analysis.json) are derived by `analyze.py` and `profile_analyze.py`. [Runtime metadata](operations/window-epoch2/evidence-extra/run/) records exact commands, binary hashes, hardware, per-run cache reads, memory/disk gates and profiler output. Full indexes, input and raw profiles remain on the stopped task worker’s retained disk after closure.

The 9,216,000,000-byte input has SHA256 `18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba`. Every one of 32 source chunks is checked against its retained object version and whole-object checksum; the first 100K is reused locally and matches the preceding experiment. Both frozen source archives, all operative scripts and successful native multi-batch checks are bound in [epoch2 admission](operations/admission-epoch2.json). Native synthetic checks exercise batch boundaries and save/reopen; remote checks validate the actual 3M workload.

The four profiles contain 84,844–91,327 samples, 1.34–1.44% unknown leaf symbols overall and no reported lost samples or unparsed lines. Phase events include adjacent diagnostic snapshots; sampling shares are approximate CPU attribution, not exclusive elapsed-time attribution. Only the top 80 symbols per phase/category are summarized locally. Nested and concurrent timers overlap. Leaf-clustering input rows do not count clustering iterations or internal clustering. No per-job critical-path trace is available. This experiment establishes correct membership and saved representations, not held-out recall parity; the reused-input query is only a smoke check. Profiling overhead is shared but not separately measured with an unprofiled control.

The first preparation window stops before any 3M input restoration, build or benchmark because a usable-filesystem-size assertion is too strict and profiling is disabled by the host setting. Its failure artifacts remain preserved. The reviewed correction checks actual device capacity and available space, then temporarily permits user-process profiling on the exact dedicated worker. The original setting is restored and verified before API shutdown. A failed annotation symbol selector is also preserved; resolving the actual compiled navigation symbol succeeds using the same profiles, with no repeated benchmark.

The [final lifecycle receipt](operations/window-epoch2/final-receipt.json) confirms that worker and driver are API-stopped, the profiler setting is restored, the local watchdog is retired, the worker’s original security group is restored and the temporary group is deleted. No retained data is removed. The two 3M campaign windows cost at most **$1.6002 in compute**, including the failed preparation window. The 96 GiB root expansion adds **$7.68/month** and is counted once. Covered research compute now totals **$17.6089**; identified recurring storage components total **$19.52/month**, giving **$37.1289** for compute plus one reserved storage month. This is task accounting, not an account-wide bill. The [spending ledger](../SPEND-LEDGER.json) retains each source receipt. The [independent interpretation review](INTERPRETATION-REVIEW.md) confirms the arithmetic and attribution limits.
