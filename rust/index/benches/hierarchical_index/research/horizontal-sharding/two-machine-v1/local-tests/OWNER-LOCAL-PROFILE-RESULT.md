# Initial-owner CPU profile

The early part of balancing spends most sampled CPU in clustering and distance calculations. This moves the clustering path above posting compaction and navigation allocation in the investigation queue. The profile does not explain the entire elapsed gap against the reference: it samples only the early part of one balance and measures running CPU rather than time blocked on locks.

## What ran and what passed

One fixed 100K FineWeb build uses the exact corrected epoch9 distributed executable, 768-dimensional full-precision construction, L2 distance, 32 workers, one 100K insertion interval, branching 100, split threshold 2,048 and merge threshold 512. Both owner processes run on separate sandbox machines with 32 physical cores each. All construction in this interval occurs on the initial owner; the other owner remains empty until ordinary placement. Full vectors are preloaded, and the OS page cache is not explicitly flushed. Construction seeds and scheduler ordering are uncontrolled.

Construction, saving and fresh-reader validation pass. The balance lasts 15.2577 seconds and uses 47.34 process CPU-seconds on the initial owner plus 0.88 on the idle peer across phase snapshots. These counters include snapshot/endpoint overhead. The phase completes 68 splits, 119,376 transferred rows and 485 balance jobs; navigation records 7,469,370 child distances and vector access records 7,151,504 fetches. The tree has one internal root and 69 leaves at the end of balance. Only 149 framed application bytes cross the peer connection during this phase. The distance counter excludes clustering distances, so it cannot normalize the dominant clustering samples.

The final placement contains 54,804 and 45,196 current vectors, exceeding the 30% smaller-owner requirement. Total construction plus save is 17.9440 seconds. These timings come from a profiled, independently constructed tree; they are not another unprofiled speedup estimate. The earlier two-50K-batch diagnostic remains the evidence for useful construction on both machines.

## What the sample shows

The observer attaches to the verified owner process after observing the flushed balance-start event. Recording uses 99 Hz userspace CPU samples, frame-pointer stacks and a nine-second normal timeout with one second of forced-stop grace. The collection envelope, including startup and flush/cleanup, is 9.6373 seconds. The report displays approximately 2,000 samples and zero lost samples. Raw data is 565,752 bytes and remains on the worker disk; the small text report is retained locally.

| Sampled instruction location | Share of recorded CPU samples |
| --- | ---: |
| Full-precision Euclidean distance calculation | 49.60% |
| Clustering split routine, excluding separately attributed distance instructions | 40.16% |
| One-bit posting-code generation | 3.55% |
| Resident full-vector handle lookup | 0.75% |
| Transfer state-machine application | 0.60% |

These are mutually exclusive top-level instruction-location percentages from a report with child costs disabled. They describe the sampled CPU, not percentages of total construction time. Within the distance row, the displayed caller context places about 46.90 percentage points under clustering split. The split routine's own samples largely occur in accumulation operations. Compiler-inlined stacks contain noisy intermediary frames, but the dominant leaf symbols and clustering path are usable. Do not add parent and child stack percentages.

The intended sample is an early balance slice. The runtime does not timestamp its phase events, so exact leading/trailing coverage and possible boundary overhead cannot be reconstructed from these receipts. The nine-second recording ends before the approximately 15-second phase is expected to finish, and the observer does not see a balance-complete event while recording. This supports treating it as early-phase evidence rather than whole-phase attribution. Later reassignment/navigation work and time blocked on synchronization remain unmeasured. A small sample share does not rule out a cost outside this interval.

## Decision and next question

Keep the active-reservation correction, whose fixed-work diagnostic separately establishes removal of unnecessary history scans. The source comparison finds the same clustering kernel in the distributed build and the exact optimized reference source used by the retained benchmark binary. Check wrappers, compilation and performed work before changing anything: count split attempts, input rows, convergence iterations and discarded preparation, separately from committed splits. The strongest next experiment should replay identical split input through any differing kernels with fixed initialization or equivalent work counts; it should not require a larger index. If the kernels and settings match, investigate differing clustering work and the unsampled later interval rather than declaring clustering inefficient from its sample share alone.

Repeated source compaction, transfer copies and navigation materialization stay in the queue as source-supported hypotheses. This observation lowers their priority for the early interval; it does not clear them for the entire balance. No larger benchmark or additional algorithm change follows automatically.

## Evidence, lifecycle and accounting

The [summary](../operations/window-epoch11/summary.json), [profile report](../operations/window-epoch11/100k-profile/perf-report.txt), [observer receipt](../operations/window-epoch11/100k-profile/profile-result.json), [final collected profile files](../operations/window-epoch11/100k-profile/profile-final-files.json) and [workload receipts](../operations/window-epoch11/100k-profile/run-receipts.json) preserve exact source/binary/input bindings, measurements and validation. The observer uses privilege only for the reviewed exact-process sampling and its own cleanup; perf_event_paranoid remains 4. Both workload supervisors and collector confirm cleanup, the runner exits successfully, and AWS confirms both instances stopped.

Epoch10 retains the failed unprivileged preflight with no workload launched. Epoch11 adds $0.35508287628173824 of task compute once. Covered task compute is $32.550870422233998983; identified recurring storage remains $19.52/month. This is task-receipt accounting, not a whole-account invoice. No new storage or corpus transfer occurs.

The [exact clustering-path comparison](CLUSTERING-PATH-COMPARISON.md) confirms byte-identical kernel source and matched optimization settings, with explicit dependency/machine-code limits. It identifies discarded split preparation and immediate job scheduling as possible causes of unequal clustering work. These are source-supported mechanisms, not measured costs. The next proposed diagnostic uses two neighboring oversized leaves and controlled changes to distinguish attempted clustering from committed splits.
