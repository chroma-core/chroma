# One-owner construction comparison

The one-owner implementation passes all four matched 100K construction and saved-state checks. Its build-and-save times are 10.61 and 7.46 seconds, versus 6.80 and 7.83 seconds for the optimized reference. The candidate is slower in the first pair and slightly faster in the second. Its descriptive mean is 23.5% higher, but two random constructions per arm do not establish a stable regression or equivalence. The benchmark is complete and the worker is API-confirmed stopped; further experiments are paused as requested.

## What is implemented

One owner holds the complete growing tree and calls its writer directly. Resident nodes have individual locks, balancing workers take individual leaf or internal-node jobs with immediate follow-up admission, and short reservations protect structural publication in affected ancestor regions. Postings stay separate from leaf metadata. Membership changes explicitly remove the source and add the destination. Saved readers use four-bit centroids and one-bit posting codes. Cross-machine operations reject as unsupported. The [implementation record](IMPLEMENTATION.md) describes the objects, workflow and remaining limits.

The [independent review](review.md) binds the scoped source digests. The combined native suite passes 249 test executions across four wrappers, including repeated shared cases. Five packed-centroid library tests pass. Both benchmark executables compile and pass synthetic save/reopen checks. Failed native attempts remain in [local evidence](local-tests/).

## Comparison and timing boundaries

Both arms use the same retained 100,000 FineWeb vectors with 768 full-precision components, the same ordering, squared L2 metric and 32 configured workers on one machine with 32 physical cores. Each starts a fresh index, inserts one complete 100K batch, balances, saves, drops the writer and validates the reopened state. The reference is a separate checkout of the optimized version-free writer; both arms include the same persisted-centroid representation and benchmark harness. The fixed order is reference, candidate, candidate, reference.

Build-and-save time includes initialization, insertion, balancing, commit and flush. The report also accounts for input loading, writer retirement, reader opening, full validation and query smoke separately. Validation checks exactly one posting per input vector, tree links and reachability, full-vector bits, one-bit posting codes and four-bit centroid codes. Compilation is execution overhead, outside index construction time, and counts toward spending.

Every process has fresh provider caches and a new output directory. A bounded read warms the same input file before each run; the operating-system cache is not flushed. Construction randomness and worker ordering are uncontrolled, so tree shape and work counts must accompany elapsed time. Validation warms storage before the reused-input query smoke. That smoke is not held-out recall evidence.

## Runtime outcome

All four runs validate exactly 100,000 reachable current postings and preserve full-vector bits, one-bit posting codes and four-bit centroid codes after dropping the writer and reopening the saved reader. All final leaf occupancies are within 512–2048. Raw [runtime evidence](operations/window-epoch2/evidence/), the [machine-readable summary](summary.json), [source admission](operations/admission-epoch2.json), binary receipts and complete command logs support the figures below.

| Execution order | Add (s) | Balance (s) | Commit + flush (s) | Build + save (s) | Total inside harness (s) | Leaves | Balance CPU cores on average |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Reference 1 | 0.095 | 6.074 | 0.625 | 6.795 | 7.303 | 71 | 2.98 |
| Candidate 1 | 0.119 | 9.867 | 0.620 | 10.607 | 11.106 | 72 | 2.15 |
| Candidate 2 | 0.117 | 6.717 | 0.625 | 7.459 | 7.949 | 67 | 2.80 |
| Reference 2 | 0.096 | 7.124 | 0.610 | 7.831 | 8.328 | 67 | 2.64 |

The external process durations are 7.432, 11.241, 8.084 and 8.434 seconds respectively; they also include process startup and teardown. Input loading takes 0.238–0.248 seconds and full validation 0.239–0.250 seconds. Peak resident memory is 762–777 MiB across the four runs. No cache-capacity crossing is demonstrated. Release compilation takes 136.322 seconds for the reference and 42.421 seconds for the candidate, which can reuse dependency artifacts; those durations are not an index-performance comparison.

| Work, in execution order | Reference 1 | Candidate 1 | Candidate 2 | Reference 2 |
| --- | ---: | ---: | ---: | ---: |
| Navigation distance calculations | 5,302,376 | 7,599,330 | 6,985,767 | 4,888,949 |
| Leaf split attempts | 70 | 72 | 66 | 66 |
| Rows processed across split inputs | 625,324 | 609,920 | 594,656 | 613,635 |
| Completed vector transfers | 56,430 | 28,419 | 32,167 | 52,209 |
| Deferred transfer-group retries | 2,351 | 4,880 | 3,659 | 2,415 |
| Leaf claim conflicts | 1,098 | 1,403 | 1,090 | 1,156 |
| Balance passes | 8 | 2 | 2 | 8 |

The candidate also uses more CPU during insertion: 1.35–1.38 CPU-seconds versus 0.58–0.77 for the reference, despite only about 22 milliseconds of extra wall time. The process high-water memory mark observed through the end of balancing is 576–591 decimal MB for the candidate versus 460–464 MB for the reference. These are cumulative process peaks, not isolated phase allocations; current resident memory at the balance boundary is about 419–420 MB for the candidate versus 432–435 MB for the reference. Similar final peaks therefore do not erase the candidate’s larger transient peak before saving. The causes remain unproven.

Balancing explains almost all of the average timing difference. The candidate averages about 43% more navigation distance calculations while completing fewer transfers and retrying more of them. Split input volume is slightly lower, and cumulative k-means task durations overlap substantially between arms (14.18–16.68 worker-seconds). Transfer retries count deferred routed groups, not unique vectors or machine messages; pending IDs may repeat before coalescing and supersession. These are different construction trajectories, not identical decisions with only a sharding wrapper added. They warrant investigating scheduling, destination search work and transfer retries before claiming unavoidable sharding overhead.

The measured structural reservation is not a consequential waiting cost in this fixture. Candidate reservation acquisition totals about 20 and 22 microseconds, with 25.6 and 36.8 milliseconds of cumulative holding time across 73 and 66 publications. These counters do not measure every node or storage lock. Candidate scheduler condition waits total 291.6 and 193.8 worker-seconds, while peak active jobs are 23 and 17. Waiting across workers overlaps; neither this sum nor time inside a worker proves CPU utilization or a scheduler defect. Effective balancing CPU use is only 2.15–2.80 cores for the candidate and 2.64–2.98 for the reference.

The fixture reaches repeated leaf splitting and reassignment, but all trees end with one internal root and 67–72 leaves. Candidate internal-job counts are zero. Starting from one oversized leaf creates an initial chain of dependent splits before much independent work exists; measuring the critical path is a leading next question. This result does not test concurrent publication in separate deep subtrees. Posting and embedding load counters are zero during this fresh in-memory construction, so it does not diagnose lazy persisted-data fetching during construction. Zero load counters do not imply zero cache access or decoding throughout the process; saving and validation still access blockfiles. The initial add phase also routes into the initial leaf, rather than exercising navigation through an established tree.


## Interpretation limits

This workload exercises leaf splitting and membership reassignment after one full batch. Its measured tree shape determines which internal balancing and parallelism conditions it actually reaches. It does not establish cache-capacity behavior, deep-tree query pruning, physical distributed execution or a scaling law. Two repetitions describe these runs rather than a stable performance distribution.

The implementation retains bounded navigation retries and structural-work budgets. Report any remaining occupancy violations and differences in work rather than treating equal vector counts as identical work. Exact membership validation is necessary but does not establish clustering equivalence. Stored identifiers remain 32 bits; the code does not represent a 100B index. Multi-machine protocols and general bounded-memory construction remain unfinished.

## Decision and closeout

An independent [result interpretation review](result-review.md) checks the counter meanings and conclusions. Its adverse CPU and memory findings and qualifications are incorporated here.

Retain this implementation and the complete adverse/favorable evidence as the one-owner baseline, without declaring performance or quality parity. The next proposed investigation is attribution of balancing's critical path and changed navigation/transfer work at fixed input and decisions where possible. Node-lock micro-optimization and cache tuning rank below that question for this fixture. An established-tree append is a separate future question because fresh 100K does not exercise internal balancing or sustained insertion navigation. No additional experiment starts now.

The [first window](operations/window-epoch1/closed.json) fails at SSH connectivity before upload or timing and costs at most $0.197336. The [successful window](operations/window-epoch2/closed.json) costs at most $0.453163, including builds and shutdown observation. Both total $0.650499. These upper bounds are counted once in the [spending ledger](../SPEND-LEDGER.json); no storage is added. Covered research compute totals $16.008763, with $11.84/month of separately recorded retained storage commitments. The covered first-month bound is $27.848763, not a whole-account bill.

Shutdown is verified through the AWS API. The original worker security-group attachment is restored, the temporary group is deleted after verifying no attachments, and the local watchdog is retired. The [final cleanup receipt](operations/window-epoch2/final-receipt.json) records these checks. The approved driver is also API-confirmed stopped. Retained disks and remote index artifacts remain intact. The broader distributed/100B goal is incomplete. The research heartbeat remains paused. This implementation-and-benchmark task stops here.
