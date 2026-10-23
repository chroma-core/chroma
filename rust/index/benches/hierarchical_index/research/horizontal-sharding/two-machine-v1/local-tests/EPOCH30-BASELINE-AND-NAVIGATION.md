# A reusable writer checkpoint and a measured navigation comparison

Both optimizations are accepted for their measured CPU savings. Keep compact navigation result handling and conditional routing-view transport in the intended implementation. Each passes independent validation; their combination still requires integration and validation. Preserve the immutable baseline as the comparison control. Latency improvement is unproven, and the small observed elapsed-time increases remain a possible tradeoff.

The same full-precision 2M writer checkpoint now supports repeatable final-million construction runs on two owners. Even placement is part of the baseline. The first navigation candidate reduces CPU spent arranging candidates, but does not improve end-to-end construction time in three repetitions. The separate [transport comparison](EPOCH30-TRANSPORT-COMPARISON.md) also completes; it reduces CPU and bytes but does not improve latency.

The workflow follows [the current plan](../../CURRENT-PLAN.md) and [the existing-data investigation](EPOCH28-EXISTING-DATA-INVESTIGATION.md). [The implementation and attempt log](EPOCH30-WORKFLOW-PROGRESS.md) records preparation and harness failures. Raw results, per-second samples, histograms, source manifests, and executable hashes are under [the experiment directory](../operations/development30).

## Checkpoint and placement

The checkpoint preserves full-precision centroids, tree records, ownership, memberships, posting codes, the shared routing view, ingestion history, identifier counters, configuration, input digest, and next input position. Saving closes mutation admission and rejects active jobs or undrained queues. Restoration verifies the checkpoint checksum and configuration, reconstructs owner-local reservation state, and opens the next mutation epoch in fresh processes. Old requests cannot enter the new epoch. Compressed reader generations are not used for restoration.

The two checkpoint files contain exactly 2,000,000 distinct IDs with no overlap. Both contain the same shared routing view. Owner counts are 999,258 and 1,000,742. The [checkpoint manifest](../operations/development30/checkpoint-manifest.json) binds the files to their hashes; the [membership proof](../operations/development30/checkpoint-membership-proof.json) retains the exact bitmaps checked for disjoint, complete coverage. Both files remain on their respective development machines and every measured arm restores those exact files.

Placement prefers even vector counts while retaining the 70% capacity ceiling and configured insertion reserve. Whole-branch moves must reduce the sum of squared owner counts. Existing branches are refined when necessary; leaf granularity can leave a small residual difference. The workload redistributes at 2M and 3M. An attempt that redistributes every 100K during preparation exhausts a shared-balancing dependency replan budget and is preserved as `prepare-v3`; it is not a successful baseline.

## Baseline measurements

The immutable baseline executable has SHA-256 `e79dfd8300d3220bc16348985d751c959232aba9b0f934a2934c9b250baac331`. Its [complete source manifest](../operations/development30/baseline4-source-manifest.json) and [source archive](../packages/source-epoch30-baseline.tar.gz) are retained. The source archive SHA-256 is `f2fe21b82c9661c36e647f65161cb563a02baac37a4a2a1f284ff8b2cdfa4af5`.

Each timing run restores at 2M, adds the final million, balances, redistributes, and then verifies exact global membership and every posting code against the full-precision centroid and immutable vector. Restoration and the approximately 11-second validation are outside construction timing. Saved-reader publication is outside this experiment. All three baseline timing runs pass.

| Run | Insert, seconds | Balance, seconds | Redistribute, seconds | Balance CPU, seconds |
|---|---:|---:|---:|---:|
| Baseline 1 | 12.907 | 23.791 | 1.295 | 618.32 |
| Baseline 2 | 13.060 | 21.381 | 2.704 | 703.16 |
| Baseline 3 | 13.191 | 22.144 | 2.530 | 648.44 |

Median balance time is 22.144 seconds, and median gross final-million construction time is 37.994 seconds. The first run starts balance with 1,506,441 and 1,493,559 vectors, so its input load is near even. Scheduling changes the resulting construction work: these runs perform 734, 745, and 736 splits. The checkpoint fixes the starting state, but parallel construction is not deterministic.

## Detailed navigation costs

The separate detailed baseline run takes 21.803 seconds to balance. The following CPU times sum the same named scope across both owners. Scopes are inclusive; the subscopes are already inside candidate processing and must not be added again to overall navigation or transfer-driver time.

| Scope | CPU-seconds |
|---|---:|
| Canonical-order reconstruction | 62.373 |
| Reference-compatible sorting | 17.299 |
| Gathering sorted records | 9.333 |
| Sort-index allocation and construction | 3.801 |
| Remote-reply validation | 63.938 |

Canonical-order reconstruction includes parent lookup, ordinal validation, temporary storage, and record movement. The allocation scope includes index construction rather than isolated allocator calls. The measured candidate-processing scopes motivate replacing temporary full child records with compact index permutations. All reply validation and complete-result requirements remain necessary.

## Workers, queues, and long jobs

Each owner records its own CPU and work counters approximately once per second. The detailed run averages approximately 16.19 and 14.78 CPU cores over complete sample intervals. Both reach 32 active admitted jobs. This counter describes logical admitted work; it is not a count of all operating-system threads or all insertion workers.

The coordinator owns the shared scheduling queue. Its observed maxima are 1,541 ready jobs and 46 blocked jobs. The peer's local queue stays empty while it executes remotely dispatched jobs; that does not imply the peer is idle. The first samples reaching the final completion counts occur 21.826 and 22.430 seconds after balance starts. Maximum observed sample intervals are 1.027 and 1.009 seconds, so the apparent completion difference does not establish a subsecond ordering.

The longest scheduled balance jobs take 6.428 seconds on owner 1 and 7.436 seconds on owner 2. The longest neighbor jobs take 0.221 and 0.334 seconds; the longest reassignment jobs take 0.254 and 0.303 seconds. These wall durations include nested computation and waits. Full logarithmic duration histograms and cumulative counters remain in the raw receipts; [the telemetry summary](../operations/development30/baseline-detailed/telemetry-summary.json) records sampling bounds.

## Transport measurements and next mechanism

The detailed balance phase exchanges 13.756 GB of application-framed bytes, observed once at the coordinator endpoint. It contains 38,801 coordinator-initiated calls and 62,293 coordinator-served calls. These counts include the small phase-observation requests. TCP/IP headers and retransmissions are excluded.

| Coordinator endpoint scope | CPU-seconds | Summed wall-seconds | Median latency upper bound | 95th-percentile upper bound |
|---|---:|---:|---:|---:|
| Outgoing call, inclusive | 36.407 | 853.312 | 4.096 ms | 32.768 ms |
| Connection establishment | 0.898 | 5.334 | 0.128 ms | 0.512 ms |
| Outgoing JSON encoding | 5.471 | 5.472 | 0.008 ms | 1.024 ms |
| Reply JSON decoding | 23.640 | 23.649 | 0.064 ms | 4.096 ms |
| Incoming JSON decoding | 14.199 | 14.220 | 0.128 ms | 1.024 ms |
| Reply JSON encoding | 36.433 | 36.448 | 0.002 ms | 4.096 ms |
| Server execution, inclusive | 60.234 | 309.444 | 0.128 ms | 8.192 ms |
| Parent-side handler-thread creation | 1.206 | 1.218 | 0.032 ms | 0.064 ms |
| Handler thread start delay | Not measured | 12.363 | 0.128 ms | 1.024 ms |

Histogram values are bucket upper bounds, not exact percentiles. Concurrent calls overlap; summed call wall time is not phase elapsed time. Thread start delay measures dispatch to handler execution. Socket backlog delay and child-thread setup CPU are not isolated. Thread creation includes parent-side bookkeeping. Exact transport timing scopes are retained at the coordinator; both owners have work counters and CPU profiles, but the peer's transport counters are not emitted by its forced process shutdown.

JSON encoding and decoding consume 79.743 CPU-seconds at the coordinator endpoint alone. Source inspection shows that bulk expansion scores and queries already use bounded binary fields inside JSON. A blanket claim that all vector floats are textual would therefore be wrong. A concrete remaining source of repeated work is fetching the complete shared routing view before every peer navigation call. The next isolated candidate acknowledges an exactly matching publication sequence and sends the complete view whenever the sequence changes. Connection pooling remains a later option: measured parent-side connection and creation costs are smaller, while their unisolated kernel and scheduling costs remain uncertain.

## Transfer work

The detailed run records 180,149 moved vectors on owner 1 and 188,815 on owner 2. Logical posting-ID and posting-code copies amount to approximately 5,906 and 6,063 bytes per moved vector, respectively. These include copying retained source rows during compaction; they are not network bytes or physical memory-controller traffic. Additional payload clones and query-vector copies have separate counters and are not silently included in this ratio.

Owners record 5,582 and 5,948 compactions, and approximately 6.03M and 6.10M membership probes. Membership comparison counts are conservative bounds rather than measured comparisons. Preparation CPU scopes are retained. The batched path does not isolate compaction CPU from the surrounding commit scope; a zero value in the legacy pair-transfer compaction timer must not be read as zero compaction cost.

## CPU profiles and instrumentation overhead

Both owners retain frame-pointer call-stack recordings. The balance-filtered reports contain approximately 49K hardware-cycle samples per owner and report zero lost samples. [Owner 1's flat report](../operations/development30/baseline-profile/owner1-balance-flat.txt), [owner 2's flat report](../operations/development30/baseline-profile/owner2-balance-flat.txt), and the adjacent call-stack reports preserve the evidence. Filtering maps coordinator phase boundaries through each owner's local realtime/monotonic samples. Hardware-cycle percentages are sampled attribution, not exact thread-CPU percentages.

Distance calculation accounts for 13.09% and 12.45% of sampled cycles. SipHash's write function accounts for 7.61% and 6.46%, while navigation assembly itself accounts for 7.08% and 7.58%. Sorting, JSON conversion, allocation-related work, page faults, and kernel cross-CPU work also appear. Some addresses remain unresolved. These samples support the measured navigation and transport costs, but do not establish an exclusive causal decomposition.

The fine-instrumentation run's 21.803-second balance lies inside the lightweight range of 21.381–23.791 seconds. One diagnostic run cannot tightly bound overhead because construction work and scheduling vary. The profiled run takes 27.296 seconds, approximately 23% above the lightweight median; it is excluded from timing comparisons. CPU profiles and timing repetitions must remain separate.

## Navigation candidate result

The navigation candidate preserves the original eight-byte sorting representation and comparator. It validates complete index permutations before moving rows, then rearranges the existing row storage. Canonical parent/ordinal validation and all remote-reply checks remain intact. Its fixed 256-query results match the baseline byte-for-byte, including wide IDs and tie order. All three full-workload repetitions pass exact membership and posting-code validation, and the native suite passes 273 tests.

| Metric, median of three timing runs | Baseline | Navigation candidate |
|---|---:|---:|
| Balance wall time, seconds | 22.144 | 23.274 |
| Gross construction wall time, seconds | 37.994 | 38.819 |
| Balance CPU, seconds | 648.44 | 606.27 |
| Candidate-processing CPU per candidate row, ns | 101.15 | 77.31 |

Candidate-processing CPU per row falls approximately 23.6%, and median balance CPU falls approximately 6.5%. Median balance wall time rises approximately 5.1%; gross construction rises approximately 2.2%. The three-run ranges overlap, and work counts differ. This is evidence of lower candidate-processing CPU, not evidence of an end-to-end speedup. The candidate is retained for comparison and is not promoted as the latency baseline.

The separate detailed candidate run reduces canonical-order CPU from 62.373 to 34.701 seconds for similar candidate counts. Gathering and sort-index construction increase from 9.333 to 10.329 and from 3.801 to 6.874 CPU-seconds. The improvement is therefore concentrated in canonical reconstruction rather than every substage. Long-job dependencies, transport waits, and changed schedules remain plausible explanations for the wall-time result; this experiment does not isolate their individual causal contributions.

The [navigation-only patch](../operations/development30/candidate1.patch), [source archive](../packages/source-epoch30-candidate1.tar.gz), and [source manifest](../operations/development30/candidate-source-manifest.json) are retained. Executable SHA-256 is `2ced3d06543160063346017195affd582f1fedcc8448aca80d1cde7d3ffcd2cc`. The [transport comparison](EPOCH30-TRANSPORT-COMPARISON.md) is recorded separately so its effect is not confounded with this change.
