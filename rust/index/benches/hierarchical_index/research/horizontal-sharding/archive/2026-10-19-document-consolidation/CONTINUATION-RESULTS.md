# Continuing the shared-tree scale study

The continuation validates one-million and ten-million real FineWeb vector indexes with 128 fixed subtrees. Each subtree belongs to one worker, while searches retain a single global branch and candidate budget. Operational handoff state is recorded in [CONTINUATION-STATE.json](CONTINUATION-STATE.json). This report records the continuation measurements; the chronological evidence and failures are in [CONTINUATION-RUNS.md](CONTINUATION-RUNS.md).

## Construction across machines

Adding a second machine reduces construction time by 1.70–1.82 times at the matched allocations below. Every point processes the same million rows, converges across all 128 jobs, and passes coverage and publication checks. Independent review verifies all 1,024 snapshot, mapping, and audit bindings across the eight cases, including complete and unique global ID coverage. Each owner flushes once at completion; repeated-checkpoint storage overhead remains unmeasured.

| Machines | Logical CPUs per machine | Concurrent jobs per machine | Construction | Through publication |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8 | 4 | 28.89 s | 36.59 s |
| 1 | 8 | 8 | 19.82 s | 27.39 s |
| 1 | 8 | 16 | 18.38 s | 25.71 s |
| 1 | 16 | 8 | 15.69 s | 23.02 s |
| 1 | 16 | 16 | 11.05 s | 18.37 s |
| 1 | 16 | 32 | 11.42 s | 18.75 s |
| 2 | 8 | 16 | 10.10 s | 16.97 s |
| 2 | 16 | 16 | 6.49 s | 12.99 s |

These CPU allocations run on larger machines with abundant memory and local disks. Eight logical CPUs correspond to four physical cores with two hardware threads each; sixteen correspond to eight physical cores. These measurements do not establish equivalent performance or cost on smaller instances. Each point is a single engineering measurement. Initial construction and append compaction are separate experiments.

## Read coordinator throughput

Distributing the request adapters across four processes improves the 100,000-vector pilot without adding machines or duplicating subtree readers. An adapter receives network requests and forwards them to the index readers. The original single adapter uses about one CPU core; four adapters can process requests concurrently. All 100 query traces match across configurations, including IDs, distances, visited branches, candidates, and leaves.

| Concurrent clients | One adapter | Four adapters |
| --- | ---: | ---: |
| 1 | 7.43 queries/s | 12.44 queries/s |
| 4 | 11.49 queries/s | 31.84 queries/s |
| 8 | 11.55 queries/s | 38.32 queries/s |

A separate eight-client resource check completes 589 requests without failures at 38.81 queries/s and 201.3 ms mean latency. It measures 5.48 total CPU cores and 648.3 MiB aggregate resident memory across twenty processes. These short warm-query windows diagnose the coordinator; they do not establish million-vector read capacity. The million-vector calibration and placement comparison appear below.

## Million-vector recall calibration

The frozen grid targets 95% recall at 100 results before held-out evaluation. Completed settings use the same 128-owner tree, four adapter processes, and sixteen logical CPUs on one host. All completed points below process 100 calibration queries without failure. All ten settings complete with zero failed configurations. The frozen lowest-mean-latency qualifying rule selects global B512/M1000 before held-out evaluation.

| Global beam | Candidate budget | Recall@100 | Mean latency |
| ---: | ---: | ---: | ---: |
| 64 | 1,000 | 80.05% | 134.59 ms |
| 64 | 4,000 | 80.05% | 232.13 ms |
| 128 | 1,000 | 86.28% | 225.96 ms |
| 128 | 4,000 | 86.29% | 419.84 ms |
| 256 | 1,000 | 92.74% | 314.83 ms |
| 256 | 4,000 | 92.80% | 702.49 ms |
| 512 | 1,000 | 97.85% | 420.53 ms |
| 512 | 4,000 | 97.94% | 1,144.81 ms |
| 1,024 | 1,000 | 99.89% | 456.64 ms |
| 1,024 | 4,000 | 100.00% | 1,379.43 ms |

The selected 512/1,000 setting meets the primary target. Wider candidate budgets add latency while recovering few results at smaller beams. Held-out recall is 97.88% on both placements, with all 100 traces exactly equal in IDs, distances, branches, candidates, leaves, and counts. Mean latency is 496.94 ms on one host and 422.55 ms on two. Readers restart before each placement, while operating system cache remains warm/uncontrolled. These are single passes. A fifteen-second offered-load sweep records 877 successes and zero failures across ten windows. Throughput is shown below; independent resource attribution is described below.

| Concurrent client processes | One host | Two hosts |
| ---: | ---: | ---: |
| 1 | 1.97 queries/s | 2.32 queries/s |
| 2 | 3.58 queries/s | 4.26 queries/s |
| 4 | 5.15 queries/s | 6.99 queries/s |
| 8 | 6.17 queries/s | 8.88 queries/s |
| 16 | 6.72 queries/s | 9.90 queries/s |

The sixteen-client ratio is 1.47 times, while the eight-vCPU driver is oversubscribed. Independent review verifies all 877 outcomes and shows that two-host sixteen-client load uses 7.44 of eight driver vCPUs, while server adapters use 5.65 of eight available interpreter cores. The driver constrains this result. Each point is a single short engineering observation rather than a confidence-bounded machine-capacity result.

A separate 32-vCPU driver completes three alternating repetitions of the same one- and two-host sweep. All thirty windows finish with 2,906 successful requests and zero request or client failures. The table gives the mean throughput across three windows at each setting. Worker CPU allocations, search settings, index, and query schedule remain fixed.

| Concurrent client processes | One host | Two hosts |
| ---: | ---: | ---: |
| 1 | 1.982 queries/s | 2.333 queries/s |
| 2 | 3.582 queries/s | 4.295 queries/s |
| 4 | 5.414 queries/s | 7.467 queries/s |
| 8 | 6.622 queries/s | 11.077 queries/s |
| 16 | 6.929 queries/s | 12.354 queries/s |

At sixteen clients, two hosts deliver 1.783 times the mean throughput of one. One-host windows range from 6.892 to 6.952 queries/s; two-host windows range from 12.331 to 12.365. These short warm windows reuse a limited overlapping query schedule, so they support this paired comparison rather than broad workload capacity or tail-latency claims. Independent review reconciles every request, barrier, throughput value, and held-out oracle trace hash. At sixteen clients, the driver uses 2.69 and 5.22 of its thirty-two CPU cores for one- and two-host cases. Request adapters use 3.94 of four interpreter cores and 7.09 of eight; Rust readers use 3.01 and 5.39 cores. Every resource window has complete bracketing. The larger driver has headroom, while adapter interpreters constrain the current serving path.

## Appending to the published index

The compaction study appends 100,000 canonical rows to identical million-vector starting snapshots on one and two hosts. Every case preserves all original ID-map prefixes and publishes a fully audited 1.1-million-row tree while old readers retain their earlier results. Independent review passes 896 owner bindings across the integration and six measured arms. Raw journals verify seventy-two successful old-reader checks in the paired arms and fifteen in the integration.

| Pair | One-host writer phase | Two-host writer phase | One-host through publication | Two-host through publication |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 5.779 s | 4.342 s | 10.702 s | 10.175 s |
| 2 | 6.031 s | 8.427 s | 10.968 s | 14.169 s |
| 3 | 5.640 s | 5.354 s | 10.557 s | 11.056 s |

The geometric mean writer-phase speedup is 1.001 times; this execution path establishes correctness but no aggregate speedup. In the second pair's slow arm, a critical job waits 7.921 seconds before its Rust wrapper starts, then executes in 0.466 seconds. A separate guarded worker-local pool and parallel verification epoch will measure removal of that launch overhead. Internal split initialization also uses randomness, so fixed input and starting snapshots do not imply identical internal balancing operations.

Independent storage inventory passes all 128 owners. The current 1.1M-row snapshot references 3.687 GB; retaining both published generations requires 4.370 GB of object files, with zero unknown or unreferenced objects. Entire checkpoint directories consume 4.388 GB. These are logical file bytes; allocated object bytes are 4.380 GB. The retained/current ratio of 1.1853 describes one append commit and two published generations. Repeated private checkpoints and peak staging remain separate measurements.

A separate guarded execution path runs owner jobs within each worker and verifies hosts concurrently before publication. All six measured arms pass their 128-owner audits, publication, pinned-reader checks, and old-generation reopening without failures or outlier exclusions.

| Pair | One-host critical writer span | Two-host critical writer span | One-host through publication | Two-host through publication |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 5.948 s | 2.652 s | 19.799 s | 11.383 s |
| 2 | 5.677 s | 3.396 s | 20.157 s | 12.338 s |
| 3 | 6.723 s | 3.645 s | 21.059 s | 12.193 s |

The geometric mean paired ratio through publication is 1.6994 times. Mean elapsed times are 20.3383 seconds on one host and 11.9713 on two. Driver control and reconciliation remain substantial. These measurements use a separate frozen execution protocol; its absolute one-host elapsed time exceeds the earlier SSH path, whose paired mean is about 10.74 seconds through publication. The ratio demonstrates scaling within the guarded protocol and does not establish an improvement over the fastest earlier absolute result. Independent six-arm reconciliation passes all 768 owner maps, audits, immutable bindings, host terminals, clone inventories, and 111 matching old-reader checks. The monitor exercises one fixed query and proves overlap with the driver construction interval, rather than each individual Rust mutation. Internal split work is randomized.

## Required ten-million-row progression

Ten million real rows ingest in 676.928 seconds with 14.803 GB of requested public-source bytes and 33.629 GB of output. A deterministic 65,536-row sample fits 128 slots. Routing produces nonempty owners containing 27,583–170,731 rows, with the largest 2.185 times the mean. The first validation attempt fails on signed/unsigned index arithmetic; its outputs and report remain preserved. The corrected retry reuses ingestion without downloading data and reproduces the model hash exactly.

The retry passes complete ID coverage, bitwise vector equality, and recomputed owner membership across all ten million rows in 156.036 seconds. Sample checks and 256 UUID provenance checks pass in 20.746 seconds. Fresh full-prefix references pass for all hundred calibration queries in 100.326 seconds and all hundred test queries in 99.628 seconds. Three independent full-prefix float64 scans and global sorts pass in 56.134 seconds. The final preparation attempt passes and its resource guard exits cleanly. A 48-row public-source spotcheck passes in 28.795 seconds. The complete authoritative archive and version-pinned restore pass in 566.329 seconds, verifying every hash across 646 unique objects and 64.639 GB. Failed routing copies are represented by byte-verified aliases rather than duplicated authoritative vectors. Worker-only input staging totals about 30.803 GB. The isolated preparation machine has an unconditional stop deadline of October 14 at 10:49:40 p.m. Toronto time; guarded index construction will measure repeated private checkpoints before any 100M work.

The first two-host 10M construction attempt completes all 128 writers and both host reconciliations in a 159.143-second launch/control/writer phase. Publication stops safely because repeated navigation stdout messages produce about 3.15 GB of logs, exceeding the bounded collector. Snapshots and full logs remain preserved. The streaming collector repair passes its source and Linux gates. Separate snapshot-verification and publication recovery pass in 59.688 seconds without rerunning writers. Every owner snapshot/map/audit reopens, complete ten-million-ID coverage passes, terminal records match, and atomic publication succeeds. The failed construction and recovery remain separate operations; no uninterrupted elapsed time through publication is claimed. A matched fresh one-host build follows with the same binary and checkpoint policy.

All 128 completed owner storage diagnostics pass. Their snapshot references occupy 33.612 GB; all physical object files occupy 44.397 GB, a ratio of 1.321. Allocated object bytes are 44.476 GB, and entire checkpoint directories occupy 44.478 GB of logical bytes. The 10.785 GB outside enumerated owner snapshot references remains conservatively retained. These counts exclude prepared inputs, staging, and peak scratch. Global publication passes in the separate recovery step described above.

A fresh uninterrupted one-host build passes all 128 jobs, full ten-million-ID coverage, actual reopening, and atomic publication. Its host-local writer span is 284.859 seconds; construction including control and full-log proof is 393.069 seconds; elapsed time through publication is 401.981 seconds. Complete-log inspection accounts for 102.378 seconds, with prepared-input SHA preflight excluded. The fresh uninterrupted two-host comparison passes under the same binary and checkpoint/collector protocol. Its critical host writer span is 154.399 seconds, construction is 212.377 seconds, and elapsed time through publication is 219.556 seconds. Complete-log inspection accounts for 52.580 seconds.

| Placement | Critical host writer span | Full-log proof | Through publication |
| --- | ---: | ---: | ---: |
| One host | 284.859 s | 102.378 s | 401.981 s |
| Two hosts | 154.399 s | 52.580 s | 219.556 s |

This single matched pair gives 1.845-times writer-span and 1.831-times elapsed scaling through publication. Each host assigns sixteen of its sixty-four logical CPUs to writers. Internal balancing initialization uses randomness. The frozen eight-setting calibration passes all eight hundred queries. Its results are below; same-immutable-tree held-out placement consistency passes all hundred queries on each placement with zero failures and identical full traces/results. Held-out recall is 95.87% on both. A known archive transfer overlaps the first pass, so its latency is excluded. A clean fresh-reader repeat passes all two hundred queries and all hundred full trace comparisons without overlapping archive or worker I/O. Mean latency is 453.934 milliseconds on one host and 367.420 on two; medians are 432.585 and 347.055 milliseconds. Operating-system cache remains warm/uncontrolled. These sequential passes do not establish load capacity.

| Global beam | Candidate budget | Calibration recall@100 | Mean latency |
| ---: | ---: | ---: | ---: |
| 256 | 1,000 | 79.41% | 288.447 ms |
| 256 | 4,000 | 79.43% | 333.889 ms |
| 512 | 1,000 | 86.34% | 248.576 ms |
| 512 | 4,000 | 86.36% | 383.362 ms |
| 1,024 | 1,000 | 91.22% | 278.951 ms |
| 1,024 | 4,000 | 91.24% | 454.538 ms |
| 2,048 | 1,000 | 95.45% | 324.469 ms |
| 2,048 | 4,000 | 95.50% | 534.064 ms |

The frozen rule selects B2048/M1000 before held-out execution as the fastest observed qualifying point. Calibration uses two hosts, memory-v4, optional adapter reduction, four adapters and sixteen logical CPUs per host. Persistent readers and ascending grid order make this a selection under declared warmed-cache conditions; causes of nonmonotonic latency are not isolated. The separate logging-only binary removes the repeated messages for future epochs; it is not substituted into these measurements.

## Real-data preparation and recovery

The checked columnar ingestion path processes one million vectors in 100.8 seconds, consuming 45.7 CPU seconds and peaking at about 949 MiB resident memory. All ten vector and identifier chunks match the original preparation exactly. The two-file fault/recovery check preserves canonical order and values across 1.1 million rows. The paired 2,156,499-row ingestion check takes 221.451 seconds with one worker and 122.648 seconds with two, a 1.806-times improvement. All vector and identifier hashes match. Peak process memory rises from approximately 0.98 GB to 1.89 GB; this is one paired observation.

A private versioned sandbox archive passes a complete recovery check. The restored source, executable, and 5,002-row checkpoint verify by hashes; reopening produces identical query results. Independent inventory binds all 128 owners and measures 3.351 GB of data-store logical bytes, with no unaccounted objects. Each owner retains only its first generation and flushes once. A live 23,001-row, two-owner smoke test completes twenty-four private flushes. Its final live objects occupy 77.216 MB, while all physical object files occupy 194.710 MB, a 2.522-times ratio. The remaining 117.495 MB is retained conservatively; no garbage collection occurs. The required 10M gate measures larger-owner memory, repeated checkpoints, temporary copies, and disk headroom before 100M.

## Next completed-run reports

The GPU pilot passes all 200 exact-reference queries over the verified million-row corpus, with identical top-100 IDs and maximum float64 distance difference of 2.22e-16. Three million score-bound checks and four adversarial cases pass. The warm-cache scan takes 12.742 seconds including host conformance overhead and excluding runtime installation. AWS confirms the GPU machine terminated before its stop deadline, with approximately $0.25 in compute and preserved evidence. Million-vector calibration selects B512/M1000 at 97.85% recall before the held-out comparison. A separate compaction experiment appends the next canonical 100,000 rows to identical starting snapshots on one and two machines, while readers retain the old published generation. No 100-million-row or billion-row run has completed.


## Current hundred-million-row and dynamic-sharding work

The hundred-million-row real-data preparation job runs under an independent six-hour stop at October 15 08:01:30 UTC, with an $8.238 compute ceiling. Twenty-five producer tests, ten Linux resource-guard tests and a twenty-one-row full preparation/archive/handoff workflow pass before launch. A final receipt-schema error is corrected separately without rerunning that workflow. No hundred-million-row preparation or index result is complete yet.

The opt-in worker-balancing and topology implementation passes fifteen Linux tests, including nine fixtures using actual Rust readers. These exercise subtree migration, split, merge, reassignment, append compaction after a split, destination-only reopening, pinned old readers, global query budgets and competing deployment publication. A fresh Astra medium review checks the frozen v7 fixes. These small fixtures establish correctness properties; they do not measure migration performance or establish distributed operational readiness. Exact evidence and remaining limits are in dynamic-review/ and DYNAMIC-SHARDING.md.


The selected published 1.1M-row tree passes a separate post-compaction quality check at the unchanged B512/M1000 policy. Both placements return 97.41% recall on all hundred held-out queries, with zero failures and all hundred full logical traces equal. This confirms same-tree placement consistency after append compaction. Exploratory means are 286.971 and 220.960 milliseconds; the unprofiled sequential check does not measure load capacity. Two setup failures occur before readers start and remain separate from the successful query run.

Astra's completed dynamic-sharding code review identifies four remaining bugs in cosine routing, crash durability, publication-journal reconciliation and placement count admission. The implementation worker fixes them in a separate source epoch. Sixteen passing Linux fixtures, including ten actual Rust fixtures, cover the additional advertised-endpoint readiness check; they do not close those independent review findings.


Independent review verifies the clean 10M held-out results against complete references and all ten million map IDs. Sampled interior process CPU averages are 4.345 cores for the one-host placement and 2.569 plus 2.601 cores across two hosts; these are scoped process observations, not whole-machine utilization. The dynamic v7 fixes pass twenty-three Linux tests, including fourteen actual Rust-backed methods across three metrics, and undergo another Astra medium review. Actual two-host dynamic migration remains the next correctness milestone.


## Full-node ten-million-row construction

The logging-only V5 epoch passes a sequential one- versus two-host pair using all sixty-four logical CPUs and up to sixty-four concurrent owner jobs per host. Both arms pass all 128 owners, complete ten-million-ID coverage, audits, actual reopening and publication without failures.

| Placement | Critical writer span | Through publication | Peak aggregate writer RSS |
| --- | ---: | ---: | ---: |
| One host | 127.643 s | 144.953 s | 23.59 GB |
| Two hosts | 81.793 s | 95.899 s | 18.52 / 20.13 GB |

This single pair gives 1.560-times writer-span and 1.512-times elapsed scaling. Input hash verification of 27.426 and 28.027 seconds is excluded. One-host child CPU is about 14.2% higher; randomized internal balancing prevents treating this as identical work or a confidence-bounded estimate. Absolute comparisons against V3 sixteen-CPU results also change the logging epoch, so they cannot isolate CPU allocation. Independent final reconciliation passes; full checkpoint archival remains pending.

The idle 32-CPU driver is stopped with its root evidence retained. Remaining active compute is approximately $11.4338/hour, excluding storage/network. The hundred-million-row ingestion continues independently. Astra clears all four v7 fixes and identifies three further content-admission/path/durability gaps for the next dynamic source epoch.


The full-node pair passes independent metadata, audit, timing and resource reconciliation. Sampling gaps reach 4.578 seconds, so reported memory peaks are sampled lower bounds. Full checkpoint preservation is separate: worker zero fails post-export verification and reconciles its children while worker one reaches upload. No complete archive/restore claim is made yet. The dynamic-v8 epoch passes twenty-nine Linux tests, including eighteen real Rust-backed methods; its canonical-content, append and namespace fixes undergo fresh Astra review.


The checkpoint archive verification failure is reproduced as an equal-distance tie in its auxiliary legacy top-one probe while all snapshot/map bytes remain unchanged. A deterministic pinned-ID probe revision passes six Linux tests and undergoes independent retry admission; failed outputs remain retained. Worker one is restoring its archive, with completion still pending. Astra clears the three v8 findings under the trusted immutable-worker contract and identifies two further destination-path/reconciliation bugs for v9. Cross-host components have local unit evidence, but no actual two-host migration run has started.


Worker one's selected-10M half passes complete checkpoint restoration: 64 owners and 5,001,462 rows, 22.257 GB source files, all hashes and reopened probes. This takes 648.802 seconds. Worker zero retries separately, so complete-tree preservation remains pending. The local dynamic-v9 gate passes forty-three Linux tests, including nineteen actual Rust-backed methods, while cross-host orchestration and its execution admission remain unfinished.


The selected 10M index now passes complete private preservation across both workers: all 128 owners and ten million rows, 35,037 files and 44.457 GB source bytes. Both archives are fully restored with every hash and snapshot/map/audit/root checked. Failed and recovered archive attempts remain separate; these operations do not change construction timings. Worker one's V5 half is also preserved; worker zero's V5 stores remain in progress.

The separate batched-reference engine passes twelve new and thirty-two combined CPU numerical tests. Independent review and CUDA validation remain pending, and both larger exact-reference pools are incomplete. The 100M preparation has completed receipts for 78.19M rows and continues under its unchanged stop deadline.


All 100M source rows have complete-file markers, but authoritative ingestion publication and UUID checks still run; no completed ingestion receipt exists yet. Baseline 1.1M worker one's checkpoint half passes full preservation. Independent batched-engine review identifies an exhausted-iterator correctness bug and NumPy backing-storage retention; fixes and a complete executable wrapper precede hardware admission. No new exact-reference pool is complete.


The campaign is capped at 100M rows by Lyon’s instruction. The 250M progression is held; current 100M validation and dynamic-sharding work continue.


## Experiment policy and the 100B objective

The goal is a measured capacity model for a sharded index that could reach 100B vectors or more. Dataset size increases only when a smaller test cannot answer the question. Each run declares the unresolved hypothesis, smallest representative dataset and topology, observed metrics, stopping rule and the implementation decision it informs. Correctness fixtures stay tiny. Working-set tests use representative subtree sizes; coordination and fan-out tests use the fewest hosts needed. The model includes bytes per live vector, retained/checkpoint amplification, memory per subtree, upper-routing memory, query CPU and network fan-out, append/compaction throughput, migration bandwidth and skew. Estimates must identify unmeasured limits and cannot extrapolate cache-resident results through a disk-bound regime without validation. Existing successful measurements are reused. Current expensive 100M downstream stages and automatic 10M append work are reassessed before launch; the 250M progression stays held.


## Current correctness and lifecycle status

The selected 10M checkpoints, both V5 construction placements, selected baseline 1.1M generation-two tree, and original collector-failure logs have complete private version-pinned archive and full-restore proofs. The authoritative aggregate receipt is fleet/preservation-complete-20261015.json. These preservation completions do not add construction or query performance measurements.

Both original workers and the control driver are API-confirmed stopped at 04:55:57 UTC with required artifacts retained. Remaining compute is the preparation node at approximately $1.373/hour, excluding retained storage and network charges. Lyon explicitly approves discarding its unfinished reconstructible 100M payload and stopping that node; completion awaits lifecycle verification. The preparation pipeline itself is intentionally stopped before completed ingestion, so emitted file markers do not establish a successful 100M ingestion result.

The repaired V12 source passes 23 local focused tests with two Linux-only checks skipped. Independent Astra medium review still finds two failure/cleanup races and one missing placement-preparation journal. Repairs and independent admission precede remote runtime. A global write-search planner passes four local tests; connecting its selected leaf to the Rust writer remains pending. The plan uses the existing compactor/query fleet separation and single ownership. The two query traversal strategies will be compared at matched recall when ready; no comparison or larger scale run has started.


Seven native Rust write-interface tests pass, including persisted selected-leaf registration and explicit global-ID scalar checks. An independent reviewer verifies the frozen eighteen-file source and preserved results within the in-process contract. Remote protocol, frozen upper-boundary routing and global publication integration remain unfinished. The separate eight-vector native CLI smoke passes; a newly built binary still requires distinct admission before use in the frozen correctness harness.

All task compute is stopped following a second approval-rejected driver upload. No remote runtime occurred. Automatic review also blocks the next Astra invocation; local implementation and independent review continue. The incomplete 100M data is discarded under explicit authorization, with timing/provenance evidence and root disks retained. The overnight follow-up remains active and redispatches useful local work.


Two local agents using the separately admitted native Rust writer pass authenticated checkpoint transfer and split/merge/reassignment correctness in a focused 5.087-second gate. A separate global-write source epoch passes eleven native tests, including committed upper-boundary routing and single-owner ledger rejection cases. These are local correctness results; physical-host/Linux process acceptance and complete compactor transport/publication integration remain pending. No larger benchmark runs, and the last verified task fleet state remains stopped.


A retained eighteen-row, two-owner native fixture confirms that globally selected leaves survive private commit, flush and reopen. An independently admitted older reader verifies both appended IDs in those exact saved leaves. The complete saved-deployment publication path remains awaiting source/binary review and failure tests; this result establishes private persistence only.


Three local real-index tests pass for complete saved publication, partial-owner orphan rejection, and stale-parent rejection. A separate older-reader check verifies selected-leaf membership after final publication. A stricter duplicate-posting policy passes its local regression but awaits independent admission. Actual flush-I/O failure, Linux process and physical-host checks remain pending.


Four native publication tests pass independent reconciliation, including an actual first-blockfile write failure that preserves parent files and the published pointer, followed by a successful fresh-parent retry. A separate ownership-page component passes six local tests and proceeds to independent review and deployment integration. It does not yet remove full-map or root-metadata scaling limits.


Ownership pages and saved index roots now pass local tests through one complete deployment publication, including page-write failure and repeated append. Independent integration review remains pending. Changed-page verification reads less page payload in the tiny fixture, but full-map validation and linear metadata work remain, so no end-to-end scaling improvement is claimed yet.


Paged split, merge and reassignment pass three local real-index tests, including partial page failure and stale publication. Independent review of this topology revision is pending; the preceding append integration passes independent complete-record validation. Topology still rewrites all ownership pages. Hierarchical metadata implementation begins as a separate small component; no large-scale capacity claim is added.


## Reader memory progress at 08:46 UTC

The optional streaming path passes three complete local query comparisons and whole-request capacity-error cleanup. It avoids copied posting arrays while retaining a capped map of all distinct candidate minima. The empty-leaf repair separately passes four native Rust tests and independent review; its new CLI replay remains pending. A reviewed next collector will retain the nearest M minima plus every cutoff tie, with exact distinct counts and explicit transient limits. Worst-case ties remain linear, and provider caches, allocator overhead and process memory remain unbounded by these payload counters. These are local correctness results, not distributed performance or 100B capacity evidence. The detailed run journal records source epochs, failures and limitations in Runs 320–323.


## Local integration status at 12:57 UTC

The complete first-append fixture now combines actual saved record roots, hierarchical vector leaves and an explicit record-offset/global-ID mapping in one registration proposal. Independent review verifies both selected-leaf memberships and exact IDs/vector bits. A real sidecar failure poisons its attempt before a separate fresh attempt succeeds. This remains a local fixture with a fake collection authority; repeated append and placement-preserved reuse are in progress. Detailed evidence and failure separation appear in Runs 329–341.

Actual registration-operator tests and actual materializer/record-writer tests pass in focused harnesses. The complete worker build remains blocked by fourteen baseline quantization API mismatches in its mandatory index dependency. Production context/server wiring and physical-machine execution are not established. Record offsets remain u32; the explicit mapping does not by itself prove a scalable global namespace.

Reader improvements preserve tested query results: optional posting streaming avoids copied arrays, candidate pruning preserves all cutoff ties, and optional provider cache controls reduce retained cache objects. Cache targets are soft, and decoded allocation peaks, allocator overhead and total process memory remain separate unresolved bounds. Pre-decode admission requires storage/decoder integration rather than a check after allocation. No larger dataset run or cloud restart accompanies these local milestones.


## Local distributed topology correctness

Two local worker agents pass an eighteen-vector transfer, split, cross-agent merge and reassignment fixture. Every operation publishes a complete proposal through one test collection authority and preserves record identity. Independent review verifies nine actual record audits, exact saved inventories and all eighteen remote vector results after each topology operation. The successful fixture takes 3.431 seconds; this is correctness-test duration, not a throughput benchmark. The earlier listener-permission failure is retained separately.

This proof uses real local transport and native vector readers with injected identities and a fake publication authority. It does not establish physical fleet operation or production registration. Rebuilds consume canonical vector payloads and retain full mirrors for validation, so this path is not yet evidence of bounded maintenance cost at 100B vectors. Global write-beam insertion through this transport is the next integration step. Evidence is in `dynamic-review/record-namespace-transport-v2-approved-attempt/independent-runtime-review.json`.
