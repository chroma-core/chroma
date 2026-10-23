# Attribute one-owner construction costs at three million vectors

Lyon authorizes a matched 3M-vector experiment to measure the latency difference between the implemented one-owner design and the optimized unsharded reference, and attribute that difference to concrete implementation behavior. This resumes work only for the comparison and its diagnosis. The broad research heartbeat remains paused. Preserve the completed 100K evidence and source archives.

Use the same canonical FineWeb prefix in both arms: 3,000,000 vectors with 768 f32 components in source order. The first 100K matches the previous comparison. Assemble the prefix on the approved sandbox worker from exact retained S3 object versions, verifying every source checksum and the final contiguous output. Never copy the corpus to the Mac or contact protected/excluded instances.

The structural question is construction after the tree has grown past one internal root. Use 100K insertion batches through 1M, then 1M batches to 2M and 3M, with balancing after each batch. This produces 12 cycles and exercises stable-tree insertion, follow-up balancing and expected internal splits. Record actual depth, per-level nodes, leaf occupancy and internal-job counts to verify which conditions occur. This is not a cache-capacity experiment or a held-out quality comparison.

## Predictions and attribution

1. If stable insertion still consumes extra CPU with similar navigation work, record access, resident-node handles/locks or temporary allocation become leading explanations. Compare phase counters and CPU samples rather than infer their cost from wall time alone.
2. If balancing performs more navigation and neighbor-reassignment evaluations, quantify how much measured time accompanies that extra work. Immediate job admission, changed retirement visibility and transfer retries can alter the construction trajectory. Neither equal input rows nor equal split input rows means identical work.
3. If structural coordination contributes materially, reservation waits/holds and CPU samples should grow with the affected phase. Separate scheduler waiting for work from publication waiting; overlapping worker times cannot be summed into added wall time.
4. If an initial chain of splits limits available parallelism, later established-tree cycles should use more CPU cores. This does not establish linear scaling or equal construction quality.

Reuse the measured 100K baseline as the initial feedback loop. The request is to measure and explain performance, not fix a deterministic correctness failure, so a synthetic red/green latency test or automatic minimization would not answer the user's question. Use the diagnosing-bugs skill's measure-before-fix discipline without imposing a false deterministic threshold on random tree construction.

## Matched protocol

Run reference, candidate, candidate, reference on the approved 32-physical-core worker, with 32 insertion/balance workers, squared L2, full-precision construction, branching 100, split 2048, merge 512, write beam 10–16, tau 1.5 and 32 reassignment neighbors. Both save four-bit reader centroids and one-bit posting codes. No tuning or algorithm change is part of this experiment.

The shared harness records each add and balance duration, process CPU/current RSS/high-water RSS, complete existing counter deltas, coordinate/ID preflight, grouping/append costs, tree shape and candidate coordination counters. Preserve incremental cycle records if a later phase fails. Save/reopen checks cover complete unique membership, reachable links, full vectors and saved codes. Export unavailable or incomplete counters explicitly: reassignment substep counters are unpopulated; leaf split size does not count k-means iterations or internal clustering.

Compile both arms with the same compiler, allocator, release settings, frame pointers and symbols. Collect low-frequency user-space CPU samples on both arms if the host probe succeeds, and preserve profiling overhead as a shared condition. Keep separate binaries tied to compiler-artifact records and input/source/config hashes. Record how samples map to phases and distinguish sampled CPU share from wall-clock delay. Construction randomness remains uncontrolled; the report must separate direct measurements, implementation-backed explanations and unresolved attribution.

Use fresh processes and output directories, with one full input read before each run and no OS cache flush. Retain raw profiles and indexes remotely, and collect only small results/logs/profile summaries locally. Do not infer storage-cache costs from fresh resident construction.

## Resource and stop bounds

Only sandbox account 079819354890 in us-east-1 and approved worker i-011ff180f03584f40 are admitted. The existing root may grow from 160 to 256 GiB under Lyon's standing disk-expansion permission if needed to preserve inputs, profiles, source/builds and all four saved indexes. Record the exact incremental storage commitment before admission; never delete retained roots to make room.

A fresh execution window has independent local and remote absolute shutdown guards, a 90-minute deadline and a $6.20 compute cap at the verified $4.032/hour rate. Each 3M process is capped at 600 seconds. No automatic deadline extension, size sweep, source tuning or retry after a correctness failure is allowed. Finish local harness validation and focused review before remote admission. After measuring and attributing the difference as far as the evidence supports, collect evidence, stop and verify the worker, reconcile spending, record findings and stop. An attribution limit is reported explicitly rather than replaced with a causal claim.
