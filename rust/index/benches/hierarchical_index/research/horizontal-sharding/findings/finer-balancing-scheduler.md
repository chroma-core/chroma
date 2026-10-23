# Finer balancing jobs

Individual leaf jobs substantially reduce balancing latency on a retained unsharded Wikipedia tree. This supports separating machine ownership from worker scheduling: a machine can own a large subtree while many workers balance leaves inside it. The measurements do not establish distributed safety or a speedup on every workload.

## Question and mechanism

The experiment tests whether dynamic assignment of leaf jobs reduces the long tail caused by statically assigning whole subtrees to workers. Each round snapshots eligible leaves, orders them by descending posting size with node ID as a tie breaker, and distributes unique queue positions through an atomic counter. A worker takes another job immediately after finishing its current job. Jobs recheck live node type and size; retired or no-longer-eligible leaves are skipped. The undersized global root is excluded from merge jobs.

The candidate retains joined round boundaries. Newly created leaves and newly oversized destinations appear in the next round, not through within-round refill. Leaf claims protect membership and leaf lifetime; structural edits retain the existing tree mutation lock and current-parent recovery. Failed claims preserve work for retry, and partial mutation failures block publication. Unique queue positions alone do not provide mutation safety.

## Measured results

The saved-tree comparison starts each replay from an independent copy of the same 8M-vector snapshot and adds 1M Wikipedia vectors. Both variants use full-precision construction, 32 insertion and balancing workers, the same 32-physical-core Xeon host, branching factor 100, split/merge thresholds 2048/512, a configured 1 GiB cache and 100 fixed queries with matching precomputed neighbors. The second pair reverses execution order. Each pair uses one local NVMe volume; the volume differs between pairs. This is not a controlled cold-cache experiment.

| Measure | Reference, pair 1 | Finer jobs, pair 1 | Reference, pair 2 | Finer jobs, pair 2 |
| --- | ---: | ---: | ---: | ---: |
| Balancing | 47.856 s | 16.692 s | 45.264 s | 16.101 s |
| Insertion + balancing + commit/reopen | 63.254 s | 32.228 s | 60.773 s | 31.615 s |
| Retry phase | 0.368 s | 1.678 s | 0.421 s | 1.541 s |
| Sampled average logical cores used during balancing | 5.39 | 17.49 | 5.41 | 17.06 |

Balancing falls by about 65%, and the measured phase sum falls by 48–49%. Insertion and commit times remain similar. Sampled CPU work increases slightly, from approximately 249/240 CPU-seconds to 265/258, while retry time rises. The evidence supports reduced idle time rather than less total computation. CPU sampling covers most, but not all, of the balance interval; it is not exact phase accounting.

The gain isolates a scheduling package on the same version-free writer: leaf granularity, dynamic assignment, largest-first ordering and next-round discovery. It does not isolate each component individually. Neither variant includes the separate batched-transfer or temporary-deletion experiments. The diagnostic counter named `partitions` counts subtree roots in the reference and leaf jobs in the candidate, so those counts are not directly comparable work units.

All four saved-tree outputs pass complete exact-once membership and topology audits, posting/code length checks, and sampled old/new embedding and code checks through persisted reopen. Recall differences across the reported query settings are at most 0.13 percentage points; 100 queries do not exclude smaller changes. Both variants reach the shared eight-round cap with a few residual size violations. Mandatory transfers complete before publication. Fresh 1M construction shows no established gain: the single finer-scheduler run is 3.4% slower in the measured phase sum.

The phase sum includes production preflight, insertion, balancing, commit preparation/flush and writer reopen. It excludes benchmark input loading, diagnostics, separate correctness audits and other harness work, and is not total production latency. These results cover one retained tree and two execution orders; they do not support extrapolating the speedup to larger trees or physical shards. The full report retains residual leaf sizes and raw work, timing, memory and query evidence; it does not provide a complete tree-depth/occupancy characterization.

## Decision for the sharded design

Adopt individual leaf jobs, dynamic assignment across an owner's subtrees, and larger-posting-first ordering as the initial heuristic. Preserve bounded queue memory, live job revalidation and follow-up work. Treat deferred internal-node work explicitly. Do not bind an entire owned subtree to one worker or confuse ownership boundaries with worker jobs.

Resolve mutation protection separately. The measured implementation's leaf claims and tree lock do not implement the sharded design's affected-region protection, shared-parent updates or cross-machine transfers. Its round barriers and eight-round cap are evidence conditions, not adopted sharded requirements. No sharded implementation change or benchmark is performed by this documentation update.

## Sources and reproduction

- The source chat is [Memory Optimizations](codex://threads/01a0d6c5-eb50-7d21-889a-359926993a01). Lyon authorized asking it for the scheduler handoff; its October 20, 2026 reply confirms these mechanics and qualifications.
- The [experiment design](../../version-free-writer/balance-experiments/scheduling/DESIGN.md) and [implementation handoff](../../version-free-writer/balance-experiments/scheduling/HANDOFF.txt) record safety reviews and 173 passing test executions.
- The [full results report](../../version-free-writer/balance-experiments/benchmarks/REPORT.md) links commands, source/binary hashes, raw logs, audits and sampling limitations.
- The reference commit is `eea4ada6f21554adb535d1ffdea95f74cb76650b`; its direct-child scheduling candidate is `14dc9698eb6e400ba6f677575eff7a5659deeed6`, on `codex/finer-balance-jobs`.
- The retained [source patch](../../version-free-writer/balance-experiments/scheduling/candidate.patch) replaces coarse subtree assignment with `balance_jobs` and an atomic queue cursor. The [implementation checkout](/private/tmp/chroma-finer-balance-jobs/rust/index/benches/hierarchical_index/writer/writer.rs) contains the queue and existing mutation helpers.
