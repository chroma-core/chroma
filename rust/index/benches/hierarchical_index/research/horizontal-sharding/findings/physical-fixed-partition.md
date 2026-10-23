# Physical experiments with fixed ownership boundaries

The physical one/two-host evidence belongs to a fixed-boundary subtree engine. It demonstrates useful transport, publication and query mechanisms, but it does not establish physical execution of the current dynamically evolving engine or unrestricted cross-owner balancing.

The [pilot report](../RESULTS.md) covers one million MS MARCO vectors in 16 fixed spatial subtrees. Its best measured construction times are 17.86 seconds on one host and 22.37 seconds on two; the small task graph does not show a machine-capacity advantage. All 100 held-out query traces match between placements. Recall is 90.01%, following a calibration setting that misses its declared 90% calibration gate. These results retain their own source version and protocol.

The [continuation report](../CONTINUATION-RESULTS.md) covers fixed 128-subtree FineWeb runs. At matched 16-logical-CPU allocations with 16 jobs per host, one-million-vector construction is 11.05 seconds on one host and 6.49 seconds on two; through-publication times are 18.37 and 12.99 seconds. The allocations run on larger machines with abundant memory and local disks, so equivalent small-instance cost is not established.

The selected million-vector query policy reaches 97.88% held-out recall on both placements, with exactly matching traces. With a separate larger driver and three alternating repetitions, sixteen-client mean throughput is 6.929 queries/s on one host and 12.354 on two. The short warm windows reuse a limited query schedule, and adapter processes constrain serving. These are paired observations rather than broad workload or p99 capacity claims.

Appending 100K vectors to the one-million-vector snapshots preserves old readers and complete current coverage. The first three pairs have a geometric-mean writer-phase speedup of 1.001, so they establish no aggregate speedup. Launch overhead motivates a separate guarded execution path; the continuation report retains its subsequent results, the 10M study, storage inventories and qualifications.

**Decision:** Reuse validated transport, generation checks and benchmark methods where their assumptions fit. Keep fixed-partition timings separate from dynamic construction. The data and independent reviews remain in [measurements](../measurements/), [pilot results](../pilot/results/) and [subtree experiments](../subtree/), linked in the two reports. Their proposed size sweeps and next-step sections are archival, not the current plan.
