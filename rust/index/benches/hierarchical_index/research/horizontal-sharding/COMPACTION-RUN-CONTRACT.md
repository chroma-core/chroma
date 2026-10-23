# Measuring compaction across machines

This experiment measures how quickly workers append real vectors and rebalance existing subtrees while readers continue using the published index. It uses identical starting snapshots and the same added rows on one and two machines, so placement is the comparison variable.

## Inputs and ownership

The base contains the verified first million FineWeb rows in 128 fixed subtrees. The delta contains the next 100,000 canonical rows, preserving source values and global identifiers. The existing upper routing model assigns the delta; the experiment does not refit that model. Preparation records counts and hashes for every changed owner before timing.

Each case begins from a separate copy of the same generation-one snapshots. Staging and input preparation occur outside the timed interval. Each machine receives sixteen logical CPUs, corresponding to eight physical cores with two hardware threads each, and runs up to sixteen owner jobs. Each owner uses one balancing thread and two runtime threads. The run records the actual changed-owner placement and load imbalance.

## Timing and correctness

1. **Pin readers to the existing version.** Save old-version query traces and continue issuing those queries during mutation and publication. The old readers must preserve their original results.
2. **Append and rebalance the same rows.** Measure the changed-owner task graph from writer startup through completed audited snapshots. Record elapsed time, CPU time, aggregate sampled resident memory, warnings, and any failed attempts. Describe the checkpoint interval explicitly.
3. **Publish the complete new version.** Measure publication separately and report elapsed time through publication. Complete coverage must contain 1.1 million unique global IDs. Every unchanged owner may reuse its existing snapshot.
4. **Verify both versions.** Reopen the new version, verify root and mapping bindings, and compare equivalent one-machine and two-machine reads. Old pinned readers must retain their earlier traces. Any recall claim requires exact reference answers over the complete 1.1-million-row corpus.

The initial pair establishes correctness and timing feasibility. If it passes within the bounded slot, three paired repetitions alternate placement order. Preserve and report each run before summarizing speedup. A failed case remains visible with its completed work and failure reason; do not silently substitute a retry.

## Storage interpretation

Record physical bytes for current snapshots, the union of every retained generation, and total owned store files. Include prepared inputs, staged copies, and temporary peaks separately. Files outside the known manifest union remain conservatively classified until every retained root is accounted for. This experiment authorizes no deletion of checkpoint blocks.

Repeated private checkpoints can increase physical storage even when the final index is small. The initial construction matrix flushes only once per owner, so its final bytes alone cannot establish the storage allowance for a longer build or repeated compaction.
