# One-owner implementation and its limits

One local owner holds the whole growing tree and runs insertion and balancing directly through its writer. The source uses the optimized version-free unsharded writer as its base. The benchmark compares the changed engine with a separate checkout of that reference, rather than measuring only an owner wrapper around identical internals.

## The pieces

| Piece | Implemented representation and responsibility |
| --- | --- |
| Local owner | A writer plus an owner ID. Its ownership view reads the writer's current root, so replacing the root does not leave a stale second root record. Public phase methods require exclusive access; each phase can use parallel scoped worker threads. |
| Resident nodes | A concurrent map locates reference-counted node handles. Each handle holds its own read/write lock and an optional node value. Map buckets are released before taking node locks. Retirement removes the value; outstanding old handles cannot revive or modify a replacement. |
| Construction routing | Internal nodes publish child IDs and full-precision centroid rows together as an immutable payload. Construction reads those payloads with short protected node access. A retired selected node triggers a bounded traversal retry. |
| Postings | Leaf membership and one-bit codes are stored separately from saved leaf metadata. Opened leaves can keep lazy posting shells and append deltas. Navigation does not need to load their postings. |
| Full vectors | Stable vector IDs access full-precision values independently of subtree placement. New vectors follow the reference's in-memory ingestion and completed-phase persistence path. The current stored ID width remains 32 bits. |
| Membership coordination | Short leaf claims protect physical source removal and destination append. There is no per-vector owner directory or membership-version service. Failed or incomplete transfers prevent saving. |
| Structural coordination | A short reservation protects the affected ancestor region, including the next parent when its cached centroid row changes. Ancestor and descendant reservations exclude each other; disjoint regions can publish concurrently. Preparation happens outside this reservation. Ancestry is revalidated after admission. |
| Balancing scheduler | A shared queue holds typed leaf and internal-node jobs. Workers take ready jobs individually; newly generated work joins immediately. Active producers keep an empty queue from terminating prematurely. A finite work budget and joined residual/transfer rescans remain explicit. |
| Saved reader | Completed saves persist four-bit centroid codes. New readers install those codes directly and use full-precision queries to score them. Selected postings use the existing one-bit scorer. Older saved layouts have an explicit compatibility path. |

## Construction

An insertion batch groups rows by destination across workers before appending each leaf's group. The benchmark uses one complete 100K batch, followed by balancing. The owner adds no per-vector routing request or sequential executor queue to this path.

Balancing workers reserve affected memberships, prepare replacements while routing remains readable, and acquire a short ancestor-region reservation for publication. They revalidate parents and links before attaching replacements. Follow-up leaf and internal jobs become eligible after their producing changes are published. A root split retains the complete tree under the same local owner.

Saving follows completed ingestion and required transfers. The benchmark drops the writer, opens the saved reader, and checks all saved memberships, reachable parent/child links, full-vector bits, one-bit posting codes and four-bit centroid codes. Normal client reads use immutable saved generations; they do not navigate the mutable writer.

## Explicit boundaries

Cross-owner membership transfer, subtree handoff, shared upper-tree publication and distributed navigation return unsupported-operation errors. Their network protocols are not simulated by local success. The one-owner configuration has no separate shared upper-tree service because it owns the global root directly.

The implementation does not demonstrate 100B capacity, physical distributed execution or held-out quality parity. Stored vector/node identifiers remain 32-bit; node allocation refuses exhaustion rather than wrapping. General bounded-memory ingestion, metadata eviction and a complete distributed ownership/publication protocol remain scaling work. The fixed benchmark has conservative memory admission, which is not a general cache-budget implementation.

Navigation may return the still-live candidate subset after four retirement retries under sustained changes. Structural size-improvement budgets may leave reported occupancy violations; incomplete membership transfers remain a hard save failure. These are correctness and interpretation boundaries to retain in later experiments, not evidence that construction quality is equivalent.

The source remains in the reference's experimental benchmark module layout. The local owner interface, resident-node store, scheduler and publication implementation are actual exercised code; library extraction and production integration are outside this bounded comparison.

## Verification and evidence

The independent [review](review.md) identifies the exact scoped source digests. The [native test receipts](local-tests/) preserve both failed attempts and passing checks. The combined suite passes 249 executions across four wrappers, including repeated shared tests. Five packed-centroid library tests pass. Both benchmark executables compile and pass a 64-vector synthetic save/reopen smoke check; those native smoke timings are not performance evidence.

The [execution plan](PLAN.md) defines the fixed cloud comparison and its stopping condition. The completed [report](REPORT.md) preserves all four measurements, their interpretation and the failed connectivity attempt. Correctness checks pass; performance parity is not established. The task is stopped.
