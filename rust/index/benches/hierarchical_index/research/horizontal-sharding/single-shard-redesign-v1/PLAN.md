# Implement one shard, compare it, then stop

Status: the bounded implementation and four-run comparison are complete. Both approved instances are stopped. Read the [report](REPORT.md) for outcomes; the protocol below remains the recorded plan, not permission to repeat it.

The experiment asks whether the agreed owner engine runs comparably to the optimized unsharded engine on identical construction work. Any excess latency needs evidence that distinguishes additional sharding work from missing reference optimizations, contention, changed scheduling and changed clustering work. A failed or slower result remains evidence; it does not trigger an automatic tuning campaign.

## Implementation boundary

One local owner holds the entire logical tree, including its current root, all local nodes and postings. It accesses the writer directly. Root replacement updates the ownership view by referring to the writer's current root; it does not introduce per-vector routing. Workers share the owner engine and work concurrently. Cross-machine transfer, placement handoff, shared routing authority and distributed navigation remain explicitly unsupported.

The engine derives from optimized version-free reference commit `14dc9698eb6e400ba6f677575eff7a5659deeed6`. Required changes are locks attached to resident nodes, deferred leaf/internal balancing with immediate follow-up admission, short protected structural publication, and saved four-bit navigation centroids. Existing explicit physical membership transfer, lazy posting storage and full-batch append grouping remain. Posting codes remain one bit. The historical sharded implementation and its measurements remain preserved.

The first implementation still uses the reference's 32-bit stored vector/node identities. It cannot represent 100 billion vectors. Widening identities and their stored schemas is a required later scaling change, not a capability demonstrated by the 100K experiment. Allocation must reject exhaustion rather than wrap. No current result may be presented as a complete 100B implementation.

## Acceptance before timing

- Independent nodes can mutate without holding map-bucket locks. Removed handles cannot mutate replacement objects.
- Typed work queues admit follow-ups while producers run and terminate only when queued and active work is exhausted or an explicit structural-work budget is reported. Incomplete membership transfers prohibit saving.
- Conflicting publications cannot lose links, duplicate memberships or retire nodes still needed by an unvalidated writer. Concurrent leaf/internal splits, merges and transfers exercise this directly, including ancestor/descendant conflicts.
- Every input ID has exactly one reachable posting. Completed save/reopen preserves vectors, centroids and posting representations after writer retirement. Older saved readers remain valid.
- New readers load persisted four-bit centroids without recomputing them from full vectors. Unused postings remain on disk during navigation.
- One focused independent review checks the combined implementation and benchmark fairness. Material findings are resolved before remote timing.

## Fixed experiment

Use the approved sandbox worker `i-011ff180f03584f40`, with 32 physical cores and 32 configured insertion and balancing workers. Use the retained FineWeb100K input: 100,000 vectors, 768 f32 components, same order and input hash in both arms. Verify the historical retained input and hardware after guarded start. Before each arm, require at least 16 GiB available memory: this conservatively covers about 1 GiB of fixed input/state plus 256 MiB per worker and remaining margin for this fixed workload. Record available memory and reject insufficient capacity; this is workload admission, not proof of a general scalable cache or memory-budget implementation. Do not copy a corpus to the Mac or access the protected reference machine.

The relevant structural condition is many independently eligible leaf splits and posting reassignments after a full 100K insertion batch. This size is not expected to establish selective deep-tree query behavior or cache-capacity crossing. Small correctness fixtures with low branching separately force internal splits and ancestor conflicts; their timings are not used for extrapolation. Report the actual node counts by level, leaf occupancy, job counts and cache state from every timed run.

Both arms use full-precision L2 construction, branching 100, split threshold 2048, merge threshold 512, write beam minimum 10 and maximum 16, tau 1.5, 32 reassignment neighbors, and full-precision neighbor reassignment. One full 100K batch precedes balancing. Both save four-bit navigation centroids and one-bit postings. Use the same compiler, allocator, build flags and physical host. Record source, binary, input and configuration hashes. Construction randomness and scheduler ordering can change work; record their limitations rather than calling different trees fixed-decision parity.

Use two sequential paired repetitions with reversed arm order, subject to one absolute guarded execution window. Record loading, insertion, balancing, commit, flush, reopen and validation separately. Include writer work counters, structural publication counts and wait/hold durations, job counts, overlapping worker occupancy, process CPU and peak RSS. Occupancy includes waiting and is not effective CPU parallelism. Validation and input loading do not disappear from total accounting merely because they are outside construction timers.

The initial window is capped at 45 minutes and $3.10 compute at the quoted $4.032/hour rate, including builds, checks and shutdown. Each benchmark process is capped at 180 seconds. A compile failure, invariant failure, deadlock, timeout, mismatched input/configuration or exhausted window stops timing admission. Preserve the failure; do not silently extend the window, change workload size, or retry with new tuning. A fresh window needs a concrete reason and fresh independent shutdown guards within the standing budget.

This is construction evidence. Reader smoke checks validate saved-state execution but do not establish quality equivalence, matched recall, a 95% requirement, physical distribution or a scaling law. Do not multiply component speedups into an end-to-end forecast.

## Closeout

Save raw measurements and their interpretation under this folder, link findings from the canonical index, and reconcile the plan, continuation state and spending ledger. Record which requirements are implemented, which are explicit stubs and which remain unverified. Stop the worker, verify its stopped state through AWS, then report and stop work as requested.
