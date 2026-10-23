# Design: one evolving tree with machine-owned subtrees

The index is one logical search tree with a shared upper tree and machine-owned subtrees, including their posting lists. Each machine owns one or more subtrees. Adding machines changes where branches are maintained while preserving global navigation and neighbor selection.

**Scope: build and test one index, which may itself be sharded.** Future read serving must multiplex very many often-small indexes across shared reader machines; a dedicated reader machine per index is not assumed. That future read-side fleet is outside this implementation milestone and does not expand the writer into multi-index orchestration. Its obligations and experiments live in the [plan](CURRENT-PLAN.md#future-read-side-fleet-obligations).

**Status: this is the agreed target to implement.** The measured single-shard implementation is complete. Distributed implementation and testing are now authorized; the protocols below remain requirements until implementation and passing evidence establish them. The [plan](CURRENT-PLAN.md#approved-next-milestone-two-machine-correctness) owns execution sequencing and validation. The [3M report](single-shard-3m-attribution-v1/REPORT.md) and [implementation findings](findings/architecture-and-implementation.md) describe current evidence. The [research goal](RESEARCH-GOAL.md) defines the path to 100B evidence; 95% recall is aspirational.

Follow the optimized unsharded implementation by default. Preserve its algorithms, data access and batching unless a structural requirement, an explicit theory to test or measured benefit justifies a difference. A one-machine configuration must exercise the intended owner/subtree architecture.

Read in this order: [components and storage](#the-pieces), [construction cycle](#construction-cycle), [concurrency](#parallel-work-and-synchronization), [membership operations](#current-membership-and-reassignment), [ownership and publication](#ownership-and-shared-publication), [saved queries](#search-and-saved-state), then [implementation obligations](#implementation-details-to-validate) and [acceptance checks](#acceptance-requirements).

## The pieces

A **leaf** is a terminal tree node. A **centroid** is a representative vector used to navigate a branch. A **posting** contains a vector ID and its compressed vector representation relative to the leaf centroid; a **posting list** holds a leaf's postings. Each vector has exactly one current posting. Full precision means 32-bit floating-point values.

An **owner** is the construction service responsible for mutating an assigned subtree. The **construction view** is the current tree and vector data available to insertion and subtree balancing. A **query generation** is an immutable saved view of the complete index, identified by a descriptor that binds compatible saved data together.

| Component | What it holds and does |
| --- | --- |
| Shared upper tree | Routing records hold centroids and child references above the owned subtree roots. Machines can cache these records. One coordinator is the mutation authority for the entire shared upper tree in the initial multi-shard implementation. |
| Owned subtree | A connected portion of the tree contains internal nodes, leaves and their posting lists. One machine owns it. Ownership roots can sit at different depths and need not be direct children of the global root. |
| Subtree owner | A service contains a local writer, a worker pool and storage/cache access. Its workers execute independent operations concurrently. |
| Subtree ownership directory | Records map subtree IDs to owning machines. The same coordinator controls directory mutations and shared-upper-tree changes. Placement counters distinguish ownership changes. Routine routing uses these records rather than a vector-ID-to-owner lookup. |
| Posting storage | Separate records or pages hold leaf memberships and 1-bit vector codes. Leaf IDs connect the postings to leaf metadata. Owners perform posting updates and scans near this data. |
| Full-vector storage | Saved records hold full vectors accessed by stable identities and a valid storage view. Their location is independent of current subtree ownership. Local caches hold useful vectors within explicit budgets. |
| Query navigation data | Each internal node's reader entry holds its child IDs, compressed centroids and scoring information together. Query navigation can score the children without loading their posting lists. |
| Query-generation descriptor | Saved references bind compatible routing, ownership, postings and full-vector data after an ingestion job finishes. Reader workers use that immutable version independently of any later writer job. |

### What exists on two machines

Both machines run the same owner engine, each with its own worker pool, mutable subtree records and postings. Each keeps bounded caches for routing and full vectors. Both may cache common upper-tree records; neither requires the other machine's complete postings or full-vector collection in memory. Shared durable storage may hold both owners' data, but only the assigned owner mutates a subtree's current state.

One coordinator holds mutation authority for the entire shared upper tree and subtree ownership directory, together with bounded request state. It orchestrates shared publication, subtree handoff and saving; ordinary posting transfers coordinate directly between owners. Other machines may cache routing records but do not independently mutate shared routing or ownership entries. Query workers hold generation descriptors and caches separately from construction state. They may share hardware with construction workers, but their roles and resource accounting remain distinct.

One owner initially holds the growing tree. There is no fixed initial spatial partition. Read caches and retained query generations do not create additional current memberships or replicated construction ownership. Any additional per-vector ownership directory needs a concrete operational justification, including its memory and coordination costs.

| Operation | Who coordinates it |
| --- | --- |
| Ordinary posting transfer between existing leaves | The initiating worker contacts source and destination owners directly. |
| Structural edit wholly inside local routing | The owner performs local protected publication. |
| Edit affecting shared routing or directory records | The coordinator grants a publication turn and publishes compatible shared state. |
| Whole-subtree ownership handoff | The coordinator orchestrates the handoff; owners transfer payloads directly or through storage. |
| Completed-job save | Owners save their data; the coordinator publishes the complete generation manifest last. |

### Posting-list loading

Load leaf metadata without loading existing posting lists. Keep metadata and postings separate on disk, connected by leaf ID. New postings accumulate in memory; an operation that needs a list's contents loads its saved postings and combines them with the current additions and removals. Navigation alone must not fetch posting payloads. This follows the unsharded writer's lazy loading, meaning that data is read only when needed.

Posting removals must survive loading and combining saved data. Reloading a list cannot restore a removed current posting. An older immutable query generation may retain its own posting for that vector because it represents a separate saved view.

Preserve dirty state and live handles if payload eviction is needed. Loaded-posting eviction beyond the reference’s lazy loading is a [deferred memory optimization](CURRENT-PLAN.md#other-deferred-implementation-comparisons), selected from measured limits.

### Full-vector access and identity

Read full vectors independently of mutable placement. Reassignment and branch moves do not require rewriting full-vector data or finding its current construction owner before reading it. Durable data location, current subtree ownership and cached copies are separate concepts.

Keep these identities and counters distinct:

| Identity or counter | Purpose |
| --- | --- |
| Stable vector ID | Identifies vector data independently of membership and placement. IDs must support the intended population without truncation. |
| Node identity and revision | Identify an authoritative mutable node and detect relevant changes to its writer-side state since snapshot preparation. |
| Placement counter | Distinguishes changes to a subtree’s owning machine. |
| Transfer operation ID | Identifies one transfer attempt for phase tracking and duplicate-message handling. |
| Writer-run identity | Distinguishes writer executions so delayed requests from a previous run are rejected. |
| Immutable query generation | Identifies a compatible saved view for a query. |

Construction needs a current view that exposes every required vector and centroid. Reuse a current published vector view where it meets that requirement. Keep old roots available while live operations or saved readers need them; reclamation policy is outside this research scope.

### Reader and writer lifecycle and scope

The reader and writer trees serve independent workloads. The reader is read-only and serves client traffic from the latest published index version. A writer is created to ingest a backlog of logged writes into the index. It completes that ingestion and the required balancing, then saves and publishes a new reader version. Client traffic does not navigate the writer's mutable construction tree.

Between published versions, the client-serving path combines the latest reader version with newer writes from the backlog log, so clients can read live data without waiting for another index publication. This is the surrounding system's established contract, confirmed by [Chroma's WAL3 architecture description](https://www.trychroma.com/engineering/wal3). The immutable index and the log suffix together provide freshness; this project does not redesign that mechanism.

WAL/client integration, log-result merging, freshness orchestration and saving while the writer continues mutating are outside scope. Production integration, deployment readiness, failure recovery, fleet hardening and retained-root garbage collection are also excluded. Concurrent construction navigation, completed-job save/reopen and safe retention of live references remain required.

The [save/reopen protocol](#saving-and-reopening-a-completed-generation) preserves complete durable generations and older readers. Construction workers still navigate mutable state during balancing; the [protection protocol](#protecting-an-affected-region) governs that workload.

## Construction cycle

Use distinct names for changes to the logical tree, its placement and the set of shards:

| Term | Meaning |
| --- | --- |
| **Subtree balancing** | Split or merge nodes and reassign vectors between leaves to maintain the logical search tree. Candidates may cross subtree or machine boundaries. |
| **Shard balancing** | Redistribute existing subtrees, including responsibility for their posting lists, between machines to restore capacity for further construction. The logical tree and memberships remain unchanged. |
| **Resharding** | Add or remove shards. Moving an existing subtree between machines is shard balancing. |

Construction alternates insertion, subtree balancing and shard balancing. Placement stays fixed during insertion and subtree balancing. Logical splits, merges and posting reassignments remain available throughout subtree balancing; a reassignment can change a vector's owner without moving the branch itself.

1. **Reserve headroom.** Budget for the insertion interval, uneven arrivals, subtree balancing scratch space, publication and transfers. Reserve memory per owner; aggregate free memory cannot protect an owner that receives most of the work.
2. **Insert in parallel.** Workers navigate the stable tree and collect additions by destination leaf. Combine their contributions over the reference's logical input batch, then apply each leaf's additions through its owner. Complete those additions before subtree balancing starts.
3. **Balance subtrees in parallel.** Workers split oversized leaves, merge eligible groups and reassign vectors using the reference's global neighbor discovery and selection rules. Assign independent affected regions concurrently. Queue follow-up subtree balancing instead of recursively executing it. Cross-subtree and cross-owner candidates remain eligible.
4. **Balance shards.** After outstanding mutations finish, move selected ownership units to restore headroom for the next cycle. Refine oversized units into existing descendant subtrees where needed, following the ownership-boundary rules. Preserve logical identities, topology, centroids and memberships. Keep full-vector placement independent; account separately for mutable-state transfer, directory updates and cache warming. The coordinator completes required whole-subtree handoffs before insertion resumes.
5. **Publish after the ingestion job finishes.** The coordinator confirms required mutations and handoffs are complete, asks owners to save their own data, collects compatible durable references and publishes the complete generation manifest last. A required save failure prevents publication. Reuse unchanged saved objects. Existing readers retain their chosen generations independently of the writer's lifetime. Benchmark checkpoints may represent successive completed ingestion jobs; they do not require saving while mutations continue.

Internal splits and new roots do not inherently require moving descendant postings to other machines. Shard balancing is a distinct phase after insertion and subtree balancing; it moves responsibility for existing subtrees without changing the logical tree.

Logical tree growth is independent of the ownership boundary. A leaf split replaces a leaf with sibling leaves under its parent. A parent with too many children becomes eligible for an internal split; splitting the root creates a new root above its replacements and increases tree height. Follow-up splits are deferred jobs. These structural changes require consistent ownership records, but do not by themselves call for moving a subtree to another machine.

### Batching leaf appends

Match the reference's logical batch size and combine contributions across workers before updating each leaf. The agreed baseline schedule is:

| Cumulative additions | Input vectors per insertion/subtree balancing interval |
| --- | --- |
| Through the first 1,000,000 | 100,000 |
| After the first 1,000,000 | 1,000,000 |
| Final incomplete interval | Use the remaining input and then perform subtree balancing. |

These are total input counts across workers. Each leaf receives the subset routed to it; the counts are not fixed posting-list sizes. The [benchmark settings](CURRENT-PLAN.md#baseline-benchmark-settings) specify worker counts and matched comparisons. Publication cadence remains separate.

Prepare and combine a leaf's additions outside its edit lock. Acquire that leaf's lock once to revalidate the destination, append the combined postings and update its metadata. Combining additions at the destination owner can avoid a central copy of every owner's buffers while preserving the same grouping.

Smaller computation windows, storage staging chunks or transport messages do not automatically justify smaller leaf appends. A different grouping boundary needs a concrete memory, correctness or durability constraint, or a measured benefit for the complete operation. Record the reason, memory accounting, lock acquisitions and owner requests. Sharding alone does not justify a 64-vector worker flush limit.

### Contiguous centroids for construction navigation

Each published parent navigation image stores its child centroids in one contiguous array, aligned with the child identifiers and routing metadata. Workers score slices of this array without separately loading each child node. Build the array when publishing a changed parent, rather than packing it on each query. Publish the child metadata and centroids together so insertion, splitting, merging, centroid refresh, restore and ownership handoff cannot expose mismatched versions.

The current packed image retains the existing per-child centroid fields for mutation and wire compatibility, so the packed array is an additional in-memory copy. It removes scattered access from scoring; it does not itself reduce resident memory. Measure that tradeoff rather than claiming a memory saving.

Construction continues to use full-precision centroids. Saved query navigation retains its four-bit centroids and full-precision query vector; posting scans retain one-bit codes. Count comparisons in local variables during scoring and publish the accumulated count at a bounded operation boundary. Shared instrumentation must not perform an atomic update for every centroid comparison.

## Parallel work and synchronization

Independent work should proceed concurrently while conflicting updates coordinate only over the state they affect. A **reserved region** is the portion of the tree protected during a balancing job’s short validation and publication step. Reserving that region does not imply excluding mutations throughout destination discovery or expensive preparation. The protected region is not necessarily a machine-ownership boundary.

### Concurrent owner execution

Each owner supports concurrent execution through its worker pool, including in a one-owner configuration. Dispatch queues and deferred-work queues may feed multiple workers; ownership does not require one sequential operation queue or execution thread.

Local workers should use direct access through the owner's synchronized interfaces where possible. Remote operations obey the same correctness rules. Preserve ordering for conflicting node and posting updates, overlapping structural regions, transfers and required phase boundaries. Bound active workers by available independent work, the configured limit, physical cores and memory headroom.

### Reusable connections and bounded remote execution

Remote calls reuse established connections while preserving exclusive ownership of each request and reply. A client borrows one socket until its complete reply is decoded. It can replace a stale idle socket before sending, but a failure after sending does not authorize automatic replay of a mutation.

Notification-based listeners assemble bounded frames without assigning idle sockets to execution threads. Fixed decoder pools feed execution pools with separate capacity for construction requests, their publication/export dependencies, terminal chunk installation and control requests. Connection admission and queued work remain bounded; overload fails explicitly. Reserved dependency capacity covers the implemented nested request pattern, not every possible recursive call graph. The benchmark configuration uses 336 processing threads per server plus two readiness reactors; these are mostly waiting threads, not a claim of 336 runnable cores.

Complete routing-view installation uses a bounded versioned wire representation that preserves full-width identifiers, ordered children and exact finite centroid bits. Both peers use the same executable. Counts and child dimensions are validated before dependent allocation. The 16 MiB frame limit still applies, and larger complete views require future chunked publication. Checkpoint serialization remains unchanged. The [epoch33 report](two-machine-v1/local-tests/EPOCH33-TRANSPORT-AND-NAVIGATION.md) records implementation limits and validation.

### Operation identities, phases and duplicate requests

Every remote construction operation carries a writer-run identity, construction phase epoch, unique attempt ID, operation kind and expected subtree placement counters where relevant. An **epoch** identifies one admitted construction phase. An attempt ID combines its initiating owner's stable identity with a monotonically increasing sequence within that run; coordinator-initiated attempts use the coordinator's stable authority identity. A genuinely new attempt gets a new ID, while retransmission uses the original ID. Stable wide node IDs use checked allocation, and node revisions are checked 64-bit counters. Exhaustion fails the run before an identity or counter can wrap.

Reject requests from a stale run, a sealed epoch or an incorrect placement before dispatching to mutable state. Keep the attempt's participant set and operation payload immutable: reuse of its ID with a different payload is a protocol error. Commands advancing one attempt have distinct step identities; retransmitting the same step must carry the same body and return its recorded result or current state. This distinguishes a legitimate next-phase command from a conflicting duplicate.

The initiating driver serializes one terminal decision: abort while the operation is still abortable, or begin non-abortable completion. It never issues both decisions. Timeouts do not steal locks or unlock partly applied work. Bounded control and acknowledgement handlers have execution capacity independent of construction workers waiting on remote operations.

For the initial bounded fixture, keep terminal operation records until their phase is sealed. Closing admission rejects new attempts but permits accepted attempts' remaining steps and status requests to drain. Resolve all accepted operations and release acknowledgements, drain their drivers and handlers, then permanently seal the epoch before reclaiming its records. Check this fence before looking up an operation record, so a delayed network packet cannot recreate an operation after reclamation. Apply this envelope and duplicate-request handling to reservations, transfers, publication, handoff and saving as applicable; save commands use their admitted save phase after mutation admission closes.

Bound both active and retained operation memory. If the bound prevents safe admission or completion, stop or fail the run without accepting unresolved work as complete. Do not evict records that can still be replayed, or wait indefinitely for phase completion while withholding the resources needed to finish that phase. Incremental retirement using acknowledged progress watermarks is a [future optimization](CURRENT-PLAN.md#other-deferred-implementation-comparisons). These in-memory records do not provide recovery after process failure.

### One edit lock per node

Each resident mutable node has its own edit lock. Attaching the lock to the node is the default; another representation is acceptable if it provides the same per-node exclusion more efficiently. Unrelated nodes must not share an edit lock because their IDs map to one of 64 buckets. The lock type and lookup container remain implementation choices.

Release a concurrent map's bucket guard before waiting for the node lock or performing an edit. A bucket guard protects the map's internal bookkeeping; holding it throughout an edit would make unrelated nodes in the same bucket contend. A node handle must keep the node and its lock alive while an operation holds or waits for that lock.

Eviction and reloading must preserve authoritative identity, lock and revision continuity for each logical node while overlapping operations or outstanding snapshot expectations can refer to it. Reloading cannot create a second writable copy while an existing handle can still edit the first. Revalidate retired or replaced nodes before mutation, and account for handles and locks in resident memory. This requires locks for resident mutable nodes, not a permanent in-memory lock for every node on disk.

### Per-node revisions for snapshot validation

Each authoritative mutable node has one monotonically advancing revision counter in its writer-side in-memory control metadata, alongside its identity and lock. Each owner validates its own nodes; no central revision directory is required. An operation retains a temporary list of node identities and expected revisions for the state it prepared against.

Copy snapshot fields and their revision together under a brief node read guard, and retain a reference-counted authoritative control record or pinned expectation until the operation releases it. Several coherent per-node snapshots may be taken at different times because all true edit dependencies are revalidated together under full mutation protection. Before applying an edit, follow the [two-level acquisition sequence](#protecting-an-affected-region), validate every relevant identity/revision and the topology and placement assumptions, and retain protection through completion. A mismatch requires safe revalidation, replanning or retry rather than unchecked application. The exact dependency set must include all state on which preparation relies; checking only the source revision is insufficient when other nodes matter.

Increment the revision under protection for each logical edit affecting relevant node state, including postings, centroids, child links, parent links and lifecycle or retirement changes as appropriate. Replaced node identities must not be reused ambiguously. Revisions detect stale assumptions; they do not replace safe publication, region protection or node lifetime guarantees. One counter per node is the agreed initial mechanism. Possible counter splitting is a [deferred optimization](CURRENT-PLAN.md#other-deferred-implementation-comparisons).

Maintain authoritative identity, lock and revision continuity throughout an active writer run while any operation can hold a snapshot or expected revision. Retain the control record with outstanding handles even if its payload is evicted. Reloading must not create an apparently new lock or reset counter that makes a stale expectation appear valid. If control metadata is discarded, no surviving operation or reference may validate against its reset value. Account for retained control records separately from resident payload memory.

These revisions coordinate the mutable writer. They need not be persisted in the immutable reader/checkpoint format solely for validation, and read-only generation readers do not need them. A fresh writer may initialize fresh counters only with writer-run identity and fencing that rejects delayed requests from prior runs; those requests may still arrive. Use the [common request envelope](#operation-identities-phases-and-duplicate-requests), checked 64-bit revisions and checked node allocation. Validate identity, liveness, revision, relevant membership and centroid contents, parent/child relations and placement under protection; increment revisions there. Implement eviction and handoff without breaking control-record continuity. Counters must never wrap or reset into a value that validates an obsolete expectation.

Node revisions, placement counters, immutable query-generation identities and transfer operation IDs have separate purposes. This mechanism does not restore per-vector membership versions, owner membership maps or a vector-identity lock subsystem. The separate two-level protection protocol below is the agreed initial approach; its concrete implementation and correctness still require validation.

### Claiming a source leaf during split preparation

Claim the source leaf before reading the membership used to prepare a split, and retain that claim through replacement installation. A claim marks that leaf as unavailable to competing membership mutations; it does not hold its node write lock or reserve the whole subtree during clustering. Navigation continues reading its coherent routing image, and workers can prepare independent leaves concurrently. This follows the reference writer and prevents transfers or another split from repeatedly invalidating expensive preparation.

Competing transfers, merges, appends and handoffs must respect the claim. They defer safely and release any protection acquired for a rejected attempt; they must not wait for the claim while holding a region needed by its installer. Release the claim on completion, cancellation or error. Reassignment after the split starts after the source claim is released.

Installation still acquires the required structural protection and validates current topology. A parent-only change may require refreshing the source parent and retrying installation without repeating clustering, provided membership, centroid and authoritative leaf identity remain unchanged. A claim never permits applying replacements against unchecked dependencies. Do not hold a conflicting leaf-region reservation while requesting an ancestor region. Keep installation retries bounded by progress and execution limits, and retain a prepared result only while its true preparation dependencies remain protected.

### Protecting an affected region

Each owner has one reservation manager for its current local forest. A **region** is a subtree identified by its stable root ID and expected ancestry and placement. A reservation excludes conflicting mutations anywhere in that region, including edits protected by an ancestor or descendant. The region root's ordinary node lock alone does not provide that exclusion. Normalize each requested local bundle by removing descendant roots already covered by another root in the same bundle.

Choose regions from the actual edit footprint. An ordinary cross-owner posting transfer reserves the source leaf at its owner and the destination leaf at its owner; it does not reserve their global nearest common ancestor. Structural edits include the local parent and ancestor records they change. Edits spanning authorities decompose into local region bundles plus a coordinator publication turn for shared records. Boundary edits also reserve the affected owner region. Holding a shared ancestor's publication turn cannot bypass owner reservations when changing ancestry on which their conflict accounting depends.

The initial acquisition sequence is:

1. **Prepare without long-held node locks or region reservations.** Copy coherent snapshots and revisions, then release their read guards before destination discovery, expensive preparation or waits. A split retains its source-leaf membership claim as described above. Propose the full affected set, including every edit dependency.
2. **Obtain the shared-publication turn when required.** Take this turn before any owner reservation or node edit lock. Ordinary posting transfers do not request it. The turn is serialization permission, not a bookkeeping mutex held throughout the operation.
3. **Reserve complete local bundles in owner order.** Request owners in increasing stable owner-ID order, regardless of source or destination roles. Each owner grants all its requested regions atomically or queues the bundle with none granted. Earlier-owner grants may remain held while waiting for a later owner; there is no global atomic bundle grant.
4. **Lock nodes after every region grant.** Acquire edit locks in lexicographic `(authority ID, node ID)` order, assigning the coordinator a stable authority rank when its records need locks. Hold no node locks while awaiting reservations or the publication turn. Hold no map, scheduler or reservation-manager bookkeeping mutex across a wait or remote call.
5. **Validate and complete under protection.** Recheck all identities, liveness, revisions, membership/centroids, parent-child relations, ancestry and placement before mutation. Follow the operation's completion and acknowledgement contract. Release node locks and owner bundles in reverse acquisition order, with the publication turn last.
6. **Release and replan changed dependencies.** If the affected set changes, release all acquired protection and replan before irreversible mutation. Do not upgrade a bundle in place or discover additional required locks after completion becomes non-abortable.

The reservation manager uses authoritative current parent and lifecycle bookkeeping. Under a short manager mutex, compare requested and granted roots in both ancestor directions and atomically grant or queue the complete local bundle. Do not base admission on cached depths or traversal intervals that structural edits can invalidate. Preload and pin required ancestry metadata outside the mutex, then recheck its coherence inside; if a coherent decision is unavailable, return a replan/reload response. Perform no storage I/O, node-lock wait or remote call while holding that mutex.

Update parent and retirement bookkeeping under the same manager synchronization as structural publication. If a local publication gate is needed, acquire it before the manager mutex, after required node protection; reserve every owner-dependent region before changing shared ancestry. This keeps grant decisions from observing partially updated topology. Snapshot and traversal readers release their brief routing/read guards before requesting reservations or node locks, so they cannot reverse that order.

Use FIFO order among conflicting requests while allowing disjoint bundles to proceed. Resolve grant/cancel races under the same manager mutex. An operation record preserves cancellation, so a delayed request for that attempt cannot gain a grant afterward. All mutation paths, including local posting edits, participate compatibly with structural reservations. A retained handle preserves lifetime but does not itself establish a coherent snapshot.

An unresolved timeout or lost participant stops the run and blocks phase completion and saving. It never authorizes release of partial protections so the same run can continue mutating uncertain state. Validate this concrete protocol's ordering, fairness and cancellation behavior; the design does not claim that the implementation is already deadlock-free or provides failure recovery.

### Navigation during structural changes

Keep immutable per-node routing images separate from protected mutable posting contents. A routing image contains the coherent child links and scoring data used for navigation. During split or merge preparation, readers continue using valid images while workers prepare replacements privately. Insertion uses the stable tree between balancing phases; saved queries use immutable generations.

Each owner has a short routing-publication read/write gate and a routing sequence. Traversal briefly takes the read side to copy or pin coherent images. A publisher takes the write side only to install ready image/link changes and advance the sequence, with the required node and region protection already held. Prefetch outside the gate. Perform no storage I/O, node-lock waits or remote calls under it. The gate serializes brief local structural installation; it does not serialize expensive preparation, ordinary posting edits or complete independent balancing jobs.

A traversal spanning several expansions is not a frozen snapshot of the changing tree. Return explicit stale, replaced or publishing status for an unusable selected route, and follow the [navigation retry contract](#search-budgets). Revalidate all true edit dependencies under protection before a resulting mutation. Changing posting contents follow the [blocking visibility rule](#visibility-while-posting-contents-change), while ordinary transfers keep routing visible.

Pin one immutable shared routing view for each navigation attempt. A centroid-only refresh with identical shared topology, route identities, eligibility and ownership placement does not invalidate that attempt; its scoring continues from the pinned images. Compute compatibility information when publishing the immutable view, rather than scanning the shared tree on every expansion. A changed root, parent/child structure, route type, ownership unit or placement requires a fresh attempt. Owned expansions still validate live target identity, and retired or moved selected routes return explicit stale status. Keep whole-attempt retries finite and include the final retry reason in failures. This separates harmless scoring refreshes from routing changes without holding shared locks across remote calls.

Coordinate simultaneous refresh requests for the same owned boundary before preparing their shared routing updates. Each request waits on that boundary's gate, then takes a fresh snapshot and validates its own owner revision, epoch and routing view. It still obtains the global publication turn and completes the existing broadcast before reporting success. The gate does not reuse another request's acknowledgement or protect unrelated owner mutations. Include gate waiting in the refresh deadline, release bookkeeping locks before waiting, and retain gate entries only while requests need them. This reduces repeated speculative preparation and publication-turn contention; the [epoch31 measurements](two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md) record its latency and CPU tradeoff.

Repeated route invalidation during required reassignment may postpone destination discovery without dropping the work. Preserve the exact pending vector IDs and exclusion, and admit the continuation only behind other structural balancing attempts that are currently active. Exclude the originating attempt and reassignment-only jobs from those dependencies. Bind replay to the original request and producer context, acknowledge accepted work before the original job finishes, and release conflicting claims or reservations before waiting. Pending continuations prevent phase completion and saving. A merge continuation must retire its source only after evacuation actually drains; a queued evacuation is not a completed merge. If no other structural producer remains, preserve the navigation failure rather than retrying indefinitely. This conditional continuation handles pending-ID reassignment; insertion and separate neighbor-selection stages retain their own finite failure rules.

For a multi-record owner update whose old routing images cannot safely remain usable, mark only the affected routing entries as publishing so traversals refresh/retry. Never hold the whole owner's routing gate across network coordination. Retiring records reject mutations; retain their control records and routing images until live handles and accepted requests release them. Durable saved readers retain their own independent data.

Shared structural changes use one compatible [shared routing view](#publishing-a-boundary-split). A worker must not navigate back through its own exclusive lock or hold data locks while issuing a blocking navigation request. The [plan](CURRENT-PLAN.md#future-experiment-compare-leaf-visibility-during-split-preparation) owns alternative source-visibility strategies.

### Deferred subtree balancing

A subtree balancing task completes its own split or merge and queues any further subtree balancing it triggers. Follow-up jobs include an overflowing internal parent, a reassignment or merge destination that needs subtree balancing, and newly created nodes that remain imbalanced. Revalidate queued work before applying it because another task may have changed or removed the node.

Each completed task must leave a valid, readable tree. Temporary overfull or underfull nodes may wait for subtree balancing, but their links and membership changes must be complete. Deferred work cannot repair a dangling child reference or complete a partially published transfer. The implementation must define valid temporary imbalance and preserve progress toward processing the queue.

### Assigning workers

Schedule individual leaf-balancing jobs across each owner's worker pool, rather than statically assigning whole subtrees to workers. All of an owner's subtrees contribute work to that pool. Owning one large subtree must not restrict its balancing to one worker. Required internal-parent maintenance runs as separate deferred jobs; the leaf queue alone does not implement it. Determine which internal structural operations the reference requires rather than assuming an additional cross-owner internal-root merge algorithm.

Use dynamic job assignment: a worker that finishes takes the next eligible job. Start larger postings first as the initial scheduling heuristic, following the measured unsharded candidate; posting size estimates work but does not guarantee its cost. Revalidate a queued node's existence, type, size and ownership before acting. A split, merge or placement change can invalidate its queued description. Skip obsolete work, retain unresolved conflicts for retry, and preserve necessary follow-up jobs.

Among similarly useful ready jobs, prefer work in different branches when that reduces expected conflicts. This preference must not bind whole subtrees to workers, strand runnable work or restrict global neighbor eligibility. Scheduling reduces collisions; reservations, locks and revalidation preserve correctness when neighboring work still overlaps.

Job assignment and mutation protection are separate. Assigning a leaf ID once does not protect its neighboring leaves, parent or remote transfer destination. Protect the actual affected region during validation and publication, with at most one conflicting protected structural update in that region. Do not reserve the entire owned subtree merely because it contains the job, or hold an exclusive source reservation throughout destination discovery and preparation. Preparation may overlap other work using safe snapshots and subsequent validation; shared-parent publication may still conflict. Implement and validate the selected stable-root bundles, operation messages and node-lock ordering; job assignment does not replace those contracts.

Keep available workers supplied with ready, nonconflicting work, subject to resource limits. A slower unrelated job must not prevent dispatch of another eligible job. Bound queue memory and discover work across owned subtrees without materializing every node or posting list merely to schedule jobs.

Queue pages and arbitrary batch or round boundaries must not restrict the runnable work available to workers. A page groups stored queue records; scheduling must admit work across pages and replenish available slots as individual jobs finish. Remove the restriction to one globally claimed page. Preserve bounded memory, conflict exclusion and required phase boundaries.

Recompute eligibility as jobs complete and the tree changes. The scheduler knows currently admitted independent work, not all future jobs or their durations. Measure effective concurrency and distinguish waits for conflicts, dependencies or resources from avoidable dispatch gaps. Validate the scheduler with sparse pages and uneven job durations, not just a configured thread count.

New leaf and internal-node jobs become eligible immediately after their producing change is fully published and conflicting reservations are released. Queue termination accounts for active producers. Conflicts must not starve unrelated work, and a residual-imbalance budget cannot declare incomplete membership or publication complete. See the [scheduling evidence](CURRENT-PLAN.md#scheduling-evidence-and-baseline-constraints) for reference measurements and limits.

### Storage, cache and scheduler synchronization

Shared bookkeeping still needs appropriate coordination. Review each storage, cache and scheduler lock against the reference, reuse its mechanisms where they fit, and remove locks for eliminated components. Per-node locks do not automatically protect these structures.

| Shared state | Required protection and lock scope |
| --- | --- |
| Storage | Preserve pending updates and save compatible roots after the ingestion job's mutations finish. Choose any necessary coordination among save workers; concurrent mutation during saving is not required. |
| Cache | Preserve valid entries, node-handle lifetimes, eviction rules and memory accounting. Keep bookkeeping brief and release its locks before storage reads or waiting on owner operations. |
| Scheduler | Prevent duplicate conflicting work, retain follow-up jobs and enforce region reservations and resource limits. Release queue bookkeeping locks before executing jobs. |

Choose the simplest mechanism that meets each requirement. Any broad lock or lock held across storage work needs a concrete justification; this is not a blanket decision to remove all bookkeeping locks.

### Measuring retries, lock duration and progress

Measure whether short protection reduces waiting without wasting excessive preparation or delaying job completion. Record attempts and completed jobs so retry counts have meaningful rate denominators, and separate the following causes: source contents changed during preparation; topology or ownership changed; discovering a larger lock set required an ordering restart; and lock acquisition conflicted when nonblocking acquisition is used. Record ordered blocking lock waits separately from actual retries.

Record wasted preparation wall time and CPU work, region-reservation wait time, node-lock acquisition wait time, node-lock hold time, and each job’s end-to-end completion time across all attempts, including tail latency. Track repeated retries, starvation and jobs that fail to finish rather than reporting only successful jobs. These measurements determine whether the baseline needs a different conflict policy; a raw retry count alone is insufficient.

### Alternative if progress is poor: wound-wait

The [plan owns this deferred conflict-policy experiment](CURRENT-PLAN.md#future-experiment-wound-wait-if-progress-is-poor). Implement ordered owner bundles and node locks as the initial approach.

## Current membership and reassignment

Each vector has exactly one current posting, owned by the machine responsible for its leaf's subtree. Adding a new ID creates that posting without a per-vector membership version. Duplicate-ID detection is a separate concern whose mechanism remains open.

Reassignment explicitly removes the source posting and adds the destination posting. Coordinate the affected leaves and revalidate that the source still contains the vector before transferring it. Concurrent splits, merges and reassignments must preserve membership without accepting a partial transfer as complete. Same-owner moves use the reference-style grouped local path below. Cross-owner moves use inactive staging, protected completion and the explicit cancellation boundary described separately.

The target removes the machinery used to retain and later discard obsolete memberships:

- Per-vector membership versions and their checks.
- Owner membership-version maps and their read/write locks.
- The service that supplies version checks and the obsolete-version cleanup path.
- Separate vector-identity lock arrays and guards held across directory and owner calls.

Affected-leaf locks, source revalidation and region or cross-machine coordination replace that machinery. They do not require a tree-wide lock for every membership change. Structural and placement counters and immutable query generations retain their separate purposes. Removing membership versions is decided; it does not depend on completion of the unsharded-index experiment.

### Grouped posting transfers within one owner

Move postings between local leaves through direct access to their protected state. Group destinations for a source and compact its posting list once for the accepted groups. A local move does not need remote endpoint messages, serialized transfer payloads or the cross-owner acknowledgement sequence.

Claim the affected leaves without waiting while holding another node lock. Revalidate source membership, live node identity, placement and current destination centroids before moving postings. Prepare codes outside node edit locks while the claims prevent conflicting membership or centroid edits. Reserve required destination capacity before changing membership, then update all accepted destinations and compact the source under ordered protection. Publish coherent routing metadata and release claims. Retain still-current source rows for later routing when a destination is unavailable; do not spin while holding a source claim or discard pending work.

Local transfers still participate in construction admission and draining. A save cannot cross an incomplete move, and split, merge and handoff operations must respect their claims. Navigation continues to read stable routing images. The cross-owner protocol remains responsible for moves between machines, including retries and uncertain acknowledgements. Normal construction executes reassignment at the source owner and therefore uses this grouped path for its local destinations. A controller directly requesting a transfer between two leaves on a different owner still uses the retained protocol; forwarding that special call into one remote bulk operation remains outside this implementation.

### Direct coordination for ordinary cross-owner posting transfers

The initiating worker drives a posting transfer directly with the source and destination owner services. Both leaves keep their owners and shared routing remains unchanged, so the coordinator does not participate in an ordinary transfer. Each owner keeps an operation record keyed by the common attempt ID, with fixed participants and payload identity.

The normal states advance monotonically through `acquiring/prepared → staged → completing → applied → completed → released`. `Aborted` is terminal and reachable only before the driver decides to complete. Each endpoint records the applicable local steps; the driver tracks acknowledgements from both. An inactive staged copy is not current membership.

1. **Prepare and protect.** Snapshot the source, discover the destination and prepare its posting/code privately. Obtain complete owner bundles in owner order, then node locks in authority/node order. Revalidate source membership, destination liveness, centroid assumptions, ancestry and placement. Release and replan a stale proposal before changing membership.
2. **Stage the destination.** Install the inactive destination posting and obtain its `Staged` acknowledgement while the source remains current. The operation may still abort by discarding staging, preserving the source and acknowledging release of acquired protection.
3. **Decide completion on both endpoints.** The driver records its non-abortable completion decision and sends `BeginCompletion` to both owners. Receive both acknowledgements before changing current membership. After this decision, cancellation waits for completion; the driver cannot send an abort decision.
4. **Remove, then activate.** Send `RemoveSource` and await the source's `Applied` acknowledgement. Then send `ActivateDestination` and await the destination's `Applied` acknowledgement. Keep both leaves' changing contents protected throughout. The endpoint on the initiating machine executes these cross-owner steps through direct calls; a move whose two leaves share one owner uses the grouped local path above.
5. **Complete before releasing.** Obtain both owners' `Complete` acknowledgements while protection is still held. Only then send `Release`, await all release acknowledgements and report success. Release node locks and reservations in reverse acquisition order as specified by the protection protocol.

These changes are not physically simultaneous. Other operations cannot read the protected intermediate posting contents, and an incomplete move cannot enter a saved generation. Exactly one current posting is visible before and after completion; this is not arbitrary multi-leaf snapshot isolation. Stable routing remains visible during the transfer.

On a lost acknowledgement, retransmit the same idempotent step or request `Status`. Replies distinguish unknown, prepared/staged, completing/applied, completed/released and aborted states. Unknown never means success. Duplicate commands return their recorded outcome or current state; out-of-order or conflicting commands cannot move the state backward. A step received before its prerequisite is not applied and reports that prerequisite/current state. The [phase fence and record-retention contract](#operation-identities-phases-and-duplicate-requests) prevents late requests from replaying reclaimed attempts.

A batch may contain several vectors for the same validated leaf pair under one immutable batch identity. A partially applied batch cannot abort. Keep completion/status handling runnable when construction workers block, and bound retained payloads and operation records. An unresolved timeout or lost participant stops the run, prevents further phase admission and blocks saving; it does not release partially applied protections so other mutation can proceed. In-memory records do not promise durable recovery.

### Visibility while posting contents change

Once both affected leaves are protected and validated, exclude other operations from reading or mutating their changing posting contents. Those operations wait for the protected update instead of repeatedly retrying merely because its contents are unavailable. Keep stable, coherent routing and centroid information readable so navigation still considers both leaves. An operation that needs protected posting contents waits and revalidates as needed; hiding an in-flight leaf from routing must not silently change candidate eligibility.

Waiting does not provide arbitrary multi-leaf snapshot isolation. Waiters must revalidate current state and must not combine stale pre-transfer reads into a new mutation decision. The implementation must separate access to coherent routing information from access to changing posting contents, preserve record lifetimes and enforce the agreed staging, completion and cancellation boundaries. A lock arrangement that blocks all routing for the entire transfer does not satisfy this visibility requirement.

The [plan](CURRENT-PLAN.md#future-experiment-blocking-versus-retry-or-snapshot-access-during-transfers) owns alternatives to this blocking/direct-owner baseline.

### Merging a leaf across owners

A leaf merge drains an undersized source into existing surviving destination leaves, following the reference’s neighbor and eligibility rules. Some vectors may select additional destinations through reassignment. This is the behavior of the inspected single-owner `merge_leaf` path and the basis for its cross-owner extension. Keep each destination leaf on its existing owner. Group same-owner moves through the local transfer path and use the staged transfer protocol for cross-owner moves. The operation neither requires relocating surrounding owned subtrees nor assumes that it creates one combined leaf.

Keep the source valid while it contains postings and visible to navigation throughout preparation and transfers. Preserve global neighbor eligibility. Each posting transfer follows private preparation, protected revalidation, inactive destination staging and acknowledged completion; changing contents are protected while coherent routing remains readable. Do not hide the source to prevent new arrivals or silently narrow candidate eligibility.

Competing merge proposals must revalidate their assumptions under ordered protection, including proposals that share a source or propose reciprocal transfers. Check current source eligibility and size, source membership, destination validity and the affected structure. Obsolete proposals stop or replan. A changed destination centroid invalidates posting codes prepared against the old centroid; a destination split or removal requires rediscovery and revalidation before transfer.

Destination overflow queues normal reference subtree balancing. The 70% machine-placement target governs whole-subtree placement and must not change merge destination eligibility. Track repeated concurrent arrivals that prevent a source from draining, including retries and unfinished jobs, and ensure those jobs do not monopolize workers. The scheduling mechanism must preserve progress without hiding the source or bypassing membership revalidation.

After the required transfers complete, release their protections before starting structural removal. Determine every routing and ownership record affected by removing the source, including regional or ancestor centroid updates wherever reference semantics require them. The immediate parent is not necessarily the entire affected set. Choose the removal path from that set:

- **All affected routing records are local.** If the source lies inside an owned subtree and every affected routing record is local, use the owner’s local node/region protection and publication rules. Cross-machine posting movement alone does not require the coordinator.
- **Shared routing or ownership records change.** If the source is an ownership root or removal affects shared parent/ancestor routing or directory entries, request a serialized coordinator publication turn before acquiring any removal reservations or edit locks. Never wait for that turn while holding transfer protection, region reservations or node edit locks. After receiving the turn, follow the two-level sequence of owner bundles then node locks, and revalidate. Publish compatible shared routing and directory changes through the coordinator’s existing mechanism. Deleting a source ownership root removes its obsolete directory entry; destination ownership remains unchanged.

Under removal protection, revalidate that the source is still valid and empty, its current parent relationship is correct, and all affected routing and ownership assumptions still hold. Concurrent arrivals can leave it nonempty after the transfers; do not delete it in that case. Release protection and any publication turn, then replan or retain appropriate follow-up work. The gap between transfer completion and removal is intentional and requires this revalidation.

Complete source-link removal and required routing updates consistently before retiring the empty source. Keep retained handles safe and make stale references retry or revalidate. Parent imbalance may become a deferred follow-up job, but dangling links, partial memberships and incomplete publication cannot be deferred as balancing work. Exact messages, validation sets and resource-lifetime mechanisms remain implementation obligations; the leaf-merge sequence is agreed, not yet a claim of distributed implementation.

### Pruning empty ancestors after last-child removal

When a successfully drained source leaf is its parent’s last child, remove the resulting empty ancestor chain up to the first ancestor that retains another live child or branch. This prunes empty structure; it does not merge populated internal ownership roots. The initial sequence is agreed, while its concrete protection, publication and lifetime mechanisms still require implementation and validation.

1. **Identify the proposed chain and surviving anchor.** During preparation, identify the empty source, the ancestors that would become empty and the surviving ancestor from which the chain will be detached. Include every routing, centroid and ownership record that reference semantics require updating, not only the immediate parent. Treat these observations as a proposal to revalidate, not permission to prune.
2. **Obtain publication authority before edit protection.** Release completed transfer protections first. If any affected routing or ownership records are shared, obtain the coordinator’s serialized publication turn before any removal reservations or edit locks. Local-only changes use local publication rules. Never wait for a coordinator turn while holding transfer protection, region reservations or node edit locks.
3. **Protect and revalidate the complete affected set.** Acquire ordered protection for the actual affected nodes, regions and metadata. Revalidate source emptiness, current parent/child relationships and that every ancestor proposed for retirement has no surviving child outside the chain. Concurrent added postings or children, changed ancestry or an expanded affected set require a revised proposal or safe release/retry under the global ordering rules. Do not prune from snapshot assumptions alone.
4. **Publish complete removal and retire safely.** Detach the empty chain from the surviving ancestor and consistently update all required routing, centroid and ownership metadata through the existing local/shared publication mechanism. Remove obsolete directory entries for any ownership roots in the chain; surviving destination ownership stays unchanged. Then retire removed nodes safely. Old references retry or revalidate; retained handles preserve memory lifetime without permitting stale mutation.

The published result must have valid routing and compatible ownership coverage. Do not expose dangling links or defer incomplete removal as balancing work. Destination overflow or a parent that is underfull but still valid may follow the existing queued maintenance rules. A surviving parent with one child remains usable; collapsing it is not required to complete this cleanup.

An ordinary successful leaf merge transfers postings to surviving destination leaves elsewhere, so pruning its empty source branch must not empty the entire index. Validate that invariant rather than treating this sequence as a general empty-index or deletion algorithm. The exact global lock/region order, validation set, publication mechanics and retirement lifetimes remain implementation obligations; this design does not claim they are implemented or proven.

A hypothetical merge of internal ownership roots that reparents whole descendant branches is not an adopted algorithm or an automatic blocker. First establish whether the reference algorithm requires that operation before designing or implementing an additional protocol. Required parent maintenance still follows the valid deferred-work rules; this does not establish that internal merging is either absent or required.

## Ownership and shared publication

Ownership changes placement; subtree balancing changes the logical tree. Use the following protocols without conflating an ordinary posting transfer, shared structural publication and whole-subtree handoff.

### Choosing and changing ownership boundaries

Choose existing tree nodes as the roots of owned subtrees. Each leaf belongs beneath exactly one ownership root; owned subtrees do not overlap. The ancestors above those roots form the shared routing tree. Ownership roots may sit at different depths, and each machine may own several subtrees. Choosing these roots and assigning them to machines are separate decisions.

Choose the coarsest ownership boundary that distributes memory and work adequately. With one shard, its engine owns the entire tree, including the global root. Navigation starts directly at that root; it needs no separate shared-tree service or ownership lookup at each step. A global-root reference still exists, but it does not require a separate routing layer.

Keep ownership boundaries as coarse as practical. First attempt overload relief by moving existing whole ownership units under the agreed vector-budget thresholds. Refine only when whole-unit moves cannot produce a feasible relief placement and an indivisible unit blocks placement. The initial practical heuristic below defines how to refine that unit; size alone, a desired uniform depth or worker count does not justify refinement.

For example, suppose machine A owns subtree P, whose children are X and Y. Refinement makes X and Y separate ownership units, initially both on A, while P becomes shared routing under the coordinator. A subsequent ownership transfer can move Y to machine B. This changes ownership responsibility without changing the logical tree topology, centroids or posting memberships. Full-vector storage remains independent of placement.

| Situation | Agreed boundary behavior |
| --- | --- |
| One machine owns the index. | The whole tree, including the global root, is one owned subtree. Access it directly through the local engine. |
| An owned subtree grows within its owner's budget. | Keep its ownership boundary. A split beneath a shared parent gives both replacement roots the existing owner. A split of the sole owner's global root keeps the whole tree under that owner, rooted at the new global root. |
| An overloaded machine owns several subtrees. | Move suitable whole subtrees to other machines. Their ownership roots need not change. |
| An indivisible unit blocks feasible overload relief using whole units. | Expose its existing children as ownership units, initially retaining their owner. Publish the intervening parent as shared routing under the coordinator, then transfer suitable children. Refine only children that still block placement. |
| Shards are added. | Use the new receiving capacity for suitable existing ownership units. Adding machines alone does not trigger refinement or increase the logical tree’s height. |
| Shards are removed. | Transfer their units to remaining machines with sufficient capacity. A machine can retain multiple units. When only one shard remains, coarsen the ownership boundary to the global root without changing the logical tree. |

The initial placement policy uses subtree vector counts against configured per-machine vector budgets. These counts provide a simple capacity proxy; they do not prove that a placement fits actual memory, storage or compute limits. Preserve required resource-safety checks and construction headroom. Hardware-aware placement using those resources is a future optimization, rather than a prerequisite for the first heuristic.

Ownership-unit count is independent of worker count. Workers can reserve independent regions within one owned subtree, so exposing local parallelism does not require creating more ownership units or enlarging the shared tree. Coarse units reduce shared metadata and boundary crossings; finer units improve placement flexibility. Refine only where that flexibility is needed.

Capacity-driven boundary changes and transfers occur during shard balancing, after insertion and subtree balancing finish. Logical tree changes can still replace an ownership root during subtree balancing. Update the ownership records to match those structural changes while preserving coverage and one active owner; this metadata maintenance does not trigger capacity-driven transfers.

### Initial shard-placement policy

Pack owned subtrees onto as few machines as practical. Preserve existing placements where suitable and prefer whole-subtree moves over unnecessary refinement. This is a heuristic, not a claim of optimal packing. Placement runs in the scheduler’s shard-balancing phase, so each move needs no separate phase test. Correctness, ownership and capacity checks remain mandatory; the first heuristic does not add separate estimated imbalance-improvement or transfer/cache-warming-benefit gates.

A source becomes overloaded when its vector count exceeds 80% of its configured vector budget, leaving a nominal 20% headroom target at that trigger. Choose the fullest eligible destination whose vector count will remain at or below 70% of its budget after the proposed move and all pending incoming moves. Reserve that incoming capacity so concurrent placement decisions cannot spend it twice. The gap between the 70% incoming-placement target and the 80% overload trigger provides hysteresis: a newly placed subtree has room to grow before its owner becomes an overloaded source.

The 70% receiving/relief target and 80% overload trigger are configurable initial hypotheses, not validated thresholds. Vector counts do not guarantee RAM headroom. Keep the baseline while collecting evidence for the [planned tuning experiment](CURRENT-PLAN.md#future-experiment-tune-vector-budget-thresholds).

For a source triggered above 80%, choose its largest existing whole ownership subtree that fits an eligible destination. For that subtree, choose the fullest eligible destination whose projected total, including the proposed move and pending incoming moves, remains at or below 70%. Repeat until the source’s projected vector count is at or below 70% of its budget. Account for planned outgoing moves and exclude already selected units so the same subtree cannot be selected or transferred twice. A whole-unit move may take the source below the target; do not refine a fitting unit just to hit 70% exactly. If no whole unit fits and further relief is required, follow the selective refinement and capacity-shortage rules below.

These thresholds govern ownership placement, not nearest-leaf insertion. Insertion follows clustering decisions and can grow a machine beyond 70%; it does not redirect vectors to a worse leaf merely to meet the incoming-placement target. The initial placement policy is concrete enough to implement. Deterministic tie-breaking and scheduling among multiple overloaded sources remain implementation details. The coordinator-orchestrated whole-subtree handoff sequence below is agreed; its concrete message and resource-lifetime mechanisms still require implementation and validation.

Stop ingestion and report insufficient capacity if the agreed placement and selective-refinement policy cannot establish the required capacity and headroom. Stop before starting the next ingestion batch. Report whether measured aggregate receiving headroom is insufficient or the heuristic could not find a feasible placement; a failed greedy search does not prove that no placement exists. Preserve current valid state and completed transfers. Unresolved handoffs still follow the existing stop/fail rules and cannot be saved or reported as successful. Automatic provisioning to obtain more capacity and retry remains future work.

Placement alternatives and hardware-aware policies are [future experiments](CURRENT-PLAN.md#future-experiment-pack-subtrees-versus-spread-them-across-machines), not requirements for this baseline.

### Refining ownership units only when placement requires it

The initial practical heuristic selects the largest fitting existing whole ownership unit before refining a boundary. A source triggered above 80% is relieved to a projected count at or below 70%, while each destination remains at or below 70% after proposed and pending incoming moves. Only when no whole unit fits and further relief is required should the heuristic consider refinement. Preserve suitable existing placements and identify the particular unit whose indivisible size blocks overload relief; do not refine a unit merely because it is large.

Check aggregate eligible receiving headroom against the remaining relief required to bring the source to at most 70% of its vector budget, accounting for already planned outgoing moves. Count only receiving capacity available under the destination limits and pending reservations. Insufficient aggregate headroom cannot be repaired merely by refining units; it requires revisiting capacity or threshold policy, or adding capacity. Sufficient aggregate headroom is necessary but not sufficient because whole units are indivisible and must fit individual eligible destinations.

Expose a blocking unit’s existing child subtrees as separate ownership units. Initially every child inherits the same owner, while the intervening parent becomes shared routing under the coordinator’s authority. Apply the coordinator’s boundary-publication protocol to publish compatible routing and ownership, then move suitable children through the separate ownership-transfer protocol. Recursively refine only children that still block feasible placement. Do not expand all branches to a uniform depth or create ownership units to match the worker count.

Ownership refinement stops at a leaf because a leaf has no smaller existing child subtrees. An actual leaf split changes the logical tree and belongs to subtree balancing under its separate protocol; ownership refinement must not silently perform it. Refinement itself changes only ownership responsibility and shared-routing responsibility, preserving logical topology, centroids and membership.

Largest-fitting-unit selection and fullest-eligible-destination selection define the initial greedy heuristic. Efficient search, deterministic tie-breaks and scheduling among multiple sources remain implementation details. An exhaustive packing solver is not required, and a failed greedy attempt does not prove that no feasible whole-unit placement exists. Record whether a decision reflects insufficient aggregate headroom, an identified indivisible unit, a leaf boundary or the search heuristic’s limits rather than presenting every failed attempt as a capacity shortage.

### Handing a whole subtree to another owner

Move a subtree during the drained shard-balancing phase. One coordinator drives a handoff attempt identified by its attempt ID, old/new owners and old/new checked placement counters. Its states are `reserved → destination staged-ready → source fenced → ownership published → destination active → source cleanup acknowledged → complete`. Use the [common operation envelope](#operation-identities-phases-and-duplicate-requests) and recorded idempotent step outcomes throughout.

The directory has one logical assignment and at most one owner has mutation authority. A temporary interval with neither owner serving mutations is allowed during this drained phase. Inactive transfer copies do not add current membership. The move preserves topology, node IDs, centroids and postings; full-vector data remains independently accessible.

1. **Reserve real receiving capacity.** Include pending incoming moves in the destination's vector budget and actual memory/storage requirements. Identify the outgoing unit so it cannot be selected twice. Apply capacity transitions exactly once under the handoff ID.
2. **Stage complete current state.** Transfer current routing and postings, dirty overlays, removals and necessary writer control state, or reconstruct that control state safely after the drain. Saved references may be reused only when they include all current changes. The destination verifies the manifest, counts, integrity and resource reservation before returning `Ready`; it remains unable to mutate staged data.
3. **Fence the source.** Close admission for this subtree and drain accepted requests before returning `Fenced`. A ready destination is not enough to publish the handoff. Retained source handles preserve lifetime but cannot restore mutation authority after fencing.
4. **Publish ownership, then activate.** After the fencing acknowledgement, atomically publish the new owner and incremented placement counter in the compatible shared routing view. Use the same short coordinator arbitration of publish, abort and status as [structural publication](#publishing-a-boundary-split), with the handoff attempt identity/sequence bound to the visible view and recorded outcome. The destination verifies that published assignment before activating and returning `Active`. Requests carry run and placement identities; reject stale placement and pre-activation mutation requests. Authorized handoff control steps validate against the recorded attempt and its phase-specific old/new counters, so source fencing does not prevent status resolution or required cleanup.
5. **Acknowledge safe source cleanup.** Keep the source fenced. After destination activation, retire source state once outstanding handles permit it, acknowledge cleanup, and only then complete the handoff. Convert destination capacity from pending to assigned and release source capacity exactly once; account for temporarily retained state separately. Never discard the sole usable current state before activation succeeds.

Before ownership publication, abort may discard staging and restore source admission only after proving that publication did not occur. After publication, finish forward or stop the run; do not roll ownership back on a timeout. Resolve a lost response from recorded operation/publication status. Unknown status is not evidence that a handoff failed before publication. An unresolved outcome prevents ingestion, phase completion and saving. Recovery after machine failure remains outside the prototype scope.

### Splitting an owned subtree root

Both replacement roots inherit the existing owner when an owned subtree root beneath a shared parent splits. If P belongs to machine A and splits into P1 and P2, the result is two owned subtrees on A. The split itself transfers no responsibility or vector data between machines. Later shard balancing may move either subtree independently.

Splits entirely inside an owned subtree use the owner’s local visibility and synchronization protocol and require no directory update. An owned-root split beneath a shared parent changes both the shared parent and the ownership directory. Shared-upper-tree structural changes also require coordinated publication. Capacity-driven ownership handoff is a separate phase and protocol; the split does not trigger a placement transfer.

The initial distributed protocol uses one serialized publication queue at the coordinator for boundary and shared-routing updates. Local preparation and ordinary independent local edits remain parallel. This queue serializes publication of shared state, not every local split or all construction work.

### Publishing a boundary split

A shared structural change publishes one immutable view that binds shared-tree/root references and the subtree-directory root with a monotonically advancing sequence (`SharedRoutingView`). Use structural sharing so publication does not copy the complete upper tree. This mutable construction-view sequence is separate from an immutable saved query generation.

1. **Prepare privately.** Capture source snapshots and revisions, release their short read guards, and prepare replacements outside mutation protection. Describe all affected shared and owner records.
2. **Obtain the coordinator turn, then owner protection.** Take the shared-publication turn before owner bundles and node locks, following the global acquisition order. Revalidate shared state, owner dependencies, source membership, ancestry and placement. On a changed affected set, release and replan before publication.
3. **Make replacements ready.** Owners install and pin replacement records, retain safe old routing images and acknowledge `Ready`. New references must address records that are ready for the proposed view. If old multi-record images cannot remain usable, only affected entries return publishing/retry status. Release each short owner routing gate before any network wait; operation reservations and edit protection remain held as required.
4. **Publish one compatible view.** After all readiness and validation conditions pass, the coordinator atomically swaps one `SharedRoutingView` pointer. For a split of owned root P into P1 and P2, this changes both the shared parent references and `P → owner` into `P1 → owner, P2 → owner`. The pointer swap is the shared publication point. The visible view carries the publication attempt identity and sequence. Serialize `Publish`, pre-publication `Abort` and publication `Status` against the same coordinator operation state in one short control critical section: make the pointer swap and recorded `Published` outcome indivisible to these handlers. Perform no remote calls, storage I/O or other blocking waits inside that section. A racing status/abort cannot report `NotPublished` after the visible swap, and a delayed publish cannot revive an aborted attempt. Return `Published` with the recorded sequence; duplicate `Publish` or `Status` requests return the same outcome.
5. **Acknowledge and retire safely.** Finish acknowledgements and retirement under the operation's protection before releasing it and the turn. Mark old mutable records retired so they reject edits; retain control records and safe images until handles and accepted requests release them. Old routes either remain safely readable or return explicit stale/replaced status, never an unexplained missing child or permission to mutate stale state. Saved readers use their separate durable generation.

Before the pointer swap, cancellation can discard unpublished state and restore the prior usable view. After the swap, finish acknowledgements and retirement; a timeout triggers status resolution, not undo. The driver serializes publication and cancellation decisions under the common attempt contract, and no operation may be reported or saved as complete while its outcome is uncertain.

The same compatible-view publication governs shared structural edits and placement changes. Owner-local structural edits use the [short local routing gate](#navigation-during-structural-changes). A turn is serialization permission, not a bookkeeping mutex held across network calls. Control handlers remain runnable independently of blocked workers. Measure publication queue wait, protected duration, network coordination, retries and throughput separately without adding overlapping spans as elapsed time. These contracts require implementation and correctness tests; they do not establish failure recovery.

### Follow-up shared and single-shard root changes

If the shared parent becomes overfull, queue its split as a separate subtree-balancing job. Splits of shared routing nodes use the coordinator’s serialized publication queue and coordinated routing-view publication, preserving ownership entries for unaffected descendant subtrees. Splitting a shared global root creates a new shared root above its replacements; this adds a tree level without changing descendant ownership.

When one shard owns the entire tree, a global-root split keeps the entire tree as one ownership unit. Create the new global root above P1 and P2 on the same owner and update the sole ownership root to that new root. P1 and P2 remain ordinary children inside the owned tree. This structural growth requires neither separate ownership entries for the children nor a shared routing service. Publish the root replacement consistently for construction readers and preserve older saved generations.

## Search and saved state

Index queries read one immutable generation through the read-only reader workload. The separate writer consumes a backlog and publishes its successor after ingestion finishes, as defined in the [reader and writer lifecycle](#reader-and-writer-lifecycle-and-scope). Saving must bind compatible routing, ownership, posting and full-vector roots. A query holds its saved descriptor for its lifetime, independently of writer jobs.

The following steps describe searching the saved index. Applying newer backlog-log writes to client results belongs to the surrounding serving system and is outside this project's query implementation and benchmark scope.

1. **Choose a saved view.** Hold one generation descriptor and its required data references.
2. **Navigate the tree.** Score branches through the shared upper tree and selected owned subtrees. Use the subtree ownership information when traversal crosses an ownership boundary.
3. **Scan selected postings.** Use their compressed codes to rank approximate candidates. The coordinator applies the collection-wide search budget and merges results.
4. **Rerank candidates when requested.** Fetch full vectors and calculate exact distances for the candidates selected by the reranking policy.

Missing or incompatible selected work must not appear as a successful complete query. Saved generations must remain coherent after further construction and after live writers retire.

### Saving and reopening a completed generation

Save one complete ingestion result after mutation stops. The coordinator assigns a save-attempt ID and generation ID, closes mutation admission, drains ordinary operations, shared publications and handoffs, then pins the final compatible routing, placement and full-vector view. Unresolved operations prevent saving. Owners write their payloads directly; the coordinator need not relay them.

Each owner handles `SaveGeneration` idempotently for that frozen attempt. Write immutable node and posting data, including dirty additions/removals and persisted 4-bit centroid codes, then write an immutable owner manifest containing owned-root coverage, durable data references, counts, format and integrity information. Ensure independently located full-vector data is durable too. Duplicate commands for the same frozen attempt return the same accepted manifest/result; a conflicting body under its ID is rejected.

Each owner materializes independent posting blocks with a bounded pool of at most 32 workers. Each task serializes, writes and verifies one immutable block. The owner joins every task before publishing its manifest, preserves deterministic reference ordering and error selection, and leaves its previous accepted cache intact on failure. A later save reuses verified references for unchanged blocks; changed blocks follow the same write-and-verify path. This is part of the accepted saving implementation, with measured validation recorded in the [epoch31 report](two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md).

Before publishing, the coordinator verifies that expected owned roots are covered exactly once, all owners have acknowledged durable results, and dimensions, metric, precision, format and full-vector references agree. Counts and checksums check coverage/integrity; they do not by themselves prove exact-once vector membership. That invariant requires the membership checks described in the plan. A versioned immutable generation manifest binds shared routing, directory coverage, owner manifests and the full-vector view. Write this manifest last and expose its descriptor only after the write is confirmed.

Reader manifests contain no transient node revisions, reservation tokens or unfinished transfer state used solely for writer coordination. Saved placement describes durable data coverage, not addresses for the original live writer's RPC service. A fresh reader verifies the supported format and coverage and reopens lazily from durable references alone, without writer processes, caches, identities or memory.

Reuse the backend's descriptor/root publication mechanism where it supplies the required guarantee. If a mutable latest-generation pointer is used, publish it with an atomic conditional replacement against the expected prior generation, never independent per-owner latest pointers. A lost publication response is resolved by reading the generation/publication result under the same save identity, not by issuing a conflicting new generation. Failure before successful publication preserves the previous descriptor; unreferenced immutable objects may remain for future garbage collection.

**Storage adapter requirement.** The inspected [writer flusher](/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/writer/persistence.rs:98) flushes five blockfile stores and returns their IDs; it does not implement the combined distributed manifest or latest-pointer publication. The storage API exposes [conditional writes](/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/storage/src/lib.rs:533) through `PutMode::IfMatch(ETag)` and `IfNotExist`. The [S3 implementation](/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/storage/src/s3.rs:803) maps these to conditional object writes and reports precondition failures. The [local implementation](/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/storage/src/local.rs:76) ignores these options and writes the path directly. Therefore the API alone is not evidence of atomic publication on every backend. Implement and test the manifest/descriptor adapter with the selected backend's actual guarantees; a local fixture needs an explicitly tested publication adapter if it exercises a mutable latest pointer. The current one-owner reopen benchmark does not establish this contract.

Saving during ongoing mutation, client backlog-log merging and recovery after machine failure remain outside scope. The two-owner implementation validates this contract with fresh durable-only readers, as recorded in the [saving results](two-machine-v1/local-tests/EPOCH29-3M-RESULT.md) and the current epoch31 report.

### Quantization and vector precision

Compressed codes reduce data read and arithmetic during query execution. Code widths below mean bits per vector dimension; formats also contain metadata.

| Operation | Required representation |
| --- | --- |
| Construction navigation and subtree balancing | Use full-precision vectors and centroids. |
| Query navigation | Compare the full-precision query with stored 4-bit centroid codes. |
| Posting-list scanning | Keep 1-bit posting codes and the existing 1-bit scorer. |
| Exact reranking | Use full-precision query and candidate vectors. |

Persist ready-to-score 4-bit centroid codes during commit and load them directly when opening a query reader, following [PR #7910, Persist reader centroid codes](https://github.com/chroma-core/chroma/pull/7910). Reader open must not quantize full-precision centroids again. This applies throughout the shared upper tree and owned subtrees.

Remove experimental 4-bit posting-code generation, posting overlays and 4-bit posting-scoring paths from active sharded code and benchmarks. Preserve recorded results as evidence. The existing 1-bit scorer's temporary 4-bit query representation remains supported: it is part of scoring 1-bit postings, not a 4-bit posting format.

### Internal-node navigation data

Keep each parent's child-scoring data together in its query-navigation entry. Store child IDs, 4-bit child centroid codes and distance-correction values in contiguous buffers with matching order. These correction values are metadata used by the compressed-distance scorer. Following [PR #7903, Compact reader child centroids](https://github.com/chroma-core/chroma/pull/7903), scoring the children should require no separate lookup for each child's centroid. Complete child nodes and posting lists remain separate.

Build the buffers from one consistent child list for the saved generation. Preserve child order, scoring and branch selection; reject missing children, duplicate child references and mismatched buffers. Loading or releasing postings must leave navigation data valid. At an ownership boundary, score the child using its parent's entry, then use the ownership information to continue traversal.

Prepare these buffers from persisted centroid codes and account for copied IDs, codes and correction values in reader memory. The contiguous layout defines an in-memory representation; it does not itself require a new checkpoint format or change construction visibility.

### Search budgets

A **beam** limits which branches remain after a navigation step. One navigation driver owns a global frontier and accumulated leaf candidates across all touched shards. Preserve the actual reference comparator, candidate selection, child ordering/tie behavior, mixed-depth leaf accumulation and distance-based beam policy when enabled. Construction uses full precision; saved-reader navigation uses the agreed compressed centroids.

For each reference expansion step, group the selected internal nodes by authority and issue parallel `Expand` requests. Each carries a navigation-attempt ID, step/request ID, the query or construction vector, ordered node IDs, and either a pinned saved generation or the construction run and phase epoch, shared-view sequence and relevant placement counters. An owner expands exactly those selected nodes by one step and returns all scored immediate children needed for reference selection. It does not independently traverse deeper or truncate to a shard-local beam.

Replies include node IDs and types, parent/child ordinals needed to preserve ordering and ties, scored distances, observed revisions/view/placement and explicit success or stale/replaced/publishing status. They contain routing data, not posting payloads. Deduplicate by navigation attempt, step and request. The driver applies global selection once, only after every required expansion succeeds. Bounded reply chunks must collectively represent the same pinned expansion result; a partial reply, failed owner or timeout is never an empty successful expansion.

Bound remote expansion messages without dropping candidates. Group queries into bounded requests and preserve complete child coverage for every requested parent. If a read-only expansion receives an explicit size rejection, subdivide its query group and, when necessary, its parent list. Gather and validate every part before global selection. A single complete parent that cannot fit fails clearly. All subcalls share the original absolute deadline; subdivision does not renew the budget. Unexpected connection closure, arbitrary serialization failure and mutation errors do not authorize automatic retry. In particular, a response-size failure after a mutation does not imply that the mutation was unapplied.

Use a lossless, explicitly versioned wire representation for query coordinates and scored children. Preserve full logical IDs, ownership and placement identity, revisions, child ordinals, row order and exact finite float bits. Check encoded size, decoded size and count-derived lengths before allocating decoded rows. Whole-frame limits remain independent. This representation does not change construction precision, centroid quantization, global beam semantics or required remote validation.

Construction expansions use coherent owner-local routing snapshots, not a global frozen mutable tree. On an invalidated route, discard the affected navigation attempt's whole frontier and accumulated leaves, refresh routing/placement and restart with a new attempt ID. Ignore late replies from older attempts. Bound and measure these retries and stop/fail on the progress budget rather than claiming a partial result. Any mutation based on navigation still revalidates its complete edit dependencies under protection.

A saved query pins one immutable generation throughout. Incompatible replies fail the attempt rather than silently switching generations. With one shard, call the same expansion and selection behavior directly without an RPC loop. Verify distributed selection against the actual reference implementation on fixed trees, including ties and leaves at different depths; simply retaining an equal number of candidates is insufficient.

Use this shared global beam and reference-compatible reranking as the baseline. Ordinary bounded caches remain permitted. [Independent beams](CURRENT-PLAN.md#future-experiment-independent-shard-beams), [routing caches](CURRENT-PLAN.md#future-experiment-cache-saved-reader-routing-at-the-coordinator) and [reranking alternatives](CURRENT-PLAN.md#future-extension-choose-where-to-shortlist-and-rerank) remain future comparisons.

### Validation for immutable reader navigation

Validate saved routing structure when loading it, then avoid repeating structural checks that the validated immutable representation already guarantees. This is the target reader design; removing repeated checks remains an implementation and measurement task, not an established reader speedup. The current saved reader constructs local expansion replies and passes them through the shared navigation reply validator.

Before exposing a routing entry to queries, verify its generation and object integrity, required child references, unique child positions and references, complete ordered child list, and compatible dimensions and scoring buffers. Resolve or validate referenced metadata before relying on it. Bind the validated representation to the pinned generation and keep it immutable while queries use it. For lazily loaded or evicted entries, perform the required validation when admitting or reloading each entry; this does not require loading the entire index at reader startup. A declared child count alone does not prove that the saved structure is complete.

Local reader navigation should iterate the validated child list directly and preserve complete expansion, canonical ordering and reference tie behavior. It should not manufacture protocol replies merely to recheck owner, attempt, step or mutable-parent revision fields that local execution and the pinned immutable representation already guarantee. This simplification applies to local saved-reader execution; construction navigation retains its concurrent-mutation validation.

Keep checks that depend on each query or newly calculated result. Validate query dimensions and finite input values, and reject nonfinite computed distances; finite inputs alone do not guarantee finite arithmetic results. Preserve candidate completeness and ordering when combining expansions. These requirements remain even when per-parent coverage bookkeeping is unnecessary for direct iteration over validated local data.

Remote reader expansions still require response matching, pinned-generation compatibility, ownership or destination checks where applicable, duplicate/conflict handling and complete response assembly before global selection. An immutable index does not prevent missing, delayed, misrouted or truncated replies. Remove a repeated check only when a documented representation or execution invariant supplies its guarantee. Verify equivalent results, including ties, malformed saved data and partial remote replies, and measure reader validation CPU and query latency separately from construction before claiming a benefit.

### Navigation with one shard

One worker traverses the entire tree through the local engine, following the unsharded navigation algorithm. The engine supplies node records from resident state or its cache and reads storage when required. There are no inter-shard requests, per-step ownership lookups or round trips through a sequential owner executor. Multiple workers can traverse concurrently under the agreed [construction visibility rules](#navigation-during-structural-changes), whose concrete implementation still requires validation.

For a fixed beam of two, one navigation proceeds as follows:

1. **Start at the global root.** Use the current construction view for insertion or subtree balancing, or the pinned saved generation for a query. If the root is already a leaf, return that leaf.
2. **Score the root's children.** Compare the input vector with their centroids and retain the best two candidates overall.
3. **Collect leaves and expand internal nodes.** Add selected leaves to the result list. For selected internal nodes, score all their children and combine those candidates into one list before retaining the best two. The beam applies across the combined candidates, not separately to each parent.
4. **Repeat until no selected internal nodes remain.** Each step collects selected leaves and continues with selected internal nodes. With leaves at different depths, the accumulated leaf results can exceed the per-step beam.
5. **Return leaves ordered by centroid distance.** Navigation identifies candidate leaves. Posting scans and posting updates happen afterward.

Two is an illustrative fixed beam, not a required setting. Use the reference's configured candidate-selection rules, including its distance-based beam policy when enabled.

| Purpose | Navigation and subsequent work |
| --- | --- |
| Insert new vectors. | Navigate with full-precision vectors and centroids. Choose the destination leaf, combine additions across workers over the logical batch, then append under that leaf's edit lock. |
| Find neighbors during subtree balancing. | Navigate with a full-precision vector or centroid against full-precision centroids. Apply the operation's neighbor eligibility and revalidation rules before merging or reassigning. |
| Answer a query. | Compare the full-precision query with persisted 4-bit centroid codes. Scan the selected posting lists with the 1-bit scorer, combine candidates and optionally rerank using full vectors. |

Query navigation scores children using the IDs and compressed centroid data stored together in the parent's navigation entry. It does not load full child records merely to obtain their centroids, and it does not load posting lists while choosing branches. The one-shard path uses direct local access through the intended owner/subtree engine.

### Future reader fleet and shared cache budgets

Ordinary bounded reader caches remain permitted, bound to the selected immutable generation. Reader-cache placement does not grant construction ownership. The [future fleet obligations](CURRENT-PLAN.md#future-read-side-fleet-obligations) and [caching experiments](CURRENT-PLAN.md#future-experiment-popularity-guided-reader-caching-across-indexes) are excluded from this milestone; single-index routing-memory arithmetic does not establish shared-fleet capacity.

<a id="open-decisions-and-alternatives"></a>
## Implementation details to validate

The architecture and initial protocols above are chosen. The remaining work is to implement the selected contracts and validate their concrete representations, not reopen accepted baselines. The [two-machine milestone](CURRENT-PLAN.md#approved-next-milestone-two-machine-correctness) owns work sequencing and test coverage; alternatives stay in the plan.

### Remaining mechanisms

| Implementation obligation | Primary requirement |
| --- | --- |
| Implement stable-root bundles, live parent/lifecycle bookkeeping, ordered admission, conflicting-request FIFO and cancellation records. Test shared-metadata interactions. | [Two-level protection](#protecting-an-affected-region). |
| Enumerate every operation’s snapshot dependencies; implement checked 64-bit revisions, run fencing and pinned control-record continuity through eviction and handoff. | [Writer-only node revisions](#per-node-revisions-for-snapshot-validation). |
| Implement the selected attempt/step state machine, completion-before-release acknowledgements and phase-sealed record retention with bounded memory. | [Direct posting transfers](#direct-coordination-for-ordinary-cross-owner-posting-transfers). |
| Implement the local routing gate, compatible shared-view pointer swap, complete dependency validation and safe retirement. | [Boundary splits](#publishing-a-boundary-split), [leaf merges](#merging-a-leaf-across-owners) and [empty-chain pruning](#pruning-empty-ancestors-after-last-child-removal). |
| Implement checked placement publication, source fencing/drain, verified destination activation and acknowledged source cleanup, including dirty state. | [Boundary publication](#publishing-a-boundary-split) and [whole-subtree handoff](#handing-a-whole-subtree-to-another-owner). |
| Define deterministic placement ties, source scheduling and efficient searches without changing the selected heuristic. | [Placement](#initial-shard-placement-policy) and [refinement](#refining-ownership-units-only-when-placement-requires-it). |
| Specify efficient posting removal and duplicate-new-ID detection independently of membership versions. | [Membership](#current-membership-and-reassignment) and [posting storage](#posting-list-loading). |
| Implement one-step Expand replies and whole-attempt stale retries; implement owner/generation manifests and test the selected backend publication adapter. | [Global search budgets](#search-budgets) and [save/reopen](#saving-and-reopening-a-completed-generation). |
| Bound active work and queue memory, define valid residual imbalance and stopping budgets, and choose brief storage/cache bookkeeping synchronization. | [Scheduling](#assigning-workers) and [bookkeeping](#storage-cache-and-scheduler-synchronization). |

A merge of populated internal ownership roots is not a required new algorithm. Establish its need from reference behavior before expanding scope. Safe cleanup of empty branches and valid deferred parent maintenance are already required.

## Acceptance requirements

These are checks against the primary protocols, not alternate protocol definitions. Passing small local fixtures does not establish physical distribution or 100B feasibility; the plan defines the required actual two-machine coverage.

| Area | Required evidence and primary specification |
| --- | --- |
| Membership | [One visible current posting](#current-membership-and-reassignment), reachable vectors and nonoverlapping ownership; no per-vector membership-version service/maps or identity-lock subsystem. Inactive copies and saved readers do not create current membership. |
| Protection and progress | [Bundle/node ordering](#protecting-an-affected-region) excludes ancestor/descendant conflicts while independent regions progress. Test reciprocal operations, multi-owner bundles, late dependency expansion and shared-publication interaction without blocking acknowledgement processing. |
| Snapshot validation and lifetime | [Node revisions](#per-node-revisions-for-snapshot-validation) detect every relevant changed dependency; eviction/reload, retirement, identity reuse, overflow and prior-run requests cannot validate stale state. |
| Ordinary transfers | [Staging/completion](#direct-coordination-for-ordinary-cross-owner-posting-transfers) and [visibility](#visibility-while-posting-contents-change) survive duplicate/late messages and cancellation without exposing partial membership or hiding routing candidates. Uncertain outcomes stop successful completion/saving. |
| Structural changes | [Splits](#publishing-a-boundary-split), [merges](#merging-a-leaf-across-owners) and [pruning](#pruning-empty-ancestors-after-last-child-removal) preserve compatible routing/directory state, including concurrent arrivals and changed ancestry. Stale references cannot mutate retired records; only valid residual imbalance is deferred. |
| Placement and handoff | [Placement/refinement](#initial-shard-placement-policy) preserves logical topology and eligibility, accounts for pending moves and stops ingestion when headroom cannot be established. [Handoff](#handing-a-whole-subtree-to-another-owner) transfers current dirty state with at most one active mutation authority and no premature source cleanup. |
| Construction | [Full-batch leaf appends](#batching-leaf-appends) and [dynamic job assignment](#assigning-workers) preserve reference-compatible work and do not strand eligible jobs behind page/round barriers. Record effective concurrency and required [progress metrics](#measuring-retries-lock-duration-and-progress). |
| Navigation and representation | [Global beam selection](#search-budgets) and [one-shard direct traversal](#navigation-with-one-shard) preserve reference policy. [Precision](#quantization-and-vector-precision) and [packed child metadata](#internal-node-navigation-data) match the agreed format; routing loads no unused postings. |
| Save/reopen | [Manifest-last publication](#saving-and-reopening-a-completed-generation) admits only complete compatible durable state. Fresh readers reopen without original writer processes/memory/machines; failed required saves preserve the previous generation. |
| Resource accounting | IDs support the intended scale. Account for coordinator/owner state, caches, handles/control records, active work, retained generations and publication/transfer overlap. Vector budgets do not replace actual resource limits. |

## Implementation boundary and research scope

Use the [architecture findings](findings/architecture-and-implementation.md), [lock comparison](findings/construction-locks.md) and [historical snapshot](CURRENT-PLAN.md#historical-implementation-snapshot) for evidence about existing code. The target requirements here take precedence over those records. The [plan](CURRENT-PLAN.md) owns the next implementation milestone, execution status and all future experiments. No distributed correctness, performance or scaling claim follows merely from agreement on this design.


### Listener default

Wait for the operating system to report an incoming connection before accepting it. This notification-based listener is the intended default because it reduces individual request latency. Keep the existing control/data admission limits, worker behavior and deadlines. The accepted research launcher selects `SPANN_ACCEPT_MODE=readiness`; frozen benchmark binaries retain an explicit polling comparison mode. The epoch31 combined construction timings use polling and do not establish a construction speedup from this listener choice.
