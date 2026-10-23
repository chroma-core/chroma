# Dynamic ownership of one global search tree

Build one evolving hierarchical vector index and distribute its existing branches as capacity requires. Preserve global clustering and reassignment decisions across machine boundaries so that distribution does not impose fixed initial ownership regions. Query recall and latency take priority over construction communication costs.

This is a design and implementation mandate from Lyon's side conversation on October 16, 2026. It is not an implemented or measured result. The main continuation thread owns implementation, experiment scheduling, independent review, and reconciliation with the existing research state.

## Priority and stopping criteria

Lyon prioritizes implementing this approach and improving clustering quality. Implementation may proceed alongside other useful work where ownership of files, mutable indexes, and controllers does not conflict. Existing clustering experiments should inform the diagnosis; their completion need not block independent implementation work.

The quality milestone is to bring either sharded approach to the same clustering quality as the unsharded index on the same vectors. Both the fixed-partition approach and this dynamic approach remain candidates. Once clustering quality is controlled, return to improving end-to-end query latency and data loaded at matched recall. Efficient construction matters, but additional build communication is acceptable when it preserves query quality.

An evidenced hard ceiling is an alternative stopping condition for the quality phase. Explain the restrictive assumption, relevant mathematical argument or repeated experimental evidence, and remaining gap. A failed tuning attempt, infrastructure blockage, or slow construction alone does not establish such a ceiling. Matching the unsharded index is the target; neither index is assumed to provide optimal clustering.

Lyon asks the main thread to make as much authorized progress as possible while Lyon sleeps. This priority does not remove independent review requirements or existing operational and budget constraints. Do not create competing controllers or bypass the existing agent lifetime restriction. Parallel work means coordinated ownership using the capabilities already permitted in the main thread.

## Current direction: restore fast development before scaling

Lyon asks that the long construction experiment stop and that build performance be diagnosed with very small, bounded, profiled tests before another large comparison. Fast iteration is a requirement of the research workflow. Start with a simple implementation that uses conservative memory headroom; pursue memory optimizations when measurements show that memory limits useful throughput or dataset size. A slower implementation is not justified merely by keeping memory usage low.

The reference comparison must use the intended benchmark execution path. The current comparison harness inserts serially and calls serial balancing, whereas the linked Wikipedia baseline uses 32 insertion workers and 32 balancing workers. The comparison also differs in balancing cadence, hardware, corpus, metric and construction search settings. These differences must be separated from the cost of dynamic ownership. The retained reference result spends 543.8 seconds in balancing out of 892.9 seconds of process time; the dynamic slowdown has no measured causal attribution yet. Reader-cache churn, repeated vector copies and repeated validation are hypotheses to profile, not established explanations.

Use short diagnostic runs, initially on roughly 5K–20K vectors with a 30–60 second limit per case, as a starting proposal rather than a guaranteed sufficient fixture. Measure insertion, balancing, vector retrieval and decoding, allocations, cache behavior and owner coordination separately. Increase fixture complexity only to reproduce a specific missing behavior, such as crossing the reader-cache capacity or moving a cluster. Do not extrapolate a tiny test into a quality or large-scale performance claim.

## Preserve the reference implementation's parallel construction

Lyon directs that dynamic ownership adapt the unsharded implementation's parallel insertion and balancing rather than treat a serial coordinator as the intended construction architecture. The fact that one operation can change memberships, neighbors or parent links does not by itself justify serial execution: the unsharded implementation already partitions balancing work among portions of the tree that are unlikely to interact and handles concurrent changes when they do. The implementation task is to inspect and preserve that scheduling and its handling of missing or orphaned nodes, then extend access and coordination across owners. This direction supersedes treating serial structural balancing as the long-term baseline to optimize.

Construction routing means walking the tree to choose a destination cluster for a new vector or a vector being reassigned during balancing. It does not mean serving user queries. Parallel insertion must include this destination selection, not only concurrent registration after a single coordinator has computed every route. Preserve the reference's navigation, tie handling and membership-version checks while allowing independent insertion workers to make progress.

Balancing includes preparing vectors, clustering, splitting and merging clusters, reassigning vectors, and updating parent links or the root. Parallel input fetching within one split is useful preparatory work, but does not fulfill parallel balancing. Reuse the reference's subtree work assignment and concurrent structural-change handling as the starting point. Add owner-aware reads, mutations and cross-owner reassignment without imposing a global serial gate on the whole balancing phase. Keep necessary synchronization around actual conflicts rather than assuming every operation conflicts.

Capacity-driven ownership migration remains outside insertion and balancing. Logical splits, merges and cross-owner vector reassignment remain allowed during balancing, with one current owner per vector. Validate the adapted algorithm with fixtures that exercise concurrent balancing in separate regions and interactions between regions, including missing or orphaned nodes, shared parent changes and versioned reassignment. Verify completed memberships, routing consistency and saved-reader correctness; do not require an independently scheduled parallel build to reproduce one serial execution's exact tree.

Restoring effective parallel insertion and balancing is expected to offer a substantial construction-time opportunity because the current coordinator serializes work that the reference distributes among workers. The magnitude is a hypothesis, not a measured speedup. Measure actual concurrent useful work and end-to-end phase time at matched settings, and distinguish scheduling, load imbalance and cross-owner coordination costs. A fixture must contain enough independent tree regions to exercise the scheduler; a shallow single-leaf test cannot establish parallel scaling. Existing tiny-test restrictions and the requirement for explicit authorization before larger runs remain unchanged.

## Separate capacity planning, insertion, balancing and re-sharding

Lyon endorses distinct construction phases so that competing workloads can be optimized independently. Begin with conservative headroom rather than fine-grained memory minimization. The target cycle is reserve capacity, add in parallel, balance in parallel, then re-shard to prepare for the next cycle. This is the agreed design direction, not a claim that the current implementation already follows it.

1. **Reserve capacity before insertion.** Choose an add-batch size that leaves room for uneven arrivals, temporary balancing allocations and ownership transfer. If existing placement lacks that room, redistribute before admitting the batch. Free capacity summed across all workers is insufficient: a batch can concentrate on one worker. The first implementation should use conservative per-worker headroom rather than require a sophisticated destination predictor. No numerical headroom factor is selected yet.
2. **Add in parallel.** Insert the admitted batch with cluster ownership placement held fixed. Use the intended parallel insertion path rather than assuming that configuring a thread pool makes a serial loop parallel. Preserve the reference insertion semantics and record effective concurrency.
3. **Balance in parallel.** Perform splits, merges and vector reassignment with capacity-driven ownership migration held off. Cross-owner neighbor discovery and reassignment remain valid and necessary. A vector changing clusters can change its owner; holding placement fixed does not prohibit that logical operation. Structural operations can create, remove or merge clusters according to the reference rules.
4. **Re-shard at the phase boundary.** Move whole existing clusters or branches between workers to restore headroom for the next add phase. Finish outstanding mutations before handing off ownership. Placement-only moves preserve the logical tree and its memberships.

An explicit capacity limit remains a safeguard. If unexpected concentration exhausts reserved headroom, end insertion early and move through the remaining phases before continuing. This is a proposed exceptional path, not a reason to run placement after every tiny storage batch. The implementation must allow enough reserve for balancing itself; reaching a hard memory limit before balancing is not a workable fallback. Exact thresholds and parallel coordination require small-test validation.

The three kinds of movement have different purposes. A split divides an oversized cluster into smaller clusters. Reassignment moves individual vectors to clusters that fit them better. Re-sharding transfers responsibility for an existing group to another worker without changing the grouping. The current prototype transfers leaf clusters, which are groups at the bottom of the tree; it does not yet demonstrate arbitrary multi-level subtree transfers. The broader branch-transfer design remains a target.

## Store vector values once and simplify construction references

The vector store maps a stable vector ID to its full embedding, which is the list of floating-point numbers representing that vector. The search tree separately records routing information and cluster membership. Moving responsibility for a cluster should transfer the state needed to manage it and references to its vectors, rather than require copying all embedding values between workers. Shared storage and local caches supply the values when needed. Local actors using one shared store remain a simulation, not evidence of execution across machines.

For the append-only experiment, prefer references based on stable vector IDs and one current published view of the vector store. The current implementation publishes a storage snapshot after each small batch and retains each vector's snapshot root plus ID. A snapshot root identifies the saved view used to look up a vector. Later snapshots contain earlier vectors and share unchanged blocks; they are not complete new copies of the corpus. Retaining each vector's insertion-time root can nevertheless make construction revisit many historical readers.

Phased construction does not inherently require per-batch historical vector references. The storage library may continue creating immutable snapshots when changes are committed. Publish changes at an appropriate phase boundary, arrange for construction readers to use the newly published view, and release obsolete readers once no operation needs them. Validate that the latest view contains every referenced vector and any required centroid data before changing this mechanism. Do not assume the storage API supports unsafe concurrent mutation of an existing reader.

Storage snapshots and membership versions solve different problems. Storage snapshots identify a consistent saved view of values. Membership versions distinguish a vector's current cluster assignment from stale entries left by reassignment. Published query-index versions also remain separate from the mutable construction state. Simplifying construction storage references must preserve valid membership checks and the consistency of any already published reader version.

Cache reuse is a performance concern distinct from storage correctness. The current owner caches 16 vector-root readers, clears all of them when another distinct root is needed, copies each retrieved vector and checks every coordinate for finiteness, then releases that reader's retained blocks. A finiteness check rejects NaN and positive or negative infinity. The shared block cache and operating-system file cache remain separate; releasing reader-held blocks does not establish a disk read on every access. Profile reader reopening, shared-cache hits and misses, decoding, copying and validation before assigning blame. Prefer retaining useful data and readers within conservative memory headroom; validate immutable input at an appropriate admission boundary rather than automatically assuming every hot lookup needs the same full validation.

## The pieces

- The logical tree contains routing centers, internal child links, and leaf posting lists. It grows through the same insertion, split, merge, and reassignment rules used by the unsharded reference.
- A posting list holds vector identities and compressed representations associated with a leaf. Full vectors have durable storage in S3-backed blockstore and may be cached on a machine.
- A construction machine owns mutable tree state for one or more branches. Multiple branches per machine allow capacity balancing without forcing clustering changes.
- An ownership directory resolves logical nodes or branch roots to their current writer. Stable logical identities must survive placement changes; identity must not require a permanently fixed owner.
- A routing cache holds a bounded set of common upper-tree nodes on each machine. Its capacity is measured in bytes, and its lower edge may occur at different depths.
- A published reader version binds a complete tree and its referenced data. Reader and writer versions are separate; an individual version receives reads or writes, not both.

Each vector has one active owner within a version. Cached routing metadata and retained immutable versions do not create additional live vector memberships. The implementation must distinguish vector membership, construction ownership, and durable block location.

## Grow first and distribute as capacity requires

1. **Start with the ordinary index on one construction machine.** Insert vectors and allow leaf splits, internal splits, and new roots to occur according to the reference algorithm. There is no preliminary clustering into a fixed number of ownership groups.
2. **Trigger redistribution with headroom remaining.** Use a configurable vector-count limit for the POC. For a byte-based limit, define whether it bounds mutable state, resident cache, or another resource; durable full-vector size alone does not describe construction RAM demand.
3. **Choose existing branches to move.** Moving one subtree may be sufficient. Descend into an oversized branch to obtain smaller existing subtrees, and permit a machine to own several subtrees. Do not require half the data to move or require one root child per machine.
4. **Prepare the destination and hand off writer ownership.** Reuse durable block references. Publish or transfer unfinished mutable state, coordinate outstanding mutations, and warm caches as needed. The separate re-sharding phase is the preferred POC handoff point; the capacity-reservation step may also move branches before admitting an add batch. Reader versions continue using their own immutable state.
5. **Continue global construction.** Ownership changes where work executes; insertion and reassignment remain eligible to select leaves anywhere in the logical tree.

The amount moved is a tunable capacity policy. Small moves retain more cache locality and defer traffic but can leave the source repeatedly approaching its limit. Larger batches provide headroom and amortize setup while warming more destination data at once. Evaluate branch growth and workload concentration as well as current size. Use separated move-out and consolidation thresholds to avoid repeatedly moving branches back and forth.

A placement-only transfer must preserve topology, centroid values, current vector membership, and query semantics. It need not rewrite or re-upload durable full vectors. Count ownership metadata, unfinished-state transfer, destination block reads, and cache warming separately rather than assuming that all subtree bytes cross directly between machines.

## Preserve global split and reassignment behavior

Eligibility discovery is part of the distributed algorithm. The existing writer splits an overflowing leaf, examines vectors in its replacement clusters, navigates around the old center to find neighboring leaves, scans those leaves for affected vectors, and navigates again to choose destinations for eligible vectors. Restricting any of these steps to the current machine would retain a quality constraint.

1. **Split a leaf using the reference clustering procedure.** Initially keep the replacement leaves with the existing owner. A logical split does not itself require allocating another machine.
2. **Discover neighbors through the global tree.** Use the same navigation policy and neighbor-count limits as the reference. Return the identities and required centers of relevant leaves regardless of their owners.
3. **Evaluate eligibility near the posting data.** Send the old and replacement centers to the owners of neighboring lists. Those owners scan their entries with the reference predicates and return eligible identities. Fetch full vectors from blockstore when the configured reference path requires them.
4. **Choose destinations globally.** Navigate for eligible vectors with the reference selection rules, then change membership and ownership where required. Preserve version validation, duplicate suppression, and reassignment ordering semantics.
5. **Publish consistent writer results.** Coordinate changes affecting multiple owners so that the completed index contains exactly one current membership per vector and complete routing references.

Moving eligibility scans to remote owners should change execution location without changing predicates. Sending entire posting lists to a central coordinator is not required. Batching by destination is promising, but the implementation must verify that batching and stale routing metadata do not change construction semantics unintentionally.

Initially use global navigation for insertion and reassignment. A future shortcut could search locally when a vector appears far from other ownership regions. Treat center-distance margins as heuristics unless a valid bound proves that remote candidates cannot improve the reference decision. Introduce that shortcut only after measuring its effect on quality.

## Splits and merges have different traffic costs

| Event | Logical operation | Expected communication categories |
|---|---|---|
| A leaf exceeds its size limit. | Split and recluster locally using the reference algorithm. | Publish routing changes; discover remote neighbors; perform remote eligibility scans and any resulting reassignment. |
| An internal node exceeds its child limit. | Regroup child references and create replacement internal nodes. | Update centers, parent references, and direct child references. Descendant posting lists can remain on their machines. |
| A machine approaches capacity. | Transfer ownership of selected existing branches. | Transfer unfinished state and load destination caches; reuse durable block references. |
| Leaves should merge. | Choose the merge through the reference rules, including remote candidates. | Evaluate the merge and obtain required posting/vector data; place the result where capacity and transfer cost favor it. |
| A machine has excess capacity. | Consolidate ownership as a separate placement decision. | Transfer unfinished state and warm caches for moved branches. |

For a cross-machine leaf merge, placing the result with the larger existing list may reduce traffic when capacity permits. Selecting a poorer merge partner solely because it is local would compromise the quality objective. Avoid moving descendants solely because a parent is split or replaced.

## Let the common tree grow independently of placement

A new root can have two children while its descendants span many machines. The shared routing portion is the collection of routing nodes above the owned branches, not one fixed tree level. Adding a root does not require adding as many immediate children as machines, moving all branch roots down into different owners, or redistributing vector data.

Cache existing upper nodes plus newly introduced ancestors while capacity permits. When the routing cache exceeds its byte budget, remove selected lower nodes from the cache and fetch them on demand from their authoritative locations. Cache eviction is distinct from transferring mutable ownership or moving vectors.

Root contraction and internal merging likewise change logical references without requiring machine consolidation. Preserve the tree's native structural rules and let the ownership directory accommodate the result.

## Upper routing cache arithmetic

These are arithmetic scenarios, not observed process memory or a 100B execution claim. Assume 768-dimensional centers represented with four bits per dimension and a 12-byte encoding header, giving 396 bytes per cached center. Assume densely occupied internal nodes with approximately 100 children. Count the roots of all owned subtrees and every routing ancestor above them. Sizes are decimal MB per cache copy and exclude links, allocation overhead, ownership records, and full-precision centers.

For R owned subtree roots, a densely occupied tree needs approximately (R - 1) / 99 additional routing nodes. The table uses R = ceil(total vectors / average vectors per owned subtree) and R + ceil((R - 1) / 99) cached nodes when R exceeds one. This is a compact topology scenario; actual occupancy, shape, and redundant runtime representations must be measured. Multiple owned subtrees may reside on one machine.

| Total vectors | 100K vectors per subtree: nodes / MB | 1M vectors per subtree: nodes / MB | 10M vectors per subtree: nodes / MB |
|---:|---:|---:|---:|
| 10M | 101 / 0.040 | 11 / 0.0044 | Shared upper tree unnecessary |
| 100M | 1,011 / 0.400 | 101 / 0.040 | 11 / 0.0044 |
| 1B | 10,101 / 4.00 | 1,011 / 0.400 | 101 / 0.040 |
| 10B | 101,011 / 40.0 | 10,101 / 4.00 | 1,011 / 0.400 |
| 100B | 1,010,101 / 400 | 101,011 / 40.0 | 10,101 / 4.00 |

An illustrative additional allowance of 64 bytes per node increases these sizes by about 16%; the 40 MB case becomes about 46.5 MB. At 1024 dimensions, the code occupies 524 bytes, about 32% more. Retaining full-precision centers alongside compressed codes adds about 310 MB in the 100B / 1M-per-subtree case. The existing writer retains full-precision centers; a four-bit-only routing cache is a proposed representation whose navigation quality must be checked against the selected reference.

Caching every leaf center is a substantially larger scenario. At 100B vectors and an assumed 1,000 vectors per posting list, all leaf centers and routing ancestors would occupy approximately 40 GB of four-bit codes. This excludes posting contents. A byte budget should therefore stop caching above that scale unless measurements justify the deeper cache.

## Coordinate structural changes without disrupting readers

The potential bottleneck is the frequency and coordination of upper-tree updates, not just their stored size. A design that synchronizes every construction machine after every change could be expensive even with a tiny upper tree. Center changes may occur more frequently than root splits; do not assume routing metadata changes rarely without measurement.

Give each mutable routing node one authoritative writer and distribute versioned changes. A single authority for shared structural edits is a reasonable POC starting point if its cost is measured. Batch compatible updates, refresh or retry stale construction operations, and ensure multi-owner changes complete with consistent references. Do not silently change the reference algorithm through arbitrarily stale centers or altered reassignment order.

An internal split ordinarily updates replacement nodes, the parent, and direct child references. Avoid representation choices that require every descendant code to be rewritten when an ancestor center changes. Identify actual persistence amplification separately from logical mutation size.

Reader versions remain immutable. Publish a complete, compatible routing version and warm its common nodes before switching readers when practical. Construction coordination and cache updates must be measured separately from query latency; construction churn need not enter an already published reader's query path.

## Quality comparison and implementation sequence

First complete the small build-performance diagnosis and apply the phased construction direction above. This immediate priority supersedes another large quality run while the construction slowdown remains unexplained.

1. **Read the existing quality diagnostic and the reference benchmark's Index Quality definition.** Establish exactly what each measures before comparing outputs. Oracle ground-truth leaf coverage, centroid-based routing quality, and executable query recall answer different questions.
2. **Implement placement without fixed initial clustering.** Start from one reference tree, introduce a configurable capacity threshold, and move existing branches while preserving logical identity and membership. Prove that placement-only changes preserve query behavior.
3. **Implement cross-owner structural edits and reassignment.** Exercise remote neighbor discovery, eligibility scans, destination navigation, leaf merges, internal splits, root growth, and cache refresh. Validate one current owner per vector and consistent saved-reader results.
4. **Compare construction quality on matched inputs.** Use the same vectors, metric, queries, exact reference neighbors, posting thresholds, replication policy, and relevant construction settings. Preserve seeds and insertion order where possible; report concurrency differences. Choose an interpretable equivalence criterion before interpreting results, and use enough representative queries to avoid treating a tiny subset as definitive.
5. **Separate clustering from navigation and execution.** Compare ground-truth-informed minimum leaf and live-row costs, the reference Index Quality measure, actual leaves and vectors scanned at matched recall, and query latency under declared cache conditions. An oracle metric alone does not prove equivalent navigation or end-to-end quality.
6. **Return to query optimization after the quality milestone.** Measure posting and full-vector block reads, physical encoded bytes, cache hits, remote calls, routing work, candidate work, and exact reranking separately. Optimize measured bottlenecks while preserving matched recall.

Keep immutable reference builds and failed attempts. Use meaningful tests, whole-change independent review, and a representative experiment for each material milestone. Do not multiply isolated kernel gains into an end-to-end prediction. Build communication can be more expensive than the fixed-partition approach and still satisfy the primary objective if reader quality and latency improve.

## References and execution ownership

- The active main continuation thread is “Sharting”, ID `01a13c0a-7fd6-7d53-bc23-da97b5986de3`. Sharting2 is retired; implementation, approvals and reporting belong to the active thread.
- The implementation checkout is `/private/tmp/chroma-hierarchical-spann-sharding`. The main checkout at `/Users/chroma/workspace/chroma` remains untouched by this design handoff.
- The current split, neighbor eligibility, reassignment, and root behavior is in [the hierarchical writer](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/benches/hierarchical_index/writer/writer.rs).
- Four-bit encoding size is defined in [the quantization implementation](/private/tmp/chroma-hierarchical-spann-sharding/rust/index/src/quantization/multi_bit.rs).
- [The existing quality diagnostic design](dynamic-review/index-quality-diagnostic-v1/DESIGN.md) documents ground-truth membership and leaf/row coverage oracles. Read its current implementation and review state before relying on it.
- [The fast Wikipedia baseline](https://app.notion.com/p/trychroma/40M-Wikipedia-current-baseline-2026-10-09-3f458a6d8191812e8386f516e792e7a6) supplies the parallel construction reference and phase timings discussed with Lyon. Its first two increments total about 69 seconds across recorded load, add, balance, commit and reopen phases; this is not a matched timing comparison with FineWeb.
- [The unsharded benchmark](https://app.notion.com/p/trychroma/100M-MSMARCO-Oct-15-2026-3fa58a6d819180f396a3d8f6d7eb6cda) is the requested source for the Index Quality definition. Its 100M MSMARCO latency is not a matched baseline for the retained 10M FineWeb build.
- [The existing dynamic ownership document](DYNAMIC-SHARDING.md) describes reusable publication and ownership mechanisms. Its fixed-boundary rebuild procedures do not by themselves implement the globally evolving construction proposed here.

This side conversation creates this standalone design and relays Lyon's priority. The main thread should reconcile its current plan and assessment, coordinate implementation ownership, and continue useful work under its current permissions. This document does not launch resources, authorize production integration, or change budget and shutdown safeguards.


## Incremental query publication interface under implementation

A completed construction cut produces one durable query generation while earlier readers keep their original immutable roots. The owner writer save operation returns the complete five-root tuple only after all stores flush successfully. This return is the POC publication boundary; production manifest coordination and recovery are outside scope.

The implementation streams changed node data and authoritative owner versions into forked stores instead of assembling a complete joined writer. It copies at most one admitted leaf payload at a time. Changed-key metadata, provider buffers and stored root indexes are separate costs and must be reported; this interface alone does not bound total memory. Dirty marks and structural deletions remain authoritative through failed commits or flushes. A successful publication advances the accepted root tuple, acknowledges those changes and rebases future vector staging onto the published vector root, which contains both embeddings and routing centroids. Existing vector handles retain their immutable roots.

The implementation must distinguish an intentionally empty posting from an unloaded metadata shell, preserve unchanged centroids and new vectors together, and exclude concurrent saves or mutations during the cut. These are required invariants, not completed evidence. The independent design review is dynamic-review/incremental-owner-publication-v1/operator-design-review.json; implementation and multigeneration failure tests remain in progress.


## Active bounded topology-access implementation

Construction and queries will share ordered node-fetch waves while retaining one global mixed frontier for ranking. Each owned record carries node identity, kind, children, parent and full-precision centroid, together with a live topology epoch or immutable saved-generation binding. The saved reader will use point reads from its pinned roots instead of loading every node and centroid at open. Complete census validation remains a separate operation.

Live integration must replace direct mutation through resident-map guards with explicit owner-backed record updates. Structural preparation admits temporary pins before detaching a node; cache eviction neither discards dirty state nor calls back into the structural lock. Recursive parent splits and child-centroid reads require explicit working-set accounting. A bounded cache alone does not bound fetched wave records, scoring/frontier buffers, provider allocations or selected query candidates. This interface is under implementation and is not accepted paging or multi-machine evidence.


## Construction traversal visibility — accepted local mechanism

Experimental owner construction targets a complete structural view for each navigation call. The call reads the root, follows children and ranks its final candidates within one visibility interval. Structural edits hold the corresponding exclusive interval through detach, replacement and publication. Concurrent navigation readers may overlap. This is a stronger observation policy than the ordinary reference path, which can skip a child removed during traversal; baseline defaults must remain explicit when comparing construction work or quality.

Structural visibility does not freeze vector membership. After navigation releases its read scope, registration and reassignment still validate current membership and ownership through their existing checks. A candidate can change after discovery. The implementation must release navigation guards before taking structural or identity locks or waiting on owner callbacks, and recursive structural writers must use the existing ownership mechanism rather than attempt a read-to-write upgrade. One shared scope replaces independent nested gates.

Acceptance requires a deterministic paused-detach test that proves readers wait and then match completed-tree routing, a dangling-edge test that rejects partial results, recursive writer and overlapping-reader progress checks, and the connected mutation and old-saved-reader witness. Comparable missing-record classification must cover resident and durable-centroid traversal; a counter bypass is not evidence of zero misses. This boundary does not bound the resident mutation graph or establish physical distributed execution. The detailed proposal is dynamic-review/scrub-authority-batch-v1/TOPOLOGY-VISIBILITY-PROPOSAL.json.

Internal destination discovery during a structural edit needs an explicit exclusion rule. A merge can detach its source before searching for a destination, so the edit-owning thread may omit only nodes recorded as deliberately detached by its current transaction. Unknown missing nodes remain invariant failures, including for that thread. External readers cannot use this exception. The exclusion set must be bounded, scoped to the outer edit, and cleared on completion or failure; this contract passes the focused connected and deterministic failure tests recorded in dynamic-review/topology-visibility-v1/independent-focused-review.json.

## Authoritative mutation records — active implementation

Owner construction will locate every internal and leaf record by its immutable node home. Each full record stores parent, node kind, children or physical leaf length, full-precision centroid and its exact code, posting owner and ownership epoch. Physical posting movement changes the record rather than its lookup home. Read access returns an owned record; exclusive mutation uses a bounded lease. The owner path must remove its resident node map and global leaf-owner map, rather than retain them behind a cache.

Separate budgets cover cached clean records, active pinned drafts and dirty transaction records. Structural work reserves capacity before detach and retains the accepted tree-lock then visibility-write ordering. Navigation holds its visibility read interval during ordered point reads and releases it before registration. A node lease must not acquire a visibility read interval implicitly. Registration passes the already held record's centroid and owner into the backend instead of performing a nested locator lookup. The connected test will clear both resident maps and exercise append, split, cross-owner reassignment, scheduler drain and publication with old-reader validation.
