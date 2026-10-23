# Architecture and implementation findings

The retained evidence does not establish the complete intended two-machine architecture for the current dynamic engine. The [target design](../DESIGN.md) requires a shared upper tree and machine-owned subtrees with their posting lists. The one-machine configuration must implement those same boundaries.

This is a consolidation of retained source investigations and receipts, not a fresh full audit of the mutable checkout. Before implementation resumes, reconcile the current code and benchmark drivers against these findings. The [recovered architecture prompts](../RECOVERED-ARCHITECTURE-SIDE-CHAT.md) contain Lyon's questions and intended boundary; they contain no recovered assistant answers.

| Area | What the retained evidence supports | What remains missing or requires reconciliation |
| --- | --- | --- |
| Dynamic construction | Local owners exercise insertion, balancing, logical reassignment and leaf movement with membership checks. | Local owner actors do not establish independent machines or ownership of arbitrary multi-level subtrees. |
| Routing and node storage | Full-record adapters separate node metadata from postings and provide cached record access. | Node lookup homes, posting placement and subtree ownership need one explicit per-machine model. Independent caches do not by themselves demonstrate that model. |
| Posting storage | Leaf metadata supplies empty posting arrays; postings live separately in owner state/storage. | Separate metadata prevents incidental posting loading during metadata reads, but does not establish bounded posting residency or physical paging. |
| Full vectors | Durable handles, bounded decoded caches and reader reuse have connected correctness evidence. | Current-view reader simplification, root indexes, caller buffers and total memory still need accounting. |
| Concurrency | Parallel worker machinery exists, and counters record work and waits. | Broad structural/visibility locks and synchronous owner calls can limit useful concurrency. Configured workers and observed worker IDs are insufficient utilization measures. |
| Publication | Local immutable generations and changed-record mechanisms have tested components. | A complete descriptor and root availability across independent machines must connect to the current engine. |
| Physical transport | Older subtree transport and authenticated request machinery exist. | Current owner closures and saved-reader assumptions need typed remote interfaces; the older wire format cannot simply operate the newer engine. |

The owner executor is a local service that receives operations through a queue and applies them to authoritative owner state. This gives the prototype a place to enforce ordering and versions, but delegation itself is not a sharding requirement. The reference performs more work directly against local writer state. The architectural decision is which operations need remote authority and which can execute through ordinary local concurrency.

The reference uses structural synchronization, including attachment and internal-tree edits. The dynamic implementation's broad lock scope is therefore a question of which work is protected and for how long. Ownership of a subtree does not eliminate conflicts between workers within its owner. The [structural-lock investigation](../dynamic-review/structural-lock-scope-investigation-v1.md) and [split preparation investigation](../dynamic-review/split-preparation-lock-readiness-v1.md) provide the retained comparison.

The current dynamic navigation policy seeks a complete structural view during a traversal; reference navigation can observe nodes disappearing during concurrent work. This is a consequential semantic difference. A final census can pass in both implementations without proving that their intermediate observations or amounts of construction work match.

The [physical transport readiness analysis](../dynamic-review/connected-physical-transport-readiness.json) names the missing typed owner requests, native session integration, transfer boundary, generation-bound reads and distributed descriptor. Its proposed leaf-transfer witness is useful correctness coverage but does not alone establish full multi-level subtree ownership. The plan must connect these mechanisms rather than treating a remote adapter around the intermediate engine as completion.

**Decision:** Resolve these ownership and concurrency boundaries before resuming performance experiments. Reuse tested components and evidence, while requiring the completed one-machine path to represent the intended design.
