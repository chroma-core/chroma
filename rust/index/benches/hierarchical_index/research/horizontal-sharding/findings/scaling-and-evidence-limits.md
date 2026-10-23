# Scaling findings and evidence limits

The retained evidence does not yet establish a feasible 100-billion-vector operating envelope. It identifies resource requirements and working mechanisms, while leaving connected ownership, representative quality and physical coordination costs unresolved. A failed implementation or operating point motivates another design; it does not complete the broader project.

The [resource accounting](../dynamic-review/100B-resource-accounting-extension-v2.json) gives 307.2 TB of raw payload for one copy of 100B full-precision vectors at 768 dimensions. A single 64-bit field per vector adds 0.8 TB before indexing or allocation overhead. These are decimal-byte arithmetic, not measurements of a deployed index. The 360.1 TB partial total in the [research report](../SPANN-RESEARCH-REPORT-AND-NEXT-PHASE.md) applies to a particular representation with both one-bit and four-bit posting data; it is not a recommended layout or capacity bound.

The [tree model](../dynamic-review/100B-global-tree-arithmetic-v2.json) and [sensitivity model](../dynamic-review/100B-sensitivity-model-v2.json) expose dependence on branching, leaf occupancy, depth and skew. Routing cache size must include its representation, links, indexes and copies. Vector-cache payload is not process memory. Publication accounting must include changed pages, ancestor rewrites, temporary buffers and retained generations, without counting a shared immutable object repeatedly.

| Requirement | Evidence to retain | Remaining proof |
| --- | --- | --- |
| Population-wide identity | Checked wide-ID and ownership mechanism tests. | No truncation or hidden whole-population coordinator dependency in the connected target. |
| Bounded working state | Node, vector and metadata cache fixtures. | Total coordinator/owner memory, active work, provider indexes, postings and publication overlap. |
| Construction throughput | Phase and work measurements, including adverse results. | Representative independent work, deep-tree interactions, physical communication and maximum owner demand. |
| Selective queries | Saved-tree diagnostics and measured recall–cost points. | Fresh evaluation on meaningful pruning and working sets, with actual bytes and latency. |
| Ownership movement | Local movement and old-reader correctness. | Same-engine physical subtree transfer followed by further construction and immutable queries. |
| Practical 100B envelope | Explicit storage and structural arithmetic. | A reviewed model with measured coefficients, stated throughput/build/economic scenarios and margins. |

The [connected vector review](../dynamic-review/bounded-owner-vector-access-v1/repair-v2/independent-focused-review.json), [metadata review](../dynamic-review/unified-paged-vector-metadata-v1/independent-focused-review.json) and [physical readiness analysis](../dynamic-review/connected-physical-transport-readiness.json) are evidence for particular interfaces. They do not jointly imply that all mechanisms already run together across machines.

**Decision:** Build the connected target and measure the dependencies that determine its envelope. Do not multiply isolated kernel gains into an end-to-end forecast, treat local actors as physical hosts, infer amortization from small runs, or use timeout/build failures as a quality ceiling. Project success and experiment stopping conditions remain in the [research goal](../RESEARCH-GOAL.md).
