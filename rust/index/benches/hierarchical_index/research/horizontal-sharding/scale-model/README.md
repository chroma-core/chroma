# A conditional path from the working POC to 100 billion vectors

The POC demonstrates correct sharded insertion, saved reads and topology changes, and the retained real-data measurements support a conditional storage and work model. They do not demonstrate 100B recall, throughput or process-memory capacity. The current implementation still performs global map and version work, so increasing its row limits alone cannot produce a 100B implementation.

The acceptance scope is a working POC and measured evidence for this theoretical path. Saved inputs may be assumed immutable for the experiments. Production integration, immutability enforcement, save/recovery failures, node failures, fleet resizing and deployment hardening are outside acceptance. Existing evidence remains retained; no new corpus, cloud run or query-strategy comparison is proposed here.

## What is measured

The real-data model uses one exact 10M-vector, 768-dimensional tree with 128 lower subtrees. Every storage coefficient and calibration trace is bound to tree `6fe60910…1659`; different construction epochs are not combined. Its mean owner has 78,125 rows and its largest has 170,731, a largest/mean ratio of 2.185. This is observed skew, not a universal bound.

| State in the selected 10M tree | Measured bytes | Bytes per vector | Meaning |
|---|---:|---:|---|
| Current live root/block objects | 33,616,019,532 | 3,361.60 | Objects reachable from the current saved index. |
| Physical root/block object store | 44,376,411,362 | 4,437.64 | Includes retained objects beyond the current reachable set. |
| Complete checkpoint regular files | 44,457,046,028 | 4,445.70 | Also includes maps/manifests and other checkpoint metadata. |
| Sum of per-owner retained-posting maxima | 1,195,109,070 | 119.51 | Owned-array estimates across the trace, not simultaneous RSS or provider memory. |

These are logical file-byte counts. Allocated filesystem bytes, source inputs, cohost mirrors, other attempts and source caches are separate. Current live objects are contained in physical objects, which are contained in complete checkpoints; these rows must not be added together. The source summary and exact arithmetic are retained in `../pilot/results/fineweb10m-selected-storage-summary-v1.json` and `measured-model.json`.

The selected 10M calibration policy uses global beam 2048 and final candidate budget 1000. It scores and retains all 128 upper roots. The 100 existing calibration queries average 5.18 MB of request bodies and 1.58 MB of response bodies. About 105,265 local candidate contributions become 8000 wire contributions after eight adapters each reduce their group to 1000. Calibration recall is 95.45%; it is not a larger-tree quality guarantee.

The POC adds mechanism evidence on a deliberately small multi-block fixture. Its 96→98-row, two-owner, 32-dimensional sequence preserves exact IDs and vector bits, crosses the nearest upper boundary, handles a root-distance tie, registers each addition once in its selected leaf, and reopens the saved result. Split, merge and reassignment preserve complete coverage. The test-only 1 KiB split target creates 20/22 vector blocks; those tiny-block storage ratios must not replace the real-data coefficients above.

## What the POC removes and what it still reads

The actual writer now reads canonical rows through an immutable prefix-plus-tail view and copies only owners that receive an insertion. Both additions select owner b, leaf 17, at local IDs 48 and 49, matching the retained independent writer. A fresh saved-owner audit checks all fifty live IDs, authoritative versions/postings, exact vector bits and both leaf memberships. Source inventories remain unchanged. This is an actual write/save proof, not a proposed optimization or a read-only replay.

| Copy category in the retained 96→98 experiment | Eager copy-only baseline | Actual opt-in writer |
|---|---:|---:|
| Full canonical materializations | 25,088 B | 0 B |
| Separate append-tail file | 256 B | 0 B |
| Owner a checkpoint | 85,786 B / 39 files | 0 B / 0 files |
| Owner b checkpoint | 89,807 B / 41 files | 89,807 B / 41 files |

The baseline executes the copy operations only; it does not repeat a complete legacy writer. Comparing these copy categories is valid. Comparing its total time or total read bytes with the complete new writer would mix different work. Counts describe logical payload copied, not physical disk traffic or unique allocated storage.

The writer consumes 6656 canonical-view bytes: 256 bytes to route the two additions and 6400 bytes to emit the changed owner's fifty-row input. It still opens both parent writers, reads both maps and checks versions. Parent storage returns 29,028 encoded bytes for a and 36,362 for b; the saved provider returns 77,230. A separate fresh audit returns 66,660 encoded bytes and reads 6400 canonical bytes. These are distinct phases, not a single memory or I/O total. Successful root/block returns are counted; cache requests/hits, decode allocations, physical disk/network traffic and RSS are not.

The fixed-input validation POC also composes ownership from an admitted parent and changed owners. Its append audit returns 66,660 encoded bytes versus 128,916 for the complete-owner oracle; the split returns 52,760 versus 117,442. Unchanged-owner checkpoint payload reads are zero inside those incremental phases. Bootstrap, setup and each full oracle are separately charged. Legacy page/trie sidecars and complete record audits are not silently admitted by this experimental view. Both POC paths explicitly assume fixed immutable inputs; default publication validation remains unchanged.

Reader working sets have a separate proof. Exact reranking on the retained multi-block fixture peaks at 15/16 reader-held blocks with estimated payloads 9984/10624 bytes on the default path. The optional serial release path peaks at one block, at most 1024 estimated bytes, with identical results. Posting eviction, streaming candidate collection and provider-cache options also have bounded-fixture evidence. These results distinguish held references and owned payload from provider residency, pre-decode buffering, output containers and RSS. They do not establish a measured 100B process-memory limit.

The complete writer and validation receipts are `../dynamic-review/poc-lazy-selected-write-v1/independent-complete-runtime-review.json`, `../dynamic-review/poc-proportional-validation-v3/independent-complete-runtime-review.json` and `../dynamic-review/rerank-held-pins-v2/independent-complete-runtime-review.json`. Raw counters and source/binary hashes remain with each epoch.

## Storage arithmetic at 100B

At 768 dimensions, 100B FP32 vectors contain 307.2 TB of raw values. Applying the measured 10M coefficients gives 336.16 TB of current live index objects, 443.76 TB of physical index objects, or 444.57 TB of complete checkpoint files. These are alternative accounting scopes in decimal units, not sums or upper bounds. Checkpoint frequency, tree height, skew, topology work and retained generations can change the coefficients.

One flat eight-byte global-ID mapping is 800 GB and one dense global coverage bitmap is 12.5 GB. The complete-checkpoint coefficient already includes its persisted map; do not add that same map twice. Additional input maps, ownership pages/trie copies, identity tables and separate canonical/record vector payloads must be charged if retained. The proof implementation keeps construction inputs and archival copies; they are not free capacity. A separately retained FP32 corpus or record-embedding copy adds another 307.2 TB each. A theoretical design with one authoritative embedding payload must actually share references rather than merely omit those bytes from its budget. Full driver mirrors are not the proposed large-scale layout.

Let N be total rows and R the assumed rows per owner. Then K = ceil(N/R) owners distribute the index. The following scenarios retain one logical owner per vector; they introduce no full-data worker replication. Temporary old/new snapshots for one maintenance operation remain additional affected-owner state.

| Assumed R | K at 100B | Raw FP32 centroid array for a flat upper root | Coordinate terms scored per query at that root | Complete checkpoint bytes per average owner, linear scenario |
|---:|---:|---:|---:|---:|
| 78,125, the measured 10M mean | 1,280,000 | 3.93 GB | 983.04 million | 347.32 MB |
| 1M | 100,000 | 307.2 MB | 76.8 million | 4.45 GB |
| 10M | 10,000 | 30.72 MB | 7.68 million | 44.46 GB |
| 100M | 1,000 | 3.07 MB | 0.768 million | 444.57 GB |

The 800 GB global map and 12.5 GB bitmap are aggregate representation sizes, not necessary per-process resident allocations. An assumed 1M-row partition has 8 MB of raw u64 IDs and a 125 kB one-bit coverage bitmap; a 10M-row partition has 80 MB and 1.25 MB. Containers, versions, nodes, vectors and provider buffers add memory. The current coordinator still loads global maps, so this partition-residency argument describes the conditional architecture rather than the present implementation.

These are partition choices, not measured owner capacities. Larger owners reduce routing metadata and fanout but increase per-owner construction, compaction and transfer work. The observed largest/mean ratio of 2.185 warns against sizing every host from the average; it does not bound future skew. A disk-only host-count floor is total chosen storage scope divided by usable disk per host after explicit maintenance headroom. CPU, memory and traffic can require more hosts. No hardware count, maximum QPS or latency follows without those inputs.

Under the measured layout, a scenario reading one raw-vector pass and writing one final live index accounts for about 6.43 GB for an assumed 1M-row owner or 64.34 GB for 10M. These are logical byte-volume assumptions, not universal physical-I/O lower bounds; compression and shared existing objects can differ. The scenario excludes checkpoints, scratch, maps and old generations. Direct selected-leaf insertion avoids rebuilding the topology, but the current save/audit still scans the full changed owner and emits its full construction input. Its cost is proportional to changed-owner size, not just the new rows.

## Routing, communication and load assumptions

The flat upper root is a real scaling limit. Keeping the measured mean owner size creates 1.28 million roots at 100B, with almost one billion coordinate terms before lower traversal. A hierarchy can bound routing work by a retained frontier and depth, but quality at a bounded beam is an explicit assumption until tested. The present 10M policy retains every root; keeping beam 2048 while increasing K does not establish 100B recall. The deferred query-strategy experiment remains unexecuted.

Adding workers can increase coordinator traffic even when the logical tree stays fixed. Offline regrouping of the same 128 owners and existing traces changes only their virtual placement. Four adapters per virtual worker produce the following counts; they measure neither new latency nor new recall.

| Virtual workers | Mean request/response batches per query | Mean wire candidates | Sequential phases |
|---:|---:|---:|---:|
| 1 | 15.97 | 4000 | 4 |
| 2 | 31.76 | 8000 | 4 |
| 4 | 60.52 | 16,000 | 4 |
| 8 | 108.53 | 32,000 | 4 |
| 16 | 183.70 | 62,132 | 4 |
| 32 | 300.33 | 105,265 | 4 |

Batches within a phase are concurrent; 300 batches do not mean 300 sequential round trips. Hierarchical candidate reduction with the same distance/global-ID ordering can keep one global top-1000 result without forwarding every endpoint's whole list to a single coordinator. Mapping and validation must precede any discard. A fixed top-1000 contract needs one consistent distance/global-ID total order; a tie-inclusive contract must preserve every required cutoff tie and can return more than 1000 rows. Neither contract grants an unconditional 1000-row memory bound to the tie-inclusive collector. This is a scale-model requirement, not an implemented new reduction experiment.

A separate measured 1M warm-cache epoch reaches 6.93 queries/s on one host and 12.35 on two with a 32-vCPU driver and sixteen clients. It demonstrates a placement benefit for that fixed implementation, not saturated host capacity or a 10M/100B throughput coefficient. The existing load planner uses recorded CPU windows and declared future demand/memory/storage reservations. A 100B model must supply measured per-owner demand and explicitly reserve active work and retained state; it cannot infer equal load from disjoint ownership.

## Algorithmic prerequisites, not production-hardening gates

| Remaining cost or representation | Current POC behavior | Conditional large-scale requirement |
|---|---|---|
| All-owner map admission | Loads every map into coordinator vectors and builds a global HashSet. Raw IDs alone become 800 GB at 100B, before container overhead. | Keep per-owner counts and authenticated map paths, reuse admitted ownership under the immutable-input assumption, and handle only new IDs/affected local ranges. Do not lift the current cap and retain the global allocation. |
| Per-addition version loop | `append_owned_global` checks every current local version for every appended vector: O(N × appended rows), in addition to writer initialization. | Move invariant parent validity out of the per-row loop and validate only new or affected state. The current function remains unchanged; this condition is not yet implemented. |
| Parent writer state | Opens all owner metadata/versions even when only one owner changes. | Partition resident writer state across owners and fetch only the bounded routing frontier; load full mutable state only for selected owners. Existing task protocols prove small-scale subdivision, not large-scale memory behavior. |
| Record namespace and identity tables | One record segment has u32 offsets; the explicit global mapping is u64, and current record/table audits scan all rows. | Use partition-local record offsets with explicit global identity and bounded identity pages. A u64 vector ID does not widen the current single record-offset namespace. |
| Legacy ownership/record oracles | Full page/trie/map/canonical/record checks remain linear and are charged separately. | Use the proved parent-plus-changed-owner composition as the POC fast path and page persistent metadata; retain full checks as an offline oracle rather than per-append work. |
| Candidate/output and provider memory | Inclusive ties can retain all candidates; exact seen/output containers and pre-decode buffers remain separate from the one-block reference proof. | Apply explicit working-set/output budgets and account for worst-case ties, provider buffers and simultaneous owner work. No unconditional M-sized or RSS bound is claimed. |
| Checkpoint and proof copies | Selected-owner copying is implemented; the changed owner still retains full proof inputs and history. | Charge affected-owner headroom and bound retained proof/history state in the model. No full-corpus copy is needed for each append, but removing a copy operation does not erase already retained bytes. |
| Prototype limits | Dynamic composition and task sizes retain their explicit 100k/128-row/128-owner bounds. | Replace global algorithms and representations before changing limits; constants alone do not establish scaling. |

Under these conditions, storage grows linearly with N, per-operation validation/copy work can follow affected owners, and resident state can follow bounded owner partitions and query frontiers. The working POC supplies concrete correctness and cost evidence for that architecture. The unchanged global loops, record namespace and routing-quality assumptions prevent claiming that today's executable already runs at 100B. No additional small optimization or production-hardening test is needed to state this boundary honestly.

The preserved arithmetic and per-owner inputs remain in `measured-model.json`; its source hashes and historical measurements are unchanged. `build_model.py` reproduces that offline model from local reports without cloud work, vector search or corpus growth. New POC counters are retained in their separate reviewed epochs rather than mixed into the 10M coefficients.
