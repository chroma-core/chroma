# Hierarchical SPANN research and the next phase toward 100 billion vectors

Draft for Lyon · 17 October 2026 · Evidence through the accepted construction, quality, query-cost and bounded-vector-access studies.

## The continuing objective

The project’s objective is to find and demonstrate a credible design for indexing and searching 100 billion vectors. The completed assessment is a checkpoint toward that objective. Finding limits in the current prototype informs the next design; it does not complete the broader project or establish that a globally evolving tree cannot work at that scale.

The current result is a working local proof of concept with useful measurements and several concrete barriers. Global tree construction, ownership changes and saved-reader correctness work in the tested cases. Construction overhead remains material. A 16,384-vector quality comparison reaches about 94.3–94.7% recall and misses its 95% target. Query timings of about 4.4–4.7 milliseconds apply to those small, warm indexes. The most recent vector-access changes improve a specific repeated-decoding mechanism, but have no new large-index performance result.

The next phase should turn the largest uncertainties into design choices and discriminating experiments. It should preserve working semantics, remove whole-index memory and publication dependencies, and establish how quality and useful work change with tree shape, working set and physical distribution. No positive or negative conclusion about one implementation should substitute for this design search.

This draft consolidates reviewed evidence and proposes a continuation plan. The proposed priorities and success gates are new recommendations; the underlying experiments retain their original criteria and outcomes. The report does not restart experiments, change the main thread’s goal or alter its automation state.

## The system being investigated

The design builds one logical search tree whose branches can move between workers without changing which vectors are eligible neighbors. A query follows representative vectors through the tree, chooses groups of candidate vectors and then scores a smaller candidate set accurately.

The main pieces are:

- **The logical tree** describes groups and their relationships. A representative vector, called a centroid, guides search through each group.
- **A leaf posting list** holds the vector references and approximate codes scored when search reaches a leaf.
- **An owner** controls a vector’s current membership and related mutable state. Each vector has one current owner. Ownership placement is separate from logical tree structure.
- **The construction view** exposes current memberships and structural edits to subsequent construction operations. Version checks prevent decisions from using obsolete membership information.
- **A saved query generation** is a fixed, internally consistent version of the tree and its data. An existing reader must continue seeing its generation while construction changes the current tree.

The intended construction cycle reserves memory headroom, inserts in parallel, balances in parallel and then redistributes existing branches to prepare for the next cycle. Capacity-driven movement belongs between those phases. Logical splits, merges and vector reassignment still occur as part of balancing and must remain visible when the reference algorithm requires them.

The ordinary reference is the comparison implementation. The dynamic treatment adds explicit ownership while preserving global discovery and tree semantics. In the measured three-arm study, the dynamic tree starts with one owner; the four-owner arm redistributes branches after construction phases. All owners run on the same machine. Saved query execution reads a global saved tree, so the owner labels describe construction provenance, not four-machine query execution.

## Progress against the five goals

| Goal | Established progress | What the next phase must resolve |
| --- | --- | --- |
| Build a working globally evolving tree POC. | Local construction exercises splits, merges, redistribution and complete saved membership checks. Connected vector-access tests verify ownership movement, later structural edits and old readers. | Demonstrate scalable identity, routing and publication boundaries, then verify those boundaries across physical workers. |
| Explain and improve construction cost. | Phase timings, work counts and concurrency measurements expose expensive balancing and ownership coordination. A fixed-request change removes repeated decoding for adjacent requests. | Reduce consequential coordination and whole-state work while preserving immediate visibility. Establish costs under representative depth, cache pressure and skew. |
| Establish comparable index quality. | The fixed-tree comparison meets its relative ±1 percentage-point criterion. Successful diagnostics identify the remaining loss before final candidate reranking. | Reach the chosen quality target with selective work on deeper trees and fresh evaluation data. Preserve the failed 95% result. |
| Improve query cost at matched recall. | Warm costs are measured at approximately comparable recall on 16K vectors, with explicit measurement boundaries. | Measure bytes, CPU, storage access and communication at useful operating points beyond a small warm index. |
| Establish a credible path to 100B. | Storage arithmetic and architectural requirements identify where the current design must change. | Connect implemented mechanisms and representative measurements to an explicit resource model. The model must support a practical operating envelope rather than merely list assumptions. |

The preceding independent assessment accepts a qualified research outcome for the measured scope. Lyon’s continuing objective requires further work on the unresolved column. The project should carry forward the accepted evidence without treating that assessment’s completion status as proof that a 100B design has been found.

## What was actually tested

| Study | Indexed vectors and dimensions | Queries or operations | Question tested | Main findings |
| --- | --- | --- | --- | --- |
| Matched construction and tree diagnostics | 16,384 × 768 | Reference, one local owner and four local owners; an older 100-query cohort for diagnostics. | Does ownership preserve membership, and where does construction time go? | Complete memberships pass. Build times are 1.266 / 5.472 / 7.476 s, respectively; balancing dominates dynamic construction. Different work counts prevent a causal slowdown claim. |
| Later four-bit posting quality comparison | The same three saved 16K trees | 32 calibration queries and 256 evaluation queries per arm; all three use the four-bit scorer. | Does more accurate candidate scoring remove losses, and how do the three trees compare? | Recall is 94.31% / 94.74% / 94.44%. The relative ±1 percentage-point criterion passes, but the absolute 95% target fails. Observed trimming and exact-return losses are zero; remaining misses occur in navigation. |
| Fixed-policy query-cost comparison | The same three saved 16K trees | 256 queries, one warmup pass and three retained timing passes per arm. | What does querying cost at the frozen, approximately comparable recall? | Mean native times are 4.459 / 4.727 / 4.425 ms. These are warm, small-index observations, not a causal ranking or distributed result. |
| Connected bounded-vector correctness | 1,024 × 32 | Two local owners; movement, splits, neighbor reassignment, merges, save and old-reader checks. | Does vector access remain correct when data exceeds its owned cache? | Saved memberships, vector/code contents and old readers pass. Review catches and repairs a first-registration retry bug. The central embedding map stays empty; total process memory is not bounded by this test. |
| Fixed-request reader reuse | Identical immutable vector handles spanning multiple storage roots | An 80-request sequence plus a 16-request alternating-root control. | Can adjacent same-root requests avoid repeated decoding without changing returned vectors? | Opens and decodes fall from 80 to 5; encoded returns fall from 964,128 to 60,258 B. The alternating-root control is unchanged. This is a mechanism gain, not a whole-build speedup. |

Three-arm values appear in reference / one-local-owner / four-local-owner order. The sections below explain the findings and their measurement limits in detail.

These studies answer different questions and use different code versions. The newest bounded-vector implementation has local correctness and access evidence. Its performance and quality have not been measured by rebuilding the older 16K comparison. Older large-run artifacts also exist, but their layouts and construction paths must be checked before borrowing any coefficient from them. A stopped or incomplete comparison supplies no paired result.

Sources: [construction review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/matched-pruning-16k-v1/independent-runtime-review.json>), [quality review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/code4-posting-quality-v1/repair-v2/independent-runtime-review.json>), [query-cost review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/code4-fixed-policy-query-cost-v1/repair-v2/independent-runtime-review.json>), and [source-version boundary](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-vector-batch-reuse-v1/EPOCH-BRIDGE.json>).

## Construction findings

The dynamic implementation has substantial overhead even with one owner. In the matched 16K study, balancing accounts for most of its elapsed build time, while four-owner redistribution adds another measurable cost.

| Observation | Reference | One local owner | Four local owners |
| --- | ---: | ---: | ---: |
| Whole build process wall time | 1.266 s | 5.472 s | 7.476 s |
| Insertion time | 0.101 s | 0.468 s | 0.668 s |
| Balancing time | 0.715 s | 4.558 s | 5.344 s |
| Redistribution time | Negligible | Negligible | 1.003 s |
| Splits / merges | 137 / 40 | 144 / 46 | 153 / 50 |
| Maximum tree depth | 6 | 5 | 6 |
| Internal nodes | 77 | 90 | 92 |

The phase rows are selected components, not a complete sum of the whole process. The trees are independently constructed and perform different amounts of work. These measurements show an overhead problem; they do not isolate a causal slowdown ratio or predict how the ratio changes with scale.

Ownership checks and serialized structural work are consequential targets. The one-owner arm records 392,835 current-membership callbacks, with about 0.277 seconds of summed service time and 6.176 seconds of summed caller spans. Those caller spans include waits and overlap, so they cannot be added to wall time or treated as CPU demand. Four active subtree worker contexts also do not mean four fully utilized cores: observed whole-process CPU divided by wall time is about 1.52, 1.36 and 1.26 across the three arms.

The practical lesson is to reduce the frequency and granularity of coordination where correctness permits. Moving an eligibility calculation closer to authoritative owner state may remove repeated requests. Prefetching a batch of membership versions and using it after intervening reassignment would change semantics. The right boundary must preserve when current membership becomes visible.

Some measured optimizations reduce calls without improving total build time. Grouped split-input validation retains such an adverse result, and live-prefix batching records substantial unused prefetched work. Those experiments argue for fixed-decision or identical-request comparisons when changing access and coordination, followed by a separate end-to-end measurement. They do not justify multiplying isolated gains into a build-time forecast.

Sources: [construction review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/matched-pruning-16k-v1/independent-runtime-review.json>) and [split-input filter review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/split-input-owner-filter-v1/independent-runtime-review.json>).

## Quality and query-cost findings

The latest quality comparison measures recall among the exact top 100 neighbors. It uses full-precision navigation and a four-bit approximate posting scorer, then exactly reranks 400 candidates and returns 100 results. The navigation widths differ slightly between arms, at 36, 38 and 36, to select approximately comparable operating points using the calibration cohort.

| Observation on 16,384 vectors | Reference | One local owner | Four local owners |
| --- | ---: | ---: | ---: |
| Evaluation recall at 100 | 94.31% | 94.74% | 94.44% |
| Mean posting rows scored per query | 6,047.6 | 6,435.4 | 6,052.4 |
| Mean native query time, warm | 4.459 ms | 4.727 ms | 4.425 ms |
| Mean client-observed time, warm | 5.468 ms | 5.741 ms | 5.447 ms |
| Mean missing true neighbors before reranking | 5.69 | 5.26 | 5.56 |

All 256 evaluation queries in each arm reject some internal branches, so the comparison exercises real tree pruning. However, queries still score posting rows equal to roughly 37–39% of the indexed vector count. Exactly reranking 400 candidates does not mean only 400 vectors were examined. This distinction is essential when interpreting the latency or projecting storage and computation needs.

The full quality protocol fails its absolute 95% requirement. All three evaluation means are below 95%, and none meets the required lower-confidence-bound test. Both dynamic-minus-reference paired intervals fit inside the predefined ±1 percentage-point relative margin. That supports a conditional relative comparison on these trees and queries; it does not turn the full protocol into a pass.

The remaining error is localized to candidate coverage during navigation. The four-bit posting experiment has zero observed losses from candidate trimming or exact return selection. An earlier focused diagnostic identifies ten true neighbors excluded by approximate candidate scores, and the more accurate scorer removes that observed class of loss in the later evaluation. This is actionable attribution, not evidence of an unavoidable navigation ceiling. Tree construction, routing decisions and search allocation remain possible improvement targets.

Clustering locality has a separate lower-bound diagnostic. On an older 100-query cohort, a ground-truth-informed selector needs mean physical row counts of about 4,449, 4,487 and 4,372 to cover at least 95 of the true top 100 neighbors using whole leaves. The selector knows where the answers are and pays no routing cost. Its numbers help separate leaf grouping from executable search; they are neither achievable latency measurements nor directly comparable to the later document-query cohort.

The warm timing comparison is descriptive. It uses one saved tree per arm, a fixed arm order, reused evaluation queries and warmed storage/provider caches. Retained vector-provider counters show cache hits and no vector fetch/decode activity, which does not imply zero total I/O or zero CPU work. The roughly 0.3 ms spread does not establish an architectural winner. The document-derived evaluation cohort excludes indexed IDs and exact vector duplicates, but semantic correlation and population representativeness remain limitations.

Future quality development must keep calibration and evaluation separate. The 256-query evaluation is consumed. It can inform an explicitly exploratory diagnosis, but a newly selected policy needs a fresh, frozen evaluation cohort for confirmation. The original 95% failure remains in the record.

Sources: [quality and inference review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/code4-posting-quality-v1/repair-v2/independent-runtime-review.json>), [fixed-policy cost review](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/code4-fixed-policy-query-cost-v1/repair-v2/independent-runtime-review.json>), [candidate-score diagnostic](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/candidate-trim-fidelity-v1/independent-runtime-review.json>), and [leaf lower-bound analysis](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/leaf-oracle-retained100-analysis.json>).

## What the bounded-vector implementation adds

The new construction path removes the need to retain all full-precision vector payloads in the central embedding map for its tested append-only workflow. It stages durable vectors in bounded batches, gives owners immutable content handles and fetches vectors through small owned caches. Saving forks the durable vector root instead of recreating the central embedding map. Routing directories and joined posting/version publication remain separate unresolved costs.

The 1,024-vector fixture deliberately crosses a 32-vector-per-owner cache. It exercises actual ownership movement followed by structural mutation, verifies all saved current memberships and vector/code contents, and keeps an older saved reader correct. Review catches and closes a real retry bug: a failed first registration could create current authority before the vector handle existed. The repaired path keeps the pending version local until registration succeeds.

The first bounded implementation exposes heavy repeated decoding. The accepted reuse change keeps one reader for consecutive handles from the same immutable root within a fetch batch of at most 16 handles. It drops that reader at a root change or batch end.

| Fixed-request observation | Separate reader per request | Reader reuse within a batch |
| --- | ---: | ---: |
| Opens and decodes for 80 requests | 80 | 5 |
| Encoded bytes returned | 964,128 B | 60,258 B |
| Returned vector payload | 10,240 B | 10,240 B |
| Peak accounted pins observed in that comparison | 12,416 B | 12,416 B |
| Opens and decodes for the 16-request alternating-root control | 16 | 16 |

This is a mechanism result on identical requests. It demonstrates reuse when adjacent requests share a storage root and no benefit when they do not. It is not a 16-fold whole-build speedup or a measurement of disk or network bytes.

The memory claim is narrower than a total memory bound. Reader pins are admitted against a 256 KiB accounted limit per context, with up to four contexts, and the check occurs after decoding. A deliberately small 1 KiB limit observes 9,664 bytes before rejection. Root indexes, provider buffers, caller arrays and process memory are excluded. The authored blocks use a 16 KiB layout. The next architectural work must account for these exclusions rather than describing the whole construction process as memory-bounded.

Sources: [connected-vector acceptance](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-owner-vector-access-v1/repair-v2/independent-focused-review.json>) and [batch-reuse acceptance](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-vector-batch-reuse-v1/independent-focused-review.json>).

## What the adverse conclusion means for design

The adverse conclusion applies to the present implementation and present evidence. It does not establish that hierarchical SPANN, global trees or dynamic ownership are fundamentally incapable of reaching 100B. The barriers have different meanings and should receive different responses.

| Barrier | Interpretation | Design response to investigate |
| --- | --- | --- |
| Native 32-bit vector IDs cannot represent 100B identities. | A definite representational limit; not an algorithmic impossibility. | Use checked 64-bit identities end to end, or a collision-free translation with explicit capacity bounds. |
| The coordinator retains per-vector directories and whole-tree state. | Memory and coordination grow with the full population. | Partition live ownership lookup and load routing metadata on demand. |
| Saving joins full posting/version state. | Publication can repeat whole-index work and create large transients. | Publish changed immutable pages and a coherent root, measuring actual reuse and ancestor rewrites. |
| Fine-grained membership calls and a structural lock dominate some construction work. | A measured local coordination bottleneck that could become worse with network latency. | Move useful eligibility work to authoritative owners and define smaller independent conflict domains where semantics allow. |
| The tested policy misses the recall target. | A failed operating point with an identified loss stage. | Investigate routing and tree quality using exploratory data, then confirm a selected policy independently. |
| Physical distribution, skew and deep-tree behavior are unmeasured. | Evidence gaps rather than demonstrated failure. | Use focused tests that isolate communication, overload and structural scaling conditions. |

Widening IDs is necessary but will not resolve centralized coordination. A small vector cache is useful but does not bound routing metadata or publication. Increasing search effort may raise recall but can defeat the intended query-cost advantage. The next design needs these properties to work together.

## The scale of the resource problem

Simple storage arithmetic already requires explicit partitioning. At 100B vectors with 768 dimensions, one full-precision copy occupies 307.2 TB in decimal units. The following table describes components of the retained experimental representation, not a recommended final layout.

| Component and assumption | Payload at 100B |
| --- | ---: |
| One full-precision vector copy | 307.2 TB |
| One one-bit posting code per live vector | 11.2 TB |
| One additional four-bit posting overlay per live vector | 39.6 TB |
| One 64-bit ID map | 0.8 TB |
| One persisted 32-bit scalar version per vector | 0.4 TB |
| Existing one-bit posting IDs and membership versions | 0.5 TB |
| Existing four-bit posting keys | 0.4 TB |
| **Partial component total** | **360.1 TB** |

The partial total excludes centroids, other identifier copies, page and index overhead, locators, stale entries, retained generations and staging. Widening existing 32-bit fields adds further bytes. The one-bit and four-bit representations coexist in this experiment; replacing one with another would be a new layout decision. The table is neither an upper bound nor a storage-capacity recommendation.

Tree shape creates another substantial metadata problem. At illustrative mean leaf occupancies of 256 or 128 and at most four children per internal node, even a minimally populated internal structure has approximately 521 million or 1.042 billion total nodes. One 768-dimensional full-precision centroid per node adds roughly 1.6 or 3.2 TB before links, versions and indexes. The minimum possible maximum leaf depth is 15 in these examples; skew can make it worse. Occupancy, branching and centroid representation are design variables, not constants that should be frozen from the 16K fixture.

Per-vector metadata also matters at this population. An assumed 16, 32 or 64 bytes for a locator entry totals 1.6, 3.2 or 6.4 TB before allocation and indexing overhead. This component may overlap fields in another proposed layout, so a final account must identify representations and avoid counting the same field twice. Every global scan and full-state copy needs explicit justification.

The next resource model should expose the conditions for success rather than produce a precise-looking runtime forecast:

- **Memory:** every owner must fit resident metadata, caches, active operations and transfer/publication overlap. Count coordinator state separately.
- **Construction:** the required deadline must exceed CPU, storage, network, serial-service and dependency-path lower bounds. Small-test time per vector does not supply those coefficients at 100B.
- **Maintenance and publication:** count actual new immutable objects and page writes after coalescing, not merely the number of logically changed IDs. Repeated changes to one logical page can allocate a new retained object each generation.
- **Queries:** count centroid evaluations, posting rows, exact candidates, repeated block fetches and dependent communication waves at the required recall. One remote round per tree level is not guaranteed when requests are bounded and dependencies require follow-up.
- **Load distribution:** maximum owner state and work determine feasibility. At 80% average capacity use, a 1.25× load skew consumes the entire nominal reserve before transfer overlap.

Sources: [storage components](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/100B-resource-accounting-extension-v2.json>), [tree arithmetic](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/100B-global-tree-arithmetic-v2.json>), and [corrected sensitivity model](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/100B-sensitivity-model-v2.json>).

## Proposed next phase

### Define the useful operating envelope

The next phase needs explicit quality, query-cost, construction and maintenance targets before choosing its architecture. Retain 100B vectors as the population objective. Decide the relevant dimensions and metric, required recall and result count, query throughput and tail latency, build deadline, update workload, memory/storage budget and acceptable recurring cost. The research spending allowance is a separate constraint; it is not a production economics target.

These choices can begin as labeled scenarios rather than invented commitments. For example, keep 768 dimensions and recall at 100 for continuity, while recording that the 95% operating point remains unmet. A design comparison should show which scenarios it serves and which it cannot. It should not silently relax recall, replication or global-tree semantics to make a benchmark pass.

### Build one connected architecture experiment

The highest-value implementation experiment should connect scalable identity, ownership lookup, bounded routing and incremental publication around the existing construction semantics. Each part should replace a named whole-index dependency. A local model of those interfaces can establish ordering and correctness before network execution, but must expose the requests and data movement that a physical implementation would require.

| Boundary | Candidate design, not yet demonstrated | Decisive check |
| --- | --- | --- |
| Stable identity and current ownership | Persist checked 64-bit IDs. Partition ownership lookup by stable identity independently of tree placement. Cache locations only with validated ownership epochs. | IDs above 2³² survive append, movement, save and query round trips without collision or truncation. No coordinator-wide per-vector lock or location map is required. |
| Ownership handoff | Stage immutable destination data outside the conflict guard, then revalidate and commit one ownership-epoch transition. Staged data is not active authority. | Source authority becomes invalid exactly when destination authority becomes valid; conflicting operations retry or observe the new epoch. Later construction sees the change immediately. |
| Global routing with bounded metadata | Load logical child references and centroids through a bounded cache. Keep live construction topology separate from pinned query generations. | On the same logical tree, cache eviction and branch movement preserve navigation, neighbor eligibility and tie order. Revalidate topology after storage waits. |
| Incremental query publication | Write changed data and metadata pages, preserve unchanged immutable objects and publish one complete generation root after the phase cut. | Old and new readers each see a coherent tree. Measure writes, reads, root metadata, change amplification and peak old/new coexistence over several phases. |

This experiment should include a deliberately concentrated workload and an oversized branch case. Report maximum and median owner state/work, required transfer reserve and the behavior when no destination has room. Backpressure is an acceptable explicit outcome; unbounded growth is not. Preserving logical identity during movement does not by itself guarantee balanced work.

The point is to validate the connected boundaries, not to implement production recovery, consensus or fleet management. The current prototype’s hard phase limit and other fixture caps must either remain explicit in the experiment or be replaced with bounded structures before they are used as scale evidence.

### Investigate selective query quality on structurally meaningful trees

The next quality question is how to preserve the required recall while visiting a sufficiently small part of a deeper tree. Start from the diagnosed navigation loss and separate three possible causes: the grouping of true neighbors into leaves, the route used to find those leaves, and the search effort allocated to competing branches. Use the exact-neighbor diagnostics and leaf lower bounds to distinguish them.

Candidate changes include different routing representations, adaptive allocation of search effort, and different branching or leaf occupancy. These are alternatives to evaluate, not promised improvements. Comparing globally coordinated per-level traversal with independent subtree searches also remains a design question; execution requires an implementation that preserves the stated result and accounting semantics. Larger branching can reduce dependent levels while increasing centroid work, and larger leaves can reduce metadata while increasing posting scans. Those tradeoffs must be measured at matched recall.

Choose a dataset size because it produces the required depth, pruning competition or working-set pressure. State whether crossing cache capacity is part of the question. Reuse retained larger-run evidence where its semantics match. Do not choose a size merely because it is the next round number, and do not run an automatic size or tuning sweep. The first broader end-to-end comparison should bind the new architecture’s code version and test reference, one-owner and multiple-owner arms under matched settings where affordable.

### Measure physical distribution before using local timings as a scaling model

A small physical test should exercise an actual cross-owner dependency, not merely run independent local jobs. It should measure requests, bytes, service time, queueing and sequential rounds for the connected operations established above. Keep capacity-driven branch movement distinct from logical reassignment, and count warmup and transfer costs separately.

The decisive question is whether added owners increase useful capacity without introducing a serial or hot-owner bottleneck that defeats the target operating envelope. Local actor timings cannot answer this. A useful result may reject a proposed coordination boundary and motivate another design; that rejection should advance the project rather than terminate it.

## Proposed evidence gates and stopping rules

The broader project succeeds when a concrete design has a working connected POC and a reviewed model that supports the chosen 100B operating envelope. It need not execute a full 100B build, but its central claims must be tied to measured mechanisms, justified scale-dependent assumptions and explicit margins. Essential unknowns cannot all be deferred to future implementation.

The next phase should use these gates:

1. **Correctness survives the new boundaries.** Verify one current owner, complete memberships, current vector/centroid visibility, stable identity and old-reader consistency across cache eviction, movement and later structural edits.
2. **Working state and maintenance costs are accounted for.** Expose all coordinator and owner state, decoded roots, buffers, publication copies and retained objects. Demonstrate how each scales; do not substitute cache payload for total memory.
3. **Quality reaches a prespecified operating point on fresh evaluation data.** Match construction inputs and relevant settings, record seed and scheduling variability, and compare query work and cost within an explicit recall tolerance. A missed target triggers diagnosis or redesign rather than retroactive criteria changes.
4. **Communication and skew have measured consequences.** Show the important cross-owner path and concentrated workload, with request waves, serial service and maximum owner demand included.
5. **The 100B model closes within the chosen resource envelope.** Combine the measured mechanisms and explicit assumptions without multiplying unrelated kernel gains. Show sensitivity to occupancy, depth, cache misses, update frequency, retention and skew. Identify which thresholds would invalidate the design.

An individual experiment stops at its frozen runtime, cost or scientific decision boundary. A design branch can be rejected when its bottleneck or tradeoff contradicts the chosen operating envelope. The project continues by changing that design or testing a justified alternative. A negative result about the current implementation is insufficient by itself to declare the search for a 100B design complete.

## What to carry forward and what to avoid repeating

Preserve the ordinary reference, immutable inputs and saved query trees, full-census and generation-visibility tests, phase/work counters, stage-level recall diagnostics, fixed-request access comparisons and reviewed resource accounting. They provide a stronger baseline than another uninstrumented build.

Keep failures and negative results visible. These include the missed absolute quality target, call-count reductions without wall-time gains, rejected oversized allocations, the first-registration retry bug, and build/controller/analyzer failures preserved with their repairs. Distinguish infrastructure or harness failures from scientific outcomes. Avoid repeating accepted reviews or tests unless the relevant code, claim or condition changes.

Preserve the two main evidence boundaries. The 16K saved-tree quality and timing results belong to their frozen construction and query versions. The newest vector-access code has separate local tests. A claim about a combined implementation requires a combined measurement; neither receipt silently substitutes for the other.

## Resources and reproducibility

The latest retained lifecycle receipt records the task driver as stopped, with disks and evidence retained. No fresh cloud inventory is performed for this report. The reconciled covered compute upper bound is approximately $2.20, and the retained new storage commitment is $5.12 per month. These are scoped research figures, not a whole-account bill or the full cost of the project’s history. Storage continues while retained.

Future execution remains subject to the saved budget, sandbox identity, protected/excluded resources and independent shutdown guards. Existing scale and upload authorization should be read from the saved records rather than requested again for each test. New work should preserve the immutable inputs, failed artifacts and staged repository work.

The implementation checkout is [the isolated research worktree](</private/tmp/chroma-hierarchical-spann-sharding>). The latest accepted local source archive is [the batch-reuse source package](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-vector-batch-reuse-v1/source.tar.gz>), SHA-256 `c5684d1ffdd6a534a169fb0a4e8b0faf326b15613f39da58d1620782227b85f6`. The [handoff](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-vector-batch-reuse-v1/HANDOFF.json>) and [independent acceptance](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/bounded-vector-batch-reuse-v1/independent-focused-review.json>) bind its source and tests. This archive is a reproducibility anchor, not a deployment-ready release.

The [prior final assessment](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/FINAL-RESEARCH-ASSESSMENT.json>) and [acceptance receipt](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/final-research-completion-acceptance.json>) preserve the reviewed checkpoint. The [spending ledger](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/SPEND-LEDGER.json>) and [last stopped-window receipt](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/fleet/code4-fixed-policy-query-cost-v1/final-receipt.json>) provide the resource record. The [current plan](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/CURRENT-PLAN.md>) and [continuation state](</Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/CONTINUATION-STATE.json>) retain the main thread’s checkpoint; adopting this next-phase proposal into those control documents is a separate action.
