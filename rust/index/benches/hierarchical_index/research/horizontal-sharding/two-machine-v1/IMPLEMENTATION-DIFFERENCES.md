# Differences to account for in the two-machine comparison

The distributed comparison measures a new owner runtime against the optimized unsharded writer. A difference in elapsed time is not automatically the cost of sharding. The table distinguishes necessary communication from implementation choices that may miss a reference optimization. Both 100K workloads have validated reference and distributed results. The 3M reference validates; the distributed 3M result remains gated on a successful complete build, saving and fresh-reader validation.

| Component | Current distributed behavior | Reference behavior and interpretation |
| --- | --- | --- |
| Insertion navigation | The coordinator runs the configured insertion workers. Each worker navigates batches of up to 64 vectors with globally selected per-level candidates and remote owner expansion when needed. | The reference navigates directly in one process. Global selection is required, but RPC framing, grouping and coordinator work need separate accounting. Two owner machines do not imply two full insertion-navigation worker pools. |
| Posting-code preparation | One append job per destination leaf fetches full vectors and generates one-bit codes sequentially before taking the leaf lock. Worker contributions are combined across the full logical input batch. | The reference prepares codes in insertion-worker buffers before merging them into each leaf. A single initial leaf therefore exposes a potential missing parallel preparation optimization in the distributed runtime. Measure this stage before attributing its latency to sharding. |
| Duplicate detection | The append endpoint checks new IDs within its request and scans existing leaf IDs for collisions. Separate ingestion-level duplicate detection tracks inserted ID ranges because an ID can route to a different leaf after balancing. | Duplicate-ID detection is separate from ownership and membership versions. The leaf scan can cost the product of old and new posting lengths. Retain correctness, then measure whether this check duplicates the ingestion guarantee. |
| Balancing | Each owner has its configured worker allowance. Individual leaf and internal jobs are dynamically scheduled, and shared-tree changes require coordinated publication. | The reference has one local worker pool. Compare total allocated cores and CPU as well as elapsed time. Publication, transfer and reservation work may be necessary; avoid treating extra cores as an algorithmic efficiency gain. |
| Full vectors | Both owners independently cache a verified immutable corpus, with a bounded preload budget. Ownership applies to current postings; cached full-vector bytes are duplicated. | The reference also maintains and saves its embedding store. Report corpus verification, warming, persistence and cache memory separately from construction. Different persistence paths must not be counted as a sharding speedup. |
| Saved readers | A combined immutable generation references each owner's routing and posting objects. Readers open routing metadata and load posting payloads on demand. | Lazy posting access follows the intended reference behavior. The manifest protocol adds distributed coverage and publication work, which must be timed separately. |

The initial investigation order after the required correctness gates is to compare phase times and work counts, locate any material insertion preparation or balancing delay, and then attribute communication, waiting and extra computation. The current request ends after the specified comparisons and report; this list does not automatically authorize another optimization campaign.

Implementation references: `distributed/construction.rs` contains insertion and scheduling; `distributed/engine.rs` contains owner append and protocol handlers; the unsharded `writer/writer.rs` contains buffered insertion and code generation. Freeze source hashes before executing the comparison because the runtime remains under development.

## Work-count interpretation

The distributed transferred-vector counter increments at the initiating owner after all endpoint completion and release acknowledgements succeed. It counts rows moved in completed transfer payloads, rather than protocol messages, retries or unique vector IDs across the entire build. A row moved twice contributes twice. The reference transferred-vector count also requires interpretation alongside its batch and retry counters; different trees and schedules can generate different reassignment work.

The failed epoch4 report attempt records 115,987 transferred rows, 6,637,498 navigation distances and 69 final leaves before placement. The valid standard reference records 53,555 transferred rows, 5,405,317 navigation distances and 72 leaves. These observations suggest additional algorithmic work is a leading explanation to investigate, but independent construction decisions prevent a pure causal attribution to protocol overhead. The epoch4 balance phase transfers only 149 framed application bytes to the empty peer, so private-network throughput cannot explain that initial balance duration. The failed attempt remains excluded from validated build-time comparisons.


## Measurement gaps to resolve before finer attribution

Distance and expansion counters cover owner-node expansion but omit distance work performed directly over cached shared-upper-tree centroids. They undercount total navigation after handoff. Instrument that path before comparing complete distance work across distributed construction phases. Existing phase CPU, elapsed time and framed network byte measurements remain useful at their stated scope.

Peak active jobs count admitted work, including waiting jobs; they do not establish effective CPU parallelism. Report summed process CPU divided by phase wall time as an approximate average number of busy cores, with the snapshot bookkeeping caveat, and use profiles to distinguish waiting from serial computation before changing scheduling.

## Leading owner-local balancing hypotheses

The [reservation-history correction](local-tests/RESERVATION-HISTORY-RESULT.md) separates active conflicts from retained replay outcomes. The deterministic diagnostic confirms that completed history no longer adds conflict-record visits. Its end-to-end timing contribution remains uncertain because independently constructed trees perform different work. The [source attribution review](local-tests/BALANCING-COST-SOURCE-REVIEW.md) preserves the prior source findings; repeated posting compaction, transfer bookkeeping and navigation allocations remain profiling candidates.

Posting reassignment compacts the source separately for each destination group and uses the full endpoint protocol for owner-local transfers. The reference batches destinations and compacts the source once. Measure groups, source entries scanned and copied, and completed transferred rows. Preserve transfer ordering and unique membership when evaluating a faster local path.

Navigation uses immutable routing images and does not load posting lists. The benchmark preloads full vectors, so a resident fetch does not decode from disk. Query-buffer copies, reply copies and validation remain possible CPU costs. These are static findings and hypotheses; no sampled profile assigns their individual latency contributions yet.
