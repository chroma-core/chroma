# Independent review requirements

This checklist defines what another reviewer must establish before accepting the batched epoch. It is not a review receipt, hardware admission or claim that CUDA arithmetic has passed. The frozen source and CPU test outputs are identified in `manifest.json`; any changed source requires a new freeze and review.

## Source and input identity

- Verify every new source file against the frozen manifest and archive. Verify all four preserved numeric dependencies against the completed GPU epoch rather than assuming the current checkout is equivalent.
- Trace the actual imports and executed method bodies, including the unchanged CUDA scoring and transfer methods called by the new adapter. A future launcher must require the complete exact producer set and executed paths; a nonempty subset of hashed files is insufficient.
- Require the future wrapper, runtime lock, actual CUDA/PyTorch flags, driver version, prepared manifests, canonical source chunks and query bytes to be pinned separately. Confirm the 1M corpus is the exact prefix of the same 10M bytes, not another routed copy.
- Verify complete source row counts and exact query identity coverage before CUDA initialization. Require all frozen 1,000 calibration and 10,000 test IDs to remain distinct and disjoint from the 200 integration queries. Do not treat complete embeddings as complete exact references.

## Mathematical and numerical contract

- Prove the running threshold independently for each query. Every current-block upper endpoint must enter its own kth-upper computation before selection. A processed prefix smaller than k must retain every row.
- Verify inclusive lower-endpoint comparison and uncapped retention. All cutoff ties must survive, and CPU exact merging must order equal distances by stable global ID. Discarding a processed row must follow the conservative interval argument rather than an approximate rank.
- Check that vectorizing the norm-product bound preserves the reviewed error envelope. Query/vector norms are symmetric in the Cauchy-Schwarz product; both norm calculations still receive the existing upward inflation and finite/overflow checks before scoring.
- Verify float64 errors, endpoints, thresholds and top-k values without narrowing. Require endpoint bit and complete running-history comparisons around zero, cancellation, changing bounds, adjacent representable cutoffs and batches of one, three and 32 queries.
- Verify normal-or-zero finite FP32 inputs, no overflow, strict accumulation, disabled TF32/reduced precision and the internal low-precision adversarial probe. Runtime settings and random agreement alone do not establish this arithmetic contract.
- Require the designated full-score queries to bypass filtering and check every corpus row, including rejected rows. Separately compare their final answers against independent full-prefix float64 scoring; the filter cannot certify its own outputs.

## Coverage and bounded workspace

- Check dense chunk continuity, exact file sizes, declared total rows, unique query identities and partial query-batch tails. Each retained coordinate must be in range, unique and in row-major order before CPU consumption.
- Verify that the source block uploads once, query-batch size never exceeds 32, persistent upper state has at most k values per query, and only one query's retained vectors are materialized for CPU reranking. All-tie coordinate output must remain uncapped.
- Recompute the device allowance for actual dimension/block/query/k settings. Check simultaneous float64 endpoint/top-k/concatenation state and worst-case coordinate matrices. Treat the extra workspace allowance as a planning estimate, not a vendor allocation bound.
- Require projected memory below half currently free device memory, actual allocator and physical device peaks, bounded host RSS and explicit mapping closure. An OOM must preserve a failed report and abort the pool; it must never cause candidate trimming.

## Tests and future execution

- Independently rerun the CPU oracle, preserved-engine comparison, signed envelope-error, all-tie, cancellation, low-precision, subnormal, overflow, prefix-smaller-than-k, chunk-tail and query-tail tests. Add an independent adversarial case rather than only reading author tests.
- Keep hashing/staging, runtime initialization, arithmetic probes, full-score CPU validation, scoring, filter-plus-coordinate-transfer and CPU reranking in explicit timing boundaries. Host wall segments may include synchronization; they are not automatically kernel timings.
- Require an immutable new launch job, exact sandbox identity, concrete source/request/disk/RSS limits and an independent absolute 60-minute stop. Restore only private S3/public canonical inputs; no timed benchmark worker supplies traffic.
- Compare all 200 preserved CPU answers on both corpora before considering new pools. Measure fixed validation separately from incremental query costs. Larger pool admission uses measured conservative rates and the unchanged preservation reserve, not the tiny matrix kernel time.
- Publish only a complete pool with independent oracle checks, exact corpus/query coverage and version-bound output restoration. Preserve every failed attempt, retain explicit pending states, and verify new GPU termination and root deletion after evidence preservation.
