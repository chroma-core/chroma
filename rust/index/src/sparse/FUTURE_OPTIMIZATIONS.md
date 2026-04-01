# Future MaxScore Optimizations

Observations from reviewing SereneDB's MaxScore WAND implementation (PR #337).
These are ordered by expected impact. Revisit after blockfile read path investigation.

## 1. Replace HashMap with flat candidate buffers — ✅ DONE

Uses a flat `Vec<f32>` accumulator indexed by `(doc_id - window_start)` with 64K-wide
windows.  `drain_essential()` writes directly into the accumulator with O(1) access.
A separate `doc_set: Vec<u32>` tracks non-zero slots for enumeration.  No HashMap,
no sorting, no merge-dedup.

## 2. Budget-based candidate pruning between non-essential terms — ✅ DONE

`remaining_budget` is computed from non-essential block upper bounds.  Before each
non-essential term, `doc_set.retain(|doc| accum[doc] > threshold - remaining_budget)`
prunes candidates whose accumulated score + remaining budget can't beat threshold.

## 3. Adaptive window sizing — SUPERSEDED

The old narrow-window approach (window_end = min block boundary) needed adaptive sizing
to avoid many tiny empty windows.  With 64K-wide windows, even sparse doc-ID regions
are covered in a single iteration.  At 9M docs, there are only ~137 windows total.

## 4. Sort non-essential terms by score/cost ratio

**Impact: Medium** — better pruning order means more candidates eliminated early.

Currently we sort non-essential terms by `max_score` ascending. Sorting by
`max_score / posting_list_length` (score-to-cost ratio) instead means we process
high-impact short lists first, maximizing the chance of early candidate elimination.

## 5. "Required" non-essential compaction

**Impact: Medium** — when there's 1 essential term, identify non-essential terms
whose contribution is REQUIRED for any candidate to beat threshold.

Walk backward from the essential partition: a non-essential is "required" if
`total_score_without_it < threshold`. When `ScoreCandidates` processes a required
term, compact the candidate buffer — remove candidates that don't appear in that
term's posting list. This tightens the candidate set more aggressively than just
adding scores.

## 6. Skip score decoding for stale blocks — ✅ DONE

`drain_essential()` and `score_candidates()` skip blocks where
`dir_max_offsets[block_idx] < window_start` without decompressing them.

## 7. Root/non-Root specialization (compile-time)

**Impact: Low** — eliminates branch overhead in inner loops.

When cursors are used inside the MaxScore outer loop, they don't need their own
threshold-aware seek logic (the outer loop handles pruning). A `Root=false` variant
of the cursor could strip out score comparisons during seek/advance. SereneDB uses
a template bool parameter for this.

## 8. Deferred skip reader initialization — ✅ DONE

Lazy cursors (`open_lazy`) only load the directory block; data blocks are loaded
on demand.  The 3-batch I/O pipeline defers non-essential block loading until
the threshold stabilizes.

## 9. SIMD-accelerated budget pruning

**Impact: Medium** — vectorize the `doc_set.retain` cutoff comparison using
NEON/AVX2 `vcmp` + movemask, as SereneDB does with `FilterCompetitiveHits`.
Currently this is scalar.

## 10. Fused dequant+scoring in drain_essential — ✅ DONE

`drain_essential()` now reads u8 quantized weights directly from raw bytes,
fusing the dequantization scale and query_weight into a single `factor`.
This eliminates `decompress_values_into` (no `value_buf` write+read) and
saves one f32 multiply per entry.  Works for View, Lazy, and Eager paths.

## 11. Block-max pruning for non-essential `score_candidates` — NOT NEEDED

`score_candidates()` has a `min_block_score` parameter that can skip blocks
before decompression.  However, the Batch 3 lazy I/O pipeline already
prunes non-essential blocks at the I/O level (blocks below threshold are
never loaded → `ensure_forward_block` returns false).  The only gap is
blocks that were loaded but became irrelevant as the threshold increased
during later windows.  Currently passing `min_block_score = 0.0`, which
has no real effect.  To make this useful, would need per-block
`min_block_score = threshold - max_candidate_score - remaining_budget`.
