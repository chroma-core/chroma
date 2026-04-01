# Future MaxScore Optimizations

Observations from reviewing SereneDB's MaxScore WAND implementation (PR #337).
These are ordered by expected impact. Revisit after blockfile read path investigation.

## 1. Replace HashMap with flat candidate buffers

**Impact: High** — eliminates per-entry hash overhead in the hot loop.

- **Single-essential path** (most common after threshold stabilizes): drain the one
  essential iterator into a flat `Vec<(u32, f32)>`, then merge-join non-essentials
  against this sorted buffer. No HashMap, no hashing.
- **Multi-essential path**: use a fixed-size bitmask + flat score array
  (e.g. `[u64; 64]` mask + `[f32; 4096]` scores) to merge essential terms in a
  window, then drain the bitmask into a candidate buffer for non-essential processing.

SereneDB uses `FixedBuffer<doc_id_t, 4096>` and `FixedBuffer<score_t, 4096>` for
candidates, and `uint64_t _mask[64]` + `score_t _scores[4096]` for the multi-essential
bitmask window.

## 2. Budget-based candidate pruning between non-essential terms

**Impact: High** — progressively shrinks the candidate set so later non-essential
terms do less work.

Before each non-essential term processes candidates, compute
`remaining_budget = sum of max_scores of all subsequent non-essential terms`.
Filter out candidates whose `current_score + remaining_budget <= threshold`.
This can be vectorized (AVX2/NEON `vcmp` + movemask).

SereneDB's `FilterCompetitiveHits` does exactly this with `_mm256_cmp_ps`.

## 3. Adaptive window sizing

**Impact: Medium** — avoids processing many tiny near-empty windows in sparse regions.

Track the number of candidates found across recent windows. When candidates are sparse,
grow the minimum window size (up to some max like 4096 docs). Reset when candidates
become dense again. SereneDB doubles `_min_window_size` each time candidates are below
a threshold, capped at `kWindow`.

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

## 6. Skip score decoding for stale blocks

**Impact: Low** — saves a small amount of work during skip-list traversal.

When advancing a non-essential cursor into a window, tell the skip reader to skip
decoding block-level scores for blocks whose max_offset is behind `window_start`.
SereneDB's `SetSkipWandBelow(window_max)` does this.

## 7. Root/non-Root specialization (compile-time)

**Impact: Low** — eliminates branch overhead in inner loops.

When cursors are used inside the MaxScore outer loop, they don't need their own
threshold-aware seek logic (the outer loop handles pruning). A `Root=false` variant
of the cursor could strip out score comparisons during seek/advance. SereneDB uses
a template bool parameter for this.

## 8. Deferred skip reader initialization

**Impact: Low** — avoids paying skip reader setup cost for terms that MaxScore
ends up skipping entirely. Only initialize the skip reader on first actual seek.
