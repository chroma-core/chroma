# BlockMaxMaxScore — How the Query Works

This document walks through the query algorithm implemented in `maxscore.rs`.
It is a *windowed, block-max* variant of the classic **MaxScore** algorithm
(Turtle & Flood, 1995) adapted for our blocked posting-list layout.

## Background

A sparse vector query computes the dot-product between a query vector and every
stored document vector. Naively this means touching every document for every
query dimension — far too slow at 100M+ documents. MaxScore avoids this by
proving that large swaths of documents *cannot* make the top-k, and skipping
them entirely.

## Data Layout

Each dimension's posting list is split into fixed-size **blocks** of
`(offset, weight)` entries (default 1024 per block), sorted by document offset.
Alongside the blocks a **directory block** stores per-block metadata:

```
Dimension 42
├── Block 0: [(off=0, w=0.3), (off=5, w=0.9), ...]   max_offset=127, max_weight=0.9
├── Block 1: [(off=130, w=0.1), ...]                   max_offset=255, max_weight=0.6
└── Directory: max_offsets=[127, 255], max_weights=[0.9, 0.6]
```

The `max_weight` per block is the key to skipping: it lets us compute a tight
upper bound on how much any document *in that block* can contribute to a score,
without decompressing the block.

## Algorithm Outline

```
for each window of 4096 doc-IDs:
    1. Partition query terms into ESSENTIAL vs NON-ESSENTIAL
    2. Drain essential terms into an accumulator       (Phase 1)
    3. Merge-join non-essential terms with candidates   (Phase 2)
    4. Push surviving candidates into a top-k min-heap  (Phase 3)
```

### Step 0: Setup

```
Open a PostingCursor for each query dimension that exists in the index.
Sort terms by max_score = query_weight × dimension_max (ascending).
Initialize a min-heap of size k and threshold = -∞.
```

### Step 1: Essential / Non-essential Partition

For the current window `[start, start+4095]`, recompute each term's
**window upper bound** — the max block-level weight across blocks overlapping
this window, multiplied by the query weight.

Re-sort terms by window score (ascending). Walk from smallest to largest,
accumulating a prefix sum. The first term whose prefix sum ≥ `threshold`
becomes the split point:

```
terms (sorted by window_score):
  [  t_A=0.1,  t_B=0.2,  t_C=0.4,  t_D=0.8  ]
              prefix sums:  0.1   0.3   0.7   1.5
                                         ^
                                    threshold=0.6
                            ──────────── ──────────
                            non-essential  essential
```

**Essential** terms (right of the split) *might* push documents into the top-k
on their own, so we must score every document they contain. **Non-essential**
terms (left of the split) are too weak — even their maximum possible
contribution combined can't promote a zero-score document above the threshold.

### Step 2: Phase 1 — Drain Essential Terms

Each essential term's cursor walks its blocks within the window, writing
`accum[doc - window_start] += query_weight × value` into a flat 4096-slot
accumulator array. A companion 64-word bitmap tracks which slots were touched.

This is a pure sequential scan — no random access, very cache-friendly.

### Step 3: Phase 2 — Non-essential Merge-Join

Extract candidates from the bitmap into sorted `cand_docs[]` / `cand_scores[]`
arrays. Then process non-essential terms from **strongest to weakest**:

1. **Budget pruning**: compute the remaining non-essential budget (sum of
   window scores of unprocessed terms). Any candidate whose current score +
   budget ≤ threshold is eliminated via `filter_competitive`.
2. **Merge-join**: the term's cursor does a two-pointer merge against
   `cand_docs`, adding `query_weight × value` to matching entries.
3. Subtract this term's window score from the budget.

Terms with `window_score = 0` are skipped. If all candidates are pruned, the
remaining non-essential terms are skipped entirely.

### Step 4: Phase 3 — Heap Extraction

Walk the surviving `cand_docs` / `cand_scores`. Push any candidate that beats
the threshold (or the heap isn't full yet) into the min-heap. If the heap
overflows past `k`, pop the minimum. Update the threshold from the new minimum.

Finally, bitmap-guided zeroing resets only the touched accumulator slots (not
all 4096), keeping per-window cleanup O(touched) instead of O(window).

### Repeat

Advance `window_start` by 4096 and loop. After all windows, drain the heap
and sort descending by score.

## Why It's Fast

| Technique | Effect |
|---|---|
| Window accumulator | Dense array + bitmap avoids hash-map overhead |
| Essential/non-essential split | Weak terms skip most documents entirely |
| Per-window repartition | Split adapts as threshold tightens |
| Budget pruning | Candidates are eliminated *before* scoring weak terms |
| Block-max upper bounds | Entire blocks are skipped when their max is too low |
| Bitmap-guided cleanup | Only touched slots are zeroed per window |
