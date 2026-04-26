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
accumulator array. A companion 64-word bitmap (`[u64; 64]`, one bit per
slot) tracks which slots were touched.

**Why a bitmap?** After draining, we need to collect the touched slots
into sorted candidate arrays and later reset only those slots to zero.
Scanning the bitmap is more efficient than a full scan of the window —
the bitmap (`[u64; 64]`) makes both operations proportional to the number
of candidates rather than the window width.

```text
  window_start = 8192        window_end = 12287
  ┌─────────────────────────────────────────────────────────────────┐
  │                   accum[0..4095]  (f32 × 4096)                  │
  │  [0.0] [0.0] [0.72] [0.0] [0.0] [1.05] [0.0] [0.0] [0.31] ...│
  └───┬──────┬─────┬──────┬─────┬──────┬─────┬──────┬─────┬────────┘
      │      │     │      │     │      │     │      │     │
      0      1     2      3     4      5     6      7     8  ← slot index
                                                              (doc = slot + 8192)
  ┌──────────────────────────────────────────────────────┐
  │              bitmap[0]  (u64, bits 0..63)             │
  │  ...0 0 1 0 0 1 0 0 1 0 0 ...                        │
  │        ↑     ↑       ↑                                │
  │      bit 8  bit 5  bit 2                              │
  └──────────────────────────────────────────────────────┘

  drain_essential (term "cat", qw=1.0):
    cursor has doc 8194 (val 0.36) → slot 2: accum[2] += 1.0 × 0.36
                                              bitmap[0] |= (1 << 2)
    cursor has doc 8197 (val 0.53) → slot 5: accum[5] += 1.0 × 0.53
                                              bitmap[0] |= (1 << 5)

  drain_essential (term "food", qw=0.5):
    cursor has doc 8194 (val 0.72) → slot 2: accum[2] += 0.5 × 0.72  (now 0.72)
    cursor has doc 8200 (val 0.62) → slot 8: accum[8] += 0.5 × 0.62
                                              bitmap[0] |= (1 << 8)

  candidate extraction (bitmap walk):
    word 0 = ...101000100₂
    trailing_zeros() → bit 2 → cand_docs=[8194], cand_scores=[0.72]
    clear bit, trailing_zeros() → bit 5 → cand_docs=[8194,8197], ...
    clear bit, trailing_zeros() → bit 8 → cand_docs=[8194,8197,8200], ...
    word 0 = 0 → next word ...

  selective reset:
    same walk zeros accum[2], accum[5], accum[8]; clears bitmap words
```

- **Candidate extraction**: iterate 64 words; for each non-zero `u64`,
  `trailing_zeros()` pops set bits in constant time, yielding only the
  occupied slots in doc-id order.
- **Selective reset**: after the window is scored, the same set-bit walk
  zeros only the touched `accum` entries and clears the bitmap words,
  avoiding a full 4096-entry `memset`.

When the window is densely filled (many essential terms, dense posting
lists) the bitmap scan approaches a linear sweep — no worse than brute
force. When it's sparsely filled (common for high-dimensional SPLADE
vectors where each term covers a small fraction of documents) the
savings are significant.

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

---

## Worked Example

A small end-to-end trace showing every step. We search for
`query = {cat: 1.0, food: 0.5, cute: 0.3}` with `k = 2`.

### Index contents

```text
doc 0: {cat: 0.9, cute: 0.4}
doc 1: {food: 0.8}
doc 2: {cat: 0.5, food: 0.6, cute: 0.7}
doc 3: {cat: 0.2, cute: 0.1}
doc 4: {food: 0.3}
```

Posting lists (one block each, all fit in a single window):

```text
dim "cat":   [(0, 0.9), (2, 0.5), (3, 0.2)]   dim_max = 0.9
dim "food":  [(1, 0.8), (2, 0.6), (4, 0.3)]   dim_max = 0.8
dim "cute":  [(0, 0.4), (2, 0.7), (3, 0.1)]   dim_max = 0.7
```

### Step 0 — Setup

Open cursors. Compute `max_score = query_weight × dim_max`:

```text
term   qw    dim_max   max_score
cute   0.3   0.7       0.21
food   0.5   0.8       0.40
cat    1.0   0.9       0.90
```

Sort ascending by max_score: `[cute=0.21, food=0.40, cat=0.90]`.
Heap is empty, `threshold = -∞`.

### Window [0, 4095] — the only window needed

#### Step 1 — Partition

All blocks fall in this window, so `window_score = max_score` for each
term. Prefix sums:

```text
         cute    food    cat
score    0.21    0.40    0.90
prefix   0.21    0.61    1.51
                  ↑
            threshold = -∞, so prefix ≥ threshold immediately at cute
```

Threshold is `-∞`, so every prefix sum ≥ threshold — the split is at
index 0. **All three terms are essential** (this is typical for the first
window before any candidates enter the heap).

#### Step 2 — Phase 1: Drain essential terms

Process each essential term, accumulating into `accum[]`:

```text
drain cute (qw=0.3):
  doc 0, val 0.4 → slot 0: accum[0] += 0.3 × 0.4 = 0.12    bitmap set bit 0
  doc 2, val 0.7 → slot 2: accum[2] += 0.3 × 0.7 = 0.21    bitmap set bit 2
  doc 3, val 0.1 → slot 3: accum[3] += 0.3 × 0.1 = 0.03    bitmap set bit 3

drain food (qw=0.5):
  doc 1, val 0.8 → slot 1: accum[1] += 0.5 × 0.8 = 0.40    bitmap set bit 1
  doc 2, val 0.6 → slot 2: accum[2] += 0.5 × 0.6 = 0.30    (now 0.51)
  doc 4, val 0.3 → slot 4: accum[4] += 0.5 × 0.3 = 0.15    bitmap set bit 4

drain cat (qw=1.0):
  doc 0, val 0.9 → slot 0: accum[0] += 1.0 × 0.9 = 0.90    (now 1.02)
  doc 2, val 0.5 → slot 2: accum[2] += 1.0 × 0.5 = 0.50    (now 1.01)
  doc 3, val 0.2 → slot 3: accum[3] += 1.0 × 0.2 = 0.20    (now 0.23)
```

Accumulator state:

```text
slot:   0      1      2      3      4
accum:  1.02   0.40   1.01   0.23   0.15
bitmap: 1      1      1      1      1     (bits 0-4 set)
```

#### Candidate extraction (bitmap walk)

Walk set bits → build sorted candidate arrays:

```text
cand_docs   = [0,    1,    2,    3,    4   ]
cand_scores = [1.02, 0.40, 1.01, 0.23, 0.15]
```

#### Step 3 — Phase 2: Non-essential merge-join

No non-essential terms this window (all were essential), so this phase
is skipped.

#### Step 4 — Phase 3: Heap extraction (k=2)

Walk candidates, pushing into the min-heap:

```text
doc 0, score 1.02 → heap [1.02]               (heap not full)
doc 1, score 0.40 → heap [0.40, 1.02]         (heap full, threshold = 0.40)
doc 2, score 1.01 → 1.01 > 0.40 → push, pop min
                     heap [1.01, 1.02]         (threshold = 1.01)
doc 3, score 0.23 → 0.23 < 1.01 → skip
doc 4, score 0.15 → 0.15 < 1.01 → skip
```

#### Reset

Bitmap walk zeros `accum[0..4]`, clears bitmap. Ready for next window
(none remain).

### Final result

Drain heap, sort descending:

```text
rank 1: doc 0, score 1.02
rank 2: doc 2, score 1.01
```

Verify by brute force:

```text
doc 0: 1.0×0.9 + 0.3×0.4           = 1.02  ✓
doc 1: 0.5×0.8                       = 0.40
doc 2: 1.0×0.5 + 0.5×0.6 + 0.3×0.7 = 1.01  ✓
doc 3: 1.0×0.2 + 0.3×0.1           = 0.23
doc 4: 0.5×0.3                       = 0.15
```
