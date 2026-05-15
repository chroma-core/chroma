# Full-Text Bitmap Index

Word-based full-text substring search using hashed token buckets with roaring bitmaps.

## Architecture

- **Tokenizer** (`tokenizer.rs`): Decomposes documents and queries into numeric keys. Owns the linguistic pipeline (word splitting, lowercasing, ASCII folding, short/long filtering) and hash mapping (murmur3 → bucket IDs). Produces `DocumentTokens` for indexing and `QueryPlan` for search. The index receives fully resolved keys and never performs string analysis.

- **Writer** (`bitmap_index.rs`): Accumulates per-batch deltas and writes bucket, trigram, and transition bitmaps to a single blockfile. Takes pre-computed `DocumentTokens` from the tokenizer.

- **Reader** (`bitmap_index.rs`): Executes a `QueryPlan` through a 2-stage pipeline and returns candidate doc IDs as a roaring bitmap. The result is an over-estimate — brute-force verification (Stage 3) is the caller's responsibility.

## Blockfile Layout

A single blockfile typed as `(prefix: &str, key: u32) → RoaringBitmap` with three logical partitions:

1. **Token buckets** — `prefix=""`, `key ∈ [0, 2^24)`. Each token is hashed to a bucket ID. The bitmap stores doc IDs containing tokens in that bucket.

2. **Transitions** — `prefix=""`, `key ∈ [2^24, 2^26)`. For each adjacent token pair, the boundary characters are hashed. Two entries per transition: `key = hash | (1 << 24)` for doc-ID bitmap, `key = hash | (1 << 25)` for bucket-ID bitmap.

3. **Trigrams** — `prefix="{trigram}"`, `key ∈ {0, 1, 2}`. Maps character trigrams to bucket IDs with disjoint positional keys: 0 = prefix (first trigram of token), 1 = infix, 2 = suffix (last trigram). Single-trigram tokens (3 chars) are stored under both key=0 and key=2.

Entries are written in `(prefix, key)` sorted order: all token buckets, then transitions, then trigrams.

## Query Pipeline

**Stage 1 — Candidate resolution.** For each query token:
- Body tokens (known word boundaries on both sides): hash directly to a single bucket ID.
- Partial tokens (prefix, suffix, or singleton): resolve via trigram index. For each trigram, load the appropriate positional keys and AND across trigrams to get candidate bucket IDs.
- Between adjacent tokens: filter bucket sets with transition bucket bitmaps.

**Stage 2 — Doc bitmap intersection.** Load doc-ID bitmaps for surviving bucket IDs, OR within each token, AND across all tokens and transition doc bitmaps. Sort by cardinality (smallest first) for early termination.

**Stage 3 — Brute-force verification.** Not implemented in this module. The caller scans each candidate document's raw text with substring matching to eliminate false positives.

## Design Choices

- **24-bit hash** — 16M buckets. At 40M docs with 7.1M unique tokens, 80% of occupied buckets are singletons.
- **Bigram transitions** — Boundary characters use 2 chars per side (TRANSITION_CHARS=2), reducing transition entries and index size compared to trigram boundaries.
- **Disjoint trigram keys** — Positional keys (prefix/infix/suffix) are stored separately, allowing the query to narrow candidates based on where a trigram appears within a token.
- **Over-estimation semantics** — The index is a sieve. All stages produce supersets of true matches. Stage 3 brute-force verification produces exact results.
- **Stale entries on delete** — Deleting a document removes it from token bucket and transition doc bitmaps. Trigram and transition bucket bitmaps are left stale — they only cause slightly larger candidate sets, which Stage 3 filters out.
