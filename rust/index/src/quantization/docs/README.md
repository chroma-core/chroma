# 1-Bit RaBitQ Quantization

This PR adds 1-bit RaBitQ quantization alongside the existing 4-bit implementation. If you're familiar with 4-bit RaBitQ, here's what's new.

## What changed

- **`Code1Bit`** -- a new code type that stores one sign bit per dimension (vs 4-bit ray-walk codes). Header is 16 bytes (adds `signed_sum` field) vs 12 bytes for `Code4Bit`. Packed code is `ceil(dim/64)*8` bytes vs `padded_dim*4/8` for 4-bit.

- **`QuantizedQuery` and `BatchQueryLuts`** -- query-side quantization for 1-bit codes. The query residual is quantized to 4 bits and decomposed into bit planes. Distance scoring uses AND+popcount (no floats in the inner loop). `BatchQueryLuts` is a nibble-LUT alternative.

- **`GenericCode` enum** -- runtime dispatch over code types. Replaces scattered `match bits { 1 => ..., _ => ... }` with `GenericCode::new(bits)` then `code.quantize()`, `code.distance_query()`, `code.size()`.

- **`Quantization::OneBitRabitQWithUSearch`** -- new variant in the collection schema enum. `Quantization::data_bits()` returns `Some(1)` or `Some(4)`.

- **SPANN writer and query path** -- `QuantizedSpannIndexWriter` uses `GenericCode` for all quantize/distance/size operations. `query_quantized_cluster` takes `data_bits` and dispatches accordingly. The orchestrator reads `data_bits` from the collection schema and passes it through to the bruteforce operator.

- **USearch centroid index** -- centroid HNSW stays at 4-bit regardless of data quantization. `USearchIndexConfig.quantization_bits` and `apply_quantization_metric` already supported 1-bit; the cache key now distinguishes bit widths.

- **Reranking** -- two configurable rerank steps (`centroid_rerank_factor`, `vector_rerank_factor`) in `InternalSpannConfiguration`. Centroid rerank re-scores with exact centroid distances (in-memory). Vector rerank fetches full embeddings from the record segment and re-scores with exact distances. Both default to 1 (disabled). 1-bit needs higher rerank factors (8-16x) to match 4-bit recall.

## What didn't change

- 4-bit quantization, distance functions, and USearch integration are untouched.
- The `Code` type alias still points to `Code4Bit`.
- Existing collections with `FourBitRabitQWithUSearch` or `None` behave identically.
- Centroid HNSW quantization stays at 4-bit.

## Performance summary

1-bit is 25-80x faster than 4-bit across quantize, distance_code, and distance_query. Recall at 1M vectors is lower without reranking (~0.65 vs ~0.92 at rerank 1x), but reaches >0.99 at rerank 8-16x.

## Documentation

- [PERFORMANCE.md](PERFORMANCE.md) -- benchmark comparisons (throughput, thread scaling, recall at 1M)
- [IMPLEMENTATION_SPECIFICS.md](IMPLEMENTATION_SPECIFICS.md) -- how our implementation compares to the paper, optimizations applied, correction factor algebra, error analysis
- [RERANKING.md](RERANKING.md) -- lifecycle trace, centroid/vector rerank mechanics, I/O and compute costs, recommended defaults
- [FUTURE_OPTIMIZATIONS.md](FUTURE_OPTIMIZATIONS.md) -- contiguous slab for QuantizedDelta, batch API, alignment, thread-local buffers, Graviton checklist
