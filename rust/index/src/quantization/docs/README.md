# 1-Bit RaBitQ Quantization

This adds 1-bit RaBitQ quantization alongside the existing 4-bit implementation. If you're familiar with 4-bit RaBitQ, here's what's new.

## What changed

- **`Code<1>`** -- a new code type (via `Code<const BITS: u8, T>`) that stores one sign bit per dimension (vs 4-bit ray-walk codes). Header is 16 bytes (adds `signed_sum` field) vs 12 bytes for `Code<4>`. Packed code is `ceil(dim/64)*8` bytes vs `padded_dim*4/8` for 4-bit.

- **`QuantizedQuery` and `BatchQueryLuts`** -- query-side quantization for 1-bit codes. The query residual is quantized to 4 bits and decomposed into bit planes. Distance scoring uses AND+popcount (no floats in the inner loop). `BatchQueryLuts` is a nibble-LUT alternative.

- **`RabitqCode` trait** -- shared interface for `Code<1>` and `Code<4>`. Enables runtime dispatch via `Box<dyn RabitqCode>` where the bit width is determined at runtime.

- **SPANN writer** -- `QuantizedSpannIndexWriter` dispatches between `Code<1>` and `Code<4>` based on `data_bits` for all quantize/distance/size operations.

- **USearch centroid index** -- `USearchIndexConfig.quantization_bits` and `apply_quantization_metric` support both 1-bit and 4-bit. Invalid bit widths are rejected upfront with `InvalidQuantizationBits`. The cache key distinguishes bit widths.

## What didn't change

- 4-bit quantization, distance functions, and USearch integration are untouched.
- Existing collections with `FourBitRabitQWithUSearch` or `None` behave identically.
- Centroid HNSW quantization stays at 4-bit.

## Not yet wired

- **Collection schema configuration** for 1-bit (`OneBitRabitQWithUSearch` variant, `data_bits()` method) is not yet added. Currently only 4-bit is configurable in production.
- **Reranking pipeline** (`centroid_rerank_factor`, `vector_rerank_factor`, `QuantizedSpannRerankOperator`) is not yet integrated. See `notes/removed_reranking_and_config.md` for the implementation plan.

## Performance summary

1-bit is 25-80x faster than 4-bit across quantize, distance_code, and distance_query. Recall at 1M vectors is lower without reranking (~0.65 vs ~0.92 at rerank 1x), but reaches >0.99 at rerank 8-16x.

## Documentation

- [PERFORMANCE.md](PERFORMANCE.md) -- benchmark comparisons (throughput, thread scaling, recall at 1M)
- [IMPLEMENTATION_SPECIFICS.md](IMPLEMENTATION_SPECIFICS.md) -- how our implementation compares to the paper, optimizations applied, correction factor algebra, error analysis
- [FUTURE_OPTIMIZATIONS.md](FUTURE_OPTIMIZATIONS.md) -- contiguous slab for QuantizedDelta, batch API, alignment, thread-local buffers, Graviton checklist
