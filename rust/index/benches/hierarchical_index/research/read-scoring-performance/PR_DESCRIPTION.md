## Summary

Queries spend most of their time comparing compressed vector codes in posting lists. Each comparison counts the set bits in four intersections with the query's packed bits. A portable x86 build performs that calculation with ordinary instructions even when the CPU provides dedicated bit-count instructions.

This change detects CPU capabilities once per process and stores the selected single-vector and posting-list scoring functions in a shared, thread-safe initializer (`OnceLock`). Both scoring entry points use that selection. Compatible x86 CPUs use vector population count, which counts set bits across several machine words at once. Other compatible x86 CPUs use scalar population count. Other processors use the existing portable calculation. CPU checks run during initialization before selecting any specialized function. Subsequent calls use the stored function pointers.

The public scoring functions call the cached function pointers directly. Both single-vector and batch kernels use one shared arithmetic function, which is forced inline so the selected CPU features apply to its bit-count loop. Single-vector scoring calls the selected function directly; batch scoring performs one indirect call for the entire list and then inlines the arithmetic for each vector. Without inlining, the specialized outer loop calls a separately compiled portable inner function. The reader reuses one distance buffer across its leaves, then passes the same vector IDs and distances to the existing result selection. Its additional temporary memory is four bytes per vector in the largest selected leaf, per active query.

The affected code is the hierarchical reader's posting scoring in `rust/index/benches/hierarchical_index/reader/reader.rs` and the one-bit distance kernel in `rust/index/src/quantization/single_bit.rs`. The stored format, distance formula, search thresholds, result selection, and writer algorithms stay the same.

## Additional kernel validation

A short EC2 kernel comparison against PR revision `9cd5638956` scores 4,096 codes with six alternating single-vector/batch passes at each dimension. The medians below exclude the first warmup pass. At 1,024 dimensions, single-vector scoring improves from 66.98 to 20.72 ns/code; batch scoring remains approximately 15 ns/code (15.07 to 15.11). At 64 dimensions, the figures are 15.35 to 10.51 ns/code for single-vector scoring and 6.66 to 6.66 for batch scoring. At 1,536 dimensions, they are 94.18 to 20.89 and 17.37 to 17.40 respectively. Every pass checks exact score bits. These are kernel measurements; the full-reader measurements below describe the batch optimization and do not measure an additional query-latency gain from caching the selection.

## Measurements

The comparison searches the exact same saved 1M-vector index for each real 1,024-dimensional dataset. Three paired repetitions alternate the original posting loop and the candidate. Each run executes the same 100 queries on 32 workers, with a read threshold of 1.5, 100 results, no full-precision reranking, and a 1 GiB block cache.

| Dataset | Reader state | Baseline average query | Candidate average query | Reduction | Baseline scoring | Candidate scoring |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Wikipedia | Warm | 12.13 ms | 4.90 ms | 59.6% | 10.67 ms | 3.40 ms |
| MS MARCO | Warm | 11.03 ms | 4.50 ms | 59.2% | 9.70 ms | 3.17 ms |
| Wikipedia | Cold reader state | 20.07 ms | 12.47 ms | 37.9% | 10.90 ms | 3.67 ms |
| MS MARCO | Cold reader state | 17.67 ms | 10.77 ms | 39.1% | 9.83 ms | 3.33 ms |

These are concurrent-query averages on an r6id.16xlarge with an Intel Xeon Platinum 8375C and local NVMe. Cold reader state clears posting data and block pins; it leaves the operating system's filesystem cache populated. It does not model cold object storage. No other benchmark or heavy compilation overlaps the measurements.

A separate single-worker pair gives warm query means of 11.0 → 4.1 ms on Wikipedia and 10.3 → 3.7 ms on MS MARCO. Warm p99 falls from 15.61 → 6.19 ms and 16.20 → 6.15 ms respectively. These percentiles come from only 100 queries per pass and are coarse sample measurements, not production estimates.

A kernel probe scores 4,096 generated codes. The original loop takes approximately 85.35 ns/code, scalar hardware population count takes 28.85 ns/code, and vector hardware population count takes 15.11 ns/code. Disassembly confirms `vpopcntq` in the vector loop. An Apple ARM probe keeps the portable batch fallback near the original 15–16 ns/code after warmup; that short measurement establishes no cross-platform speedup.

## Validation

All 16 reader runs return identical top-100 IDs, ordering, and floating-point score bits in their cold and warm passes. This comparison covers 3,200 query invocations. Recall stays at 77.81% for Wikipedia and 81.01% for MS MARCO on these fixed indexes.

The differential test compares both public scoring entry points and the available single-vector and batch kernels with the portable arithmetic, requiring identical score bits. It exercises Euclidean, cosine, and inner-product distance; dimensions 1, 7, 8, 63, 64, 65, 300, 1,024, and 1,536; padded final words; and empty and small lists. It compares the batch entry point, portable loop, and available specialized loops with individual-code scoring. All eight one-bit quantization tests pass in release mode on the x86 benchmark node, including the extended differential test for shared dispatch. All eight one-bit quantization tests and 59 existing policy/posting integration tests pass locally. The complete profiling benchmark compiles. Formatting and diff checks pass; compilation reports the existing unused `clear_child_scoring` warning.

## Reproduction

The source base is `af6366cce6`, with Rust 1.92.0, the existing Cargo release profile, and no `RUSTFLAGS` override. Saved posting-preserving indexes from #7856 provide the same data to both reader kernels. The temporary paired harness preserves the original scalar posting loop, selects one or 32 query workers, and records result bits and percentiles outside timed search. These comparison switches and logging are excluded from this PR.

Build the original feature revision and this branch separately, then run the existing profiling benchmark against the same checkpoint with `--resume --no-commit --checkpoint 10 --checkpoint-size 100000 --metric l2 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save-dir <checkpoint>`. The saved checkpoint must already contain ten 100k additions, so the command adds no new vectors. Wikipedia uses `--dataset wikipedia-en --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0`. MS MARCO uses `--dataset ms-marco --threads 64 --balance-threads 64 --write-navigation fp --fp-npa --write-beam-tau 1.5 --write-rng-factor 4`.

Raw logs, commands, comparison source, binary hashes, and the report are preserved locally under `read-scoring-performance` in this chat's workspace and remotely under `/mnt/data/read-scoring-results` on Lyon's benchmark node. The measured binaries are copied there so another build cannot overwrite them.

The shared-dispatch implementation calls selected functions directly and keeps all distance arithmetic in one shared function. A final uncontended kernel check on commit `6208c4b78` measures 19.77 ns/code for single-vector scoring and 15.08 ns/code for batch scoring at 1,024 dimensions. Exact score comparisons and all eight x86 release quantization tests pass.
