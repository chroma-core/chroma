# Four-bit centroid navigation with FP queries

The hierarchical benchmark reader prepares four-bit centroid codes when it opens an index and scores them against the original floating-point query. The packed kernel uses single instructions that operate on several coordinates (SIMD) to process sixteen coordinates per block. Instruction selection happens once per reader; AVX-512 and AVX2/FMA have a portable scalar fallback.

The reader derives its scoring cache from the checkpoint’s saved original centroids. Existing checkpoints therefore need no format migration. Original centroids must be present with the expected dimension; an incomplete or malformed checkpoint returns a data-loss error. The cache is immutable during queries. Posting lists and reranking embeddings are loaded on demand.

The writer continues to produce the checkpoint’s existing one-bit codes. Four-bit centroid persistence is a separate format change. The measured experiment prepares the derived cache in roughly 2–3 seconds and owns about 16 MiB of scoring-cache allocations at 40M vectors. Those figures come from the comparison implementation, including its map bookkeeping; they exclude provider caches and total reader RSS. Preparation is excluded from query timings.

## Measured results

The comparison uses the saved 40M Wikipedia index, 1024 dimensions, the same 100 exact L2 queries, 32 query workers, and a 1 GiB block cache. Each row averages three runs; the middle run reverses implementation order. All root children are retained. Beam widths and reranking counts are chosen for comparable final recall.

| Setting | Internal beam | Leaf beam | FP vectors reranked | Leaf reachable recall | Recall@100 | Warm mean ms | Cold mean ms | Cold filesystem MiB/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| One-bit centroids, four-bit query | 256 | 256 | 800 | 94.57% | 94.42% | 28.54 | 950.69 | 724.70 |
| Packed four-bit centroids, FP query; fewer leaves | 256 | 192 | 800 | 94.59% | 94.44% | 24.21 | 941.23 | 723.55 |
| Packed four-bit centroids, FP query; less reranking | 256 | 240 | 400 | 95.40% | 94.51% | 25.28 | 528.38 | 441.24 |
| One-bit centroids, four-bit query | 64 | 128 | 800 | 90.18% | 90.05% | 16.28 | 977.81 | 722.47 |
| Packed four-bit centroids, FP query | 64 | 112 | 400 | 90.85% | 90.28% | 12.67 | 571.27 | 436.58 |

At approximately 94.5% recall, the smaller leaf beam scans 285,831 posting codes instead of 380,460 and lowers warm latency by 15.2%. The reduced-reranking setting scans 357,395 codes, halves original-vector reranking, and lowers warm latency by 11.4%. Its filesystem reads fall by 39.1%. The latency changes include the beam and rerank settings shown in the table.

Cold clears reader posting data and block pins. The provider cache and OS cache can retain data. Filesystem bytes are averaged over a concurrent 100-query batch, include complete block reads served through the OS cache, and do not measure production network traffic. The query set is small and fixed.

## Reproduction

The complete comparison and its instrumentation are preserved on [the experiment branch](https://github.com/chroma-core/chroma/tree/b635eee6f83e56ecf6c8e0c1eada25b7b66b4bd3). The focused implementation retains the measured packed FP-query algorithm and omits the experimental scoring modes.

```sh
cargo bench -p chroma-index --bench hierarchical_reader_precision -- \
  --index /mnt/data/wikipedia-40m-baseline-20261009 \
  --ground-truth /mnt/benchmark-source/wikipedia-40m-current/wikipedia_1m_40m_ground_truth.parquet \
  --output results.jsonl --queries 100 --threads 32 \
  --max-cache-bytes 1073741824 \
  --configurations 64:112:4,64:128:8,256:192:8,256:240:4,256:256:8 \
  --navigation 1bit-4bit,4bit-planes-4bit,4bit-packed-4bit,4bit-packed-fp
```

Run three times with distinct output paths and reverse the navigation order in the middle run. Ground-truth SHA-256: `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. The original checkpoint manifest remains unchanged after the comparison.

The Rust SIMD loops port the authors’ `ip16_fxu4_avx2` and `ip16_fxu4_avx512` implementations from [RaBitQ-Library revision 2f6b57e](https://github.com/VectorDB-NTU/RaBitQ-Library/tree/2f6b57e4907659a79fd039cc826164e7d320f83d). Attribution and its Apache-2.0 license accompany the kernel. Tests compare against the existing scalar FP estimator, cover padding and nibble values, and verify navigation on existing checkpoints with fixed and adaptive beams.
