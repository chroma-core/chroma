# Future Optimization Opportunities

Target hardware:
  - Query:  r6id.8xlarge  (32 vCPU Ice Lake, AVX-512 + VPOPCNTDQ + VNNI)
  - Index:  r6id.32xlarge (128 vCPU Ice Lake)
  - Future: Graviton 3/4  (ARM Neoverse V1/V2, NEON + SVE)

## [OPT-1] Replace `Vec<Arc<[u8]>>` with a contiguous slab in `QuantizedDelta`

**File:** `rust/index/src/spann/quantized_spann.rs`

**Context:** Currently planned as part of the Code::<4> → Code::<1> migration.

### Problem

`QuantizedDelta.codes` stores each code as a separately heap-allocated `Arc<[u8]>`:

```rust
struct QuantizedDelta {
    center: Arc<[f32]>,
    codes: Vec<Arc<[u8]>>,  // N separate allocations
    ids: Vec<u32>,
    length: usize,
    versions: Vec<u32>,
}
```

For a cluster of N=2048 points at dim=1024, a Code::<1> code is 144 bytes
(16-byte header + 128-byte packed bits): **2048 separate 144-byte allocations per cluster**.

The data lives contiguously in the blockfile, gets shredded into N individual
heap pointers on load, then gets re-flattened back into a contiguous `Vec<u8>`
on every commit (`commit()` L872):

```rust
let codes = delta
    .codes
    .iter()
    .flat_map(|c| c.iter())
    .copied()
    .collect::<Vec<_>>();
```

### Fix

Replace `Vec<Arc<[u8]>>` with a single contiguous slab:

```rust
struct QuantizedDelta {
    center: Arc<[f32]>,
    codes: Vec<u8>,  // contiguous slab, stride = code_size
    ids: Vec<u32>,
    length: usize,
    versions: Vec<u32>,
}
```

Access individual codes with `delta.codes.chunks(code_size)` — the same
pattern the blockfile reader already uses.

### Impact per call site

| Location | Current | After slab |
|---|---|---|
| `load()` L274 | `Arc::from(chunk)` × N | `slab.extend_from_slice(chunk)` |
| `commit()` L872 | flatten N Arcs → `Vec<u8>` | zero-copy: reference `delta.codes` directly |
| `split()` L600/L615 | `quantize().as_ref().into()` × N | pre-allocate slab, `quantize_into` each |
| `merge()` L353 | `quantize().as_ref().into()` per loop iter | extend slab, `quantize_into` |
| `register()` L426/L446 | single quantize, Arc wrap | extend slab, `quantize_into` |
| NPA scan L728 | `codes.iter()` with pointer chasing | sequential slab access, better cache locality |

### Prerequisite

A `Code::<1>::quantize_into(embedding, centroid, output: &mut [u8])` API that
writes the header and packed bits directly into a caller-provided slice, avoiding
the intermediate `Vec<u8>` allocation inside `quantize()`. Without the slab
change, `quantize_into` alone just moves allocations to the caller and provides
no benefit.

### Benefit assessment

**Allocation count.** Clusters split at 50 points and merge below 25, so a
steady-state cluster holds on the order of 25–50 codes. Every `load()` call
allocates one `Arc<[u8]>` per code; every `commit()` then flattens them back.
For an index with C active clusters, each write cycle pays 25–50 allocator
round-trips per cluster just to reconstitute data that was already contiguous on
disk. At C=10,000 clusters (a modest 250K–500K point index) that is 250K–500K
allocations per commit whose sole purpose is intermediary storage.

**Allocator contention.** The indexing path on r6id.32xlarge runs up to 128
threads. Each thread races for the global allocator lock on every `Arc::from`.
With a slab, the hot path is a single `Vec::extend_from_slice` per cluster load
(one allocation, no locking beyond the initial capacity reservation) and zero
allocations at commit.

**Memory overhead.** Each `Arc<[u8]>` carries two reference-count words (16
bytes on 64-bit) plus a heap allocation header (~16–32 bytes depending on
allocator). For a Code::<1> code of 144 bytes that is ~22–33% overhead per code.
The slab eliminates this entirely.

**Cache locality during NPA scan.** The NPA scan (L728) iterates all codes in a
neighbor cluster sequentially to compute distance estimates. With `Vec<Arc<[u8]>>`
each code pointer-chases to a separate heap object; with a slab successive codes
are adjacent in memory. At 144 bytes per Code::<1> code and a 64-byte cache line,
a 50-point cluster's codes occupy ~112 cache lines in the slab vs. 50 scattered
allocations that may not share any cache lines.

**Commit serialization savings.** The flatten at `commit()` (L872) does a full
copy of all code bytes into a new `Vec<u8>` before handing off to the blockfile
writer. With a slab this copy is unnecessary — `delta.codes` is already the
contiguous byte slice the writer needs. For a 50-point cluster at dim=1024 that
is 50 × 144 = 7,200 bytes saved per cluster per commit.

**Summary.** The change is primarily an allocation efficiency improvement, not a
compute improvement. The largest gains are in write-heavy workloads (bulk
indexing) where many clusters are loaded, mutated, and committed per cycle. The
cache locality benefit is secondary but may be measurable in the NPA scan for
large clusters.

### Why now (Code::<1> migration)

Code::<4> codes are 524 bytes at dim=1024 (12-byte header + 512-byte packed).
Code::<1> codes are 144 bytes. The cluster capacity in points stays the same,
so clusters with Code::<1> will have ~3.6× more codes that fit in cache — making
cache locality a more significant factor and the slab change proportionally
more valuable.

---

## Cross-cutting (X1–X4)

**Source:** `rust/index/benches/vector/quantization.rs` § Cross-cutting

### [X1] Batch API for cluster scan

Provide `distance_query_batch` that takes a slice of code byte slices and
returns distances. Enables software prefetching of the next code while
processing the current one, hiding LLC latency. Relevant at N > L1-capacity /
code_size (approx. 200 codes).

### [X2] Alignment

Ensure packed byte arrays are 64-byte aligned for optimal AVX-512 loads.
Currently heap-allocated with default alignment (16B on most allocators).
Options: `aligned_vec` crate or manual Layout allocation.

### [X3] Thread-local scratch buffers

For the indexing path, a thread-local arena (e.g. bumpalo) eliminates all
per-quantize allocation overhead. At 128 vCPUs on r6id.32xlarge, malloc
contention can be significant.

### [X4] Graviton migration checklist

- Verify simsimd dispatches to NEON/SVE (not scalar fallback) for `f32::dot`,
  hamming. Build with `RUSTFLAGS="-C target-cpu=neoverse-v1"`.
- Benchmark `count_ones()` on Graviton; if slow, use NEON vcnt intrinsic.
- signed_dot [D2] XOR approach may be better on ARM than the current
  SIGN_TABLE lookup.
- r6id Ice Lake has 2 × 512-bit FMA units; Graviton 3 has 4 × 128-bit NEON
  units. Per-core throughput may be lower but Graviton has more cores per
  dollar — measure end-to-end QPS, not single-core ns/op.

---



# RabitQ Cross-Machine performance comparison

Full raw output in `saved_benchmarks/performance_r6i.8xlarge.txt` (Intel) and
`saved_benchmarks/performance_mb_pro_m1.txt` (Apple Silicon).

- **r6i.8xlarge**: Intel Ice Lake, 16 physical cores / 32 vCPUs, AVX-512
- **MacBook Pro M1**: Apple Silicon, 8 performance cores, NEON

**Batch benchmarks** (512 embeddings/queries, dim=1024):


| Benchmark                    | M1                  | r6i.8xlarge         | Ratio (r6i/M1) |
| ---------------------------- | ------------------- | ------------------- | -------------- |
| quant-4bit (data encode)     | 27.6 ms, 145 MiB/s  | 43.2 ms, 93 MiB/s   | 1.6x slower    |
| quant-1bit (data encode)     | 361 us, 10.8 GiB/s  | 576 us, 6.8 GiB/s   | 1.6x slower    |
| dc-1bit (256 pairs)          | 2.69 us, 25.5 GiB/s | 3.99 us, 17.2 GiB/s | 1.5x slower    |
| dc-4bit (256 pairs)          | 182 us, 1.37 GiB/s  | 166 us, 1.50 GiB/s  | 1.1x faster    |
| dq-exact (f32 ground truth)  | 53.6 us, 72.9 GiB/s | 120 us, 32.7 GiB/s  | 2.2x slower    |
| dq-4f (4-bit, cold query)    | 1.17 ms, 1.88 GiB/s | 1.71 ms, 1.29 GiB/s | 1.5x slower    |
| dq-float (1-bit, cold query) | 1.06 ms, 1.91 GiB/s | 1.46 ms, 1.39 GiB/s | 1.4x slower    |
| dq-bw (AND+popcount, cold)   | 1.26 ms, 1.60 GiB/s | 2.38 ms, 870 MiB/s  | 1.9x slower    |
| d-lut (nibble LUT, cold)     | 5.28 ms, 393 MiB/s  | 3.48 ms, 594 MiB/s  | 1.5x faster    |


**Hot-scan benchmarks** (1 query, 2048 codes, dim=1024):


| Benchmark     | M1                  | r6i.8xlarge         | Ratio (r6i/M1) |
| ------------- | ------------------- | ------------------- | -------------- |
| dq-exact/scan | 172 us, 45.3 GiB/s  | 290 us, 27.0 GiB/s  | 1.7x slower    |
| dq-4f/scan    | 1.03 ms, 995 MiB/s  | 762 us, 1.31 GiB/s  | 1.4x faster    |
| dq-float/scan | 456 us, 616 MiB/s   | 933 us, 301 MiB/s   | 2.0x slower    |
| dq-bw/scan    | 40.8 us, 6.74 GiB/s | 40.0 us, 6.86 GiB/s | ~same          |
| d-lut/scan    | 371 us, 759 MiB/s   | 265 us, 1.03 GiB/s  | 1.4x faster    |


**Key primitive differences** (single-vector, hot cache):


| Primitive           | M1                  | r6i.8xlarge         | Ratio (r6i/M1)  |
| ------------------- | ------------------- | ------------------- | --------------- |
| simsimd_dot         | 260 ns, 29.3 GiB/s  | 63 ns, 121 GiB/s    | **4.1x faster** |
| signed_dot          | 198 ns, 19.9 GiB/s  | 443 ns, 8.9 GiB/s   | 2.2x slower     |
| sign_pack           | 143 ns, 26.7 GiB/s  | 320 ns, 11.9 GiB/s  | 2.2x slower     |
| fused_reductions    | 547 ns, 14.0 GiB/s  | 695 ns, 11.0 GiB/s  | 1.3x slower     |
| hamming/simsimd     | 6.54 ns, 36.2 GiB/s | 4.77 ns, 50.0 GiB/s | 1.4x faster     |
| QuantizedQuery::new | 566 ns, 6.74 GiB/s  | 2.21 us, 1.73 GiB/s | **3.9x slower** |


**Observations:**

- **M1 wins on scalar/FP-intensive work.** Data quantization (both 1-bit and 4-bit),
fused reductions, sign packing, and signed_dot are all 1.3-2.2x faster on M1.
Apple Silicon's wide FP pipeline and memory bandwidth advantage over Ice Lake show
clearly in these single-threaded benchmarks.
- **r6i wins on simsimd operations.** AVX-512 gives a 4.1x advantage on raw f32 dot
products (simsimd_dot) and 1.4x on hamming distance. This matters for dq-4f/scan
and d-lut/scan which are both ~1.4x slower on M1.
- **AND+popcount (dq-bw/scan) is identical** on both (~40 us). NEON CNT and AVX-512
VPOPCNTDQ are equally effective for this workload. This is the production-path kernel.
- **QuantizedQuery::new is 3.9x faster on M1** (566 ns vs 2.21 us). The fused
min/max + quantize + bit-plane scatter path benefits from M1's stronger single-core
throughput. This is a per-query cost that amortizes across codes.
- **d-lut is consistently slower on M1** (1.4-1.5x). The nibble LUT approach likely
benefits from AVX-512 gather/scatter instructions not available on NEON.
- **Thread scaling data is only meaningful on r6i** (16 physical cores + HT). The M1
has 8 performance cores with no hyperthreading; the 16t/32t results on M1 are just
contention noise. See the Thread Scaling section for r6i scaling analysis.

Full details below.

## Sources of performance differences between r6i.8xlarge and MacBook Pro M1

### Decode width and instruction window

The M1's Firestorm performance cores are 8-wide decode with a reorder buffer of ~630 entries. Ice Lake Sunny Cove cores are 5-wide decode with ~352 ROB entries. The wider decode means M1 can dispatch more instructions per cycle, and the larger ROB means it can look further ahead to find independent work. This shows up most in operations with mixed integer/FP work and complex data dependencies -- like QuantizedQuery::new (min/max reduction + float-to-int quantization + bit-plane scatter), where M1 is 3.9x faster. Ice Lake simply cannot keep as many operations in flight to hide latencies in these dependency chains.

### FP/SIMD execution ports

M1 has 4 NEON pipes, each 128 bits wide (4 x 128 = 512 bits of FP throughput per cycle). Ice Lake has 2 AVX-512 FMA units, each 512 bits wide (2 x 512 = 1024 bits per cycle). For a pure dot product (simsimd_dot), AVX-512 processes 16 f32s per FMA instruction and does a fused multiply-add (2 FLOPs per element), while each NEON pipe handles 4 f32s. Even with 4 pipes, M1 tops out at 16 f32s/cycle without fused multiply-add, versus Ice Lake's 32 f32s/cycle with FMA. That is the 4.1x gap.
But AVX-512 has a cost: Ice Lake Xeon throttles its clock frequency under sustained AVX-512 workloads (the "AVX-512 downclocking" penalty). This partially erodes the theoretical 2x FLOP advantage, and it means mixed workloads that alternate between AVX-512 and scalar code pay a frequency transition penalty. M1's NEON runs at full clock speed always.

### Why M1 wins on signed_dot despite losing on simsimd_dot

signed_dot is not a pure FP dot product. It first does a table lookup to expand sign bits into +/-1.0 f32 values, then dots the result. The expansion step is a sequence of byte loads and stores with irregular access patterns -- it benefits from M1's wider issue width and much larger L1 data cache (128 KB vs 48 KB on Ice Lake). By the time the dot product starts, the expanded data is hot in L1 and the dot itself is over a small vector. AVX-512's raw throughput advantage does not have enough data to amortize over.

### Memory bandwidth per core

M1 uses unified memory (on-package LPDDR, 68 GB/s) shared across 4 performance cores. A single thread can consume a large fraction of total bandwidth. Ice Lake Xeon in r6i.8xlarge uses DDR4 across multiple channels (200 GB/s aggregate), but shared across 16 physical cores. Per-core available bandwidth is roughly 68/4 = 17 GB/s on M1 vs 200/16 = 12.5 GB/s on Ice Lake. M1 also has much lower memory latency because the DRAM is on-package rather than on DIMMs through a memory controller. This per-core bandwidth advantage explains why dq-exact (pure f32 distance, bandwidth-bound) is 2.2x faster on M1: it is limited by how fast one core can stream vectors from memory, not by compute.

### Why dq-bw/scan ties

AND+popcount over packed bit vectors is both compute-light and memory-light (128 bytes per 1024-dim code). The working set fits in L1 on both architectures, and the popcount instruction is single-cycle on both (NEON CNT, AVX-512 VPOPCNTDQ). Neither core is bottlenecked on decode width, ROB depth, or memory bandwidth -- the operation is just too simple and small to differentiate the architectures.

### Thread scaling

M1 has 4 performance + 4 efficiency cores, no hyperthreading. Ice Lake has 16 physical cores with 2-way SMT (32 vCPUs). For single-threaded benchmarks M1 wins on per-core performance, but r6i has 4x the physical core count for parallel workloads, which is what matters in production. That is why the thread scaling section only uses r6i data.
