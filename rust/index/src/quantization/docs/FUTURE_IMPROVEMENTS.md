# Future Optimization Opportunities

Target hardware:
  - Query:  r6id.8xlarge  (32 vCPU Ice Lake, AVX-512 + VPOPCNTDQ + VNNI)
  - Index:  r6id.32xlarge (128 vCPU Ice Lake)
  - Future: Graviton 3/4  (ARM Neoverse V1/V2, NEON + SVE)

## [OPT-1] Replace `Vec<Arc<[u8]>>` with a contiguous slab in `QuantizedDelta`

**File:** `rust/index/src/spann/quantized_spann.rs`

**Context:** Currently planned as part of the Code4Bit → Code1Bit migration.

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

For a cluster of N=2048 points at dim=1024, a Code1Bit code is 144 bytes
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

A `Code1Bit::quantize_into(embedding, centroid, output: &mut [u8])` API that
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
allocator). For a Code1Bit code of 144 bytes that is ~22–33% overhead per code.
The slab eliminates this entirely.

**Cache locality during NPA scan.** The NPA scan (L728) iterates all codes in a
neighbor cluster sequentially to compute distance estimates. With `Vec<Arc<[u8]>>`
each code pointer-chases to a separate heap object; with a slab successive codes
are adjacent in memory. At 144 bytes per Code1Bit code and a 64-byte cache line,
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

### Why now (Code1Bit migration)

Code4Bit codes are 524 bytes at dim=1024 (12-byte header + 512-byte packed).
Code1Bit codes are 144 bytes. The cluster capacity in points stays the same,
so clusters with Code1Bit will have ~3.6× more codes that fit in cache — making
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
