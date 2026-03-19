# Centroid Rerank: `raw_centroid.get(key)` Cost Breakdown

## Current Call Chain

```
self.raw_centroid.get(key)                          [quantized_spann.rs navigate()]
  USearchIndex::get(key)                            [usearch.rs]
    parking_lot::RwLock::read()                     acquire Rust read lock on inner usearch::Index
    usearch::Index::export(key, &mut vec)            [USearch/rust/lib.rs]
      self.count(key)                               find how many vectors exist for this key
      Vec<f32>::resize(dim * count)                  HEAP ALLOCATION (e.g. 6KB for dim=1536)
      T::get(self, key, buffer)                     FFI call across CXX boundary
        NativeIndex::get_f32(key, slice)            [USearch/rust/lib.cpp]
          index_->get(key, data, count)             [index_dense.hpp get_()]
            shared_lock_t(slot_lookup_mutex_)        acquire C++ read lock
            slot_lookup_.find(key)                   HASH MAP LOOKUP: key -> slot
            vectors_lookup_[slot]                    array index -> pointer to stored vector
            memcpy(dst, src, bytes_per_vector)       COPY dim*4 bytes into output buffer
```

## Per-Call Cost Estimate (dim=1536, uncontended)

| Step                               | Cost         |
|------------------------------------|--------------|
| parking_lot::RwLock::read()        | ~20-50ns     |
| Vec<f32> allocation (6KB)          | ~100-300ns   |
| CXX FFI boundary crossing          | ~5-10ns      |
| C++ shared_lock_t                  | ~20ns        |
| Hash map lookup in slot_lookup_    | ~50-100ns    |
| Array index into vectors_lookup_   | ~5ns         |
| memcpy of 6KB                      | ~200-500ns   |
| **Total per call**                 | **~400ns-1us** |

All data is in-memory, contiguous -- no I/O. But there are two layers of
reader-writer locks and a heap allocation per call.

## Avoidable Overhead

1. **Heap allocation per call.** `export()` allocates a fresh `Vec<f32>` each
   time. For 64-256 calls per navigate, this adds up. A pre-allocated reusable
   buffer would eliminate this.

2. **Two layers of reader locks.** The Rust `parking_lot::RwLock` on
   `USearchIndex::index` plus the C++ `shared_lock_t` on `slot_lookup_mutex_`.
   For a read-only centroid index at query time, these are uncontended but still
   touch atomic cache lines.

3. **Hash map lookup.** `slot_lookup_` is a hash map from key to slot. Since
   centroid IDs are dense small integers (0..num_clusters), a direct array index
   would be O(1) with no hashing.

4. **FFI overhead.** Each call crosses the Rust-CXX boundary.

## Alternative Storage Options for Full-Precision Vectors

### A. `self.embeddings: DashMap<u32, Arc<[f32]>>` (already exists on writer)

The writer's in-memory embedding cache. Lookups are a `DashMap::get()` -- one
shard lock + hash lookup, returns an `Arc<[f32]>` reference with no copy. For
centroid rerank, this would require storing centroids there, which currently
isn't done. For data vector rerank, the implementation already uses this path
via `load_raw()`.

- Pro: no copy, no allocation, no FFI
- Con: doubles memory for centroids (USearch already holds them)

### B. Plain `Vec<Arc<[f32]>>` indexed by centroid ID

Since centroid IDs are dense, a flat vector indexed by ID avoids hashing
entirely. Access is O(1) array index + pointer follow.

- Pro: fastest possible lookup, no locks, no hashing, no allocation
- Con: requires maintaining a separate structure alongside USearch

### C. `raw_embedding_reader: BlockfileReader` (already exists)

The blockfile reader used by `load_raw()` for data vector reranking. Goes
through blockfile key lookup, potentially hitting S3 on a cache miss.

- Pro: works for any vector, persistent
- Con: orders of magnitude slower than in-memory (async, cache layers,
  potential network I/O)

### D. USearch `get()` with reusable buffer

Add a method like `get_into(key, &mut [f32])` that writes directly into a
caller-provided buffer, eliminating the per-call `Vec` allocation. Requires a
small change to the USearch Rust bindings.

- Pro: minimal code change, eliminates the allocation
- Con: still has the lock + hash + FFI overhead

## Benchmark Results (centroid_fetch.rs)

Measured per-navigate-call with 5000 centroids, dim=1024, fetching 128 vectors:

| Strategy          |   Total |  Fetch  | Distance | Per-vec fetch | Speedup |
|-------------------|---------|---------|----------|---------------|---------|
| USearch get()     | 601.3us | 585.0us |   14.4us |        4.6us  |    1.0x |
| Vec<Vec<f32>>     |  25.4us |   0.5us |   24.9us |          3ns  |   23.7x |
| HashMap           |  27.7us |   2.7us |   25.0us |         21ns  |   21.7x |
| DashMap<Arc>      |  34.0us |   6.0us |   28.0us |         46ns  |   17.7x |
| Vec<Arc<[f32]>>   |  28.1us |   0.4us |   27.7us |          3ns  |   21.4x |

USearch `get()` dominates rerank cost (~97% is fetch, not distance computation).
All in-memory alternatives reduce fetch to negligible.

## Decision: use `cluster_deltas` (Option 0)

`navigate()` now calls `self.centroid(key)` which reads from
`cluster_deltas: DashMap<u32, QuantizedDelta>` -- every centroid's f32 vector
is already stored as `Arc<[f32]>` in the delta's `center` field. This is the
DashMap<Arc> row above (~46ns/vec, ~100x faster than USearch get).

`raw_centroid` (USearch index) is kept for persistence (save/load across
generations) and the write path (add/remove), but the hot read path in
`navigate()` no longer touches it.

## Future: further options if DashMap becomes a bottleneck

If `nav_fetch` still shows up in profiles, upgrade to Option B:

- Add `centroid_vecs: Vec<Option<Arc<[f32]>>>` indexed by cluster ID.
- Populate in `init()`/`resume()`, update in `spawn()`/`drop_cluster()`.
- Fetch becomes a single array index (~3ns/vec vs DashMap's ~46ns/vec).
- Trade-off: ~30-50 lines of sync code, but 15x faster than DashMap.

Option D (USearch `get_into` with reusable buffer) is not worth pursuing --
it only eliminates the allocation, not the lock/hash/FFI overhead that
dominates.
