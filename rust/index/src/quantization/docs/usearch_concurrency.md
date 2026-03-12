# USearch Concurrency: Problems, Locks, and Fixes

USearch (HNSW index) is used by SPANN to manage cluster centroids. Two instances exist:
- `raw_centroid` -- f32 HNSW, used during writes (`navigate()`)
- `quantized_centroid` -- RaBitQ-quantized HNSW, used during reads (`search()`)

There are two independent concurrency hazards, each addressed by a different lock.

## Problem 1: `nodes_` array reallocation (use-after-free)

USearch stores its graph nodes in a `buffer_gt<node_t>` -- a heap-allocated array of
`node_t` entries, where each `node_t` is just a pointer (`byte_t* tape_`) to separately
allocated tape memory holding the actual node data (key, neighbor lists, vector).

When `try_reserve()` is called (to grow capacity):

1. A **new** `nodes_` array is allocated
2. The old node pointers are `memcpy`'d into it
3. `nodes_ = std::move(new_nodes)` triggers `buffer_gt::reset()` on the old array, **freeing it**

If a concurrent `search()` or `add()` is mid-traversal doing `nodes_[candidate.slot]`,
it's now reading from freed memory. The individual node tapes are safe (separately
allocated, not freed during reserve), but the slot-to-tape-pointer lookup array itself
is a dangling pointer. Same applies to `nodes_mutexes_` and `contexts_`, which are also
reallocated and freed during reserve.

**Original fix:** Chroma wrapped the entire `usearch::Index` in `Arc<RwLock<usearch::Index>>`.
Every `add()`, `remove()`, `search()`, `save()`, and `load()` took a **write** (exclusive)
lock, fully serializing all operations.

**Previous fix (had TOCTOU bug):** Double-checked locking, but the capacity check and the
`add()` call used separate read lock acquisitions. Between dropping the lock after the check
and re-acquiring it for the add, other threads could consume all remaining capacity --
USearch doesn't bounds-check internally and would write past the allocated `nodes_` array
(SIGSEGV). See `test_concurrent_add_search_during_resize` in `usearch.rs`.

**Current fix:** The `add()` call happens under the *same* read lock that verified capacity.
The lock is only released for a resize (which requires a write lock), after which we loop
back and re-check before proceeding. The worst-case capacity overflow is bounded by the
number of concurrent threads, which `RESERVE_BUFFER` (128) is sized to absorb.
`remove()` and `search()` always use read locks.

```rust
loop {
    let index = self.index.read();
    if index.size() + self.tombstones.load(Ordering::Relaxed) + RESERVE_BUFFER
        >= index.capacity()
    {
        drop(index);
        let index = self.index.write();
        if ... >= index.capacity() {
            index.reserve(index.capacity().max(RESERVE_BUFFER) * 2)...;
        }
        continue; // re-check with fresh read lock
    }
    // Still holding the read lock that verified capacity
    return index.add(key as u64, vector)...;
}
```

**Alternative approach (benchmarked):** Remove the RwLock entirely by pre-allocating enough
capacity at construction/load time (e.g. 128K slots) so `reserve()` is never called during
concurrent access. This avoids the lock overhead entirely but requires knowing the upper bound
up front or accepting a hard capacity limit. The approach was benchmarked and showed 10-18%
speedup on navigate and 24-33% on spawn (see benchmarks below).

## Problem 2: HNSW entry point races (torn reads)

USearch's HNSW graph has an entry point defined by two values: `max_level_` (the highest
level in the graph) and `entry_slot_` (the slot of the entry node). When a new node is
added with a level higher than `max_level_`, both values must be updated atomically from
the perspective of concurrent readers.

Without synchronization:
- Two concurrent `add()` calls could both see their level is higher, both try to update
  the entry point, and one update would be lost
- A concurrent `search()` could read `max_level_` from one update and `entry_slot_` from
  another, traversing the graph from a node that doesn't exist at the expected level

**Original fix (USearch upstream):** `std::mutex global_mutex_` acquired at the **start** of
every `add()` call. This serialized ALL concurrent adds and also blocked searches, even
though the probability of a node landing on a level higher than `max_level_` is extremely
low (exponentially decaying per HNSW's level assignment). This was the biggest bottleneck
for thread scaling.

**Current fix (our fork):** Replaced `max_level_` and `entry_slot_` with `std::atomic`
variables. `add()` reads them with `memory_order_acquire` (lock-free). Only in the rare
case when a new node's level exceeds `max_level_` does it acquire `entry_mutex_` and
perform a double-checked update:

```cpp
// Lock-free read of entry point (replaces global_mutex_ acquisition)
level_t max_level = max_level_.load(std::memory_order_acquire);
compressed_slot_t entry_slot = entry_slot_.load(std::memory_order_acquire);

// ... HNSW insertion proceeds without holding any global lock ...

// Only lock if this node might become the new entry point
if (new_target_level > max_level) {
    std::unique_lock<std::mutex> entry_lock(entry_mutex_);
    max_level = max_level_.load(std::memory_order_relaxed);
    if (new_target_level > max_level) {
        max_level_.store(new_target_level, std::memory_order_release);
        entry_slot_.store(new_slot, std::memory_order_release);
    }
}
```

## USearch internal locking (unchanged)

Beyond the two problems above, USearch has additional internal locks that we have not
modified:

- **`node_lock_(slot)`** -- per-node spinlock (bitfield-based). During `add()`, each node
  being linked is individually locked while its neighbor list is updated. Very fine-grained,
  short-held. Searches do NOT acquire node locks.

- **`slot_lookup_mutex_`** (`std::shared_mutex`) -- protects key-to-slot hash map in
  `index_dense.hpp`. Chroma uses the lower-level `index.hpp` directly, so this is not in
  our hot path.

- **`available_threads_mutex_`** (`std::mutex`) -- manages thread ID pool for per-thread
  scratch buffers. Locked on every `add()` and `search()` call. Low contention in practice
  but could be replaced with thread-local caching.

## Benchmark results

Setup: Wikipedia EN, 1-bit centroids, 16 threads, r6i.8xlarge (32 vCPU, 256 GB RAM).

### Removing the Rust RwLock (pre-allocate capacity, no lock at all)

#### 1M vectors

| Metric            | With RwLock | Without RwLock | Delta   |
|-------------------|------------|----------------|---------|
| Total index time  | 1.1m       | 1.0m           | -9%     |
| Throughput        | 15,138 v/s | 16,172 v/s     | +7%     |
| navigate (total)  | 646.34s    | 535.24s        | -17%    |
| navigate (avg)    | 209.4us    | 172.4us        | -18%    |
| spawn (total)     | 10.26s     | 6.89s          | -33%    |
| spawn (avg)       | 906.2us    | 607.5us        | -33%    |

#### 5M vectors

| Metric            | With RwLock | Without RwLock | Delta   |
|-------------------|------------|----------------|---------|
| Total index time  | 2.3m       | 2.2m           | -4%     |
| Throughput        | 7,314 v/s  | 7,626 v/s      | +4%     |
| navigate (total)  | 1270.90s   | 1113.61s       | -12%    |
| navigate (avg)    | 416.2us    | 370.2us        | -11%    |
| spawn (total)     | 22.49s     | 16.72s         | -26%    |
| spawn (avg)       | 2.04ms     | 1.55ms         | -24%    |
| split (total)     | 1779.57s   | 1596.68s       | -10%    |

#### Recall (unchanged by lock removal)

| Vectors | nprobe | R@10 (lock) | R@10 (no lock) |
|---------|--------|-------------|----------------|
| 1M      | 16     | 0.86        | 0.85           |
| 1M      | 64     | 0.92        | 0.93           |
| 5M      | 16     | 0.75        | 0.75           |
| 5M      | 64     | 0.87        | 0.87           |

### Forked USearch (atomic entry point + shared Rust locks)

See `quantization/docs/BENCHMARKS.md` "Forked USearch" section for full results including
the combined effect of the atomic entry point fix and the shared-lock Rust wrapper.
