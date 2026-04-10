# Separate Reader from Writer

Extract the search/navigation read path into a new `reader.rs` module with a
`HierarchicalSpannReader` struct that eagerly loads `centroid_code` for all
nodes but only loads raw centroids for internal nodes, lazy-loading leaf
centroids and posting data from blockfiles on demand.

## Architecture

```
hierarchical_index/
  mod.rs               -> pub mod instrumentation; pub mod writer; pub mod reader;
  instrumentation.rs
  reader.rs            -> HierarchicalSpannReader + navigate + search
  writer/
    mod.rs             -> HierarchicalSpannWriter + navigate + add/split/merge
    diagnostics.rs     -> search + diagnostic helpers (writer-side)
    persistence.rs     -> commit/open/load
```

The reader lives at `hierarchical_index/reader.rs`, a sibling to the `writer/`
directory. It is a new module declared in `hierarchical_index/mod.rs`.

## HierarchicalSpannReader struct

Minimal read-only struct -- no write-path fields (`balancing`, `tree_lock`,
`next_node_id`, `versions`):

```rust
pub struct HierarchicalSpannReader {
    dim: usize,
    distance_fn: DistanceFunction,
    config: HierarchicalSpannConfig,
    nodes: DashMap<NodeId, TreeNode>,
    root_id: u32,            // plain u32, not AtomicU32
    embeddings: DashMap<u32, Arc<[f32]>>,  // populated lazily by load_raw()
    pub stats: WriterStats,  // reuse existing stats struct

    // Blockfile readers
    posting_list_reader: BlockfileReader<...>,
    vector_data_reader: BlockfileReader<...>,
}
```

Key difference: `root_id` is a plain `u32` (reader is constructed once, never
mutated). No `versions` map needed (search skips the global version-map lookup).

## Loading strategy

| Data                                    | Internal nodes                                           | Leaf nodes                                                      |
| --------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------- |
| `centroid` (Vec<f32>)                   | Eager (needed as parent context for quantized queries)   | **Lazy** -- empty Vec at open, loaded from blockfile when needed |
| `centroid_code` (Vec<u8>)               | Eager (recomputed from centroid + parent centroid)       | Eager (recomputed from centroid at open, then centroid discarded)|
| `ids`, `codes`, `versions` (posting)    | N/A                                                      | **Lazy** -- loaded via `load()` at search time                  |
| `embeddings`                            | N/A                                                      | **Lazy** -- loaded via `load_raw()` for vector reranking        |

### Leaf centroid lifecycle at open

1. Load centroid for leaf from blockfile (needed to compute `centroid_code`)
2. Compute `centroid_code = Code::<1>::quantize(&centroid, &parent_centroid)`
3. **Discard** the centroid: set `leaf.centroid = Vec::new()`

This way `centroid_code` is always available, but the raw f32 centroid is not
held in memory.

### Lazy centroid loading

A new method `load_centroid(&self, node_id)` fetches a single leaf centroid from
`vector_data_reader.get(PREFIX_CENTROID, node_id)` and stores it in the node.
Called:

- In `search_with_policy()` before scoring a leaf's posting list (needs centroid
  for `QuantizedQuery`)
- The `load()` method (posting data) also sets `leaf.centroid` from
  `QuantizedCluster.center` as a side effect, so if posting data is loaded
  first, centroid is already available

### Centroid rerank during navigation

When `rerank_factor > 1` in `navigate_quantized`/`navigate_1bit`, the writer
reranks child nodes with `self.dist(query, n.centroid())`. For the reader:

- Internal node centroids are always present -- works as-is
- Leaf node centroids may be empty -- **fall back to the approximate code-based
  distance** (no I/O during navigate, keeps navigation fast and synchronous)

In the reader's navigate rerank loop:
`if n.centroid().is_empty() { keep approximate dist } else { rerank with f32 }`.

## Methods on HierarchicalSpannReader

### From writer/diagnostics.rs (copied, then modified)

- `search()` -- public entry point
- `search_with_policy()` -- core search logic. Modified: calls
  `self.load(leaf_id).await` and `self.load_centroid(leaf_id).await` for each
  navigated leaf before scoring

### From writer/mod.rs (copied, then modified)

- `navigate_quantized()` (lines 625-809) -- with rerank fallback for empty centroids
- `navigate_1bit()` (lines 819-1001) -- with rerank fallback for empty centroids
- `navigate_with_policy()` (lines 1004-1016) -- **without the `Fp` branch**
  (reader has no f32 navigation; if `Fp` is requested, panic or fall back to `OneBit`)
- `navigate()` (lines 1018-1029)
- `effective_beam()` (lines 597-616) -- standalone function, no `&self`

### Helpers (copied or re-exported)

- `dist()`, `padded_bytes()`, `code_size()`, `vec_norm()` -- small methods,
  copy to reader
- `code_slice()` -- free function at line 126, make `pub(crate)` or re-export

### New methods

- `open()` -- like writer's `open()` but discards leaf centroids after computing codes
- `load()` -- like writer's `load()` but also sets `leaf.centroid` from `cluster.center`
- `load_centroid(node_id)` -- fetches single centroid from `vector_data_reader`
- `load_raw(ids)` -- same as writer's `load_raw()`

## Shared types

These types are already defined in `writer/mod.rs` and used by both reader and
writer. They need `pub` visibility (currently `pub(super)`):

- `NodeId`, `NavigationMode`, `HierarchicalSpannConfig`, `ReadBeamPolicy`, `LevelBeamParams`
- `TreeNode`, `LeafNode`, `InternalNode`
- `code_slice()`

Since `reader.rs` is a sibling of `writer/` (not inside it), these need to be
re-exported from `writer/mod.rs` with `pub` visibility, or the reader imports
them as `super::writer::*`.

## search becomes async

Currently `search()` and `search_with_policy()` are synchronous. In the reader,
they need to call async `load()` / `load_centroid()` between navigation and
scoring. Two options:

- **Option A**: Make `search` / `search_with_policy` `async` on the reader
- **Option B**: Keep navigation sync, use `block_on()` for the lazy loads

Since this is a bench tool and the caller
(`hierarchical_spann_profile_quantized.rs`) already runs in a tokio runtime,
**Option A** (async search) is the cleanest choice.

## Changes to existing files

- `hierarchical_index/mod.rs`: add `pub mod reader;`
- `writer/mod.rs`: widen visibility of shared types from `pub(super)` to `pub`
  where needed by reader (NodeId, TreeNode, LeafNode, InternalNode,
  NavigationMode, HierarchicalSpannConfig, ReadBeamPolicy, code_slice, etc.)
- No changes to `writer/diagnostics.rs` -- the writer keeps its own
  search/diagnostics. The reader has its own copy that diverges (async, lazy
  loading, no f32 nav).

## Implementation steps

1. Widen visibility of shared types in `writer/mod.rs`
2. Create `reader.rs` with `HierarchicalSpannReader` struct and helpers
3. Implement `open()` with eager centroid_code / lazy leaf centroid strategy
4. Implement `load()`, `load_centroid()`, `load_raw()` for lazy blockfile loading
5. Copy navigate methods, modify rerank to fall back on empty centroids, drop f32 branch
6. Implement async `search()` / `search_with_policy()` with lazy loading between navigate and scoring
7. Declare `pub mod reader` in `hierarchical_index/mod.rs`
8. `cargo check --bench hierarchical_spann_profile_quantized` to verify clean compile
