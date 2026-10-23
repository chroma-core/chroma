# Selected-posting version retrieval: source-only opportunity assessment

The reader can potentially retrieve the current versions of selected posting IDs with fewer repeated block accesses, while retaining only bounded batches of owned integers. The source establishes this interface opportunity, but it does not establish a material performance benefit. The next step is first-use measurement, not another cache or an implementation. This assessment executes no reader workload, test, build, controller, or cloud action.

The implementation checkout is `/private/tmp/chroma-hierarchical-spann-sharding`. The actual checkout author and committer both resolve to `lyon-chroma <lyon@trychroma.com>` before this document write. This document is the only worker-owned output for this task. Source hashes below bind the inspected bytes; they do not establish that a running binary contains them.

## How posting preparation works

A posting contains candidate IDs, saved versions and compressed vector codes. Before scoring it, the reader looks up each ID’s currently published version, removes deleted or stale copies, and copies the surviving codes into an owned leaf buffer. The source calls this operation `load_node_posting_list` (`reader/persistance.rs:152–245`).

The reader first borrows the saved posting and allocates output capacities for every physical ID, version and code byte (`persistance.rs:165–182`). It then acquires one version-reader lock for the entire leaf (`184–221`). Inside that lock it processes 64 IDs at a time, permits 16 concurrent point lookups, collects owned `(position, Option<u32>)` values, clears scalar block pins and filters/copies the surviving entries (`185–219`). The lock covers lookup waits and code copying, rather than only access to the shared pin map. A successful cached-leaf check bypasses this preparation (`159–163`).

A selected-leaf request can launch 32 leaf preparations concurrently (`reader/remote.rs:325–330`). Consequently, posting borrows and full-sized output capacities can accumulate before those preparations acquire the version lock. The 64-ID version batch is not a bound on posting preparation memory. The existing `load_ns` interval measures the joined preparation stage, not version lookup CPU alone.

The current output order inside each batch follows completion order because the loader uses `buffer_unordered(16)` and pushes the collected values directly. A future helper must retain input positions and duplicate multiplicity. It must explicitly resolve whether filtering must preserve that completion order or establish an equivalent stronger contract. Equal-distance first-copy behavior can retain different floating-point bits, including signed zero, so an unproved reordering cannot be called bitwise equivalent.

## APIs, storage accesses and lifetimes

| Component | Current behavior | Consequence |
| --- | --- | --- |
| Point value lookup (`types/reader.rs:31–39`; `arrow/blockfile.rs:675–700`) | It constructs a prefix/key, resolves one block through the root index, obtains a pinned block and performs a block-local lookup. | Grouping can avoid repeated block acquisition, but does not automatically remove each key’s index lookup or block-local search. |
| Root index (`arrow/sparse_index.rs:342–360,572–584`) | It holds an immutable shared ordered map and resolves a point by predecessor search. | A bounded group can resolve individual selected keys without loading a full owner version map. Root metadata residency remains a separate cost. |
| Range lookup (`arrow/blockfile.rs:739–807`) | It selects blocks using the prefix range; the key range is applied only after block retrieval. | A narrow ID range can still fetch every block for the version prefix. Neither range streaming nor `get_prefix` is a sound selected-ID I/O optimization. |
| Key prefetch (`types/reader.rs:140–147`; `arrow/blockfile.rs:640–663`) | It collects keys, resolves target block IDs and launches `join_all` loads; it returns no values. | It has no explicit concurrency bound and does not replace the subsequent per-key retrieval. It is not the proposed interface. |
| Multi-key root lookup (`arrow/sparse_index.rs:363–399`) | It sorts keys and walks root entries, returning target blocks without input-position association. | It can traverse preceding entries and loses the mapping required for exact per-input results. Start with bounded point resolution rather than assuming this helper is proportional to requested keys. |
| Provider obtain (`arrow/provider.rs:505–516,537–619`) | It obtains a cloned owned block from cache or fetches, decodes and inserts a block. | An owned block can be held locally while extracting owned u32 values, without extending its borrow through a reader pin map. |
| Scalar value (`arrow/block/value/u32_value.rs:84–87`) | It reads an integer by value. | A returned version does not borrow Arrow buffers. The block can drop after all requested values are extracted. |
| Block-local lookup (`arrow/block/types.rs:318–333`) | It searches prefix and key within a block. | Grouping fetches does not itself vectorize these searches. |

Reader pins keep borrowed values alive. The Arrow reader shares its `loaded_blocks` map across clones (`arrow/blockfile.rs:483–493`), and `get_block` extends a block reference’s lifetime with unsafe code (`582–630`). Clearing the map is sound only after every borrowed value and in-flight future using it has dropped (`560–579`). Owning the u32 results satisfies that condition for those results, but does not authorize clearing unrelated concurrent borrows. The generic exclusive clear checks actual pin-map uniqueness and still has a no-live-borrow precondition (`551–557`).

The ordinary loader’s lookup error at `persistance.rs:197` returns before its clear at `199`; that path does not establish zero scalar pins. Missing or invalid version errors occur after the clear. The independent audit clears before propagating its collected lookup error (`reader/audit.rs:207–221`). These are lifetime observations, not an assignment to add failure handling. Another scalar lookup in `remote.rs:367–370` does not take the version lock, so the lock is not evidence that every possible scalar accessor participates in one universal lifecycle protocol.

The existing public loaded-block statistics cover posting and vector readers, rather than scalar version pins (`persistance.rs:303–307`). A measurement must observe the actual scalar reader instead of inferring version residency from those two families. The alternate streaming path holds the version lock over its request and clears scalar pins after each point lookup (`reader/streaming.rs:172–175,242–256`); its behavior is separate from the ordinary loader assessed here.

Cache hits need precise labels. Plain Foyer obtains clone an in-memory value (`cache/foyer.rs:693–704`), whereas hybrid obtains call the persistent cache implementation (`490–501`). A hybrid hit need not mean zero storage or reconstruction work. Existing provider callback counters describe storage-origin Arrow decodes, not all persistent-cache reconstruction. Root retrieval has a separate cache/fetch/decode path (`arrow/provider.rs:749–781`), so opening the reader must be charged separately or explicitly excluded.

## Smallest plausible interface, conditional on measurement

A bounded selected-key helper inside the blockstore could return owned versions while holding each required block only long enough to extract its requested integers. Its inputs would include the pinned root, exact version prefix, at most 64 selected keys and their original positions. It would resolve each key with the existing point index, group positions by target block, obtain each distinct block once for that batch and perform the existing prefix/key lookup for every requested position. It would return position-associated `Option<u32>` values, including duplicates and missing entries, then drop each owned block. No whole-owner map or prefix-wide range read is required.

The narrow implementation seam is an owned `BlockManager` retrieval used from within the Arrow reader, where the manager and root are already accessible. It is not composition of today’s public borrowed point getters. Fetching an owned block avoids adding these scalar reads to the lifetime-extended reader pin map. The generic Memory reader still needs an explicit equivalent branch if the API is exposed through `BlockfileReader`.

The first design should keep the existing version lock and bound both distinct groups and simultaneous owned blocks. Removing that lock or moving filtering outside it is a separate concurrency change. A batch of 64 versions contains 256 raw integer bytes, but positions, options, grouping structures and decoded block buffers add memory. A block-count bound is not a pre-decode byte, provider cache, transport, root metadata, heap or RSS bound. Repeated blocks across adjacent batches can still require repeated provider obtains; measurement should expose this rather than silently introducing another cache.

Grouping does not remove the full posting read, full-capacity leaf allocations, valid-code copying, vector scoring or root-index residency. It also does not remove per-key root resolution and in-block search unless a separate measured algorithm replaces those operations. The already reported small warm-load fraction is insufficient justification for a new cache or for an end-to-end speedup claim.

## Minimal first-use measurement proposal

Measure one fixed selected-leaf preparation on an exact pinned generation, then the same operation with warm provider state and separately with the leaf already installed. Keep query, selected nodes, IDs, source roots and version prefix fixed. A fresh process is not an OS-cold storage experiment. The independent version/posting oracle belongs outside the measured interval. No measurement is authorized by this document.

| Counter or interval | Required attribution |
| --- | --- |
| Request and leaf spans | Record joined request wall time, each leaf’s start/cache bypass/posting fetch/output allocation, centroid fetch/copy and final installation. Count competing preparations whose final installation loses the double-check. |
| Lock intervals | Record enqueue, acquire and release timestamps per leaf. Separate time waiting for the version lock from lookup batches and filtering/code copying while holding it. |
| Selected keys and block locality | Count physical IDs, requested version keys, unique IDs, duplicate positions, exact target blocks per batch and repeated blocks across batches. Restrict the existing point-key observer to the version prefix. |
| Pin and provider work | Record scalar pin hits/misses, provider obtains/hits/misses, successful storage callbacks, encoded returned bytes and storage-origin decode attempts/successes per block. Record cache implementation/configuration. |
| Residency | Record scalar pin baseline, highwater, end state and clears at the actual insert/clear boundaries. Report estimated decoded payload separately from block count and output capacities. |
| Filter and copy work | Count live entries, deleted versions, stale mismatches, missing/out-of-range versions, IDs/versions appended and code bytes copied. Separate physical capacity from retained length. |
| Coverage limits | Charge root opening separately. State whether persistent-cache reconstruction, posting/vector buffers, allocator capacity and OS I/O are observed. Bound per-block/event maps and mark overflow as an invalid measurement rather than silently truncating. |

The current optional observer already records most block/provider/storage-origin decode counters (`arrow/blockfile.rs:447–480`) and is shared with the actual pin map. It caps block and point-key maps at 256 and exposes an overflow flag. It must be attached to the scalar reader, reset only when no relevant futures are active, and snapshotted after completion. A reader-shared observer is not automatically request-exclusive when other operations can access that reader. Concurrent misses can perform multiple obtains or decodes before one block is installed in the pin map; insertion count alone cannot establish fetch count.

Concurrency makes summed timing different from latency. Sixteen lookup futures overlap within a batch and up to 32 leaf preparations overlap outside the lock. Summed lock waits are not a percentage of request latency. Provider and decode spans can overlap; their sums describe work intervals rather than exclusive CPU. Retain interval timestamps, maxima and the observed serialized lock sequence alongside sums. Avoid subtracting nested wall spans to claim exact CPU time. The measurement needs bounded requests, events, report size and child timeout in a separately reviewed controller; no budget is asserted as executed here.

The decision depends on measured amplification. Grouping is promising if many selected IDs map to few blocks and repeated point acquisition, provider obtains or lock-held lookup overhead constitute meaningful work. It is less promising if selected keys mostly require distinct blocks, storage dominates, or code copying dominates. Required unknowns are the actual version-block locality, cache kind/state, repeated callback/decode count, lock interval distribution and copy cost on the retained workload. None is established by source inspection.

## Correctness requirements for a later proposal

A later implementation must preserve exact prefix/key/missing-value behavior, each duplicate’s input association, current-version validation, deletion filtering and saved-version equality. It must preserve first-equal minimum bits and inclusive cutoff ties after any ordering change, with the current completion-order issue explicitly resolved. Returned integers must not retain block borrows; scalar cleanup must not invalidate unrelated values. Root identity and source generation must remain pinned. A full candidate/distance-bit oracle must remain independent of the new helper. These are proposal prerequisites, not tests executed in this phase.

## Inspected source identity

Every reference above is relative to the implementation checkout. The following SHA-256 values identify its current source bytes.

| File | SHA-256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/reader/persistance.rs` | `3718a1a1b1e87d5f86b58922c274df09918c25350f8be26a2f410f0a651bcf96` |
| `rust/index/benches/hierarchical_index/reader/mod.rs` | `ba5bae4e685c43260ea5c9a44763ed3600fc875dcd3071933fa79f45bbdc6da4` |
| `rust/index/benches/hierarchical_index/reader/remote.rs` | `57ee3bfe88e00e6bdadd143839ec3ea17b968de145497399630b173c1760aac1` |
| `rust/index/benches/hierarchical_index/reader/streaming.rs` | `e1d28efb0c49488b9324396002f694a9157dc84aeeeeb6782a12077b0aa3a59d` |
| `rust/index/benches/hierarchical_index/reader/audit.rs` | `eda43a8b35c7e591ad92138078955277cdc3588dfdb130c043c66cb4d9ef4f46` |
| `rust/blockstore/src/types/reader.rs` | `0ab81785490f1e17729a771ca9f05dbbe885bd49e2a90fed89f3eef448e8f27c` |
| `rust/blockstore/src/arrow/blockfile.rs` | `8668aab3f41f192a0415c5452c903d6965a1e2e141d76437ff3320d96b7edf70` |
| `rust/blockstore/src/arrow/sparse_index.rs` | `ecefc89cc96fe91750f09b437e21c1cdd0bd47239297d7e5a82541789dc220e9` |
| `rust/blockstore/src/arrow/provider.rs` | `d48d368a0cea84111f48de260a7239f93c4a0eaec4a57fac377f821cf3a0a529` |
| `rust/blockstore/src/arrow/block/types.rs` | `7706891df8fbf79dbab3f956f91a70c397e8bd47e3eed0f541284359d95d6f99` |
| `rust/blockstore/src/arrow/block/value/u32_value.rs` | `c3f90bf65564f20af295db0a65337faf4896408f6c8a803557ab672235687f08` |
| `rust/cache/src/foyer.rs` | `8d91bc91f97ac66dc2c4445217a69676ea053824ef63de60e14abf8d37360788` |
