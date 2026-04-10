# Compact u8 Versions with Deletion Flag

SPFresh-style encoding: replace the current `u32` per-vector version with a single `u8`.

```
  bit 7      bits 6..0
 [deleted]  [version 0-127]
```

## Semantics

- **add(id)**: bumps `(version_number + 1) & 0x7F`, clears delete bit
- **delete(id)**: sets the delete bit on the current global version
- **is_valid**: `posting_version == global_version` (since add clears delete bit and delete sets it, a deleted id's postings never match)
- **Staleness**: all current `version >= current_ver` / `version < current_ver` checks
  become `version == current_ver` / `version != current_ver`. This is not a logic
  change -- the two are equivalent because the global version is bumped before the
  posting is registered, so `posting_version > global_version` is unreachable. The
  switch to `==` is only necessary because `>=` would break under wrapping (e.g.
  version 1 >= 127 would incorrectly read as "valid")

Helper constants:

```rust
const VERSION_MASK: u8 = 0x7F;
const DELETED_BIT: u8 = 0x80;
```

## Wrapping safety

With 7-bit versions (0-127), versions wrap after 128 increments to the same vector
without an intervening scrub. If exactly 128 updates happen to the same vector ID
between scrubs, an ancient stale posting could appear valid. In practice this is
extremely unlikely -- SPFresh uses this scheme successfully.

The key invariant: `posting_version <= global_version` always holds at the time of
registration, and the global version only increases. So `==` is equivalent to `>=`
for validity, and `!=` is equivalent to `<` for staleness.

## Persistence boundary

The hierarchical index continues to use `QuantizedCluster` (which stores `&[u32]`
versions) for blockfile I/O. At the boundary:

- **commit**: widen `Vec<u8>` to `Vec<u32>` via `.iter().map(|&v| v as u32).collect()`
- **open/load**: narrow `u32` back to `u8` via `v as u8`
- Global versions in scalar metadata: same widening/narrowing

## Files to change

### writer/mod.rs

- Add `VERSION_MASK` and `DELETED_BIT` constants
- `LeafNode::versions`: `Vec<u32>` -> `Vec<u8>`
- `HierarchicalSpannWriter::versions`: `DashMap<u32, u32>` -> `DashMap<u32, u8>`
- `add()`: change version bump from `*v += 1; *v` to `*v = (*v).wrapping_add(1) & VERSION_MASK; *v`
- `register_in_leaf()`: `version` param `u32` -> `u8`
- `reassign()`: same version bump pattern change; `version` locals become `u8`
- `scrub()`: change `version < current_version` to `version != current_version`
- `split_leaf()`: change `ver >= current_ver` to `ver == current_ver`
- `is_valid()`: already uses `==`, just change param types to `u8`
- All NPA/neighbor loops: change `version < current_ver` to `version != current_ver`; variable types `u32` -> `u8`
- `merge_leaf()`: same staleness check update
- Add `pub fn delete(&self, id: u32)`: set `DELETED_BIT` on the global version entry

### writer/diagnostics.rs

- All `version >= current_ver` validity checks become `version == current_ver`
- Local variable types change to `u8`

### writer/persistence.rs

- **commit()** leaf path: build a temporary `Vec<u32>` from `leaf.versions` for the `QuantizedCluster`
- **commit()** global versions: cast `u8` to `u32` in the blockfile write
- **open()** global versions: narrow with `value as u8`
- **load()**: narrow cluster versions with `.iter().map(|&v| v as u8).collect()`

### instrumentation.rs

No changes needed -- instrumentation only tracks counters, not version values.

### hierarchical_spann_profile_quantized.rs

No changes expected -- the bench file doesn't directly manipulate version values.
