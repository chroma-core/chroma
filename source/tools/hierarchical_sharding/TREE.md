# Preparing and publishing one distributed tree

One frozen root assigns vectors to owned subtrees, and one coordinator searches those subtrees with a global frontier. The root's centroid slots are part of the committed tree. Each selected slot represents its persisted lower root: an internal lower root expands directly to its children, while a leaf lower root becomes a reached leaf. The traversal does not insert an extra edge or rescore the lower root's centroid. Local and HTTP traversal use this same rule.

## The pieces

- The preparation file records the source prefix, frozen root model, input vectors, and explicit original row IDs (`input.json`).
- The root model records the construction epoch, metric, seeded sample, clustering settings, and float32 centroids (`model.json`).
- Each lower snapshot records its actual root, persisted block references, and immutable local-row-to-global-ID map (`manifest-G.json` and `id-map-G.u64le`).
- The committed tree binds the upper root to those lower snapshots and maps. A separate placement assigns the same tree's ownership slots to physical workers.
- The published pointer identifies an immutable tree manifest and the base directory used to resolve its artifacts.

## Prepare the boundary

The preparation utility samples source rows without replacement using a recorded seed and clusters that sample. It freezes float32 centroid values before assigning the full corpus. Cosine and inner-product models use spherical centroids; squared Euclidean models use ordinary means. Assignment ties choose the lowest centroid slot. An empty owned subtree fails preparation. The utility preserves source float32 vectors and assigns their original dense row IDs; ownership is not inferred from a modulo rule.

```sh
OPENBLAS_NUM_THREADS=4 python3 -m tools.hierarchical_sharding.spatial_prepare --source real-prefix.f32 --output prepared --source-id pinned-source-family --construction-epoch epoch-1 --dimension 1024 --metric cosine --children 2 --sample-size 8192
```

The output contains each subtree's input vectors and full little-endian unsigned-64-bit map. Each map entry gives the global ID of the corresponding local vector row. The descriptor records count, byte length, and SHA-256. The boundary token hashes the frozen model hash together with its centroid slot. Build each lower snapshot with its full map and that token (`--id-map` and `--boundary-hash`).

An append reads the previous preparation file and routes only the new source suffix with the same centroids. Its vectors file contains new rows; its map contains the old prefix followed by newly assigned global IDs. The output records the old local row count and new row count separately. Changed source identity, noncontiguous global suffix, changed model bytes, or changed construction settings fail preparation. The writer and publisher independently check the map prefix and boundary token.

## Bind the actual snapshots

The assembly function reads the actual lower manifests and their persisted maps. It requires each map to match the prepared ownership and includes the actual persisted root reference. Paths in the returned manifest resolve against the supplied base directory.

```python
from tools.hierarchical_sharding.tree_manifest import assemble_tree, canonical_bytes

tree = assemble_tree("prepared/input.json", ["owner0/manifest-1.json", "owner1/manifest-1.json"], tree_generation=1, base="bundle")
```

The tree has `format_version`, `tree_generation`, `construction_epoch`, `dimension`, `metric`, `source`, `model`, `upper_root`, and `subtrees`. Each subtree has `subtree_id`, `centroid_index`, `routing_boundary_hash`, its lower `generation`, `vector_count`, `snapshot`, `local_to_global`, and `root_ref`. Artifact descriptors contain `path`, `bytes`, and `sha256`. A root reference contains `kind` and integer `id`. The global tree generation and lower snapshot generations are distinct. An append that changes only one owner publishes that owner's new snapshot and retains every unaffected owner's older immutable snapshot by its own generation and content hash. The single global compare-and-swap publication binds that complete set, while the prior global tree remains queryable.

Validation hashes the actual local model, map, and snapshot files. A disk-backed bitmap checks global IDs for range, uniqueness, and complete coverage of the dense source prefix. It uses one bit per source row and bounded input buffers. The lower manifest must name the same persisted map, boundary token, root, metric, dimension, source family, and count. Optional local block artifacts are also hashed. These checks do not parse lower block contents; opening every lower reader and verifying its persisted structure remains a separate required integrity gate.

## Publish one generation

Publication holds an operating-system file lock while comparing the current pointer with the expected old content hash. A conflicting publisher fails without replacing the pointer. A successful publisher validates the complete bundle, writes the immutable content manifest, flushes it, replaces the pointer atomically, and flushes the parent directory. Within one construction epoch, the model and ownership slots remain fixed and every map retains its existing prefix. Automatic repartitioning and cross-owner reassignment are unsupported.

```python
from tools.hierarchical_sharding.tree_manifest import publish, load_published

pointer = publish("published/current.json", tree, expected_old_hash=None, base="bundle")
pinned_tree, artifact_base = load_published("published/current.json")
```

The content hash uses UTF-8 JSON with sorted keys, compact separators, finite numbers, and one trailing newline. The published pointer records that hash, the immutable manifest filename, the tree generation, and the artifact base. Readers pin this complete identity for a request. Failed validation, changed ownership, digest mismatch, or compare-and-swap conflict preserves the complete prior generation.

## Change placement without changing the tree

A placement has `format_version: 1`, the exact `tree_sha256`, a positive `placement_epoch`, and `owners`. The owner map uses subtree IDs as keys and `{owner_id, endpoint}` as values. The same endpoint can serve multiple co-located ownership slots. `validate_placement` requires complete slot coverage and the same tree content hash. Placement labels do not independently prove the number of physical hosts; the experiment records actual contacted hosts and network round trips.

The current real mechanics input uses the first 10,000 normalized MS MARCO rows. Its seeded two-child root assigns 8,593 rows to one child and 1,407 to the other. Full preparation takes 0.701 seconds on the isolated worker, including clustering; this is one mechanics run, not a throughput study. The skew is retained rather than silently rebalanced. The FineWeb scale study uses a separate exporter that preserves source float16 values when converting to float32 and does not renormalize them. Its subset exact references must be recomputed before scale claims.
