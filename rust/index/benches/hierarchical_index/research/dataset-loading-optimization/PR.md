## Summary

Each checkpoint needs a specific range of vectors from the dataset. The loaders currently decode every column in the overlapping Parquet file, then discard vectors preceding the checkpoint. A Parquet file stores independently readable sections called row groups. Selecting the required groups avoids decoding earlier vectors repeatedly.

This change selects only the embedding column and row groups intersecting the requested range. It applies the offset and length within those groups while retaining the dataset’s physical row IDs and existing float conversion. Wikipedia and MS MARCO share the range-selection helper. Existing source caches and ground-truth files remain usable without conversion or new arguments.

## Performance

On the existing EC2 benchmark node, three sequential paired runs load the first 1M vectors in ten 100k requests. The second pair reverses execution order. Both paths use the same optimized binary and jemalloc allocator. These are warm-OS-cache measurements from local NVMe; vector hashing occurs outside the loading timer.

| Dataset | Baseline median | This PR median | Loading time reduction |
|---|---:|---:|---:|
| MS MARCO | 80.64 s | 15.78 s | 80.4% |
| Wikipedia | 16.17 s | 15.85 s | 2.0% |

MS MARCO stores roughly 1M rows per shard, so avoiding repeatedly decoded prefixes saves about 65 seconds per 1M-vector input load. Wikipedia shards are already roughly the checkpoint size; its small result does not establish a meaningful performance improvement. This measures dataset loading rather than index construction, balancing, or commit latency.

Median loader-process peak RSS is 737 MiB versus 701 MiB for MS MARCO and 692 MiB versus 705 MiB for Wikipedia. The change adds no retained vector cache or additional persistent dataset files.

## Validation

Both datasets pass exact comparisons over the first 1M IDs and every f32 component. All twelve timed runs produce matching per-dataset checksums. Two regression tests cover f32/f64 embeddings, a preceding text column, null rows, row-group boundaries, empty ranges, and a missing embedding column. The hierarchical benchmark target compiles on the current feature branch. The real EC2 harness also completes two 100k checkpoints on each dataset with full-precision writing, parallel balancing, saving, and reopening. These are integration smoke checks; recall is skipped at the 200k boundary. Changed-file formatting and diff checks pass.

## Scope

The changes apply to benchmark dataset loaders under `rust/index/benches/datasets/`. The index algorithms and query/ground-truth loading code are unchanged. The baseline dataset sources are from `af6366cce6`; the candidate is `48b8c656e`. Raw measurements and reproduction sources are saved in this task’s `dataset-loading-optimization` report directory.
