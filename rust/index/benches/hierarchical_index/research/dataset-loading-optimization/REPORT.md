# Dataset loading optimization

Reading a checkpoint should decode the vectors that checkpoint adds. The current loaders also decode unrelated text columns and repeatedly decode earlier vectors in the same file before discarding them. Avoiding that repeated work reduces MS MARCO loading by 80.4% in paired 1M-vector tests.

## Results

These are sums of ten `load_range(offset, 100000)` calls over the first 1M vectors. The same optimized Rust binary contains the exact baseline and candidate loaders. Both use jemalloc, matching the real benchmark harness. Hashing and validation occur outside the timed loading calls.

| Dataset | Baseline median | Candidate median | Loading time reduction | Speedup |
|---|---:|---:|---:|---:|
| MS MARCO | 80.64 s | 15.78 s | 80.4% | 5.11× |
| Wikipedia | 16.17 s | 15.85 s | 2.0% | 1.02× |

The MS MARCO difference saves about 65 seconds of input loading per 1M-vector run. The Wikipedia difference is small and does not establish a meaningful improvement. These measurements exclude index construction, balancing, committing, and recall evaluation; they do not establish a change in index latency.

## Implementation

The shared Parquet reader selects the embedding column and only the file sections containing requested rows. Those independent file sections are called row groups. It then applies the request offset and length within the selected groups. The dataset loaders keep physical row IDs, skip null embeddings, and use the existing float conversions and ground-truth files.

MS MARCO stores about 1M rows per shard, so ten 100k checkpoints repeatedly decode increasing prefixes of the same shard in the baseline. Wikipedia shards already contain about 100k rows; little prefix decoding is avoided there. The optimization uses the existing compressed source cache. It requires no conversion, additional persistent dataset copy, or new benchmark arguments.

## Measurements

The machine is the existing lyon EC2 r6id.16xlarge instance with 64 vCPUs and 512 GiB RAM. Input shards reside on local NVMe. The OS file cache is warm: complete correctness comparisons read both paths before paired timing. There are three sequential baseline/candidate pairs per dataset, with the second pair reversing execution order. Other benchmark jobs hold their timed runs throughout these measurements.

| Dataset | Trial | Baseline | Candidate |
|---|---:|---:|---:|
| MS MARCO | 1 | 82.473 s | 16.194 s |
| MS MARCO | 2 | 80.429 s | 15.778 s |
| MS MARCO | 3 | 80.637 s | 15.765 s |
| Wikipedia | 1 | 16.696 s | 15.846 s |
| Wikipedia | 2 | 16.173 s | 15.846 s |
| Wikipedia | 3 | 16.109 s | 15.500 s |

Median peak process RSS is 737 MiB versus 701 MiB for MS MARCO and 692 MiB versus 705 MiB for Wikipedia. These are loader-only process maxima, including hashing and process lifetime, rather than index memory use. Each timed call materializes at most 100k requested vectors. The implementation adds no retained decoded-vector cache.

## Correctness and integration

Both real datasets pass an exact comparison over 1M IDs and all 1.024 billion f32 components per dataset. Every float is compared by its bits. All twelve timed runs produce the matching per-dataset digest: MS MARCO `25dbe9fb2de8f4e5`; Wikipedia `885629448e3714e5`.

Two regression tests check f32 and f64 list columns, a preceding text column, null rows, row-group boundaries, empty ranges, and a missing embedding column. The current feature branch’s hierarchical benchmark target compiles with the new loader. Ground-truth/query loading code is unchanged.

The real EC2 hierarchical harness also completes two 100k checkpoints on each dataset with full-precision writing, parallel balancing, saving, and reopening. Both smoke processes exit successfully. These smoke runs use the existing remote benchmark checkout with the updated loader; they verify integration rather than compare index performance. Recall is skipped because precomputed ground truth is available at 1M/10M rather than the 200k smoke boundary. Exact commands and logs are saved beside this report.

## Pull request

[PR #7860](https://github.com/chroma-core/chroma/pull/7860) targets `hierarchical-spann` and is authored by `lyon-chroma`. CI is pending at publication.

## Revisions and artifacts

The baseline dataset sources come from feature commit `af6366cce6`; the candidate is `48b8c656eab3584780b0b21c2ac6ed2f05e99808`. The remote probe checkout uses the existing benchmark dependency environment at `2b547c4f2`, with both baseline and candidate dataset source files copied in. The index implementation is irrelevant to loader-only timing.

The raw timing logs, `/usr/bin/time -v` reports, exact comparison logs, source snapshots, runner scripts, binary SHA-256, and `summary.json` are saved beside this report. The preliminary system-allocator check is kept separately on EC2 and is excluded from these results.
