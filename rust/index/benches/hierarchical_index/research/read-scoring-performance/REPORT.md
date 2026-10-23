# Posting-list read scoring performance

Implementation: [PR #7861](https://github.com/chroma-core/chroma/pull/7861), commit `9cd5638956f8904eda00d5eec06afe6773340f27`, targeting `hierarchical-spann`.

## Result

The optimized scoring loop reduces warm query latency by approximately 59% on both real 1M-vector datasets. It uses the CPU's instructions for counting set bits while preserving the stored code format, distance formula, search parameters, returned vector order, and exact score bits. The baseline and candidate search the same saved index for each dataset.

| Dataset | Reader state | Baseline average query | Candidate average query | Reduction | Baseline scoring | Candidate scoring |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Wikipedia | Warm | 12.13 ms | 4.90 ms | 59.6% | 10.67 ms | 3.40 ms |
| MS MARCO | Warm | 11.03 ms | 4.50 ms | 59.2% | 9.70 ms | 3.17 ms |
| Wikipedia | Cold reader state | 20.07 ms | 12.47 ms | 37.9% | 10.90 ms | 3.67 ms |
| MS MARCO | Cold reader state | 17.67 ms | 10.77 ms | 39.1% | 9.83 ms | 3.33 ms |

Each row is the mean of three paired runs. Each process executes 100 queries first with cleared reader posting state and block pins, then with the populated reader state. Queries run on 32 workers, matching the existing harness. These are concurrent-query averages. The cold pass keeps the operating system's filesystem cache intact; it does not measure cold object storage.

## Single-query execution

A separate pair runs the same 100 queries sequentially, using one worker. Its averages and percentiles describe this sample, not a production latency distribution. Percentiles come from 100 observations and are especially coarse at the upper tail.

| Dataset | Warm mean, baseline | Warm mean, candidate | Warm p99, baseline | Warm p99, candidate |
| --- | ---: | ---: | ---: | ---: |
| Wikipedia | 11.0 ms | 4.1 ms | 15.61 ms | 6.19 ms |
| MS MARCO | 10.3 ms | 3.7 ms | 16.20 ms | 6.15 ms |

## Why the loop is faster

A query compares four packed arrays of query bits with each stored one-bit vector code. Each comparison counts the set bits in the bitwise intersection. A portable x86 release build implements these counts with multiple ordinary instructions even though this CPU supports instructions dedicated to that calculation.

The candidate selects a scoring loop once per posting list. It uses vector population count when the CPU supports it, scalar population count on other compatible x86 CPUs, and the existing portable calculation elsewhere. Population count means counting the number of set bits in a machine word. The shared distance function is forced inline so its bit-count loop compiles under the selected CPU features. Without that step, the specialized outer loop calls a separately compiled portable inner function and fails to obtain the hardware benefit.

The reader reuses one distance buffer across its selected leaves. It pairs those distances with the unchanged vector IDs before the existing deduplication and result selection. It adds no persisted metadata and changes no search thresholds.

A deterministic kernel probe scores 4,096 generated 1,024-dimensional codes, repeating six alternating comparisons. It isolates the instruction cost; the end-to-end results above use the real datasets.

| Kernel | Typical time per code |
| --- | ---: |
| Original portable loop | 85.35 ns |
| Scalar hardware population count | 28.85 ns |
| Vector hardware population count | 15.11 ns |

The vector kernel is approximately 5.6 times faster in isolation. Disassembly shows `vpopcntq` instructions in that kernel. The gain in the full scoring stage is smaller because that stage also reads posting data, writes results, and executes the distance correction formula. An Apple ARM probe keeps the portable batch fallback near the original 15–16 ns/code after warmup; that short check does not establish a cross-platform speedup.

## Correctness and scope

All 16 reader runs return identical top-100 vector IDs, ordering, and floating-point score bits for both the cold and warm query passes. The exact comparisons cover 3,200 query invocations. Recall remains 77.81% for Wikipedia and 81.01% for MS MARCO on these saved indexes. No full-precision reranking is enabled.

A differential unit test exercises all three supported distance metrics, dimensions from 1 through 1,536, padded final words, and empty and small lists. It compares the public batch entry point and both available x86 kernels with the individual-code calculation. The release test passes on the EC2 CPU. The existing 59 policy and posting integration tests also pass locally.

The node is an r6id.16xlarge with an Intel Xeon Platinum 8375C, 64 virtual CPUs, 512 GiB RAM, and local NVMe. Both dataset indexes contain the same first 1M vectors used in the existing experiments, at 1,024 dimensions. They are the posting-preserving candidate checkpoints from October 6, so the comparison does not use a baseline that loses vectors. The cache budget is 1 GiB, the read threshold is 1.5, and each query requests 100 results. No other measured job or heavy compilation overlaps the read measurements.

## Reproduction and artifacts

The source base is `af6366cce6`, the merged hierarchical feature branch. Rust is 1.92.0, the Cargo release profile is unchanged, and `RUSTFLAGS` is unset. The temporary paired harness retains the exact original scalar posting loop under `READ_SCORING_BASELINE=1`; it adds one-worker selection and records result bits and percentiles after the timed search. Those comparison switches, result logging, and the extra reference method are excluded from the PR. The shared distance formula in the measured candidate is the PR's formula.

The report directory contains raw process logs, exact commands, the alternating run script, the source patch for the temporary harness, the kernel probe, the summary parser, and hashes of the measured binaries. The remote artifacts and preserved binaries are under `/mnt/data/read-scoring-results`. The saved source indexes are `/mnt/data/pr7856-perf-20261006-wikipedia-candidate-1` and `/mnt/data/pr7856-perf-20261006-msmarco-candidate-1`. The commands resume those indexes with no commit and no additional checkpoints, so they do not rebuild or modify them.

## Shared dispatch validation

The updated implementation is commit `198146ff1` on PR #7861.

CPU capabilities are selected once per process through a shared initializer. Single-vector scoring and batch scoring use the selected implementations. Batch loops retain direct, inlined arithmetic inside the specialized function.

The comparison uses PR revision `9cd5638956` as its baseline and the updated implementation on the same EC2 CPU. It generates 4,096 codes, checks identical output score bits after every pass, and alternates single-vector and batch scoring for six passes per dimension. The table reports medians excluding the first warmup pass. These are isolated kernel measurements; the full-reader figures above do not measure an additional latency gain from caching selection.

| Dimensions | Single baseline | Single updated | Batch baseline | Batch updated |
| --- | ---: | ---: | ---: | ---: |
| 64 | 15.35 ns/code | 10.51 ns/code | 6.66 ns/code | 6.66 ns/code |
| 1,024 | 66.98 ns/code | 20.72 ns/code | 15.07 ns/code | 15.11 ns/code |
| 1,536 | 94.18 ns/code | 20.89 ns/code | 17.37 ns/code | 17.40 ns/code |

The `dispatch` directory preserves the probe source, raw baseline/candidate logs, and test output. Both public entry points and all supported CPU-specific single-vector/batch kernels are compared with the portable arithmetic in the differential test. All eight one-bit quantization tests and 59 policy/posting integration tests pass locally; the profiling benchmark compiles.

All eight one-bit quantization tests also pass in release mode on the x86 benchmark node.

## Flattened scoring implementation

Commit `6208c4b78` removes the forwarding methods from the cached scorer pair and places the shared arithmetic directly in the scoring function. Both public functions call the selected pointers directly; specialized batch loops inline the same arithmetic. The change removes 26 lines overall. All eight one-bit tests pass locally and in x86 release mode, and the profiling benchmark compiles.

An uncontended kernel probe confirms identical score bits and retains the dispatch performance. At 1,024 dimensions, single-vector scoring takes 19.77 ns/code and batch scoring takes 15.08 ns/code. The probe logs and x86 test output are `dispatch/flattened-serial.log` and `dispatch/flattened-tests.log`.
