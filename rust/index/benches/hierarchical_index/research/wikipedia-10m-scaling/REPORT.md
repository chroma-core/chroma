# Wikipedia index latency from 1M to 10M vectors

## Result

The index reaches 10M vectors successfully. Warm-query latency changes modestly as the index grows, while balancing, cold-reader work, and memory grow substantially. A separate audit confirms that all 10,000,000 persisted vector IDs are reachable from the tree root and every child link resolves.

The measured baseline uses the merged feature branch with the posting-preservation fix and the eight-round balancing cap. Its first 10M vectors have 1,024 dimensions. The build saves ten checkpoints of 1M vectors. All subsequent read measurements use those exact saved checkpoints and the same 100 query vectors, with exact neighbors computed separately for each prefix.

| Measurement | 1M checkpoint | 5M checkpoint | 10M checkpoint |
| --- | ---: | ---: | ---: |
| Add the next checkpoint's vectors | 3.00 s | 6.73 s | 7.58 s |
| Balance the checkpoint | 43.39 s | 13.34 s | 43.78 s |
| Prepare the commit | 3.42 s | 4.60 s | 5.92 s |
| Flush to storage | 2.06 s | 2.96 s | 3.85 s |
| Reopen the writer | 6 ms | 25 ms | 65 ms |
| Sampled checkpoint peak memory | 9.21 GiB | 34.71 GiB | 64.28 GiB |
| Open a reader in a separate process | 2 ms | 8 ms | 24 ms |
| Warm query, average | 11.8 ms | 12.1 ms | 13.4 ms |
| Reader-cold query, average | 19.7 ms | 31.3 ms | 60.0 ms |
| Vectors scanned per query | 142.5k / 14.25% | 133.9k / 2.68% | 137.6k / 1.38% |
| Recall at 100 neighbors | 77.69% | 75.40% | 74.76% |

The first checkpoint performs ten early balances after 100k additions each. Its balancing time and round count are sums across those calls, so they describe initial construction rather than one ordinary balance of an established tree.

## Writing and committing

Each row adds 1M vectors. Timings include the actual load, add, balance, commit, and reopen phases. Memory is the checkpoint's sampled process resident memory, including caches and allocator memory, rather than only the logical index contents. The displayed durations have rounding.

| Indexed vectors after checkpoint | Dataset load | Add | Balance | Balance rounds | Commit preparation | Flush | Writer reopen | Peak memory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1M | 14.63 s | 3.00 s | 43.39 s | 23 across ten calls | 3.42 s | 2.06 s | 6 ms | 9.21 GiB |
| 2M | 15.63 s | 5.98 s | 8.23 s | 2 | 3.80 s | 2.50 s | 11 ms | 15.88 GiB |
| 3M | 15.76 s | 7.07 s | 9.74 s | 3 | 4.03 s | 2.25 s | 14 ms | 21.56 GiB |
| 4M | 15.81 s | 6.89 s | 10.63 s | 2 | 4.35 s | 2.86 s | 22 ms | 27.87 GiB |
| 5M | 15.81 s | 6.73 s | 13.34 s | 1 | 4.60 s | 2.96 s | 25 ms | 34.71 GiB |
| 6M | 15.74 s | 7.24 s | 14.86 s | 1 | 4.92 s | 3.18 s | 35 ms | 40.51 GiB |
| 7M | 16.04 s | 7.29 s | 17.24 s | 1 | 5.15 s | 3.16 s | 45 ms | 46.61 GiB |
| 8M | 15.99 s | 7.78 s | 18.13 s | 2 | 5.35 s | 3.32 s | 50 ms | 50.29 GiB |
| 9M | 15.84 s | 7.08 s | 21.85 s | 3 | 5.60 s | 3.39 s | 43 ms | 53.07 GiB |
| 10M | 16.03 s | 7.58 s | 43.78 s | 2 | 5.92 s | 3.85 s | 65 ms | 64.28 GiB |

No balancing-cap warning appears. Established-tree checkpoints require one to three rounds. The benchmark's process exits successfully, and the separate exact persisted-ID audit also passes.

The total build figure is approximately 553.3 seconds, or 9.2 minutes. The full process, including final recall, takes 557.52 seconds. Printed phase sums are 66.64 seconds for adding, 201.19 seconds for balancing, 157.28 seconds for input loading, 47.14 seconds for commit preparation, 29.53 seconds for flushing, and 0.316 seconds for writer reopens. The remaining approximately 51 seconds include the benchmark's final full posting-list materialization, tree diagnostics, garbage collection, and other bookkeeping. That remainder is not an additional production open or commit phase.

Dataset loading remains approximately 15–16 seconds per million vectors. Adding after initial construction stays near 6–8 seconds per million. Balancing grows from 8.23 seconds at 2M to 43.78 seconds at 10M; the final checkpoint still needs only two rounds, so a large balance duration does not require reaching the round cap. Commit preparation and flushing together grow from 6.29 seconds at 2M to 9.78 seconds at 10M.

The logical writer memory and the storage blocks it retains explain much of the memory growth. At 10M, the writer reports about 20.25 GiB of logical contents, including 18.77 GiB of full-precision embeddings and 1.39 GiB of posting data. It also retains 35.76 GiB of storage blocks before releasing their references. Its full-precision centroids and the packed navigation copy are about 28.6 MiB each. These estimates omit some container and allocator overhead. They are separate measurements and should not be treated as a complete additive accounting of resident memory.

A 1 GiB shared block-cache limit does not limit all reader references to blocks. At the final checkpoint, retained storage blocks comprise 34.51 GiB of vector data and 1.25 GiB of posting data. Releasing those references reduces process resident memory by about 27.60 GiB in this run. This makes block-reference lifetimes and embedding reuse important targets for larger indexes. The process high-water mark reported by the operating system is 64.31 GiB. The final live index occupies approximately 41.34 GiB on NVMe.

## Reading

These measurements run separately after the build. Each process reopens one immutable checkpoint, performs no writes, and evaluates the same 100 real corpus queries. The first pass clears reader-owned posting data and block references; the second pass reuses data populated by the first. The operating system's filesystem cache is not cleared. The measurements are means across concurrent queries, not latency percentiles or an object-storage benchmark.

| Indexed vectors | Tree depth | Nodes, rounded | Reader open | Reader-cold query | Warm query | Warm navigation | Warm posting scoring | Scanned vectors / fraction | Recall at 100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 1M | 3 | 721 | 2 ms | 19.7 ms | 11.8 ms | 0.188 ms | 10.4 ms | 142.5k / 14.25% | 77.69% |
| 2M | 3 | ~1.5k | 4 ms | 22.1 ms | 12.5 ms | 0.373 ms | 10.8 ms | 128.0k / 6.40% | 76.35% |
| 3M | 3 | ~2.3k | 7 ms | 23.7 ms | 11.5 ms | 0.515 ms | 9.7 ms | 131.1k / 4.37% | 75.86% |
| 4M | 3 | ~3.0k | 8 ms | 29.3 ms | 12.1 ms | 0.697 ms | 10.2 ms | 137.2k / 3.43% | 75.81% |
| 5M | 3 | ~3.7k | 8 ms | 31.3 ms | 12.1 ms | 0.884 ms | 10.0 ms | 133.9k / 2.68% | 75.40% |
| 6M | 3 | ~4.4k | 14 ms | 34.9 ms | 12.5 ms | 1.0 ms | 10.2 ms | 137.8k / 2.30% | 74.87% |
| 7M | 3 | ~5.1k | 12 ms | 36.1 ms | 12.8 ms | 1.2 ms | 10.3 ms | 139.2k / 1.99% | 74.90% |
| 8M | 3 | ~5.8k | 17 ms | 43.7 ms | 12.9 ms | 1.4 ms | 10.3 ms | 139.3k / 1.74% | 75.01% |
| 9M | 4 | ~6.6k | 22 ms | 40.0 ms | 13.1 ms | 1.7 ms | 10.2 ms | 137.0k / 1.52% | 74.89% |
| 10M | 4 | ~7.3k | 24 ms | 60.0 ms | 13.4 ms | 1.9 ms | 10.2 ms | 137.6k / 1.38% | 74.76% |

Warm reads remain close to constant time because the number of scored vectors remains approximately constant. The tree's beam limits bound the candidate set as the index grows. Posting-list scoring still consumes about 76% of warm latency at 10M. Navigation grows about tenfold, from 0.188 milliseconds to 1.9 milliseconds. Preparing the query relative to leaf centroids takes 0.580 milliseconds at 10M, and selecting results takes 0.329 milliseconds. The read path uses compressed distances without full-precision reranking.

Cold-reader latency grows much more than warm latency. At 10M, the separate replay records 60.0 milliseconds; the final evaluation immediately after the build records 94.5 milliseconds on the same saved index. Warm latency is 13.4 and 13.3 milliseconds respectively. These two observations show that process and cache state matter even when the indexed data and search settings match.

The current timers do not attribute most of the reader-cold increase. At 10M on replay, navigation, query preparation, posting scoring, and selection sum to about 15.65 milliseconds, leaving about 44.35 milliseconds for lazy loading, waiting, and other work. We should instrument those operations rather than assume disk latency or shared-cache capacity explains the difference. The build process reports no physical filesystem input and no major page faults, which reinforces that these measurements do not establish cold physical-storage latency.

Recall falls by 2.93 percentage points between the 1M and 10M checkpoints. That is a size comparison under fixed search settings, not a comparison between optimization PRs. The fixed beam bounds and the changing tree and neighbor population can affect quality. These 100 query vectors are sampled from the first million corpus vectors, rather than from an independent query dataset.

## Where to focus next

1. Instrument reader-cold posting loading, decoding, copying, and waiting separately. This work accounts for most of the cold latency at 10M and scales more than the scoring stage.
2. Refresh the balancing profile on the larger tree. Normal round counts still permit expensive checkpoints, and the final balance takes 43.78 seconds. Separate routing, neighbor embedding access, registration, and storage-block retention before choosing another optimization.
3. Shorten storage-block reference lifetimes and avoid retaining both decoded blocks and copied embeddings for most of the old corpus. The 1 GiB shared cache does not cap these references. This is a memory target with a possible latency benefit that still needs measurement.
4. Continue posting-scoring optimization. The baseline spends about 10 milliseconds there at every index size. Once that work becomes cheaper, navigation's 1.9 milliseconds at 10M becomes a larger fraction of query latency.
5. Separate commit preparation further and evaluate independent flushes. The final commit costs 9.78 seconds; it is smaller than balancing but remains measurable. Opening itself is a lower priority on this local-storage fixture.

## Method, provenance, and limitations

The baseline source is `af6366cce6badf39cb5fb10f739a35c45f7e0a64` on `hierarchical-spann`. The executable SHA-256 is `af1bc09dd4139e82ffdab281648f748e4651f635c0ac377bdbd11823d6c48f42`. It is built from an isolated Git archive with Rust 1.92.0, standard release/bench settings, unset `RUSTFLAGS`, and jemalloc. The package is cleaned before compilation to prevent an executable from another checkout being treated as fresh in the shared Cargo target. No index algorithm or loader changes are included.

An exploratory run uses the read-scoring agent's cached executable, SHA-256 `44e5783ab5a5b3602bd95eeea274dffd956f2859b32efb5c048c9cc79402d564`, which includes the unmerged reader optimization and temporary comparison instrumentation. Those artifacts are preserved separately and excluded from the baseline tables. The source diff changes reader scoring and read instrumentation, with no writer implementation change. Its write timings nevertheless show large parallel-build variability: initial balancing takes 26.67 seconds and final balancing takes 23.63 seconds, compared with 43.39 and 43.78 seconds in the primary baseline. One clean baseline build supports the observed scaling pattern, not a statistical performance estimate.

The machine is Lyon's existing EC2 r6id.16xlarge, with 64 virtual CPUs and 512 GiB RAM. All measured jobs run sequentially on local NVMe. Source Parquet files and filesystem data remain cached. No physical cache eviction, object storage, production cluster, or network transfer is part of these measurements. Checkpoints have hardlink snapshots to support later reads; snapshot creation takes 0.026–0.262 seconds per checkpoint and is logged separately. Snapshot overhead is not included in the phase timers, though it runs alongside preparation for the following checkpoint. The benchmark retains its normal garbage collection behavior.

The common command arguments are `--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save`. There are no per-level minimum percentages. The tree uses branching factor 100, split threshold 2048, merge threshold 512, and one replica. Read-only replays add `--resume --no-commit`, select the matching checkpoint count, and use the saved directory; they load no new corpus vectors and create no checkpoint.

All 100 cached Wikipedia shard identities, sizes, and their lexical order match the exact-ground-truth source. Sampled query vectors also match after conversion to float32. The separate exact scan produces top-100 neighbors for all ten 1M prefixes in 170.6 seconds. It uses float64 accumulation for distances and the same fixed 100 queries. All output neighbor IDs are unique and within their prefix; the 1M and 10M neighbor sets match the existing exact files for all three metrics. Only L2 is used in this benchmark. Input preparation and ground-truth computation are separate from the timed index operations.

The exact persisted-posting audit uses a small temporary harness addition after the build and all timed reads. It loads postings, checks the root-reachable valid-ID set equals `[0, 10,000,000)`, and checks that traversed child links exist. Its code patch, executable hash, output, and timing are preserved. The audit is not part of the performance measurements.

Earlier 1M results use 100k checkpoints. This run uses 1M checkpoints and only subdivides the first checkpoint into 100k early balances. Commit frequency, the working set retained between balances, and policy refresh boundaries therefore differ. Use the within-run size trend and per-vector costs rather than attributing every difference from earlier builds to tree size alone.

The clean build runs from 11:34:49 to 11:44:07 PDT on October 8, 2026. Intermediate ground-truth generation and the ten read-only measurements finish by 11:49:54. The separate persisted-posting audit finishes at 11:50:54. The EC2 timing window is released, and the machine remains running for other work.

## Artifacts

The `baseline/` directory contains raw build and reader logs, exact commands, resource measurements, executable hashes, revisions, parsers, the audit patch, and the exact ground-truth file with its manifest. `baseline/summary.json` contains checkpoint write measurements; `baseline/read-summary.json` contains the reader measurements. The `exploratory-unmerged-read-kernel/` directory contains the excluded exploratory run and its provenance note. The scripts in this report directory reproduce setup, loading checks, snapshot capture, read replays, and auditing.

Remote results are under `/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008`. The final index is `/mnt/data/wikipedia-10m-scaling-baseline-20261008`; all ten snapshots are under `/mnt/data/wikipedia-10m-scaling-baseline-snapshots-20261008`. These index files are on instance-local NVMe and are lost when the instance stops. The report, logs, and exact ground truth also have local copies beside this document.
