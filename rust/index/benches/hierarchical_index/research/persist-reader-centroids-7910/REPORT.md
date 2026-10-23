# Persisted reader centroids: 40M Wikipedia benchmark

Persisting prepared four-bit centroid codes reduces reader-open time by 64.4% on an identical 40M tree. Opening and performing the first query falls by 12.9%. The measured normal 39M→40M commits cost 1.40 seconds more on average, while complete checkpoint wall time rises by 1.77 seconds. This change trades commit work for lower open latency; it does not establish a faster index build or a faster warm query.

## Fixed-tree reader comparison

Both revisions use the same logical saved 40M index. The candidate converts a separate copy without adding vectors, changing centroids, or balancing. Each variant has nine measurements, alternating three baseline opens and three candidate opens across three pairs. Every open creates a fresh provider with a 1 GiB cache. The first operation uses the same exact query, tau 2, maximum beam 256, minimum percentages 50/5/0, and rerank factor 8. The process uses 32 Rayon and Tokio workers. OS file caches remain warm.

| Metric, mean of nine samples | Baseline | Persisted codes |
|---|---:|---:|
| Reader open | 245.1 ms | 87.3 ms |
| Reader open range | 214.2–290.1 ms | 77.1–100.8 ms |
| First query | 871.6 ms | 885.7 ms |
| Open + first query, including provider creation | 1116.7 ms | 973.1 ms |
| Open + first query range | 1009.7–1536.3 ms | 928.9–1033.3 ms |
| Reader-owned numeric navigation payload | 18.08 MiB | 14.23 MiB |

All 18 first-query measurements return exactly the same ordered result IDs and scan counts. This component checks one fixed query rather than the full recall sample. Numeric navigation payload falls by 3.85 MiB (21.3%) because the reader no longer retains one-bit node codes. This payload estimate excludes allocator and block-cache memory.

A separate no-vector-change commit profile measures the initial format upgrade at 400 ms, versus 145 ms for the baseline no-op commit. Following candidate no-op commits take 143 ms. The extra work in the first upgrade is approximately 255 ms. These no-op commits do not estimate the full cost of a populated write checkpoint.

## Incremental writes and full recall

Each revision resumes independent writable copies of one saved 39M Wikipedia tree and adds the next 1M 1024-dimensional vectors. The baseline retains legacy node codes. The first-upgrade candidate starts from the same legacy format. The normal candidate starts from a copy converted to the new format without changing the logical tree. Each case has two repetitions, ordered baseline, upgrade, normal, normal, upgrade, baseline. The candidate's initial 39M conversion is excluded from normal-write timings and recorded separately.

| Metric, mean of two runs | Baseline | Initial upgrade | Normal persisted format |
|---|---:|---:|---:|
| Add | 12.76 s | 13.00 s | 12.88 s |
| Balance | 39.65 s | 40.29 s | 39.79 s |
| Commit | 21.34 s | 21.59 s | 22.74 s |
| Commit range | 21.21–21.47 s | 21.57–21.60 s | 22.05–23.43 s |
| Writer reopen | 315 ms | 301 ms | 326 ms |
| Complete checkpoint wall time | 137.02 s | 138.65 s | 138.79 s |
| Reader open after indexing | 705 ms | 140 ms | 155 ms |
| Warm query, tau 2 / rerank 8 | 25.45 ms | 26.70 ms | 26.60 ms |
| Warm query range | 25.4–25.5 ms | 26.1–27.3 ms | 25.2–28.0 ms |
| Cold query, tau 2 / rerank 8 | 821.25 ms | 819.50 ms | 807.70 ms |
| Recall@100 | 95.380% | 95.375% | 95.375% |
| Whole-process mean peak RSS | 210.52 GiB | 210.32 GiB | 210.68 GiB |

Normal commits rise by 6.6%, and complete checkpoint wall time rises by 1.3% in these samples. Reader open after indexing falls by approximately 78%, although that context includes allocation and cache effects from the preceding write. The isolated fixed-tree comparison provides the cleaner measurement of opening work. Query scoring itself uses the same kernel and prepared code values; these two-run means do not establish a warm-query improvement. The normal candidate's warm latency range overlaps the baseline, and the initial-upgrade range does not. More fixed-tree query measurements would be needed to distinguish a small runtime effect from tree and run variation.

All six runs exit successfully and validate 40M reachable valid IDs, no indexed embeddings without a reachable posting, no missing child nodes, and zero orphaned vectors. The recall sweep uses 100 precomputed exact L2 queries, taus 0.7/1/2, and rerank factors 1/8. Small recall differences across independently balanced trees do not establish a change in recall. The reported per-level coverage columns use writer one-bit diagnostics, not the reader's four-bit navigation, and are excluded from reader-navigation conclusions.

Complete checkpoint wall time includes pre/post-commit posting validation, cleanup, writer and input release, and garbage collection. The narrower individual phase timings exclude some of these costs. Resume throughput printed by the CLI uses the cumulative 40M count and is excluded from conclusions about the 1M increment.

## Reproduction and preservation

The baseline is `b788497434f865d7080096cb484f38de4b6477e8`; the candidate is `99683dc980bc0ccec395a41638a234690cee5277`. Both binaries use Rust 1.93.1 and the same optimized profile on lyon, an r6id.16xlarge with 64 vCPUs, 32 physical cores, and 512 GiB nominal RAM. All builds and runs execute sequentially under the shared benchmark lock. Input shards and blockfiles reside on local NVMe. These application-cold measurements retain OS caching and do not measure production object-storage latency.

Both source archives have the same benchmark-only ground-truth path override and an added component driver. The override reads the precomputed 1M–40M ground truth directly instead of changing the shared query cache. The component driver measures opening, one first query, and no-vector-change commits; the incremental runs use the existing CLI. Source-archive hashes, driver source, build logs, executable hashes, exact commands, complete CLI logs, resource measurements, and summaries are retained with the results. The PR's algorithm commit is unchanged during benchmarking.

The source 39M and 40M checkpoint metadata hashes and every source file's name/size/modification-time manifest match before and after all runs. The ground-truth SHA256 remains `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. Copies share immutable UUID-named block/root files, while checkpoint metadata has independent inodes. Forked writers create new UUIDs and garbage collection only unlinks the writable copy's names. The protected indexes and data caches remain intact.

The source checkpoints are `/mnt/data/wikipedia-40m-baseline-20261009-snapshots/39000000` and `/mnt/data/wikipedia-40m-baseline-20261009`. Remote evidence is `/mnt/data/persist-reader-centroids-7910/results`; local evidence is `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/persist-reader-centroids-7910/results`.

The incremental CLI settings are:

```sh
binary --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0 --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --verify-valid-postings --resume --save --save-dir <task-owned copy>
```
