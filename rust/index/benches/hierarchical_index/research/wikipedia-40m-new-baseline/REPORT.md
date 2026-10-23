# Wikipedia 40M baseline

This run builds and persists an index for the first 40 million Wikipedia vectors on the current feature branch. It records every one-million-vector increment and becomes the reference for subsequent performance experiments. The four retained intermediate indexes contain 9M, 19M, 29M, and 39M vectors; the final index contains 40M.

## Results

The full process completes successfully in **43 minutes 56.57 seconds**. The CLI build summary is **42.5 minutes**, including final index diagnostics and harness overhead. The sum of the measured load, add, balance, commit, and reopen phases is **35.98 minutes**. The difference is not separately instrumented; it includes final diagnostics, garbage collection, snapshot handling, and measurement overhead. These timings should retain their distinct meanings in future comparisons.

| Measured phase | Sum across 40 increments |
|---|---:|
| Load source vectors | 3.72 min |
| Add vectors | 6.47 min |
| Balance the tree | 16.21 min |
| Persist changed blocks | 9.48 min |
| Reopen the saved writer | 5.97 s |

The final increment adds its million vectors in 13.45 seconds, balances in 43.03 seconds across eight rounds, and commits in 20.77 seconds. It loads source vectors in 5.65 seconds and reopens in 461 milliseconds. The process peaks at **209.89 GiB RSS** and averages **12.13 CPU cores**. The final tree reports 40M valid unique vectors and zero orphans.

The saved same-machine reference run takes 9 hours 13 minutes 40 seconds including recall, versus 43 minutes 56.57 seconds here: an observed **12.6× reduction in elapsed time**. Its source is `22eb704`, while this run includes both metadata-block version caching and parallel source-shard decoding. Both runs use the same approved writer settings and read sweep. This comparison measures their aggregate effect; it does not isolate an individual change or establish repeat-run variance.

## Comparison with historical runs

Index computation means placing new vectors and restoring the tree after insertion. The graph compares add plus balancing for the new baseline with integrated index time for historical Quantized SPANN. It excludes input decoding and commits from that common measure. Missing historical phases remain absent rather than becoming zeros.

![Wikipedia build comparison](wikipedia-build-comparison.png)

| Through 40M vectors | New baseline | Historical hierarchical | Historical quantized |
|---|---:|---:|---:|
| Index computation | 22.68 min | approximately 337.63 min | approximately 86.24 min |
| Commit | 9.48 min | Not reported | approximately 51.87 min |
| Final increment computation | 56.48 s | approximately 1110 s | approximately 162 s |
| Final increment balancing | 43.03 s | approximately 570 s | Included in index time |

The historical hierarchical page is https://app.notion.com/p/34458a6d8191808b8f9ed1330132c1b0. It uses 32 threads and write minimum percentages of 50, 5, and 0, which the new run omits. It does not document hardware or equivalent commit/reopen behavior and reports 579 orphaned vectors. Its minute-valued timings are rounded.

The quantized reference is the “42M Wikipedia en split full” section at https://app.notion.com/p/34558a6d81918068b089d1e0e2f51025. The graph uses its actual 1M through 40M rows from the 41.49M run. Its split/merge thresholds are 512/128, it requests two replicas and RNG factor 4, and its hardware/thread count is unstated. The new run uses thresholds 2048/512, one replica, and RNG factor 0. These are historical observations with different configurations, not a matched hardware or equal-recall experiment.

## Reads

Recall measures the fraction of the exact nearest 100 vectors returned across 100 fixed queries. The 1× setting performs no full-precision reranking; 8× checks additional candidates with original vectors before returning the nearest 100. Warm latency is the immediate second query pass. “Cold” clears application buffers between rows but retains the OS file cache, so cold results depend on preceding rows and do not represent a production network cache miss.

| Search tau | Rerank | Recall at 100 | Cold mean latency | Warm mean latency |
|---|---|---:|---:|---:|
| 0.70 | 1x | 59.44% | 215.3ms | 1.6ms |
| 0.70 | 8x | 66.33% | 772.5ms | 9.6ms |
| 1.00 | 1x | 59.44% | 11.3ms | 1.9ms |
| 1.00 | 8x | 66.33% | 731.5ms | 8.4ms |
| 2.00 | 1x | 75.45% | 78.9ms | 21.5ms |
| 2.00 | 8x | 94.38% | 810.9ms | 29.7ms |

## Checkpoints and validation

The selected snapshots preserve unchanged file manifests after later commits and garbage collection. The snapshot and final index directories reside on instance-local NVMe and do not survive an EC2 stop. Independent metadata files refer to immutable blocks that share disk space. Changed blocks and roots receive new UUIDs, so later writes create new files; deleting an unselected snapshot only removes its directory entries. The early retention test verifies identical content hashes for 1M and 2M after a later 3M commit and successfully reopens both.

The snapshot directory is `/mnt/data/wikipedia-40m-baseline-20261009-snapshots`, with subdirectories `9000000`, `19000000`, `29000000`, and `39000000`. The final output is `/mnt/data/wikipedia-40m-baseline-20261009`. The four snapshots share 171.32 GiB of unique data; final 40M adds its new blocks. Snapshot creation is outside printed checkpoint phase timings, but the full process includes it. Unselected snapshots are pruned after their completion marker; their manifests remain in the results directory. Source vectors, exact ground truth, and all benchmark evidence remain preserved.

Every retained snapshot and final 40M passes an exact persisted-ID audit. The valid IDs equal the complete prefix from zero to its vector count, and every child link resolves. Their file manifests remain unchanged after the audits. The old original 40M, 21M, and 25M index directories are removed after these checks; their manifests, logs, commands, hashes, and reports remain preserved. Source vectors and ground truth remain intact. There is 659 GiB free on NVMe after replacement.

## Reproduction and complete output

The machine is an AWS r6id.16xlarge with 64 vCPUs, 512 GiB nominal memory, and Intel Xeon Platinum 8375C processors. The run uses 32 add workers and 32 balancing workers. Source shards and block files are cached on local NVMe; the operating system file cache is not dropped. These results measure local filesystem behavior rather than production network latency.

The production source is `f3ac04b1ae9df1025cf0a960d5a91d9b451f6d18`, which includes merged PR #7874 and the parallel loading change #7872. Harness-only snapshot retention is commit `bec6e7213b37ca97066545535bc82d4252f9c0dc`; it changes no index algorithm. The executable SHA256 is `a3bd63919af743118373aa6b17c7f80d27d522dcff0a4f9ad56a12b1cc35af8c`. The complete CLI output SHA256 is `0ecd0621e20553f0cccb8dda11d743502592044dee20a583fd80fd8635af7a5d`.

The dataset is the ordered English prefix of `CohereLabs/wikipedia-2023-11-embed-multilingual-v3`, with 1024 dimensions. Exact answers are precomputed for the same 100 queries at each million-vector boundary using float64 distance accumulation. Their saved ground-truth SHA256 is `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. Data download, ground-truth generation, and compilation are excluded from the timed process.

```sh
/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline/binary --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _\,_\,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _\,_\,_ --read-level-min-pcts 50\,5\,0 --recall-rerank-vectors 1\,8 --recall-tau-values 0.7\,1\,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir /mnt/data/wikipedia-40m-baseline-20261009 --checkpoint-snapshots-dir /mnt/data/wikipedia-40m-baseline-20261009-snapshots
```

The complete output starts with `Loading Wikipedia EN from HuggingFace Hub...` and is saved as `full-cli-output.txt`. It contains every checkpoint and the full read sweep. Local evidence resides beside this report; the remote directory is `/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline`. It includes command/source/binary hashes, resource measurements, checkpoint manifests, audits, pruning records, and the snapshot harness patch.

## Every checkpoint

Times below are seconds; reopen is expressed in seconds even when the CLI prints milliseconds.

| Index size | Load | Add | Balance | Rounds | Commit | Reopen |
|---|---:|---:|---:|---:|---:|---:|
| 1M | 4.95 | 3.26 | 27.11 | 20 | 5.48 | 0.006 |
| 2M | 5.10 | 6.47 | 10.20 | 3 | 6.36 | 0.010 |
| 3M | 5.08 | 7.21 | 10.28 | 2 | 6.93 | 0.016 |
| 4M | 5.51 | 6.94 | 9.12 | 1 | 7.32 | 0.022 |
| 5M | 5.52 | 7.32 | 10.84 | 1 | 7.63 | 0.024 |
| 6M | 5.46 | 7.26 | 12.99 | 2 | 8.17 | 0.037 |
| 7M | 5.58 | 7.45 | 12.59 | 1 | 8.64 | 0.044 |
| 8M | 5.46 | 7.46 | 12.88 | 2 | 8.98 | 0.039 |
| 9M | 5.48 | 7.51 | 18.30 | 1 | 8.97 | 0.055 |
| 10M | 5.49 | 7.68 | 14.60 | 1 | 9.26 | 0.070 |
| 11M | 5.43 | 7.87 | 15.45 | 2 | 9.96 | 0.075 |
| 12M | 5.48 | 7.91 | 16.62 | 1 | 10.64 | 0.060 |
| 13M | 5.41 | 8.23 | 19.36 | 2 | 11.44 | 0.099 |
| 14M | 5.51 | 8.25 | 24.52 | 8 | 11.43 | 0.073 |
| 15M | 5.54 | 8.54 | 26.30 | 8 | 11.93 | 0.115 |
| 16M | 5.51 | 8.74 | 20.76 | 3 | 11.99 | 0.126 |
| 17M | 5.46 | 8.65 | 19.93 | 2 | 12.73 | 0.087 |
| 18M | 5.49 | 9.04 | 19.85 | 2 | 12.91 | 0.088 |
| 19M | 5.58 | 9.33 | 20.89 | 2 | 13.43 | 0.095 |
| 20M | 5.53 | 9.26 | 22.09 | 3 | 13.93 | 0.105 |
| 21M | 5.51 | 9.54 | 23.99 | 3 | 14.19 | 0.177 |
| 22M | 5.53 | 10.07 | 22.37 | 2 | 14.84 | 0.198 |
| 23M | 5.71 | 10.02 | 24.15 | 4 | 14.76 | 0.233 |
| 24M | 5.51 | 10.15 | 25.68 | 1 | 15.21 | 0.144 |
| 25M | 5.44 | 10.73 | 25.82 | 2 | 16.32 | 0.154 |
| 26M | 4.99 | 10.79 | 27.26 | 3 | 16.91 | 0.156 |
| 27M | 6.06 | 11.20 | 28.13 | 2 | 16.59 | 0.175 |
| 28M | 5.52 | 11.19 | 26.94 | 2 | 17.13 | 0.163 |
| 29M | 6.27 | 11.52 | 35.95 | 8 | 17.38 | 0.333 |
| 30M | 5.45 | 11.78 | 37.01 | 8 | 18.15 | 0.356 |
| 31M | 6.18 | 11.79 | 37.96 | 8 | 18.33 | 0.200 |
| 32M | 5.52 | 11.83 | 34.70 | 8 | 19.14 | 0.339 |
| 33M | 5.54 | 12.15 | 37.45 | 8 | 18.95 | 0.210 |
| 34M | 6.03 | 12.26 | 29.47 | 2 | 19.33 | 0.220 |
| 35M | 6.22 | 12.47 | 30.52 | 2 | 20.03 | 0.225 |
| 36M | 6.15 | 12.88 | 32.89 | 2 | 20.12 | 0.234 |
| 37M | 5.76 | 13.10 | 33.85 | 3 | 22.56 | 0.243 |
| 38M | 5.59 | 13.46 | 32.32 | 2 | 24.63 | 0.246 |
| 39M | 5.80 | 13.47 | 38.58 | 8 | 25.38 | 0.257 |
| 40M | 5.65 | 13.45 | 43.03 | 8 | 20.77 | 0.461 |

## Published result

The verified wiki page is [40M Wikipedia — current baseline, 2026-10-09](https://app.notion.com/p/3f458a6d8191812e8386f516e792e7a6). It contains the rendered PNG graph, complete CLI output attachment, checkpoint CSV, phase tables, recall, and reproduction details. The source-code patch stays local and on EC2 because automatic approval review rejects exporting repository source under the benchmark-results authorization.
