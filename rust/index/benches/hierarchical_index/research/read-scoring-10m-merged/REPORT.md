# Merged posting-list scoring at 10M Wikipedia vectors

## Result

The merged reader optimization reduces average warm-query latency by 48.6% on the same 10M-vector index. Scoring the vectors in posting lists takes 63.3% less time. Three alternating pairs compare preserved baseline and merged executables against one immutable checkpoint, so the comparison includes no difference in index construction.

| Mean across three runs | Baseline | Merged reader | Reduction |
| --- | ---: | ---: | ---: |
| Warm query | 13.93 ms | 7.17 ms | 48.6% |
| Warm posting-list scoring | 10.80 ms | 3.97 ms | 63.3% |
| Warm navigation | 1.87 ms | 1.90 ms | Approximately unchanged |
| Warm query preparation | 0.614 ms | 0.633 ms | Approximately unchanged |
| Warm result selection | 0.349 ms | 0.358 ms | Approximately unchanged |
| Reader-cold query | 55.50 ms | 42.53 ms | 23.4% observed |
| Vectors scored per query | 146k / 1.46% | 146k / 1.46% | Unchanged |
| Recall at 100 neighbors | 74.55% | 74.55% | Unchanged |

Warm queries are 1.94 times faster. The scoring stage is 2.72 times faster. The remaining navigation and query preparation now account for a larger fraction of latency. Scoring contributes approximately 55% of merged warm-query time rather than 77% of baseline time.

Cold-reader measurements vary more than warm measurements. Baseline means range from 51.1 to 63.2 milliseconds across processes; merged means range from 37.8 to 46.7 milliseconds. Clearing reader-owned caches does not clear the operating system's filesystem cache. This is a local-storage, cached-filesystem measurement rather than a cold object-storage measurement. Most cold latency still lies outside the currently separated navigation, preparation, scoring, and selection timers.

## Individual paired runs

Each process evaluates 100 identical queries in a reader-cold pass followed by a warm pass. Each cell is that process's mean query latency. Pair two reverses execution order to reduce systematic order bias.

| Pair | Execution order | Baseline cold | Merged cold | Baseline warm | Merged warm |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | Baseline, merged | 52.2 ms | 37.8 ms | 13.9 ms | 7.1 ms |
| 2 | Merged, baseline | 63.2 ms | 43.1 ms | 13.9 ms | 7.1 ms |
| 3 | Baseline, merged | 51.1 ms | 46.7 ms | 14.0 ms | 7.3 ms |

## Resuming the index from 9M to 10M

The merged executable successfully adds and commits the final million vectors. The writable working directory starts with checkpoint index 8 and exactly 9,000,000 vectors. Its resulting metadata records checkpoint index 9 and exactly 10,000,000 vectors. The original checkpoint remains unchanged.

| Phase for the final 1M vectors | Duration |
| --- | ---: |
| Initial open of the 9M writer | 62 ms |
| Dataset loading | 13.97 s |
| Routing and appending | 7.58 s |
| Balancing | 23.12 s, one round |
| Commit preparation | 5.87 s |
| Flush | 3.49 s |
| Reopen after committing | 60 ms |
| Sum of printed checkpoint phases | 54.09 s |
| Entire profiling process | 91.47 s |
| Sampled checkpoint peak memory | 62.87 GiB |

No balancing-cap warning appears. The entire profiling process includes initial opening, final posting-list materialization, tree diagnostics, and read evaluation, which explain time beyond the printed checkpoint phases. The operating system reports a process memory high-water mark of approximately 62.92 GiB.

The new index's reader opens in 18 milliseconds. Its mean warm query is 7.2 milliseconds, including 1.8 milliseconds of navigation, 0.627 milliseconds of query preparation, 4.0 milliseconds of posting scoring, and 0.355 milliseconds of selection. Its cold mean is 81.5 milliseconds. It scans approximately 145.9k vectors per query and achieves 74.57% recall at 100 neighbors.

These new-index values are not the controlled improvement comparison. Parallel balancing can produce a different tree, and resuming in a fresh process changes cache state. The improvement percentages above come exclusively from baseline and merged readers using the same original 10M checkpoint.

The profiling harness prints an overall throughput using all 10M indexed vectors as its numerator even though this process adds only 1M. That value is excluded from this report. The printed add-phase throughput of 131,968 vectors per second uses the actual million additions and is valid for that phase.

## Correctness

The six timed runs agree on scan counts and recall. Separate temporary harnesses also record every returned ID, its position, and each float32 score's exact bits for both cold and warm passes. Those diagnostic runs are excluded from latency comparisons. All 200 query/pass combinations match exactly: every returned ID, its ordering, and all 100 float32 score bit patterns. Their equality result is preserved in `correctness-result.txt`.

## Method and provenance

The merged source is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34` on `hierarchical-spann`, which includes the merged posting-scoring PR #7861 and dataset-loader PR #7860. Its executable SHA-256 is `d8388df10f7e90b1115589617c3e853b0d837a670bca03729fa5f0fbf007b3d6`. The baseline source is `af6366cce6badf39cb5fb10f739a35c45f7e0a64`; its preserved executable SHA-256 is `af1bc09dd4139e82ffdab281648f748e4651f635c0ac377bdbd11823d6c48f42`.

The merged package is cleaned and recompiled from an isolated Git archive with Rust 1.92.0 and standard bench settings. Compilation takes 53.53 seconds, and the executable is copied before any diagnostic rebuild. Source hashes, package-clean output, build output, and both executable hashes are preserved. There is no temporary result-printing instrumentation in either timed executable.

The resume uses an independent file copy of the preserved 9M snapshot. Every copied file's inode is checked to differ from the original. This avoids allowing a writable working copy to change files shared with a preserved fixture. The original 9M and 10M metadata hashes match before and after the timed work.

The machine is Lyon's existing EC2 r6id.16xlarge with 64 virtual CPUs and 512 GiB RAM. All jobs execute sequentially on local NVMe. Exact ground truth and cached source files are reused; no new dataset download or baseline index build is necessary. The queries are the same 100 corpus vectors used by the earlier exact-prefix benchmarks. Timings describe concurrent-query means rather than percentiles.

The common settings are `--dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false`. There are no per-level minimum percentages. The resume adds `--save --resume` and uses the independent working directory. Read pairs use `--resume --no-commit` against the original fixed 10M fixture, so they add no vectors and create no checkpoint.

The diagnostic harness change adds only query numbering and printing returned IDs with score bits after timing each query. Baseline and merged diagnostic sources retain their respective scoring implementations. Their patches, source revisions, executable hashes, raw outputs, and validation script are preserved. A diagnostic dependency rebuild adds preparation time but affects none of the six timed runs.

## Artifacts

This directory contains raw resume and reader logs, exact commands, process resource measurements, source and executable hashes, before/after checkpoint metadata, the paired-run summary, diagnostic outputs, and their comparison result. `summary.json` preserves every timing row and the computed means. `run.sh`, `correctness.sh`, and `summarize.py` document the experiment.

Remote results are under `/home/ubuntu/benchmark-data/read-scoring-10m-merged-22eb704`. The new 10M index is `/mnt/data/read-scoring-10m-merged-22eb704`. The preserved input and comparison fixtures are `/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000` and `/mnt/data/wikipedia-10m-scaling-snapshots-20261008/10000000`. Index files are on instance-local NVMe; logs and reports also have local copies beside this document.
