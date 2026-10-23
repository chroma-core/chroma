# Deferred destination balancing

Implementation: [PR #7866](https://github.com/chroma-core/chroma/pull/7866), targeting `hierarchical-spann`.

The focused change reduces median balancing time from 24.46 seconds to 22.13 seconds, an observed 9.5% reduction when adding the final million Wikipedia vectors to a saved 9M index. The median total of the printed checkpoint phases falls from 55.61 seconds to 53.58 seconds, or 3.6%. Four fresh baseline runs and three final candidate runs execute sequentially on the same EC2 machine with the same input checkpoint and settings.

## What changes

A split or merge scans vectors and publishes replacement postings into destination leaves. Each successful reassignment can make its destination need balancing. The writer records that follow-up instead of recursively entering another split or merge during the source's scan. Once the current split or merge completes, its worker drains the pending destinations.

Each worker uses its own stack and set of pending `(leaf ID, depth)` pairs. Duplicate requests at the same depth share one pending task. Requests at different depths remain distinct because depth controls neighbor reassignment. Newly generated cascades run before older pending destinations, which preserves the depth-first character of recursion. Tasks beyond depth four are discarded exactly as recursive calls beyond that limit are; the existing outer rounds discover any remaining size violations, stop after eight rounds, and report nonconvergence. A task rechecks its leaf before acting, so another worker can split or remove a queued destination safely through the existing checks.

Replacement posting publication and vector-version updates happen immediately in the existing order. The deferral covers destination balancing only. It retains the current round scheduler, region assignment, merge reservations, and leaf balancing guards. It requires no shared queue lock or new writer fields. The implementation is confined to `rust/index/benches/hierarchical_index/writer/writer.rs`.

## Measured results

| Measurement | Baseline median, four runs | Deferred median, three runs |
| --- | ---: | ---: |
| Balance | 24.46 s | 22.13 s |
| Add | 7.64 s | 7.68 s |
| Commit preparation and flush | 9.31 s | 9.54 s |
| Sum of checkpoint phases, including data loading | 55.61 s | 53.58 s |
| Scrub calls | 212,800 | 34,500 |
| Reassignment calls | 343,000 | 345,800 |
| Average split depth | 2.735 | 2.57 |
| Process peak resident memory | 63.09 GiB | 63.76 GiB |
| Recall at 100 neighbors | 74.545% | 74.66% |

Deferral reduces scrub calls by 83.8% while keeping the number of reassignment calls within about 1% of the baseline median. Multiple appends can share one destination cleanup instead of repeatedly loading and scanning that leaf. Peak resident memory increases by about 0.67 GiB, or 1.1%. Commit time is slightly higher in these samples. All final candidate and baseline runs finish balancing in one round without reaching the convergence limit.

| Run | Balance | Add | Commit | Checkpoint phases | Recall | Peak memory |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline 1 | 24.17 s | 7.63 s | 9.31 s | 55.25 s | 74.55% | 63.10 GiB |
| Baseline 2 | 24.41 s | 7.64 s | 9.03 s | 55.02 s | 74.52% | 62.72 GiB |
| Baseline 3 | 24.50 s | 7.64 s | 9.31 s | 55.96 s | 74.56% | 63.09 GiB |
| Baseline 4 | 24.94 s | 7.61 s | 9.76 s | 56.36 s | 74.54% | 63.16 GiB |
| Deferred 2 | 21.89 s | 7.71 s | 9.58 s | 53.79 s | 74.59% | 63.76 GiB |
| Deferred 3 | 22.26 s | 7.68 s | 9.48 s | 53.58 s | 74.66% | 63.72 GiB |
| Deferred 4 | 22.13 s | 7.60 s | 9.54 s | 53.45 s | 74.89% | 63.94 GiB |

The improvement is consistent across these samples, but it describes one saved Wikipedia checkpoint and one machine. The four baselines are fresh controls shared with the continuous-scheduler experiment; their executable is unmodified. The three final candidates run consecutively after those controls. Parallel balancing produces different valid trees, so exact query outputs need not match and the small recall differences do not establish a recall improvement. Each run uses the same 100 corpus queries and exact precomputed ground truth. Warm reader latency remains 7.2–7.4 milliseconds across the baseline and final runs.

## Correctness

The two integration test targets pass all 63 test executions. New tests prove that reassignment publishes reachable replacement postings while an oversized destination remains unsplit until the source scan finishes. They also verify causal depth, the depth-four neighbor-reassignment cutoff, repeated-task deduplication, re-enqueueing completed work, draining pending tasks, and a destination disappearing before execution. Existing tests exercise concurrent splits, overlapping merges, and persisted posting/version behavior.

All three saved 10M outputs pass the existing exact posting-audit executable. Audit results are recorded in separate logs and excluded from timing. The audit requires all 10,000,000 IDs to have a current-version posting reachable from the root and verifies every referenced child exists.

## Alternatives considered

A FIFO queue is a useful comparison but changes the traversal more substantially. Its exploratory run takes 23.73 seconds and performs 533,700 reassignments with average split depth 1.87, versus roughly 343,000 and 2.74 in the baseline. Its peak memory is 68.17 GiB. The depth-first pending stack keeps the work volume closer to the recursive implementation and gives the repeated results above. The FIFO executable and log are retained separately; they are excluded from the final candidate summary.

A continuous global scheduler also defers destination work, but changes how workers discover and claim regions. This PR deliberately measures deferral within the existing rounds. It overlaps conceptually with PR #7864 and should be reconciled with that implementation rather than merging duplicate queues without review.

## Reproduction and provenance

The baseline is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. The final candidate is `4747e1207d27ae09cde30e0406c3416393e38f0c` on `codex/deferred-destination-balancing`, based on `hierarchical-spann`. The baseline executable SHA-256 is `d8388df10f7e90b1115589617c3e853b0d837a670bca03729fa5f0fbf007b3d6`. The final executable SHA-256 is `5e20c3e529b273f5b18b65ac0402693368ff4e06d9a4c344ec618791cd4accc2`. A package clean and isolated source directory prevent stale shared Cargo outputs from entering the comparison. Compilation takes 55.75 seconds and is excluded from measurements. Rust is 1.92.0 with the standard bench profile and existing allocator configuration.

The machine is the existing r6id.16xlarge with 64 virtual CPUs and 512 GiB RAM. Builds and measured jobs are serialized with `/mnt/data/chroma-benchmark.lock` and explicit handoffs between agents. Every replay uses an independent copy of `/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000`; copied-file inode checks prevent accidental hardlink sharing with the immutable input. Input checkpoint metadata hashes match before and after execution. Source vectors and ground truth are reused from the existing local cache.

```text
--dataset wikipedia-en --metric l2
--checkpoint 10 --checkpoint-size 1000000
--threads 32 --balance-threads 32
--write-navigation fp --fp-npa
--write-beam-tau 0.5 --write-level-taus _,_,0.30
--write-rng-factor 0
--num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1
--max-cache-bytes 1073741824 --compute-gt-clusters false
--save --resume --save-dir <independent writable copy>
```

Raw logs, commands, process resource measurements, source hashes, executable hashes, local test output, and scripts are stored beside this report. The remote result directory is `/home/ubuntu/benchmark-data/deferred-destination-balancing`. The final saved outputs are `/mnt/data/deferred-destination-balancing-candidate-2`, `candidate-3`, and `candidate-4` under the same directory prefix. `candidate-1` is the exploratory FIFO run.
