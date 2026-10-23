# PR #7856 performance comparison

## Result

The merge correctness fix increases average MS MARCO build time by 10.5% in these runs. It decreases average Wikipedia build time by 3.6%. The MS MARCO increase comes from balancing, including one run that reaches the 100-round limit at the 900k checkpoint. Two repeats per version establish the observed costs but do not establish a stable long-term performance estimate.

All eight processes exit successfully. An exit status of zero does not imply balancing converges: the second MS MARCO candidate run stops balancing with two undersized leaves remaining. The PR remains open; resolving or characterizing this behavior is a useful next step before merging.

## Comparison

Each cell is the mean of two runs. Total build time comes from the benchmark's reported throughput, before recall. Phase times are sums of the ten printed checkpoint durations, which have rounding. Total build time also includes bookkeeping and the final tree summary; it is not exactly the sum of the listed phases. Loading is the dataset batch-loading phase, while lazy reads during balancing are included in balancing time.

| Dataset | Measurement | Baseline | PR #7856 | Change |
| --- | --- | ---: | ---: | ---: |
| MS MARCO | Total build time | 131.8 s | 145.7 s | +10.5% |
| MS MARCO | Add time | 2.55 s | 2.59 s | +1.5% |
| MS MARCO | Balancing time | 35.13 s | 50.20 s | +42.9% |
| MS MARCO | Dataset loading time | 83.94 s | 82.55 s | -1.7% |
| MS MARCO | Commit time | 6.73 s | 6.84 s | +1.6% |
| MS MARCO | Peak process memory | 7.17 GiB | 7.31 GiB | +1.9% |
| Wikipedia | Total build time | 64.5 s | 62.2 s | -3.6% |
| Wikipedia | Add time | 2.93 s | 3.11 s | +6.1% |
| Wikipedia | Balancing time | 34.31 s | 32.09 s | -6.5% |
| Wikipedia | Dataset loading time | 16.25 s | 16.04 s | -1.3% |
| Wikipedia | Commit time | 6.94 s | 6.89 s | -0.8% |
| Wikipedia | Peak process memory | 7.86 GiB | 7.77 GiB | -1.2% |

## Individual runs

| Dataset | Version | Repeat | Build time | Add time | Balance time | Balancing rounds across checkpoints |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| MS MARCO | Baseline | 1 | 134.1 s | 2.54 s | 37.46 s | 11 |
| MS MARCO | Baseline | 2 | 129.6 s | 2.56 s | 32.80 s | 11 |
| MS MARCO | PR | 1 | 135.0 s | 2.61 s | 39.58 s | 14 |
| MS MARCO | PR | 2 | 156.3 s | 2.56 s | 60.81 s | 110 |
| Wikipedia | Baseline | 1 | 63.2 s | 2.94 s | 33.32 s | 12 |
| Wikipedia | Baseline | 2 | 65.9 s | 2.92 s | 35.30 s | 11 |
| Wikipedia | PR | 1 | 61.4 s | 3.04 s | 31.52 s | 16 |
| Wikipedia | PR | 2 | 63.1 s | 3.18 s | 32.65 s | 14 |

## What causes the MS MARCO increase in these measurements

The second candidate run spends 24.48 seconds balancing at 900k vectors and reaches the 100-round cap. The baseline runs spend 2.70 and 2.88 seconds at that checkpoint and require one round each. The cap warning reports zero oversized leaves and two undersized leaves remaining. At this checkpoint the candidate records 179 splits, 209 merge calls, and about 140k reassignments. The exact cause of the repeated work requires investigation; these logs do not identify which leaves or vector transfers keep the loop active.

Initial balancing also costs more on MS MARCO. The two candidate runs spend 18.97 and 17.25 seconds at the first checkpoint, compared with 15.64 and 10.50 seconds for the baselines. Subsequent checkpoints usually remain near the baseline outside the capped event.

The instrumentation does not show expensive direct merge calls as the main cost. Summed time inside instrumented merge calls averages about 0.09 seconds for the candidate and 0.18 seconds for the baseline on MS MARCO. These are worker durations rather than wall-clock phases, and they omit work triggered after the instrumented merge body. They do not cover all effects of retaining source leaves. The larger difference is in balancing as a whole.

## Quality and interpretation

Both MS MARCO candidate runs report 1M valid postings. The baselines report approximately 999.4k and 999.8k valid postings. These rounded summaries are consistent with the defect being fixed, but they are not substitutes for the exact ID validation performed separately. A broken baseline can complete less work, so its lower latency is not an acceptable correctness tradeoff.

Final recall at 100 neighbors, using 100 queries, averages 81.17% for the MS MARCO baseline and 81.08% for the candidate. Wikipedia averages 77.63% for the baseline and 77.82% for the candidate. With two nondeterministic parallel builds, these small changes do not establish a recall improvement or regression. The timed comparison disables posting validation; the separate correctness runs on this runtime revision pass all 80 exact checks.

## Method and artifacts

The comparison runs on Lyon's EC2 instance, an r6id.16xlarge with 64 virtual CPUs and 512 GiB RAM. Each fresh build adds the first 1M vectors from a 1,024-dimensional dataset in ten checkpoints of 100k. Runs execute sequentially. Round one runs baseline then candidate for each dataset; round two runs candidate then baseline. No other benchmark process runs concurrently. Dataset files remain cached between runs; filesystem caches are not forcibly cleared.

Both release binaries use Rust 1.92.0 and the same Cargo release build command. The baseline is the PR's exact parent, `22d32265e86ed1f295a164efec55b98a69d7ee8a`. The candidate is `63be72d31e43bef421f0dcfae6b09b1395d5994e`; the PR's final commit, `fb7a9dab90678c1bb2ef9a84bb12c9d8cfd76db3`, changes only a comment. The experiment reuses the saved matching binaries and records their hashes.

Common arguments are `--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --save`. Each run uses a fresh directory on local NVMe. Posting validation is disabled.

Wikipedia adds `--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0`. MS MARCO adds `--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4`. Neither uses `--write-level-min-pcts`.

Raw logs, exact commands, resource measurements, revisions, hashes, the run script, and the summary parser are saved beside this report. The remote results are under `/home/ubuntu/benchmark-data/pr7856-performance-20261006`. Runs start at 15:21 and finish at 15:34 PDT on October 6, 2026. This report and `summary.json` preserve the full comparison locally.
