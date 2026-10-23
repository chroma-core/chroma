# Version-free writer benchmark failure

The candidate cannot complete the requested fresh 1M Wikipedia build. It exits during checkpoint 8 after balancing reaches its eight-round limit with one oversized leaf remaining. Treat this as a benchmark failure and resolve it before assessing performance or proceeding with the PR.

The comparison targets baseline `1f433be7969b3d28c78e463fe33a299fa0c5fee9` and candidate `0e7d28fb6a84c7ca67b1da61670fec1f4baafb66` from [PR #7927](https://github.com/chroma-core/chroma/pull/7927). Both measured binaries contain the exact archived source with no harness overlays. The separate sample-audit overlay scripts are prepared but have not been applied, compiled, or run.

| Outcome | Baseline | Candidate |
| --- | ---: | ---: |
| Process exit code | 0 | 1 |
| Successfully committed vectors | 1,000,000 | 700,000 |
| Full build time | 62.827 s | Unavailable: build fails |
| Process wall time | 65.559 s | 35.128 s until failure |
| Peak process RSS | 8.202 GiB | 6.538 GiB before failure |
| Successful before/after-reopen validations | 10 / 10 | 7 / 7 |

The candidate's shorter elapsed time and lower peak memory are not comparable performance results because it stops early. The baseline logs one nonconvergence note at checkpoint 5, with one undersized leaf remaining, and continues. The candidate's explicit error is:

```text
Error: balancing incomplete after 8 rounds: 1 oversized leaves, 0 undersized leaves, 0 transfer retries
```

The candidate's last checkpoint metadata records checkpoint index 6 and 700,000 vectors. Its saved index remains at `/mnt/msmarco/version-free-writer-7927/wikipedia-candidate-1/index`. The failure arises at the balancing limit in `rust/index/benches/hierarchical_index/writer/writer.rs:1032`. No pending transfer retries remain in the reported failure. Parallel scheduling means a repeated execution is not guaranteed to fail at the same boundary; this report records one observed execution without changing the candidate or rerunning it.

The runs use fresh directories, ten checkpoints of 100,000 vectors, squared L2 distance, branching factor 100, split threshold 2048, merge threshold 512, 32 write and balance threads, full-precision write navigation and neighbor reassignment, write tau 0.5, write beam 10–16, level taus `_,_,0.30`, and a 1 GiB cache. Reader pins clear at each checkpoint. Read settings are beam 10–256, level minimum percentages `50,5,0`, tau 1.5 and 2, and rerank factors 1 and 8. Both runs enable posting validation, whose time is reported separately. No replication flags or write-level minimum percentages are passed.

The baseline's exclusive phase times are 15.879 s loading input, 2.542 s adding vectors, 31.461 s balancing, 4.322 s preparing the commit, 2.545 s flushing, 0.026 s reopening, and 3.105 s validating postings. Its final recall@100 is 77.86% / 98.70% at tau 1.5 and rerank 1 / 8, and 78.00% / 98.97% at tau 2. The candidate does not reach final recall evaluation, so recall parity is unknown.

Posting validation checks coverage of every inserted ID before commit and after reopening. The candidate's validation additionally rejects duplicate physical IDs and unreachable tree nodes. All seven completed candidate checkpoints pass. The candidate's transfer conflict counts across those checkpoints are 1702, 1159, 1173, 505, 383, 286, and 216; corresponding retry counts are 1717, 1197, 1215, 537, 426, 312, and 266. Detailed counters remain in `comparison.json` and the raw log. The planned persisted embedding/code sample audit does not run after this failure, and full-run correctness is not claimed.

Both Wikipedia and MS MARCO exact ground-truth files pass preparation checks. Their hashes match recorded provenance; the first 1M source shards are byte-identical to their recorded source shards; all 100 query vectors match source values bit for bit; and every query has 100 unique, in-range L2 neighbors for the 1M and 10M boundaries. Preparation uses existing exact results and is excluded from benchmark timing. Brute-force GT is disabled, and the successful baseline uses the exact indexed-prefix query bucket. The current-source byte verification covers the 1M prefix; a 10M run would require extending that check.

Both builds use Rust 1.92.0, the release benchmark profile, and `-C target-cpu=native` on the same idle `r6id.16xlarge` machine, with 64 logical CPUs, 32 physical cores, and approximately 495 GiB RAM. All new source, cargo caches, temporary files, binaries, and run output reside under `/mnt/msmarco/version-free-writer-7927`. The shared `/mnt/data/chroma-benchmark.lock` is held across both builds and both runs, then released when the candidate fails. The original Wikipedia and MS MARCO checkpoints remain untouched.

Lyon explicitly authorizes restoring SSH access. Only the designated port-22 ingress rule changes from `134.128.209.150/32` to the verified current address `149.88.16.244/32`; unrelated rules are verified unchanged. The instance identity and sandbox account are verified through the Cops-managed SSO session. Firewall before/after evidence is saved alongside this report.

The MS MARCO runs, reverse-order repetition, and fresh 10M runs do not start because the candidate failure stops the sequence. Do not merge or mark the PR ready based on these results. The next step is an implementation-owner fix followed by a new explicitly identified candidate comparison.

The exact reproduction command is in `wikipedia-candidate-1.command`, its environment is in `run.py` and `environment.txt`, and the full output is in `wikipedia-candidate-1.log`. Source archive hashes, binary hashes, compiler logs, process timing, command arrays, GT verification, and the last checkpoint metadata are retained in this directory. Remote binaries and all data artifacts are retained under `/mnt/msmarco/version-free-writer-7927`.
