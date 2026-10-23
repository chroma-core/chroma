# Balancing experiments

The objective is to retain the CPU savings from removing versions while reducing transfer coordination and idle workers. Each experiment starts from `eea4ada6f21554adb535d1ffdea95f74cb76650b` and remains independently reversible. Results will be recorded here with commands, exact revisions, raw logs, correctness audits, recall, and resource use.

## Priority and ownership

1. The `batched_balance_transfers` agent owns an isolated transfer experiment. It will find a safe boundary for collecting moves and applying them with one owner per source and destination. Structural changes, retirement, and partial-failure handling must remain correct.
2. The `finer_balance_jobs` agent owns an isolated scheduler experiment. It will divide expensive work more finely while retaining round boundaries and preventing overlapping structural mutations.

Implementation can proceed concurrently in separate checkouts. EC2 builds and performance runs must execute serially under `/mnt/data/chroma-benchmark.lock`. Neither implementation agent runs benchmarks. Independent reviewers will check concurrency and persistence before measured candidates run.

## Measurement plan

Use Wikipedia only, established full-precision writer settings, and matching precomputed ground truth. Start with fresh 1M builds and 100k checkpoints. Test promising transfer changes at 3M, with reversed-order repeats sufficient to distinguish useful gains from variation. Measure insertion, balancing, commit preparation/flush, and reopen separately; exclude benchmark validation and diagnostics. Record transfer queue volume, source scans, retry drain, worker wall time, peak memory, and recall. Audit saved indexes separately for exact-once IDs and embedding/code correctness.

For the scheduler, replay 8M to 9M from independent copies of the preserved snapshot at `/mnt/msmarco/version-free-followup-20261020/wikipedia-diagnostic-9m-1/snapshots/8000000`. Confirm format and harness compatibility before use. Compare an unchanged reference with the candidate using identical inputs/settings; do not treat earlier diagnostic overlay timings as an exact matched baseline. Record worker distribution and CPU utilization to verify that the long tail shrinks.

Preserve original datasets, checkpoints, and all previous evidence. Check available NVMe/RAM and live jobs before running. No paid provisioning, security-group updates, stopping others' work, or merges are authorized by this plan. Stop unsuccessful experiments once evidence is sufficient; combine changes only after independent benefits are established.

## Status

Both implementations pass correctness tests and independent concurrency/storage review. Batched transfers are committed at `f76e8c6541c363c1068d864eb6ff9835093167e1` (189 test executions). Finer scheduling is committed at `14dc9698eb6e400ba6f677575eff7a5659deeed6` (173 test executions). Implementation and reviewer agents have finished. The separate `balance_experiments_benchmarks` agent owns serialized measurements, prioritizing batched transfers. Performance results are pending.

Interpretation caveats: batched transfers can choose a retained destination for forced evacuation, so placement and recall can change. Its retry timer includes parallel batch preparation and is no longer a serial-only duration. The scheduler partition counter measures eligible leaves rather than subtree roots. Both candidates may change round counts; compare residual leaf sizes as well as elapsed time.

## Completed results

The standalone finer scheduler reduces balance time from 46.56 seconds to 16.40 seconds on average across two matched 8M-to-9M Wikipedia replay pairs. Insertion, balancing, commit, and reopen together fall from 62.01 seconds to 31.92 seconds. Recall and memory remain comparable; both variants reach the eight-round cap with a few residual size violations. The fresh 1M scheduler run shows no gain, so the result applies to the measured saved-tree workload. Batched transfers show no useful gain at 1M and only a marginal 2.1% improvement at 3M; retain that experiment separately. All nine saved indexes pass correctness audits. See [the complete report](benchmarks/REPORT.md) for raw evidence, caveats, and recommendations. No changes were published or merged.
