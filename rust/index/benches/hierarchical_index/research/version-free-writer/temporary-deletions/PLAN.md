# Temporary posting deletion experiment

The experiment defers physical removal of moved vectors until balancing finishes. Its reference is the finer scheduler at `14dc9698eb6e400ba6f677575eff7a5659deeed6`. It does not include the separate batched-transfer experiment.

An optional per-leaf bitmap marks deleted posting positions. Physical positions remain stable until cleanup or leaf replacement; a returning vector receives a new live position. Live counts govern balancing while physical counts retain their loading and storage meaning. Existing transfer claims and centroid checks remain. Splits consume live rows, and retirement checks live counts. After workers join, cleanup compacts touched surviving leaves and removes transient deletion state before publication. Cleanup must run on successful size-cap termination; errors must preserve state or poison the writer, and commit must reject unfinished cleanup.

## Owners and correctness

The `balance_tombstones` agent owns an isolated implementation branch. The `tombstone_concurrency_review` and `tombstone_storage_review` agents independently review the design, tests, and final revision. Tests must cover return-to-source moves, duplicate concurrent transfers, stale proposals, splits containing deleted rows, retirement, lazy loading, cap cleanup, failure handling, and persisted reopen with exact IDs and codes. No implementation agent runs EC2 benchmarks.

## Measurements

A separate benchmark owner will compare the exact candidate against the finer scheduler alone. Use Wikipedia only, identical established full-precision settings and matched precomputed query ground truth. Start with fresh 1M vectors and 100k checkpoints. Use 3M if needed to assess scaling and repeat promising results in reverse order. Test the saved 8M-to-9M workload if small runs establish correctness and show a plausible benefit; do not rebuild the saved prefix.

Include final scrub in balance latency. Record insertion, balance, commit, reopen, cleanup duration, source scans and copied bytes where available, claim duration, retries, peak memory, physical/live row counts, residual leaf sizes, and recall. Benchmark validation and diagnostic work remain outside the measured phases; saved-index audits run separately and cover complete exact-once IDs, topology, and sampled old/new embeddings and codes. Report uncertainty and keep unsuccessful results.

Use independent checkpoint copies, preserve original datasets/checkpoints and the retained 8M snapshot, inspect live jobs and free NVMe/RAM, and hold `/mnt/data/chroma-benchmark.lock` across EC2 builds/runs. Do not provision resources, change SSH rules, stop others' jobs, or merge without authorization. All commits and GitHub writes require verified `lyon-chroma` identity. Keep the experiment reversible and record raw commands, revisions, binary hashes, logs, environment, and results here.

## Status

Implementation and reviews are complete at `1d659e171b8456d4e0bbd6c77dbf6cbb40674470`, with 193 passing test executions. Six saved-index audits pass. Matched benchmark pairs show mixed core-indexing changes: +4.2% at 1M, −3.3% at 3M, and +1.6% for saved 8M to 9M. No convincing latency improvement is established. Keep the experiment isolated. See [the results report](benchmarks/REPORT.md) for timings, recall caveats, and preserved raw evidence. No GitHub publication or merge occurred.
