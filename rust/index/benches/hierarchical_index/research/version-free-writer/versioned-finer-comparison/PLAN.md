# Versioned versus version-free writers with finer scheduling

Compare the original versioned writer at `1f433be7969b3d28c78e463fe33a299fa0c5fee9` plus a minimal safe finer-scheduler port against version-free finer scheduling at `14dc9698eb6e400ba6f677575eff7a5659deeed6`. Temporary deletion bitmaps and batched transfers are excluded.

The implementation owner is `versioned_finer_scheduler`. Independent reviewers `versioned_finer_safety_review` and `versioned_finer_comparison_review` check concurrency and comparison validity. A shared leaf queue must not bypass ownership assumptions in the versioned implementation. Any necessary safety adaptations or other remaining behavioral differences must be documented rather than hidden in the comparison.

After tests and reviews pass, a separate benchmark owner will run fresh Wikipedia builds on EC2, starting with 1M vectors and 100k checkpoints. Compare at 3M with 1M checkpoints if useful; use reversed-order repeats to distinguish effects from variation. Both variants use established full-precision settings and matched ground truth. Fresh builds avoid incompatible checkpoint formats and different inherited trees. This is an end-to-end comparison of the two safe implementations, including replacement coordination, not an isolated version-lookup ablation.

Include production input checks, balance, commit preparation/flush, and reopen in the phase breakdown. Exclude benchmark validation and diagnostics from timed phases; run separate persisted-index audits. Versioned postings may contain stale entries but must have exact-once valid coverage; version-free postings must be physically exact-once. Check reachable topology, stored embeddings/codes, recall, memory, round counts and residual size violations. Preserve raw logs, commands, source/binary hashes, environment details and results here.

Use the existing EC2 host and benchmark lock, inspect jobs and free RAM/NVMe before starting, and preserve all original datasets/checkpoints and logs. No paid provisioning, SSH rule changes, stopping others' jobs, GitHub publication, or merges. Verify the user-required lyon-chroma identity before any commits. All changes remain isolated and reversible.

## Status

Completed. Versioned scheduler port `97fa32d84be264c42db836c72f32512ff811f0fc` passes 151 test executions and independent reviews. Eight fresh Wikipedia runs and eight saved-index audits compare it with version-free `14dc9698eb6e400ba6f677575eff7a5659deeed6`. Mean measured writer phases decrease 29.4% at 1M/100k checkpoints and 10.4% at 3M/1M checkpoints, with higher peak RSS (8.6% and 6.4%). Both reversed-order pairs improve. See [the full report](benchmarks/REPORT.md) for phase breakdowns, recall, residual balance, remaining algorithm differences, and raw evidence. No GitHub changes or merges occurred.
