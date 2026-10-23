# Version maintenance cost attribution

This experiment profiles the versioned writer directly to measure its existing version-maintenance work. It does not subtract the replacement transfer algorithm or remove correctness checks from the writer.

The exact source is `1f433be7969b3d28c78e463fe33a299fa0c5fee9`. Wikipedia runs use 1M vectors with 100k checkpoints, FP navigation, 32 insertion and balancing threads, and the established identical beam, cache, split, and merge settings. Benchmark-only posting validation is disabled; saved indexes receive separate audits using the verified baseline-format audit binary.

The attribution examines three hypotheses: version-cache lookups consume CPU during insertion and balancing; version-map updates and dirty tracking consume CPU or synchronization time; version persistence contributes measurable commit latency. Stale-entry cleanup combines version lookup with posting scans and compaction, so those nested costs must be separated rather than added twice.

A low-overhead overlay records phase boundaries and batch-level version persistence boundaries. It adds no per-vector clocks or counters. An exact uninstrumented control and unsampled/sampled/unsampled runs of the overlay quantify observer overhead. CPU sampling uses the existing perf tool with user-space CPU-clock samples at 49 Hz and DWARF call chains. No kernel settings or host permissions are changed.

Phase timestamps use the same real-time clock selected for perf, allowing samples from input loading, benchmark validation, diagnostics, and recall to be excluded from the writer attribution. Source lines and inline frames distinguish version-map operations from embedding-map operations. Coarse timers measure version gather/sort, version upserts, scalar metadata commit/flush, and dirty-version clearing. Scalar metadata also contains non-version fields, so its commit/flush is a shared-container bound rather than an isolated version-only measurement.

CPU sample totals and overlapping worker timers are not critical-path elapsed times. A measured CPU fraction cannot be claimed as the end-to-end saving from deleting versions. Posting serialization may combine version bytes with IDs and codes; unseparated costs remain explicitly unattributed. The existing scrub timer includes posting loading, lookup, scan, and compaction, so it is reported as an inclusive worker duration rather than summed into gross version CPU cost.

All work uses the shared EC2 benchmark lock and isolated NVMe output under `/mnt/msmarco/version-cost-profile-20261020`. Existing datasets, ground truth, checkpoints, and the retained 8M snapshot are preserved. The benchmark starts at 1M; 3M is reserved for a concrete remaining attribution question.
