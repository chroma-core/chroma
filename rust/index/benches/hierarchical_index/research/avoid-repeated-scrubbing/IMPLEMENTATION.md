# Skip repeated posting-list cleanup

The writer can reuse a completed posting-list cleanup until its rows or the index’s current vector versions change. Posting rows contain a vector ID and its version. A newer version or a deletion makes older rows stale, including replicas stored in leaves that receive no direct write.

## The pieces

- Each writer has a counter that advances after a logical version change. This counter is the scrub epoch (`scrub_epoch`).
- Each leaf records the counter captured before its last cleanup (`last_scrub_epoch`). An absent value requires cleanup.
- All posting appends clear the leaf’s recorded value. Newly created or reopened leaves begin without one. Lazy posting-list materialization also clears it.

## Cleanup and invalidation

A cleanup materializes a lazy posting list, acquires its leaf guard, and reads the writer counter. If the leaf records that same value, the cleanup returns without scanning versions. Otherwise it checks the rows and records the captured counter. Its statistics count completed scans; skipped calls leave the counters unchanged.

Logical version changes advance the counter while the version-entry guard remains held, after assigning the new version. A cleanup that sees the new counter must therefore wait for that version’s publication if it encounters the changed ID. The reassignment path retains its leaf-before-version lock order and holds both guards until the replacement posting is ready.

The cleanup records its starting counter, not the counter at the end. If a concurrent update makes an already-checked row stale, the update advances the writer counter and the leaf retains an older value. The next cleanup must scan again. Reads that merely fill the immutable checkpoint-version cache do not advance the counter.

## Scope and limitations

The implementation changes cleanup reuse, not work discovery or neighboring-vector distance calculations. It adds no global leaf scan, ownership map, or scheduler queue. The continuous scheduler can retain its existing notification when a completed cleanup removes rows; the same `removed > 0` boundary remains in the cleanup wrapper.

The shared counter conservatively invalidates every leaf after any logical version change. Frequent changes can therefore reduce the number of skipped scans. Each version change adds an atomic update, and each leaf adds an optional in-memory epoch. Timing and atomic-contention costs remain unmeasured; no benchmarks or EC2 work are performed for this task.

## Validation

Both existing hierarchical test harnesses pass: 33 policy/navigation tests and 40 posting-delta tests, totaling 73 executions. Seven new cases execute in both harnesses. They check unchanged cleanup skips, same-version append invalidation, deletion across replica leaves, reassignment across source replicas, captured-epoch preservation across a concurrent deletion, publication locking, newly split leaf initialization, and reopened lazy posting lists whose checkpoint-cache reads do not invalidate cleanup.

The current feature branch’s hierarchical benchmark target compiles. Changed-file formatting and diff checks pass. Existing merge, concurrent posting-load/add, split-after-reopen, reachability, and persisted-version tests remain part of the passing harnesses.

## Code reference

The independent branch is `codex/avoid-repeated-scrubbing`, based on feature commit `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`, in `/private/tmp/chroma-avoid-repeated-scrubbing`. The implementation spans the shared leaf type, writer version-update and cleanup methods, writer open/load initialization, and the reader’s shared-leaf initialization. No pull request is opened for this task.
