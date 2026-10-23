# Concurrency and topology review

## Finer balancing jobs

The reviewed candidate is `14dc9698eb6e400ba6f677575eff7a5659deeed6`, based on `eea4ada6f21554adb535d1ffdea95f74cb76650b`. No blocking concurrency or topology defect was found in the candidate diff.

Workers take unique eligible leaf IDs from a snapshot through an atomic cursor. Live size and type checks, leaf claims, and existing parent recovery remain responsible for correctness. A merge may retire a queued ID, in which case its job does no work. New or newly oversized leaves are discovered at the next joined round. The eight-round limit still permits only size residuals, while mandatory pending transfers prevent commit.

The owner adds tests for eligibility, new oversized destinations, sibling splits and merges with a small branching factor, and persistence/reopen. These check root reachability, exactly-once IDs, and codes relative to the current centroid. The owner reports 173 passing test executions; this review does not independently rerun tests. The diagnostic field named partitions now counts leaf jobs and cannot be interpreted as the same unit as the reference subtree partitions.

## Batched transfers

The reviewed candidate is `f76e8c6541c363c1068d864eb6ff9835093167e1` in `/private/tmp/chroma-batched-balance-transfers`, based on the same `eea4ada6` reference. No blocking concurrency or topology defect was found in the final candidate diff. The design reserves a stable tree interval after structural workers join, prepares each source once, combines its survivors and arrivals, and publishes each changed leaf once. A mutation guard rejects later commit after a publication panic.

The initial design allowed two forced evacuations to route into each other. The implementation excludes all evacuating leaves from destinations, retaining a deterministic sink if necessary. This changes merge routing and must be evaluated for recall as well as time. Ordinary proposals without a legal target remain in their source, so dropping that optional proposal preserves exactly-once membership.

A test-only mutex lifetime defect was identified: passing a drained queue directly from a lock expression into the batch routine retained the lock across another acquisition. The owner fixes it by extracting the pending work into a local before the call. Production already drains into a local.

Regression coverage includes simultaneous outgoing and incoming rows, overlapping ordinary and forced proposals, all-leaves evacuation, destination exclusion, lazy persisted postings through a forked commit and reopen, and failure midway through publication. The owner reports 189 passing executions across four suites, formatting, benchmark compilation, and commit hooks. This review does not independently rerun tests.

Performance and recall remain experimental. Preparing complete destination replacements can increase peak memory, static source chunks can create long worker tails, and global evacuation exclusions with sink fallback alter placement decisions. None of these is a reason to skip correctness audits or recall comparison during the benchmark.
