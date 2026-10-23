# Temporary deletion storage and failure review

The reviewed commit is `1d659e171b8456d4e0bbd6c77dbf6cbb40674470`, based on `14dc9698eb6e400ba6f677575eff7a5659deeed6`, in `/private/tmp/chroma-balance-slot-tombstones`. No blocking storage, failure-handling, or performance-design defect was found. The checkout is clean.

## Verified invariants

- Physical posting length remains unchanged until compaction, so lazy loading can still distinguish persisted entries from an in-memory append delta. Live length subtracts deleted slots only for balancing decisions and logical diagnostics.
- Transfers reserve the source bitmap before appending to destinations. Existing leaf claims keep source slot positions stable while prepared rows are appended and source slots are marked. Full-precision transfers snapshot only proposed live slot/ID metadata, rather than cloning surviving IDs and codes.
- Appending a vector that returns to its source creates a new live slot while its old slot remains deleted. Neighbor scans, retry membership, merge input, and leaf retirement use live rows or live counts.
- Splits check live size before compacting their claimed source. Their replacement leaves start with empty deletion state. Final cleanup compacts remaining affected leaves after workers join and clears bitmap/count together with physical length.
- Cleanup runs after the inner balancing routine returns successfully or returns an ordinary error, including the eight-round cap. Panic unwinding retains deletion state and poisons the writer. Commit rejects poisoned state, pending transfers, and outstanding temporary deletions before creating storage writers.
- Persistence schemas do not change. Reopened reader and writer leaf shells initialize deletion state empty; only cleaned physical rows can be committed.

## Test evidence reviewed

The implementation owner reports all 193 test executions passing across four harnesses (44 + 53 + 52 + 44), plus benchmark compile, formatting, diff checks, and hooks. The posting-delta harness contributes 53 tests. I inspected five added tests covering returned vectors and duplicate proposals in FP and quantized modes; logical-size split eligibility and live-only splits; live-only neighbor scans; cleanup on size cap and retry error; and lazy persisted-source append/round-trip, pre-cleanup commit rejection, forked commit, and reopen with exact IDs and regenerated code checks. Existing partial-append poisoning tests remain in the suite. This review does not independently rerun the owner’s tests.

## Measurement limits

Deferred deletion still scans the physical source to match proposed IDs; dead rows can increase this scan until cleanup. The final cleanup byte/entry counters exclude earlier compaction required by a split and must be labeled accordingly. Keeping dead codes until cleanup can increase peak memory; include process peak RSS alongside elapsed balance time. No speedup is asserted by this review.
