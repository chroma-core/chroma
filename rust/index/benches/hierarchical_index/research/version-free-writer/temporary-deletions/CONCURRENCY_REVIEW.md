# Temporary posting deletions: concurrency review

Reviewed commit `1d659e171b8456d4e0bbd6c77dbf6cbb40674470` against finer-scheduler base `14dc9698eb6e400ba6f677575eff7a5659deeed6` in `/private/tmp/chroma-balance-slot-tombstones`. No blocking concurrency finding remains. This is a read-only review; the implementation owner reports 193 passing test executions, formatting, benchmark compilation, and hooks. I inspected the regression tests and final commit, but did not independently rerun the suite or benchmarks.

## Ownership and slot stability

Transfers retain source and available destination claims until accepted source positions are marked deleted. Proposals remain vector IDs until the claimed source is inspected, so a stale proposal cannot directly reuse a position from before a split or cleanup. The current source scan selects only live positions. All destination buffers and source deletion storage are prepared before append; partial mutation remains covered by the existing poison guard.

Returning a vector to its original leaf creates a new live position while its old position stays deleted. A subsequent transfer scans only the live copy. Duplicate proposal IDs remain deduplicated by the transfer protocol. Splitting compacts the source while holding its claim, and final cleanup runs only after workers join. These are the two contexts in which positions change.

## Consumers and lifecycle

The balancing job queue, size checks, split eligibility, merge eligibility, retirement check, and retry membership filter use live counts or live IDs. Both full-precision and quantized neighbor scans skip deleted positions before inserting IDs into their evaluated set. This ordering prevents an old deleted occurrence from masking a returned live occurrence.

The public balancing wrapper performs final cleanup after normal completion, the eight-round size cap, and an ordinary retry error. A panic poisons the writer rather than permitting commit with partial work. Commit rejects outstanding temporary deletions, and the persisted posting format remains IDs plus codes without deletion state. Physical length remains separate for lazy posting loading.

## Regression evidence

The new tests exercise A → B → A, duplicate proposals, stale proposals, live counts preventing false splits, splitting with deleted positions, cleanup at the round cap and after a retry error, and lazy-load/append/transfer/commit/reopen. I requested a stronger neighbor regression because evaluating the old occurrence could otherwise produce the same evaluated-ID count as evaluating the returned occurrence. The final commit includes a dead-only neighbor case in both modes, asserting zero evaluated IDs before the vector returns. Existing race and failure tests remain part of the reported passing suite.

## Measurement caveat

Final cleanup is included in public balance elapsed time and has its own diagnostic timer. The source-entry scan counter still counts physical entries examined, including deleted positions. Deferred compaction can increase that counter even while reducing code copying; benchmark conclusions should use elapsed balance time, memory, and cleanup work together.
