# Batched reference CPU fixes

The CPU implementation preserves single-use chunk iterators before preflight and owns compact top-k storage. Actual scanned rows, per-query result counts, and full-score check counts must match the declared corpus. Thirty-four combined numeric tests pass, including iterator/list/oracle and retained-storage regressions. Independent review and CUDA execution remain pending.

The next numerical experiment uses the existing 100/200-query integration references, a small real prefix, and adversarial unequal bounds and ties. It compares batched and scalar float64 reranking bitwise, then compares independent float64 oracle distances within 1e-12. The existing source epoch remains unchanged. Hardware timing is unavailable. No launch is automatic.

The unfinished launch and preservation helpers are retained in deferred-source for possible later work. They are untested, unreviewed, and outside the runnable source freeze. Full 11,000-query reference production is deferred. The campaign stops at 100M; 250M and larger progression require new explicit user authorization.
