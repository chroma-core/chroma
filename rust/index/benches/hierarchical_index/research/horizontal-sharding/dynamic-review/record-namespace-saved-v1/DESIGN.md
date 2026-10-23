# Save the actual materializer result

The private record append consumes its audited attempt and passes the actual materializer result directly into the real record writer. It commits and flushes the record roots, preserves exact old namespace rows and returns a private saved result. No caller can construct or mutate that result's fields. A failure discards the attempt; no same-writer retry occurs.

A full saved-record audit compares the complete candidate canonical prefix with the exact digest of the audited parent bytes. It compares every appended vector bit with the retained actual materializer capture. It then enumerates all three candidate record maps, both lookup directions and every data vector against the owned namespace and supplied candidate ownership maps. The result makes no registration descriptor eligible on its own. Candidate canonical/owner reference binding to a real saved vector deployment remains the next composer seam.

The same tiny four-parent/two-append method runs with SPANN_RECORD_NAMESPACE_FLUSH=1 and a fresh evidence root. Its parent uses the actual captured collection database ID. The method flushes the actual record append, fully reopens/audits all six rows, rejects mutated old and new canonical bytes and reopens the old four-row reader. It retains the candidate record file roots, complete namespace table, actual capture and candidate canonical artifact. This request does not run a hierarchical writer or publish a bundle.

Parents with auxiliary record files, including a Bloom filter, reject before materialization because this opt-in method does not yet supply the actual manager needed to update those files safely. It neither drops unknown keys nor preserves a stale filter silently. Other segment and registration behavior remains unchanged. Partial record-flush I/O and sidecar failures remain required before final complete-bundle acceptance.

Limits remain the bounded full-audit prototype. The audit builds temporary canonical and JSON payloads and does not provide a process-memory or provider-allocation bound. The full worker crate remains independently blocked. No cloud or external service runs.
