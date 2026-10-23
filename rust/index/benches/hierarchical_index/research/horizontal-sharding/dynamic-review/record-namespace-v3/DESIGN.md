# Explicit record identity mapping component

The component preserves the identity of each stored record while allowing its record offset to differ from its global vector ID. An independently pinned shared source assigns each user to a global ID; equal vector values cannot establish this identity.

The auditor opens the supplied actual record segment and validates every offset, data row and reverse user mapping. It checks both lookup directions, exact vector bits, full owner-map coverage and the proposed identity table. The audited object owns its reader and private materialization allocator. New global IDs follow source-log order; actual record offsets remain separate. The default adapter still requires equal namespaces and rejects the one-based parent fixture.

This freeze is an inactive component milestone. Its source digest arguments and canonical/owner bytes must still be bound to the captured complete registered descriptor by the next adapter. It does not establish collection authority, persist a sidecar, publish a bundle, or prove the candidate record flush. No positive hierarchical writer fixture is admitted here. The planned complete bundle includes exact record roots, namespace artifact, saved vector roots and ownership roots under the existing registration authority.

The focused target compiles the actual reader and materializer APIs and exercises source identity, permuted offsets, duplicate offsets and coverage rejection. Runtime admission is requested only for its component unit tests, using the existing cached target, Rust 1.92, locked/offline, two jobs, debug information disabled and a 20 GiB free-space floor. The full worker build remains blocked by the independent baseline usearch API mismatch.

The audit remains bounded to 100,000 rows and 512 MiB canonical embedding payload. These limits do not bound provider decode allocations, strings, JSON parsing, process memory or global namespace scale. The component does not reuse a cross-call trust receipt.

The captured reader retains the entire Collection value and rejects any changed collection field before materialization. Every actual record-map stream stops at the admitted row count rather than collecting unbounded rows. Owner-map count is at most 128. This epoch repairs those component gaps; it adds no authority or publication claim.

This epoch corrects only a slice iterator compile error. V2 compilation fails before tests and its log remains separate.
