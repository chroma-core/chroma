# Collection registration integration

The compactor prepares one complete saved hierarchy and registers its descriptor through the existing collection version and pulled-log-position fence. It does not advance a separate deployment pointer. The existing query fleet reads the registered version independently of the compactor fleet.

The optional Python adapter captures the collection, every segment, the selected vector segment, the compaction session and materialized add-only source rows from an internal authority contract. Its first scope supports L2 and new vectors without metadata or documents. It calls the existing global-write producer, which selects one leaf globally and commits that mutated private writer directly. The resulting bundle binds exact source vector bits, source offsets, routing model, owner generation, saved roots and the producer's persisted selected-leaf proof. Other segment outputs and unrelated keys within the vector segment remain present.

The Rust type bridge projects the descriptor into the actual collection registration input. It preserves the captured collection version, pulled log offset and complete segment output set. It installs no RPC handler and does not authenticate or independently verify saved payloads. No existing registration caller activates this opt-in method.

A submitted registration is never automatically repeated. A pending or transport-failed attempt with the parent still visible remains unknown because the server may publish later. Only an exact complete candidate observation establishes the matching committed state. A fake authority can establish terminal rejection in contract tests; the current production APIs expose no attempt-status receipt, so a production adapter cannot use this classification without another authoritative guarantee.

Log repair requires a backend operation that checks the exact candidate and serializes against newer registration and log updates. The current generic log interface does not provide that guarantee. An old offset must never be blindly written after a descendant commit. The adapter therefore requires an explicit internal conditional reconciliation method; no production implementation is supplied.

Task replay matches complete request bytes. Conflicting replay rejects before mutation. Any private preparation error poisons the session, preserves its journal and requires a new attempt from the immutable parent. Restart-safe idempotency is unsupported. The current complete validation path and 100k prototype cap remain unchanged.

The focused authority tests establish local contract behavior only. They do not establish production SysDb atomic reads, server-side attempt terminality, remote authentication, physical assignment fencing or cross-host transport. The separate Rust bridge test compiles against actual collection and segment types; the complete worker crate and native saved-writer fixture require separate validation before an integration claim.
