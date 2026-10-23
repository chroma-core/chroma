# Corrected28 independent refresh review

The isolated refresh correction has no identified source correctness blocker and is accepted for a guarded benchmark after the isolated full test suite passes. This acceptance is conditional on that test receipt and applies only to the archive identified below. It does not accept candidate29 storage changes or establish successful physical balancing.

The correction replaces the 32-attempt limit with one 30-second deadline and a 1-millisecond yield between retries. It preserves the owner revision, parent, centroid, leaf, placement, shared publication sequence and publication-turn checks. Retry after an acquired turn aborts its proposal and releases the turn; expiry while waiting aborts before returning. The finite revision-churn regression targets the reported liveness failure without weakening those checks. The deadline limits retry admission and turn waiting; it does not preempt a blocking owner RPC.

The parent reports ten focused shared-runtime tests passing and reproduction of the original exact retry-budget failure in 0.18 seconds. The reviewer runs no Cargo command concurrently. The complete isolated suite remains an admission requirement.

The reviewer compares archive member bytes directly. Exactly two regular files differ between frozen28 and corrected28: `rust/index/benches/hierarchical_index/distributed/shared_runtime.rs` and `rust/index/benches/hierarchical_index/distributed/shared_runtime_tests.rs`. No source file is added or removed. The corrected archive SHA256 is `956b32eb3025088f1713f02bd77ab69360f87e9cf568ce4f52d09e65bb78a801`. The original archive SHA256 is `7cb1a5eeb090f53a258217dc9c098c01be9e65e03b721e47b809eb5f2866ed08`.

| Corrected source | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/shared_runtime.rs` | `a485a8489d2531ef192527a3f9487f12602e5d46a3efbf4454584235ef932214` |
| `rust/index/benches/hierarchical_index/distributed/shared_runtime_tests.rs` | `9fc8a9a038c4dffd6cc5161ad7fbca84d507e67fdde9b270c86d4e5aec599f20` |
