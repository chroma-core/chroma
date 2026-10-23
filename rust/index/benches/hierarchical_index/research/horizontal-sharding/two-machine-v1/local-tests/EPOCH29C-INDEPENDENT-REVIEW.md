# Candidate29c independent publication review

The reviewed correction has no identified source correctness blocker. A failed conditional publication performs one readback and recognizes either its exact intended descriptor bytes or a different decoded descriptor. It never retries the write or performs an unconditional replacement. Acceptance applies to the three source hashes below; executable builds and real-backend validation remain the parent’s admission responsibilities.

## Evidence and scope

The reviewer reads the actual S3 diagnostic in `operations/native-cas-diagnostic-1792685572`. Its fourth indexed round records one successful publisher and one generic error containing `ConditionalRequestConflict`, while the latest descriptor names the successful publisher. This establishes a concrete error-classification mismatch. It does not independently explain the separate cloud smoke result in which both callers reported errors although a descriptor was committed.

The reviewer compares archive member bytes against frozen `source-epoch29b.tar.gz`, SHA256 `b21a5b81818f09cdb20b281a9d38f5dadb7e22655d51ed9c80b1cdac03883c2a`. Exactly three existing files differ: publication error reconciliation, adapter diagnostic/mock support, and tests. The correction does not change save deadlines, immutable upload behavior, cache reuse, conditional-write options or the frozen predecessor map. The reviewer runs no Cargo command, source edit or cloud operation.

## Safety checks

The publication path still validates the intended generation, reads the existing descriptor and ETag, checks the expected predecessor, then submits one conditional write using that captured ETag. If that write reports an error, the path performs exactly one strong read of the latest pointer. Exact intended bytes resolve an ambiguous successful response as success. A decoded descriptor different from the captured predecessor resolves the result as a conflict. The latter is a failure classification, not acceptance or validation of the competitor’s referenced graph.

An absent pointer, malformed descriptor or unchanged predecessor preserves the original write error. The unchanged comparison uses decoded descriptor identity, so whitespace or key-order changes do not manufacture a competing publication. A failed reconciliation read returns an error retaining both the original write failure and the read failure. A descriptor sharing only the intended generation name, but differing in its attempt identity, cannot satisfy the success check.

The branch writes nothing after the original conditional operation. It cannot overwrite a winning generation, roll back the pointer or declare an unreadable/malformed result successful. A successful exact-byte readback establishes the intended descriptor at that observed instant; a subsequent legitimate publication may still supersede it. The correction does not promise an acknowledgement when the single reconciliation read is unavailable or the competing result has not yet become observable.

## Validation

The deterministic regression covers eight states after the failed write: exact intended bytes, a competing descriptor, unchanged predecessor, unavailable read, malformed bytes, absent pointer, reformatted predecessor and an attempt mismatch sharing the intended generation name. Every case asserts exactly one PUT and two latest-pointer GETs in total: the initial predecessor read and one reconciliation read. The final local suite records 269 passing tests, zero failures, two ignored real-S3 tests and one documented pre-existing filtered test in `epoch29c-distributed-tests-final.log`.

The added ignored S3 diagnostic retains every round’s caller outcomes and final pointer before asserting one acknowledged winner and one conflict. It is bounded to 1–100 rounds and requires explicit configuration. Its thread barrier synchronizes method entry rather than forcing both ETag reads to finish before either write; it is evidence about observed backend races, not a deterministic proof of a particular interleaving. The deterministic backend test supplies the precise error/readback cases.

## Reviewed source identity

| File | SHA256 |
| --- | --- |
| `rust/index/benches/hierarchical_index/distributed/durable_generation_tests.rs` | `c31f8fe655255ea27173f3fa7202a2d166ab13acafe786a2c1d9263e34d833dc` |
| `rust/index/benches/hierarchical_index/distributed/generation.rs` | `81e265cd658ce833f6df72413ac32a0ec37f6cdc08fe482bf03f8eccc261fc1c` |
| `rust/index/benches/hierarchical_index/distributed/generation_backend.rs` | `a799cff041041231c85fbc2c1e620faecb4aeb2c1862c18c240ea6590fe83aec` |

The final local suite log SHA256 is `d224277a311d30b3cd86f891523a5d00636227f7774e359a4fb7c50b45237005`.
