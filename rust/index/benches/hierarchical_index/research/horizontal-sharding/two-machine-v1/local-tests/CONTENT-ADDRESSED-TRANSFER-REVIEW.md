# Content-addressed cloud transfer review delta

The format-2 content-addressed transfer and revised conditional-publication probe have no identified integrity blocker in this reviewed snapshot. This review performs no AWS or Cargo execution. It independently runs the four pure/mock transfer tests; all four pass in 0.046 seconds. Actual sandbox execution and its receipts remain required.

The transfer maps every logical file to `sha256/<first-two-digest-characters>/<digest>`. Campaign labels remain local receipt metadata. It makes no bucket-versioning query. A successful new conditional PUT requires a non-null version ID, then exact-version HEAD validates identity and length. If PUT returns precondition failure, reuse reads the existing current body's actual bounded bytes, verifies length and SHA-256, records the returned exact non-null version, and checks that exact version. A conflicting body is never overwritten or adopted. These checks resolve the earlier ambiguous-success resume limitation for a content-identical object.

Restore requires receipt format 2 and the exact content-key layout. It checks object identities and bounds before download, requests exact versions, verifies SHA-256 and the saved reference checksum, and parses the restored dependency graph. Payloads precede owner manifests, the generation manifest and the final descriptor. A failed or incomplete receipt does not become a complete restore descriptor.

The conditional-publication probe hashes its first body, which includes the fresh campaign label, to choose an authorized content key. It then attempts different bytes at that same key with conditional creation, requires HTTP 412, and verifies that the current bytes and version remain the first publication. This intentionally conflicting second write is a conditional-creation test, not an alternative valid content-addressed publication.

The upload CLI's reported byte sum is logical transferred-generation size. It includes reused objects and is not a measurement of incremental stored bytes. Spending and recurring-storage accounting must deduplicate exact `(key, version_id)` objects and distinguish newly created versions from `reused_existing` entries. This is an accounting limit, not an integrity blocker.

| Reviewed file | SHA256 |
|---|---|
| `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/two-machine-v1/operations/generation_transfer.py` | `dbd476bcdb80220d9629cfdc386bdc6993fed7a7b7d02cb3eb138b3e40d0f609` |
| `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/two-machine-v1/operations/generation_transfer_test.py` | `ea5c16ce5e1e8bb0ef3c84a6cb4e893ada9b7214411e6df9754ca6ad015866d4` |
| `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/two-machine-v1/operations/conditional-publication-probe.py` | `9ee1f70d1421590f2dea708ac243ac9e3affd5683c29d13116f835ee2f1d341d` |
