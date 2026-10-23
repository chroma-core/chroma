# Epoch28 owner-batched transfer review

The final reviewed candidate has no remaining correctness blocker. One protected source and multiple destination leaves on one peer owner share the existing two-role completion barriers. Three concurrency/scheduling findings identified during this review are repaired and covered by controlled regressions. Physical admission and performance interpretation remain separate; this review executes no Cargo, cloud, SSH or Git operation and edits no source.

## Exact source boundary

The immutable comparison base is `source-epoch27.tar.gz`, SHA256 `10d628b01cf040fe2be1782f79860f5272deed435272584be258d0f5e2a42059`. The following current source hashes are independently read and match the author’s final checkpoint. The root agent must freeze the complete closure and bind runtime artifacts to these values before admission.

| File under `rust/index/benches/hierarchical_index/distributed/` | SHA256 |
| --- | --- |
| `batched_transfer.rs` | `d74192555e02dbbe78b75b164669bcfb86520abfc309601d1ce44fc18a4fe5ba` |
| `batched_transfer_tests.rs` | `e91820b8cd1429ba052db65d9285543a5686f781e8c4954d363188e88087246d` |
| `engine.rs` | `8967e70c396c68f57fb3d0fe3e515a59991227eaaf1cc9453705841ebc7fca78` |
| `construction.rs` | `4866e3bafdc3dd0b9b27080c18c900ebe3687aad3e46519c5dd1f8559122db5b` |
| `protocol/transfer.rs` | `e7ede024965637ee6585f6b7441a1b2577ad8d9d8e8163c0666f2da49e1854eb` |
| `observability.rs` | `48b050904669ca485600ff48a4a28037f2c6bf8ec62014e4b0d4725e8d51ad0c` |
| `mod.rs` | `6fcab853613f51fde60317bdd808d04487b40b78ba348004c05d93faf6b78ef9` |
| `local_transfer.rs` | `5b3dd2140ad8d19b7421308ed07b9b40dc488d73ec011d721bf6c4731dfa87cc` |
| `local_transfer_tests.rs` | `51cd76c4377f07448647a328b4fa08232cd48ce5a2502a9f8aa8ffbdbf9ae2e6` |

The local-transfer changes expose existing test helpers only. The pair API and same-owner direct production path remain available. The metadata dictionary, routing selection, codec bounds and adaptive deadline behavior are outside this delta and retain their reviewed implementation.

## Transaction safety

Admission fixes one versioned batch body under a run/epoch/attempt and role. The body includes complete source/destination placement identities, centroid digests, ordered IDs/codes, exclusion and actual producer identity. Full payload equality supplements its digest. Duplicate IDs across destinations and malformed code lengths are rejected. Accepted and cleanly rejected preparations retain their outcome; changed bodies cannot reuse an admitted attempt. Each endpoint serializes its own record, checks phase identity again after acquiring it, and caches every step’s proof and original result. Replaying an old successful step after release returns its retained acknowledgment rather than reapplying membership edits or returning an incompatible newer state.

The batch and pair records share one payload-independent transition function. The driver still stages the complete destination bundle, chooses completion, obtains both begin acknowledgments, removes the source, activates all destinations, obtains both completion acknowledgments and releases in reverse owner order. Destination applied means the complete bundle is installed. No barrier is inferred from a timeout. Known pre-decision rejection permits clean abort of accepted roles; an ambiguous successful preparation acknowledgment causes the coordinator to fail closed while the unknown endpoint retains unresolved protection. Lost step acknowledgments retry the same immutable step and proof, with finite retries.

Preparation acquires owners in global order and requests each owner’s complete leaf bundle. Resident handles are ordered by node ID and acquired with nonblocking all-or-retry attempts. A conflicting claim returns its producer and releases uncommitted protection. No network request occurs while holding resident locks, the routing gate or operations mutex. Preparation checks current handle identity, leaf type, placement, centroid digest, source membership and destination absence. It reserves destination posting capacity and builds the union-based source replacement before completion begins. Compatible parent-only changes are preserved because commit images clone the currently protected resident metadata.

Membership edits remain source-before-destination. Revision checks and commit images are prepared before mutation; all destination capacities are checked before the first append. In-memory appends and source replacement require no further fallible preparation. Current routing images are published while the corresponding resident guards still protect the edits. An injected failure after source removal or the first destination append poisons the run and retains unresolved records/claims. Seal and save cannot succeed. Terminal completion/abort releases only matching claims and region grants and drops resident handles and the prepared source buffer, while retaining immutable replay evidence.

The batch payload is limited to128 destination groups,32,768 IDs and512KiB of raw codes. Deterministic chunking preserves every unique ID and target, including one destination spanning chunks, and completes each chunk before the next. These are transaction bounds, not truncation of remaining routing work. Clean rejection returns the proposal to current-membership filtering and bounded producer-aware retry; it does not mutate the admitted manifest to omit one unavailable target. Multiple peer owners remain separate transactions, and local bulk work remains separate. General multi-owner atomic transactions are outside this candidate.

## Findings resolved before acceptance

1. Opening a new epoch exposed admission before clearing old batch history. A fresh preparation could be inserted and then erased. The final code clears the sealed registry while holding operations admission, before publishing the new phase. Registry lookups release their map guard before phase or endpoint locking, so the correction adds no inverse lock order. A controlled opening/preparation interleaving verifies the new endpoint survives and can abort/drain.
2. An old opener could pass its monotonicity check, wait, and resume after a newer epoch opened and sealed. The final code rechecks run and current epoch after acquiring operations, before clearing history. The delayed-opener regression verifies that stale admission cannot regress the epoch or erase newer records.
3. A successful first chunk followed by a cleanly rejected second chunk could leave the committed destination without a balancing job. The construction wrapper now schedules each committed chunk’s source and destinations before continuing. An actual two-chunk regression forces this order, verifies the oversized committed destination is queued, releases the conflicting claim, retries the remaining IDs and runs balancing to a drained queue with exact memberships/codes and bounded leaves. The successful outer batch path skips duplicate scheduling; the explicit unscheduled oracle API preserves comparable protocol call counts.

## Validation and limits

`epoch28-focused-03.log` records11 focused batch tests passing. `epoch28-suite-02.log` records257 applicable tests passing, zero failures and the same one retained local-writer exclusion, in9.22 seconds. The implementation author confirms all three native CLIs pass; `epoch28-cli-build.log` records21.65 seconds. Focused tests are included in the applicable suite and are not additional unique tests. Root must verify the compiler artifact receipt and final archive closure separately. Earlier red/failure logs remain retained.

The fixed2,048-row/eight-destination fixture moves24 IDs, preserves existing destination prefixes and two untouched leaves, recomputes every posting code and compares complete posting maps with local bulk. The existing pair diagnostic remains a separate differential baseline. The batch asserts11 protocol calls,14 remote engine calls,6 local engine calls, one source scan and one source compaction. These are mechanism counts for an unscheduled fixed transaction; they exclude construction navigation, later balancing jobs and physical network attribution. No end-to-end latency or CPU speedup follows from those counts.

Failure tests cover pre-edit rejection, claim-producer propagation, changed centroid/placement/type/membership, changed body replay, inactive staging, source-before-destination prerequisites, retained step outcomes, phase drain, reciprocal owner ordering, bounded partitioning, lost successful remote step acknowledgments and unknown successful preparation acknowledgment. The post-source and first-destination injected failures verify poisoned/unresolved state blocks seal/save rather than compensating or silently retrying. Parent-only compatibility and current centroid dependency tests support the claimed protection boundary; the batch does not add a new nearest-neighbor selection algorithm.

Preparation and record retention still cost memory and CPU. Whole-batch contention can repeat preparation for otherwise available targets; retained replay payloads live through the epoch, and save performs a quiescence audit of retained records. Terminal phase accounting uses the exact attempt rather than scanning history per completion. Peak memory, contention behavior, byte cost and physical performance remain questions for the single admitted benchmark. These observations do not require a new cache, garbage collection scheme, general transaction coordinator or further scope expansion before the bounded candidate is measured.
