# Epoch16 direct local navigation review

The coherent direct-navigation change has no source or execution-preparation blocker within this review scope. It removes local request and reply assembly while preserving globally coordinated candidate selection. It does not change the construction scheduler, saved-state implementation or benchmark settings. The performance result remains to be measured.

## Selection and visibility

The local backend borrows the query and frontier, pins the current shared view, checks its sequence and the construction run and epoch, and copies only immutable owner routing-image handles under the routing gate. It never loads postings or takes mutable resident locks while scoring. Parent images append packed child rows in the same frontier and child order used to reconstruct envelope replies. Missing or incompatible local images reject the whole navigation attempt; the caller retries from the current root rather than selecting a partial result.

Local and remote candidates enter the same global selection. Fully local rows need no reconstruction because their input order is already correct. Mixed rows are reconstructed into original frontier and child-ordinal order before the same reference-compatible tuple sort, exclusion, tau cutoff and beam selection. Leaves from different depths retain the existing accumulated-leaf behavior. The remote path still checks owner, run, epoch, shared sequence, attempt, step, query slot, child counts, ordinals, revisions, duplicates and completeness. Its reply-parent membership check now uses the actual remote frontier, preventing a remote reply from claiming a locally expanded parent.

## Work accounting and tests

Logical expansions, candidate distances and levels remain comparable between direct and envelope paths. Local owner expansion and successful-prefix distance atomics are aggregated after scoring. Local queries create no envelope query-copy or reply-validation work, so those protocol counters and timing scopes correctly fall to zero for the fully local path; they must not be interpreted as missing logical distances. Mixed and remote work retain their accounting and validation.

The differential regression uses the actual owner runtime, the retained envelope path and the reference writer as independent execution paths. It checks mixed-depth trees and a wide tied-score fixture across beam widths, tau values and exclusions, exact result order and scores, equal logical work and zero local envelope-copy counters. A mixed local/remote regression checks one global beam plus reversed, duplicate, missing, wrong-step and partial replies. A retired-local-image regression rejects a complete navigation and succeeds after publication restores a compatible image. The retired-image test uses a controlled publication seam; it is not evidence of every possible concurrent schedule.

The consolidated receipt `epoch16-suite-01.log` reports 188 passed, zero failed and one filtered test in 5.27 seconds, including the three new direct-navigation tests. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`. This review does not claim an unfiltered clean suite. The root agent runs Cargo; this reviewer inspected code and receipts without a competing build.

## Execution preparation

The epoch16 delta is limited to fresh namespaces, binary bindings and inheritance from the completed epoch15 candidate template. The fixed workload retains 500K vectors, 32 workers, 500K owner capacity, the verified epoch14 input, the 600/590-second supervisor/workload limits and existing independent window guards. It performs one explicit candidate arm with no profiler, reference rerun, sweep or automatic retry. The existing epoch15 job supervisor is unchanged. The build adapter differs only in namespace. The generator binds the completed epoch15 template through the preparation receipt; its SHA256 is `69bca3bb79ed7f30bb0777c5bbf44e53e532173d83a6ca23ec2bf0d22f1637e7`.

All eight file hashes in `operations/preparation-epoch16.json` match their current files, including the inherited run template and unchanged supervisor. The five focused preparation tests pass in `epoch16-preparation.log`. The root agent reports all three native CLI builds passing in `DIRECT-LOCAL-NAV-VALIDATION.json`, with Git index hashes unchanged. The final three-file source closure and Linux artifact hash readbacks remain separate root-operator admission checks. This review authorizes no extension of a running window or alteration of cleanup controls.

## Limits and follow-up

This change retains the packed centroid cache’s duplication of original child-centroid metadata. It makes no claim about memory reduction. Independent randomized construction can change tree shape, clustering work and reassignment counts, so the next comparison must report those work counts alongside wall and CPU time. The scheduler readiness concern and proposed separate diagnostic are recorded in `SCHEDULER-PATH-AUDIT.md`; no scheduler or extra diagnostic instrumentation belongs in this navigation comparison.

## Reviewed source hashes

Paths are relative to `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed`.

| File | SHA256 |
| --- | --- |
| `routing.rs` | `673e5aa893d98072c8952d92cf74503837e2dae4645452471e270764b36ed4a7` |
| `engine.rs` | `cc496552c409502e288fc33bb26bdf80d6e47c6515d9ce11961bd2c43585add0` |
| `routing_tests.rs` | `36e87f7ae995665478d1b98bbb6c46ecef794c5b71abd08100d16ba46e36082b` |
