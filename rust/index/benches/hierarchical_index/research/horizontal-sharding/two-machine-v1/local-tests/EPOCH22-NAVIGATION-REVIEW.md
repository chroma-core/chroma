# Epoch22 navigation compatibility and candidate preparation review

The coherent navigation repair and fixed 3M candidate preparation have no blocker within this review scope. Navigation keeps one immutable shared scoring view for an attempt and tolerates newer shared publications only when their routing structure and placement remain compatible. This removes a reproduced cause of needless retry exhaustion without treating retired or relocated owner routes as valid. It does not establish that the next physical 3M construction completes.

## Snapshot and compatibility

Each construction navigation attempt pins the shared view it starts from. Shared centroid scoring uses those immutable images throughout the attempt. A cached SHA256 describes the root and shared nodes’ identities, parents, ordered child identities, leaf/internal flags and ownership-unit directory, including owner and placement counters. Centroid values and ordinary routing revisions deliberately do not invalidate this topology identity. Checks compare the current cached digest before expansion and after the complete traversal rather than rejecting every publication sequence change.

Owner-local routing images remain live immutable snapshots read under the routing gate; remote owned nodes still expand through their actual authority. Missing or mismatched owner identities and placements reject the attempt. Shared structural or placement changes restart the whole global beam. Local and remote candidates retain the same global beam selection, order, metric and reply validation. This is a compatible shared construction view, not a frozen snapshot of every mutable owner subtree or a new saved-query generation.

The digest is prepared once at publication, initialization or view installation and compares in constant-sized form during traversal. Every production publisher calls the preparation path. The cache is skipped in serialization, does not affect semantic view equality, and is rebuilt after a decoded view is installed. A decoded unprepared cache cannot silently pass compatibility checks. Hash construction includes list counts and fixed-width identities, preserving child order and boundary placement. Returning to the same effective topology is compatible; fresh structural identities and placement counters still distinguish retired or relocated routes. The existing owner endpoint checks remain necessary even when the shared digest matches.

The attempt retains one shared-view Arc and releases it when the attempt ends; it does not clone all shared centroids for every level or hash them during each compatibility check. Computing the topology digest adds publication work proportional to shared metadata. Real structural churn still has the same eight-attempt bound, and errors now retain the last rejection and sequence context. No unlimited retry or deadline extension is introduced.

## Evidence and limits

The controlled real-runtime red test reproduces eight-attempt exhaustion when a centroid-only publication occurs after every attempt pins its view. The repaired test completes in one attempt. The focused receipt `epoch22-navigation-focused-02.log` reports seven passing tests, including the retained root-view test. New checks cover centroid compatibility, digest changes for structure, eligibility and placement, serialization/reinstallation, replacement-boundary restart, retired and relocated owned routes, finite structural-churn rejection and an actual TCP remote owner combined with local candidates under one beam.

There is no dedicated repeated-abort navigation test in this snapshot. Source review confirms publication abort changes only the attempt record and never replaces the published view or sequence; existing publication arbitration and replay tests remain in the applicable suite. This source argument is distinct from direct navigation regression coverage. Likewise, the physical epoch21 failure lacks per-attempt rejection reasons: the controlled test reproduces its exact exhaustion symptom, but exclusive attribution of the physical failure cannot be reconstructed. The next run retains the improved diagnostics.

The consolidated suite reports 213 passed, zero failed and one filtered test in 5.39 seconds in `epoch22-suite-01.log`. The exclusion remains the known unrelated reference-writer test `concurrent_parent_and_leaf_splits_revalidate_without_losing_membership`; an unfiltered clean suite is not claimed. The engine worker reports all three native CLI builds passing in 18.41 seconds in `NAVIGATION-SNAPSHOT-VALIDATION.json`; root verification of that receipt and fresh Linux artifact readbacks remain admission requirements.

## Source archive closure

This review binds `packages/source-epoch22b.tar.gz`, SHA256 `45d56253789916dbacc04a656aee18f42f3d654a0780454759925b5cde8e9af0`, with source-manifest SHA256 `bb2bf1ce11923f1d67c0561374c2e31534c354d05e1130212977ab7596594c2d`. Independent comparison with the frozen epoch20 manifest confirms exactly the four changed files listed below and no added or removed files. The save-runtime change only supplies the new cache field in an existing test literal.

The first `source-epoch22.tar.gz` omits four untracked benchmark/test entry points and is rejected before admission. It remains retained as failed preparation. The replacement packager explicitly includes those known entry points while retaining regular-file materialization and source-link validation. Its SHA256 is `1772fcb2915d934244feb7adce80afb48a0e5a6f5a7abba76a92e6ff4762c985`. Only the complete `22b` archive belongs in execution admission.

## Operational preparation

The epoch22 adapters select exactly one candidate-only 3M run against the valid epoch21 instrumented reference result. They retain the exact epoch2 input, 768 dimensions, 32 workers, 100K intervals through 1M and 1M intervals afterward, split/merge/branching settings, 2,160,000 capacity and zero first-owner override. Final acceptance still requires actual final-1M membership and construction work on both owners, complete saved-state validation and minimum 30% final owner share. There is no reference rerun, profiler, automatic retry or size sweep.

The runner differs from epoch21 only by fresh namespaces and the candidate-only arm choice. The build adapter retains the reviewed distributed-only source-manifest checks. The unchanged epoch15 supervisor, 590-second job deadline, 660-second minimum remaining window, 1,800-second host window and independent shutdown controls remain in place. The sole operator performs one bounded input/hash check per host concurrently, with exact descriptor and binary readbacks. Matching newly built writer and reader hashes are required on both hosts; absent or malformed runtime bindings fail closed.

All hashes in `operations/preparation-epoch22.json` match at review time, including the inherited epoch21 candidate template SHA256 `6871ed4fabffe7763bea0c82e79f510a012781c734ee5e68d920e81ffdad693a`. Five preparation tests pass in `epoch22-preparation.log`. The root operator binds final configuration/source metadata, native and Linux build receipts, fresh identity/preflight, actual inputs, runtime hashes and absolute guards before admission. Known cleanup failures and incomplete process evidence remain visible, and API shutdown verification remains separate. This reviewer performs no source edits, Cargo commands, cloud operations or Git writes.

## Reviewed source hashes

Paths are relative to `rust/index/benches/hierarchical_index/distributed` in the frozen source archive.

| File | SHA256 |
| --- | --- |
| `construction.rs` | `cff48e9e46b6760a1015c9927d6a580310fd88892a38e240bccbf4fd0cf9e1f4` |
| `engine.rs` | `7266b431d01829970d391ae988d372f81ffd5a52f35e62e8fe7f04617a758bc2` |
| `engine_endpoint_tests.rs` | `80a3b4ee56028fc2f0e89a7137dd55c364107edd351b2ccac0524e242d69a827` |
| `save_runtime.rs` | `cca129118d445c97251c26675cc20f2e2a00486300e346769c4d58f99aeb0d86` |
