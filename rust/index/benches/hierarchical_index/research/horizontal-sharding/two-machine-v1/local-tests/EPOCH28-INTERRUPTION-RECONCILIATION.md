# Experiment 28 interruption and handoff reconciliation

Experiment 28 has no collected physical benchmark result. Experiment 27 remains the latest validated performance baseline. The operator and implementation agents both report model usage-limit errors; their child sessions are now interrupted. The user has paused the follow-up and requests a handoff, not a new execution window.

## Cloud and experiment evidence

Both exact task instances are stopped in a fresh AWS API check at 2026-10-22T14:01:02.822314+00:00, in sandbox account 079819354890/us-east-1. The [verification receipt](../operations/window-epoch28/handoff-shutdown-verification.json) is read-only. The original [closure receipt](../operations/window-epoch28/closed.json) records both stopped at Unix time 1792668261.501896, after the absolute 11:24 UTC deadline. Disks, roots and remote source remain retained. No machine is restarted during this reconciliation.

The source-upload checks and remote shutdown guards are retained. The local Linux-build stdout contains only “Frozen28source verified and extracted”; there is no collected final build receipt, admitted Linux binary registry or benchmark result. Build success and whether additional remote artifacts exist are unknown. Do not infer a failed algorithm, a successful run or a speedup. Recover small remote logs at the start of a separately admitted future window before deciding whether a build or benchmark must be repeated. Never reuse the closed window.

The frozen candidate is source-epoch28.tar.gz, SHA256 7cb1a5eeb090f53a258217dc9c098c01be9e65e03b721e47b809eb5f2866ed08. Its local validation records 257 passing applicable tests, one documented unrelated exclusion, three built CLIs and a resolved independent review. The fixed 24-move diagnostic reduces protocol calls 88→11 and source compactions 8→1. These establish mechanism and local correctness evidence, not physical performance.

## Partial storage implementation

Candidate 29 adds a conditional durable-object backend and integrity-checked references, with owner-direct saving and reader/CLI wiring. Six existing files differ from frozen28, and two distributed source files are new: generation_backend.rs and durable_generation_tests.rs. The [source inventory](handoff-source-vs-epoch28.json) records exact hashes. Both staged Git indexes match the previously recorded hashes; no implementation or staged file is changed during this reconciliation.

The epoch29-compile-01.log records a successful native test-target compilation in 21.54 seconds. It does not record executed tests. Three tests exist for immutable-object replay/collision/integrity, conditional publication with lazy reads, and failed-owner publication. No completed test run, CLI build, independent review, frozen29 archive or cloud admission is established. Inspect and validate the partial implementation before using it. Conditional object writes use existing storage interfaces; real S3 access and conditional behavior remain unverified. Incremental reuse of unchanged blocks is a separate future change.

## Spending and next decision

Add the closed window's $4.080092862548828 compute upper bound once. The observed-stop interval is 1821.470 seconds, 21.470 seconds beyond the planned 1800 seconds; the accounting bound is $0.048093 above the planned $4.032 cap. This is a conservative lifecycle bound, not a measured AWS invoice. Future windows should reserve shutdown headroom within their cost ceiling.

Covered compute is now $55.800973009132802683; recurring retained storage remains $19.52/month. The first-month covered bound is $75.320973009132802683, leaving $924.679026990867197317 of the $1,000 allowance under that accounting. This is task-specific accounting, not a whole-account bill.

Resume from [the concise handoff](../../NEW-CHAT-HANDOFF.md). First recover the missing experiment28 evidence under a fresh guarded window when execution resumes; separately complete and review candidate29 locally. Preserve both frozen28 and the partial29 tree. Keep the automation paused until the user chooses to resume it.
