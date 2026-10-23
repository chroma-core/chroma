# Dispatch neighbor scans during a live reference split

The live split callback now sends each neighbor's predicate work to its local owner actor and waits for the result before continuing reference reassignment. This preserves mutation order: an eligible row is reassigned immediately, and subsequent version checks, neighbor scrubs and source captures read the updated reference state. Construction mutation remains centralized; this is not a multi-process distributed writer.

## The pieces

- The ordinary reference writer performs insertion, clustering, global neighbor discovery, eligibility ordering, global destination navigation, duplicate suppression, version advancement and registration.
- An optional live hook connects those existing callbacks to a placement directory and local owner actors. New and reopened writers have no hook by default.
- A persistent thread for each owner receives a fresh source installation for each neighbor. That source contains actual posting IDs, stored versions, centroid/code bytes and the vectors available to the reference FP path.
- Each per-row request identifies the installed session and source index and carries the current version that the reference loop has just checked. The owner actor reads its installed source and computes the shared exact predicate.
- A semantic trace records insertion, destination navigation, registration, splits, global discovery, pre-scrub versions, neighbor sources, per-row versions and eligibility decisions. A separate transport trace records source-copy payloads and actual actor replies/thread identities.

## Mutation order

1. **Perform the actual reference split.** The existing splitter creates replacement leaves and updates the parent or root. The hook assigns both replacement leaves to the old leaf's owner. Shared routing nodes remain centralized.
2. **Run reference self-reassignment.** The reference's existing self-reassignment code runs unchanged before global neighbor discovery. The destination-navigation and successful-registration hooks observe its actual decisions.
3. **Discover neighbors globally.** The existing callback invokes the existing navigation policy, applies its neighbor count and exclusions, then scrubs each selected neighbor in the same order.
4. **Install the current neighbor source.** Capture the same centroid, posting/version sequence, codes and FP vectors used by the reference loop, after preceding mutations and the current scrub. Send that owned data to the resolved owner thread.
5. **Check and dispatch one row.** Keep current-version comparison, evaluated-set insertion and missing-FP-vector handling in the original loop. Dispatch the row's predicate only when the reference would evaluate it. The actor checks the stored version against the request's current version and returns the exact predicate result.
6. **Mutate immediately.** If eligible, invoke the original `reassign` call immediately. It navigates globally, selects destinations, validates the old version, increments it and registers the replacement membership. Refresh the reachable leaf directory before continuing. No hook-state lock is held across this mutation or its recursive balancing.
7. **Continue with fresh state.** The next loop iteration checks the latest version. A later neighbor callback scrubs against updated versions and installs its current source. There is no precomputed batch of eligibility results whose versions could become stale.

The actor does not independently fetch current versions from durable storage. The centralized writer remains the version authority and supplies the version just checked by the live loop. The actor owns its installed predicate source, but authoritative mutable postings still reside in the reference writer. Remote durable-vector access and authoritative multi-process mutation remain separate missing interfaces.

## Placement and default behavior

The public wrapper enables the hook only with exclusive mutable access and uses sequential add/balance. Capacity transfers are deferred while a hook is enabled. Logical splits inherit the source owner, multiple branches per owner remain valid, and registration refreshes the reachable branch cover. The wrapper reconciles its image and directory at batch completion. Disable the hook before requesting another capacity transfer.

The default writer retains the existing reference predicates and ordering. Hook checks add instrumentation branches, so no zero-overhead performance claim is made. The FP path copies posting code bytes only when the hook is enabled; it does not add that payload copy to the default path. No saved schema, quantizer, clustering rule, routing policy or production configuration changes.

## Paired construction test

The local fixture starts with one ordinary leaf and five vectors at x=0, 1, 30, 31 and 32, with a shared finite four-dimensional representation. The reference split creates the existing branches before placement moves the three-row branch to a second owner. The exact saved five-root generation is reopened for each paired construction run. The test asserts actual initial leaf centers 0.5 and 31, so the new x=16 point lies just beyond their 15.75 boundary.

A fixed insertion schedule adds x=16, then three x=14 vectors. This causes an actual split of the low-valued branch: the x=16 vector on the other owner becomes eligible for the new x=14 branch. The original destination navigation and versioned registration move that vector. One more x=14 insertion causes another actual split. Its later neighbor scrub observes the earlier vector's advanced version and removes the stale old membership before installing the next actor source.

The tests run this schedule in three modes: hook disabled, reference predicates with a trace, and live owner dispatch. Both full-precision and quantized eligibility modes receive paired tests. Each requires the cross-owner eligibility, successful global versioned registration, later version visibility, exact reference/dispatched instrumented event order, and exact final tree images. Default-disabled final tree and saved census must match too.

The reference splitter uses `thread_rng`. A test-only tape records actual reference split decisions and their complete input identities, versions and vector bits, then replays those exact decisions in the other runs. This isolates dispatch from random-clustering differences. Production continues calling the ordinary random splitter; no deterministic construction rule or quality equivalence is claimed from the tape.

## Saved membership proof

Each completed run saves a new five-root fork and opens a fresh saved reader. The accepted root-reachable membership diagnostic scans all reachable postings and current versions with every expected vector ID included in its target set. It requires complete unique current coverage of all ten IDs, validates range/version/deletion semantics, and compares leaf membership and row-cost reports between the default, traced-reference and dispatched generations. This is a persisted posting/current-membership census, not only a vector rerank. The original five-vector generation is reopened and censused after the new forks are saved.

## Bounds and limitations

The wrapper keeps the existing 100,000-row and 128-owner admission limits. Source installations admit at most 100,000 entries and eight MiB of logical payload per neighbor; the payload check occurs after source copying and is not a preallocation or RSS bound. Each actor permits at most 128 nested installed sessions. Combined semantic and transport evidence rejects more than 16 MiB, with separate 100,000-entry caps. Source JSON hashing, reader/provider caches, allocator overhead and temporary serialized buffers remain outside those logical payload figures.

Five-second response waits do not imply an aggregate deadline: channel sends and thread joins are blocking. Local tests have the bounded parent subprocess recipe, two build jobs, offline dependencies and a 20-GiB free-space floor. No cancellation, panic recovery or failed-write publication protocol is claimed.

Source copies, version checks, directory refreshes and complete wrapper images still cause centralized reads and scans. Logical copied source bytes are reported separately from local channel replies; they are not measured network traffic. Source vectors come through the existing central writer's load path, not a remote durable-data owner. No query or construction speedup, matched-cohort quality result, Linux execution, cloud work or controller is part of this milestone.

Cross-owner merges, concurrent owner mutations, independent owner persistence, remote durable vector loading and multi-process ownership exclusion remain unimplemented. The ordinary reference's centralized structural logic remains in use. The local live-callback proof removes the stable-snapshot-only limitation of the preceding slice; it does not establish the complete distributed architecture.

## Retained fixture failure

The repair-v3 run passes FP but fails the additional quantized positive-eligibility assertion after paired reference/dispatched event and tree equality. The subsequent benchmark check is skipped. The original repeated-coordinate bootstrap permits the reference splitter to sample two identical centers. Its zero center-separation branch sets relative difference to zero before the minimum-partition repair, so an assumed initial partition is not a reliable fixture prerequisite. The failed run does not retain its initial parent image, so that mechanism is a source-backed confound rather than a reconstructed observation of its exact random choices. Repair-v4 uses distinct bootstrap coordinates, explicitly verifies the actual parent centers before continuation, and retains their initial images. It changes no splitter, predicate, seed, routing setting or positive-eligibility assertion. Both modes pass. All earlier source epochs and terminal results remain intact.

Transport evidence retains three source-install replies and nine predicate-evaluation replies per final mode. Each of the three neighbor sessions also sends an End command and waits for its reply; those replies are not retained in the transport vector. Thus there are twelve retained install/evaluate reply records and fifteen actual channel request/reply exchanges in these tests. The semantic trace retains all three completed session-end events.
