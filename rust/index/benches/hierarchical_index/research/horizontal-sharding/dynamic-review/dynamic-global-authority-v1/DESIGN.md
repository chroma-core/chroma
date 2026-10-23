# Give local owners authority over posting mutation

This slice makes local owner actors authoritative for postings and current versions during a bounded fixed-topology operation sequence. The coordinator retains routing nodes, branch placement, and vector-ID locations. It does not contain a canonical writer, posting arrays, compressed vector codes, a current-version table, or a corpus embedding table.

## The pieces

- A routing catalog reads the exact saved tree's node identities, ordered edges, centroid bits and write policy. It performs global reference-style navigation. ID locations resolve the one current owner and leaf; they do not carry versions.
- Owner actors load their assigned saved postings and scalar versions themselves. Each actor owns its mutable leaf rows and current-version records. The second actor starts with no branches.
- Durable vector references contain a vector blockfile UUID and a key. Owners fetch vector values through those references when encoding, looking up or scanning rows. They retain references rather than a corpus vector array.
- Direct actor channels transfer branch state and versioned row authority. The coordinator sends commands and receives metadata acknowledgements; it does not receive full branch payloads.
- Private blockfile forks provide a complete saved snapshot. Each owner writes its actual postings, versions and leaf metadata directly into cloned private writer handles. The coordinator commits the complete roots after all owners acknowledge their writes.

## Supported operations

The API opens a saved ordinary reference tree with every branch assigned to one active owner. It supports moving an existing whole leaf branch to another configured actor, registering a new dense ID at the global destination, and replacing one durable vector followed by global rerouting and versioned registration. The tested replacement has a different destination leaf; same-leaf replacement rejects because the existing reference reassign operation would not re-encode that posting.

This is a fixed-topology authority cut. It rejects structural edits, and registration rejects a destination that requires a split. It does not implement automatic leaf splitting, merging, root replacement or parent edits. The earlier live-split predicate hook remains a separate interface whose mutation authority is centralized. Connecting that hook to this actor authority and preserving its recursive structural semantics is still required for the complete evolving writer.

## Move and mutate

1. **Read owner state from the saved source.** The initial actor reads posting data, current scalar versions, leaf metadata and centroids through its own readers. Bootstrap returns only live ID/leaf locations to the coordinator. It requires complete dense initial coverage and one current copy per ID.
2. **Move an existing branch.** The source actor removes the leaf and its current records, then sends those owned values directly to the destination actor. The destination installs them. The coordinator changes placement metadata after the acknowledgement. Centroids, topology, IDs, code bytes and vector UUID/key references remain unchanged. No full vectors cross this move.
3. **Insert at the new owner.** Global routing chooses a leaf. The destination actor checks ownership and capacity, fetches the newly saved durable vector, assigns its first version and mutates its posting state. It does not mirror a central registration result.
4. **Reroute with owner-issued version advancement.** The source actor validates its current record and leaf. After destination admission, it removes its current authority and sends a private version handoff directly to the destination. The destination validates the version increment, fetches the replacement vector, encodes it against its own centroid and installs the new current membership. The old physical posting remains stale, matching the reference.
5. **Observe current state locally.** A lookup at the old owner fails instead of redirecting or falling back to a central table. The destination returns its new version and durable vector bits. Owner-local scans filter physical rows using the actor's own current table; the coordinator supplies no per-row version values.
6. **Save one complete version.** Owners write their own rows and scalar versions into shared private unordered blockfile forks. The coordinator writes the maximum-ID summary, then commits and flushes the roots. The previous saved generation remains independent.

The serialized POC operation can have no active copy between source relinquishment and destination acceptance. The coordinator marks the operation incomplete and forbids snapshot publication until the acknowledgement and location update finish. A failed mutation leaves that coordinator unusable for save; recovery and rollback are not implemented. Missing-owner and wrong-owner reads fail without fallback. Private channels and exclusive coordinator mutation provide the local operation boundary; this is not a production lease or a concurrent distributed transaction.

## Reference comparison and saved proof

The representative test uses the first 96 rows of the retained 98-row, 32-dimensional canonical artifact, pinned by SHA. It constructs and saves one ordinary reference tree on one owner, then loads actor authority from the actual five saved roots. The coordinator does not bootstrap from a central in-memory posting copy. The separate oracle reopens the same saved source into the existing writer; its full loading is an oracle cost.

The test moves the branch containing ID 0 to owner 1, registers ID 96 there using retained vector bits, reroutes ID 0 to another owner's leaf with a durable replacement vector, and registers ID 97 at the moved branch. It chooses an existing destination with enough observed posting headroom and asserts reference routing agreement. The added and replacement vectors reuse retained coordinate rows; this is a correctness workload, not the original 96-to-98 build workload or a quality comparison.

The reference performs the same two adds. For replacement it uses its existing embedding update followed by the actual global `reassign` method, whose visibility is widened within the module for the oracle; its implementation is unchanged. The test checks the reference topology digest remains unchanged. The only existing source integration changes are the new module declarations and that visibility annotation.

A fresh saved reader censuses all 98 expected IDs from reachable postings and current scalar versions. The owner and reference reports match, with 99 physical rows, 98 live rows and one stale copy. Every saved leaf's posting ID order, stored versions and code bytes match the reference. Every saved vector's FP32 bits match both the expected retained bits and the reference snapshot. The original 96-row saved generation is reopened and censused unchanged. The test also proves missing-owner lookup, invalid move destination, wrong-owner lookup and wrong-owner scan reject, and structural edits reject before mutation.

## Costs and bounds

The coordinator still loads all routing metadata and an ID-to-location directory. Those are whole-tree and whole-corpus metadata costs. Owner postings and versions are fully materialized within each actor. The POC admits at most 128 live rows, four local actor slots, 1,024 routing nodes, 128 children per routing node, and 4,096 dimensions. Registration honors the configured posting split threshold, capped at 128. These limits are not predecode, cache or RSS bounds; typed root-range reads can allocate before the metadata cap is checked.

Each actor separately counts posting/bootstrap logical payload, durable embedding gets and returned FP32 payload, centroid gets and returned centroid payload, vector-root opens, moved branch logical payload, current-record exports/imports, registered rows, actor commands and saved posting payload. Counters include the test's validation lookups and scans. Provider-cache hits, successful storage callbacks, encoded bytes, root-decode allocations, physical network I/O and process memory are unmeasured. Their absence from a counter is not zero work.

The moved-byte figure is the logical content of the branch arrays and current-reference fields. A local channel moves Vec ownership; it does not copy those heap buffers through the coordinator or measure network traffic. Owners fetch full vectors through the shared immutable saved store and copy one returned vector at a time. Those returned bytes are not physical storage bytes. The provider and its caches are shared local infrastructure, not a remote owner-local disk proof.

The saved snapshot rewrites all actor postings and active scalar versions into private forks. This remains full-corpus save work. Shared blockfile writer buffers collect persistence updates, but they are not the coordinator's mutable posting/version authority between operations. Ten-second response waits do not bound blocking sends or joins. The local test/check recipe uses Rust 1.92, offline dependencies, two build jobs, zero debug info, a 20-GiB free-space floor and a 1,200-second subprocess deadline per stage.

## Remaining boundaries

This is real local owner authority for the supported fixed-topology operations, not a label over a central canonical writer. It is not yet the complete globally evolving tree. Automatic split/NPA dispatch into authoritative actors, merge/parent/root updates, concurrent ownership, multi-process mutable state, remote durable vector access, a bounded routing cache and scalable persistence remain open. The coordinate-replacement reroute is explicit; it is not evidence of split-triggered same-vector reassignment quality. No matched two-million-vector quality result, throughput gain, Linux run, cloud action, controller or production recovery behavior is claimed.

## Retained failures

The first frozen compile fails before tests because an error-conversion helper only accepts one boxed error type while storage APIs return several concrete types. The second compile fixes that helper but fails before tests because generic flusher types need explicit arguments. The third source epoch adds those type arguments; the representative test and benchmark check pass. An earlier optional text-patch attempt for additional capacity guards fails its match assertion before writing source; the frozen first epoch uses the unchanged guarded registration/bootstrap implementation. All source archives, logs and skipped-stage distinctions remain intact.

The authority router accepts only full-precision navigation and rejects other navigation modes before reading saved roots or starting actors. Registration, branch acceptance, and same-owner rerouting enforce the same checked 256-physical-row owner budget. The new admission test exercises unsupported navigation with absent roots and checks budget boundaries and arithmetic overflow.
