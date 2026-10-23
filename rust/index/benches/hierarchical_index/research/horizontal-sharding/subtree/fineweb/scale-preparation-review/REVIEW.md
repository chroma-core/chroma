# Preparing the next corpus sizes

The next preparation stage should distribute independent source-file reads while preserving one canonical row order and one complete UUID uniqueness proof. The current verified pipeline remains the correctness reference. The existing sandbox authorization covers future scale runs; a 100M launch still requires the 10M capacity results and a concrete resource plan, followed by a separate 1B capacity decision.

## What the 10M measurements establish

The completed fresh ingestion takes 676.928 seconds and charges 14,802,824,308 requested source bytes. It produces 33,629,228,179 owned bytes, including a 1,277,358,080-byte SQLite UUID index. These values come from `fleet/prep10m/ingest.json`. The receipt does not separate UUID audit time, UUID JSON parsing, network waiting, conversion, hashing and output writes, so it does not establish that SQLite is the dominant ingestion cost.

The eleven preserved source-file receipts sum to 939.223 file wall seconds and 106.210 emit-plus-chunk-hash seconds. Metadata request timers sum to 4.363 seconds. These are concurrent task sums, not elapsed stage boundaries. Subtracting them from total ingestion time cannot recover SQLite audit time. The exact receipt hashes and aggregate are in `measured-evidence.json`.

The complete preparation retry takes 701.469 seconds and reuses completed ingestion. Its ingestion verification takes 30.604 seconds; model fitting takes 31.497; routing takes 114.163; complete ID and bitwise routing audit takes 156.036; sample and UUID spot checks take 20.746; two exact-reference scans take 100.326 and 99.628; three independent reference checks take 56.134. The process records 3,169.919 user and 134.247 system CPU seconds, with 998,336 KiB peak RSS. These CPU values cover the retry as a whole and cannot attribute fresh-ingestion work. Reused ingestion time must not be presented as fresh ingestion performance (`fleet/prep10m/final-run.json`).

The UUID audit uses one coordinator to parse every committed UUID JSON row, verify contiguous global IDs, and insert exact source strings into a disk-backed primary key. It commits batches of 4,096 rows and also maintains a unique global-ID index. Duplicate UUIDs fail completion. Completed source chunks remain immutable; a failed temporary index is rebuilt. This is a plausible serial scaling limit, but stage-specific measurements are required before calling it the bottleneck (`fineweb_stream._audit_uuids`).

## Preserve the identity and ordering contract

A planning coordinator must freeze the source revision, lexically ordered file list, each file's row count, the requested prefix length, and each selected file interval before dispatch. A row's global ID equals its source-file prefix sum plus its row offset. Scheduling, retries and node placement cannot change that calculation. The last selected file is explicitly truncated to the requested prefix. Source metadata must be verified against the pinned revision; a changed count or digest fails the attempt.

Each node receives disjoint file intervals and writes bounded chunks to a unique attempt directory. A completed chunk receipt binds its global interval, source file and row interval, decoder version, vector bytes, UUID records, hashes and byte counts. The coordinator accepts exactly one successful receipt per planned interval and proves no gaps or overlaps before publication. Failed attempts remain journaled and cannot silently replace an accepted receipt. Resume verifies hashes rather than trusting file presence.

UUID equality must preserve the current exact-string semantics. Normalizing case, removing a UUID prefix, parsing into 128-bit values or retaining only a hash can change the identity contract. An alternative format must either prove exact reversible encoding for every accepted source string or store and compare the complete original bytes. A hash may choose a reducer; it cannot prove uniqueness by itself.

## Exact uniqueness options

| Option | Complete uniqueness mechanism | Additional work and limits |
| --- | --- | --- |
| Keep the coordinator database | Insert every original UUID string into one disk-backed unique index after all chunk receipts pass. | Lowest change risk. Add UUID parse/insert/commit/hash timers and CPU/I/O counters. The coordinator and database capacity remain serial limits. |
| Distributed sort and merge | Each worker emits sorted runs of complete UUID bytes plus global ID. Reducers merge runs, reject equal adjacent UUIDs, and produce durable coverage receipts. | Bound run memory and merge fan-in; use multiple merge passes. Define one total byte order and unambiguous length-delimited encoding. A final merge or proven disjoint range partition must cover all runs, including duplicates across nodes and attempts. |
| Hash-partitioned exact reducers | Route complete UUID strings by a frozen stable hash to a fixed logical reducer set. Each reducer performs exact string comparison using a local database or sort. | All equal strings must reach the same reducer. Manifest every input partition and account for all rows. Hash collisions are harmless only when reducers compare original strings. Skew and maximum reducer load need guard limits. |

For 100M, first instrument the current 10M metadata audit and compare a bounded real-UUID replay of the coordinator database with bounded external sort. Keep the distributed download path separate from the identity proof. For 1B, fixed logical UUID reducers are a concrete way to distribute the proof without making physical node count part of identity. They need independent cross-node duplicate injection tests, restart tests, skew limits and complete receipt accounting before use.

A uniqueness verdict does not provide random UUID-to-row lookup. If downstream use requires that lookup, the sort path must build an immutable searchable artifact or retain per-reducer databases. Include that build and storage in comparisons; comparing a verdict-only merge with a database is not equivalent work.

## Bounded resource and retry design

At dimension 768, canonical float32 vectors alone require 307.2 GB for 100M rows and 3.072 TB for 1B rows. Canonical local-to-global maps add 0.8 GB and 8 GB. Owner-routed vectors may add another full vector copy; retained source chunks, UUID records, sorted runs, database indexes, object-store staging and failed attempts are additional. These are byte arithmetic, not measured total-capacity predictions. UUID lengths, compression, owner imbalance and merge-pass multiplicity must remain explicit.

Each node needs a host-local supervisor that owns its actual worker process groups, a renewed lease, an absolute deadline, aggregate RSS limits and an inventory of every owned input, temporary, output and control path. Logical bytes, allocated bytes, filesystem free space and peak scratch are separate measurements. Abort margins must cover sampling and shutdown latency. A failed or unreachable node blocks publication until its terminal process reconciliation is preserved. Cleanup requires proof that no retained manifest or active attempt references the files.

The immutable handoff should publish a single manifest only after all source intervals, UUID reducers, routing outputs and required references pass. The manifest binds the source plan, original UUID equality policy, complete row count, all chunk and reducer receipts, model and routing identities, decoder and producer source hashes, and version-pinned objects. The handoff must be restorable and independently revalidated before downstream index construction starts.

## Local metadata experiment

The accompanying script compares the current database shape with bounded sorted runs over 100,000 deterministic UUID strings. Both must reject a duplicate placed across run boundaries. It uses no vectors, downloads or external nodes. The exact commands, source hash, wall and CPU times, peak RSS and output sizes are in `fixture-results.json`.

The fixture can screen implementation overhead and exact duplicate handling. It cannot identify the real 10M bottleneck or predict 100M/1B throughput. The merge experiment emits a verdict from sorted runs rather than a UUID lookup database, and a larger implementation needs bounded fan-in across multiple passes.

## Work required before a scale launch

1. Add measured stage boundaries to a separate preparation epoch: network requests, decode, float conversion, UUID serialization, vector writes, artifact hashing, UUID parse/insert/commit, routing, and independent audits. Record parent and child CPU, RSS, disk bytes and network bytes with complete sample windows.
2. Freeze the canonical file-interval plan and test multi-node receipt assembly against the verified single-node order, including a prefix ending inside a file.
3. Test the chosen global uniqueness proof with duplicates within a chunk, across chunks, across source files, across nodes and across retries. Test empty partitions, skew, corrupted receipts and omitted intervals.
4. Measure actual scratch and output amplification at a bounded intermediate size. Admit a proposed 100M allocation only after maximum node/reducer load, failure retention and restore capacity fit explicit limits.
5. Keep real 100M validation separate from the later 1B decision. Preserve all completed and failed run outcomes, and publish complete coverage and reference proofs before comparing index performance.
