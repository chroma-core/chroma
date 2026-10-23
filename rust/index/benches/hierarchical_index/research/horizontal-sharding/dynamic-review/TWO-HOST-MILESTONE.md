# Two-host dynamic ownership milestone

The next milestone moves a complete logical subtree between two physical sandbox workers, then changes ownership across subtree boundaries and publishes each complete replacement through the driver. It proves correctness and reader lifetime with tiny real Rust indexes; it is not a performance benchmark or a production deployment.

## Scheduling and owned scope

Linux owns the current V5 construction pair and must explicitly release both workers after its resource collection. This plan launches no remote work. Astra 03 reviews frozen milestone-v7; the implementation files remain byte-identical during that review.

The proposed workers are sandbox account `079819354890`: worker 0 is `i-09b86223b5620b35d` at `172.31.69.252`, and worker 1 is `i-011ff180f03584f40` at `172.31.65.187`. Linux must reverify these identities, boot state and free space at the scheduling handoff. The existing control driver runs the single filesystem publication authority. Its exact instance identity remains an admission prerequisite.

Each machine receives a fresh attempt directory under `/mnt/spann/dynamic-twohost-v1/<attempt-id>`. The attempt directory must not exist before admission. Code, local checkpoint stores, incoming archives, generations, catalogs, logs, leases and control files stay inside that directory. No benchmark checkpoint directory is copied, walked, changed or deleted. The fixture builds its own 48–128 documents with two dimensions and stable global IDs. It uses the existing immutable V4 worker binary, whose expected SHA-256 is `7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158`; the actual hash is checked on each machine and recorded before work.

## The pieces

- The authority owns the complete immutable deployment record, canonical fixture vectors, local metadata catalog and deployment-wide compare-and-swap fence. Workers receive no credential or filesystem access that lets them publish this pointer.
- A worker agent owns only its declared fresh attempt directory. It receives immutable files, builds fresh lower trees and retains generation-specific Rust readers. It never rewrites a published lower snapshot.
- The existing host supervisor owns the agent's process group, including every Rust child, and enforces its CPU allocation, resources, renewable lease and absolute deadline.
- Pinned SSH connections authenticate each worker and carry encrypted local port forwards. A worker's control/query listener binds only to loopback; it exposes no unauthenticated VPC listener.
- A reader pins the complete deployment and its generation-specific serving handles. An older serving generation stays alive until its fixture readers close and the authority explicitly retires that generation.

## Authentication and supervision

Linux supplies the dedicated driver key path and a preserved, independently verified host-key receipt for each exact worker identity. New connections use `BatchMode=yes`, `IdentitiesOnly=yes`, `StrictHostKeyChecking=yes` and a sealed attempt-specific `UserKnownHostsFile`. A changed or absent key blocks admission. Existing unverified `StrictHostKeyChecking=no` behavior is not accepted as a substitute. If existing host-key provenance is insufficient, Linux must bind a fresh fingerprint to an authenticated control-plane instance receipt before this milestone starts.

There is one SSH tunnel per worker, plus bounded short control connections for lease renewal and terminal collection. No SSH daemon setting changes are required. Agent requests carry the run ID, a unique request ID and a per-host 256-bit token through the authenticated tunnel. Token files use mode 0600 inside mode-0700 control directories; archives contain the token hash rather than the token. Requests that reference another attempt, recipient, serving generation, snapshot or map are rejected.

The host supervisor uses one local agent argv command per worker. Rust readers and builders inherit the agent's verified process group and CPU mask; they do not detach. The driver renews worker-clock leases every five seconds, with a 30-second TTL and a ten-minute absolute case deadline. A driver failure, expired lease, resource failure or peer failure stops all owned agent/Rust groups. The driver accepts completion only after both host terminal reports reconcile every recorded PID/start-tick/PGID. Unreachable or unreconciled hosts block publication and automatic retry. The existing fleet hard-stop policy remains owned by Linux.

## Transfer and destination readiness

1. **Seal the source.** The source agent inventories every regular checkpoint file, verifies all five persisted block references and the snapshot-bound live-version audit, and confirms that the complete inventory matches the current immutable source seal. It rejects links and changing files.
2. **Export a bounded private archive.** The source agent streams a regular-file-only archive through its SSH tunnel. The driver spools it inside the declared attempt scope, enforcing its byte/object limits and recording SHA-256. The source inventory is checked again after export.
3. **Receive in a fresh namespace.** The destination agent streams into a never-used incoming directory. It rejects absolute paths, traversal, links, duplicate paths, devices, oversized files and undeclared inventory entries. No remote shell command interprets an archive-supplied path. Failed partial directories are retained and never reused.
4. **Verify and persist the replica.** Destination file hashes and sizes must exactly match the sealed source inventory. The agent fsyncs file contents and the complete directory hierarchy, then records the replica seal. No readiness response precedes durability.
5. **Probe the actual destination.** A fresh Rust reader opens the destination's immutable manifest and ID map. The authority checks the pinned generation, root, source, metric, dimension, complete inventory and audit identities. The tiny fixture reranks every document against independent scalar references. A separately prepared serving generation must answer the same pinned request before cutover.

Both export and receive happen inside the supervised agent. SSH transports bytes; it does not become the untracked owner of a remote writer process. Request body limits, deadlines and local owned-byte guards apply while streaming, not after an archive is materialized.

## Atomic cutover and old readers

The driver keeps the logical tree hash unchanged for a placement-only move. Its new deployment record changes the destination worker binding, immutable serving-generation handle, parent deployment hash and fencing token. Every changed and unchanged owner receives a fresh readiness probe. One driver-side durable CAS publishes the complete record. A transaction prepared against the previous placement loses the fence even if its tree hash is unchanged.

Serving generations are prepared and query-ready before the CAS. The pointer selects which generation new readers use; there is no required multi-worker activation step after publication that could produce a partial cutover. Old readers retain their original agent/generation handles and source files. The authority's fixture pin registry prevents retirement while a reader is open. Intermediate generations can retire only after all their fixture pins close; the initial reader remains pinned through the entire scenario.

Physical worker paths are not the driver's metadata filesystem. A new opt-in remote ownership resolver binds worker replica handles to a driver-local immutable catalog of snapshot and map bytes. Catalog descriptors must match the logical tree's original hash/byte pins. Semantic tree validation and full global-ID coverage use this catalog; actual remote block readiness comes from authenticated destination probes and sealed complete-file inventories. The local resolver remains unchanged by default.

## Cross-boundary operations

The authority prepares a split, merge and explicit reassignment against the exact current deployment. Explicit ID maps remain authoritative ownership; centroids guide search. It verifies complete before/after live-ID equality and reads affected FP32 values from the pinned canonical corpus. A remote builder adapter sends only those bounded vector/map artifacts to the selected workers, where the supervised agent builds fresh real lower trees with the existing construction settings.

The authority mirrors each resulting immutable manifest and map into its local catalog, checks the full snapshot audit, and validates the complete new routing model. Every changed owner and routing boundary enters the same deployment record and CAS. Unchanged snapshots retain their bytes and lower generation. A cosine cancellation rejects before publication. No operation changes the coordinator-wide beam or final candidate budget.

The acceptance sequence builds three logical subtrees across the two hosts, moves one whole subtree from worker 0 to worker 1, splits a subtree, merges two eligible subtrees, and reassigns explicit IDs between owners on different hosts. Each step checks full global ownership, exact scalar-reference results, direct-reference versus remote logical traces for the same new generation, advertised destination readiness, unchanged snapshots and the still-pinned initial reader. The test records physical instance IDs separately from logical worker names.

## Concrete bounds

| Resource | Admission bound |
| --- | --- |
| Hosts | Two existing released sandbox workers plus the existing control driver. |
| Corpus | At most 128 documents, dimension at most 16, at most eight logical subtrees. The first case uses 48 two-dimensional documents. |
| CPU | Explicit `0-1` logical CPU mask per worker agent/supervisor; one balancing thread and two Rust runtime threads per builder. The driver controller uses a separate explicit two-CPU allocation. |
| RSS | One GiB aggregate owned-process RSS per worker, with a 128 MiB stop margin. |
| Owned files | 512 MiB logical bytes per worker and driver attempt, with a 64 MiB stop margin. Logical/allocated/sparse counters remain separate. |
| Free disk | Preserve a 512 GiB floor plus a 64 GiB stop margin; unrelated existing benchmark data is excluded from owned-byte counts but remains reflected in free space. |
| Transfer | At most 32 MiB and 512 regular files per checkpoint archive, one transfer per host at a time. |
| Control/query | At most 256 KiB metadata response, 4 MiB query body and 64 MiB query response; fewer than eight concurrent control requests per agent. |
| Time | Ten-minute absolute case deadline, 30-second lease TTL, five-second renewals, five-second control connection timeout and 30-second individual build/probe timeout. |
| Evidence | At most 64 MiB compact evidence plus the tiny declared fixture stores. Preserve every failed attempt and raw identity/terminal journal. No benchmark archive expansion. |

These bounds require fresh Linux admission against actual process counts, free space and owned paths before execution. They are not estimates for production migration capacity.

## Failure cases and completion gate

The fixture injects a truncated transfer, an incorrect inventory digest, a missing lazy-vector block and an unavailable destination serving generation. Each must leave the current deployment pointer unchanged and the initial reader usable. A separate-process placement proposal races an already prepared cross-boundary proposal against the same parent; exactly one may publish.

A separate failure subcase expires one worker's lease while private preparation is incomplete. Its peer must stop through the declared abort/lease mechanism. Killing an agent also closes its old readers, so this fault case checks pointer preservation and terminal cleanup rather than continued query availability on the killed host. The authority must not publish, every recorded child group must reconcile, and the case must retain its partial directories and journals. Retrying uses a new attempt ID and fresh directories.

The successful case ends only after all exact/trace/ownership assertions pass, old-reader checks complete, agents close their serving generations normally, both supervisors report terminal success, and recorded process groups are absent. The evidence includes source and binary hashes, exact commands/configuration, actual host identities, transfer inventories, deployment ancestry, readiness receipts, query signatures, resource samples and terminal reconciliation. Checkpoint files remain preserved; this milestone adds no garbage collection.

## Proposed source ownership

The implementation owner adds `dynamic_agent.py` for bounded supervised worker operations, `dynamic_remote.py` for authenticated SSH-tunnel transport and catalog resolution, and `dynamic_twohost_smoke.py` for the fixture controller, with focused unit tests. The only anticipated shared interfaces are an opt-in metadata resolver in dynamic deployment validation and an injected lower-builder adapter in dynamic topology preparation. Existing local defaults, static manifests, benchmark harnesses and immutable binaries remain unchanged. No Rust changes are currently required.

Implementation begins after Astra 03 feedback is resolved. Remote execution additionally requires Linux's explicit worker release, exact host-key provenance, complete configuration admission and parent coordination. Existing user sandbox authorization covers this correctness work; the remaining gates are technical and scheduling gates rather than a request for new user permission.

## Concrete transfer and authority limits

The transfer format is an uncompressed regular-file archive. Admission checks a 32 MiB wire cap, 64 MiB expanded-total cap, 16 MiB per-file cap, and 512-member cap before extraction. Every member must match the sealed inventory, and links, duplicate names, sparse members, traversal paths, unexpected files and digest mismatches fail closed. The driver retains complete tiny checkpoint mirrors so the existing content and provenance verifier runs without a metadata-only bypass. This duplication is a bounded correctness-fixture mechanism and does not establish a scalable production storage design.

The driver must run under an aggregate owned-file, RSS, deadline and process-group guard in addition to both worker guards. Each worker admits its actual instance identity and immutable writer/producer hashes before serving. SSH admission binds authenticated control-plane host-key receipts and the exact driver identity; key scanning alone is insufficient. Generation-specific endpoints retain their persistent Rust readers until all reader pins are released.

## Current campaign admission

The user holds 250M and larger-prefix progression. This milestone only builds its tiny isolated correctness fixture; it does not download corpus data or launch a larger preparation or benchmark job. Existing authorized 100M work continues independently with its fixed limits.
