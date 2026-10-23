# Remote saved-object copying: improve the design before its relay

Central copying is a local-filesystem fixture requirement, not a necessary sharding operation. The target design explicitly says owners write durable payloads directly and the coordinator need not relay them. Prefer owner-published durable references and a reader/storage adapter that resolves those references independently. Do not build a larger parallel relay solely to reduce a benchmark cost that the intended architecture removes. This assessment changes no implementation, runs no Cargo or cloud operation, and leaves active candidate28 untouched.

## What the measurements establish

Candidate27 saves in 23.999996 seconds. Independent owner calls sum to 19.790347 seconds but overlap into 9.981234 seconds elapsed. The serial remote-copy phase takes 12.666621 seconds for 191,244,394 logical payload bytes and 1,064 objects. Local immutable reuse takes 0.261411 seconds. The final manifest and publication take 0.534505 and 0.520150 seconds respectively.

The coordinator records 5.217494 seconds of file synchronization and 2.814760 seconds of directory synchronization, across 1,068 calls of each kind. Their 8.032254-second sum is nested work for the coordinator's complete save, including final manifest/publication operations. It is not an extra phase to add to 12.666621 seconds, and not every synchronization belongs to remote copying. The profile gives no separate exclusive network, server-read, base64, checksum, local-write or synchronization breakdown for copying.

The logical average is 179,741 bytes per copied object and the phase processes about 15.10 logical MB/s. Neither figure measures network bandwidth or disk throughput. A low aggregate rate does not prove a network bottleneck. The source performs a complete remote read, local immutable write, file sync and directory sync for each object before starting the next; that serialization can prevent overlap among independent costs.

## Why the coordinator currently copies everything

The coordinator's save loop calls `remote_bytes` and `copy_saved_object` serially for every remote owner object. It then reads the owner manifest and installs it in the coordinator's namespace. The immutable writer creates a private temporary file, writes and syncs it, installs its final name without overwriting, then syncs the containing directory. Its accounting mutex merges per-operation counts after the operation; it does not hold a store-wide lock across I/O.

The current store resolves every relative object path beneath one local filesystem root. Generation validation and the saved reader call that store for owner manifests, routing and postings. Consequently a remote manifest alone is insufficient: its payloads must exist in the reader's selected root. The external S3 transfer helper exports this assembled local graph and restores it to another local namespace; it does not let the reader open owner-published objects in place. The inspected object references do not directly carry an S3 version locator.

The missing capability is a shared durable object backend, with independently resolvable owner references and verified manifest publication. Owners can publish their own immutable payloads into one configured durable namespace, then return small manifests. The coordinator verifies compatible coverage/format/integrity and publishes the combined manifest last; readers fetch selected durable objects without the original owners or a coordinator holding all payloads. Relative logical object paths can remain unchanged if the backend root supplies the durable namespace and enforces immutable names. Backend-qualified or version-pinned locators may instead require a descriptor/resolver extension; this must be decided and tested explicitly, not simulated by remote writer RPC references.

Removing the relay alone does not establish efficient small updates. The current save snapshots every resident owner node and posting, and names payloads from the save attempt, so unchanged postings are materialized again for a new attempt. The target should follow the reference's changed-block flushing and reuse prior immutable references. Neither assembling the complete index centrally nor rewriting every owner's unchanged data should become an assumed production requirement.

## Conditional short-term change, if the POSIX fixture must remain

A bounded two-object pipeline is a small safe experiment within the current adapter, but it remains a fixture optimization. Reuse the scoped, joined-batch pattern already used for owner saves. After all owners are sealed and materialized, schedule at most two distinct immutable object references at once; each task performs the existing checked chunk reassembly and complete durable `put_object`. Keep owner manifests, shared routing, combined generation and latest publication ordered after the required object barrier.

Bound memory as well as task count. `remote_bytes` reserves the whole declared object size despite fetching 1MiB chunks, so two tasks can hold two whole objects plus chunk/base64 buffers. Admit pairs under a conservative byte budget derived from retained object sizes; an object larger than that budget should use the existing single-object path, rather than silently increasing peak memory. Do not infer a maximum object size from the 180KB average. This adds a bounded scheduling choice, not streaming formats, more storage or a new transfer protocol.

Preserve these invariants:

- Close and seal every owner before copying. Bind all work to the frozen attempt, manifest and object references.
- Give each object one active writer. Deduplicate equal paths/references; reject conflicting identities rather than racing them.
- Preserve chunk identity, offset, completion, progress and length checks, whole-object checksum equality, immutable replay/conflict checks, file sync and directory sync. Do not replace per-object durability with assumed storage behavior.
- Join every started task on error or panic. Leave immutable evidence; do not publish a partial owner/generation or return while an owned writer remains active.
- Install a manifest only after all its required objects verify. Publish the generation/latest descriptor only after every owner and shared object passes the existing complete-generation checks. A failed save preserves the previous generation and its fresh-reader validity.
- Preserve deterministic manifest/reference order. Sum of concurrent job or sync durations is work, while the joined phase duration is elapsed time; report them separately.

The smallest mechanism fixture uses actual local/TCP saved objects and deterministic gates to prove two reads/writes overlap, one blocked object prevents manifest/publication, and one failure still joins its sibling. Compare exact immutable refs, bytes, sync counts, same-attempt replay/conflict outcomes and fresh-reader memberships/codes with the serial path. No timing threshold is necessary. A matched save measurement is justified only after the parent decides this fixture optimization remains useful.

## Recommendation after candidate28

First decide the durable-save seam required by the intended architecture. Owner-direct durable publication is the preferred path because it removes the bulk coordinator relay instead of accelerating it. Reuse existing immutable-object and manifest contracts, and test a fresh reader without original writer disks or processes. This is selected-backend correctness work, not fleet hardening or recovery work.

If that backend change is deliberately deferred and another research question needs the present POSIX save path, the two-object pipeline is the narrow fallback. Current timing demonstrates consequential serialized work and measured sync costs, which justify testing overlap; it does not establish a speedup. Avoid object bundling or fewer directory syncs now: bundling changes posting lookup/format and lazy reads, and sync removal changes durability. The sole replayed coordinator payload in this run is consistent with the shared vector descriptor; eliminating that tiny duplicate is not a credible explanation for 12.7 seconds. Broader duplicate-copy avoidance belongs to durable reference reuse, not another coordinator cache.

## Evidence boundary

The measurement is [candidate27's raw result](../operations/window-epoch27/3m-distributed/result.json), interpreted in [the accepted result](METADATA-DICTIONARY-3M-EPOCH27-RESULT.md). The target contract is [saving and reopening a completed generation](../../DESIGN.md#saving-and-reopening-a-completed-generation).

Inspected implementation files are under `/private/tmp/chroma-hierarchical-spann-sharding/single-shard/rust/index/benches/hierarchical_index/distributed/`: `save_runtime.rs` SHA256 `edb85623b069871f26ffc44242cb2b46fcdef864ffea2a83382befda363e773a`, `generation.rs` SHA256 `4062f5645646c9f27fee021bda9c01d76b1140dce4ba584ee607473a8d2ae5ea`, and `saved_reader.rs` SHA256 `4c88c09530bd0b524ce32c9943d3b8f7939d5a32c743b1e0be9afbae11e7eac2`. These hashes identify the observed local save code; no active source or frozen archive is edited.
