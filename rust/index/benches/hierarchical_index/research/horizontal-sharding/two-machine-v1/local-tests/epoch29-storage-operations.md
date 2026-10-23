# Candidate29 storage execution notes

Owners save their immutable payloads into one shared object namespace, and the coordinator publishes the combined generation after owner acknowledgements. The coordinator reads routing metadata and strong posting references; it does not relay posting payloads. A separate reader resolves those references through the same backend. Unchanged posting chunks retain their immutable references across completed generations. Owners snapshot and serialize changed chunks only, while rebuilding compact routing/reference manifests. Writer checkpoints remain separate work.

## Configuration and command flags

The writer and reader accept `--storage-config /path/to/storage-config.json`. The JSON contains exactly `bucket` and `prefix`. Use the retained sandbox bucket and a fresh task namespace beneath `sha256/`. Credentials and region come from the existing AWS configuration; this file contains no credentials. Validate this file before construction because the writer currently parses it when saving. The owner service receives the configuration in its save request; the peer does not need a separate configuration file for that request. The coordinator passes the configuration file to its independent reader child.

```json
{"bucket":"spann-artifacts-079819354890-20261014-ecfcca4d","prefix":"sha256/epoch29-UNIQUE-ATTEMPT"}
```

The saved-reader command accepts `--store /path/to/export --storage-config /path/to/storage-config.json --expected-rows N --output /path/to/new-report.json`. With durable storage, `--store` is an export path, and the default generation comes from the backend's latest descriptor. `--descriptor` can select a retained descriptor explicitly. `--vector-path` can point to independently located identical corpus bytes. The actual writer flag is `--storage-config`, not `--durable-store-config`.

## Explicit actual-adapter smoke

Compile the integration-test target on Linux from the same reviewed source as the reader and writer. Normal test runs ignore the cloud smoke. Only the operator should execute it with the following task-specific environment variable and an external deadline of at most 180 seconds.

```sh
SPANN_DURABLE_SMOKE_CONFIG='{"bucket":"spann-artifacts-079819354890-20261014-ecfcca4d","prefix":"sha256/epoch29-UNIQUE-ATTEMPT"}' timeout 180s /path/to/hierarchical_distributed --exact hierarchical_index::distributed::generation_backend::tests::durable_s3_adapter_smoke --ignored --nocapture --test-threads=1
```

The smoke appends a fresh `adapter-smoke-<uuid>` namespace and prints it before I/O. It checks small and 8MiB+17-byte immutable create/replay/conflicting-write behavior, exact read-back, initial latest publication, conditional replacement, stale replacement rejection, one winner among competing publishers, and old-generation membership through a newly opened store. The retained S3 default multipart threshold is 5MiB. The test retains its objects and uses no writer corpus. It is compiled locally but has not been executed against S3 by the implementation agent.

The smoke uses the actual Rust storage adapter. A separate AWS CLI probe alone does not prove that this adapter handles multipart completion errors correctly. The adapter uses the existing stream upload path, which attempts to abort failed multipart uploads. Failed conditional immutable puts resolve successfully only when a full read-back exactly equals the intended bytes. This covers generic multipart condition errors and lost successful responses without overwriting a conflicting object. A failed abort can still leave an unfinished upload; the adapter does not promise cleanup despite inaccessible storage.

## Interpretation and timing

Passing the adapter smoke permits investigation of an actual two-owner durable save, subject to the parent's review and operational admission. It is not a construction benchmark or a physical two-owner durability result. The in-memory conditional model validates local protocol behavior; only the guarded Linux run can validate the real selected backend and current access.

Saving includes repeated routing metadata reads, a complete in-memory membership uniqueness scan, and owner-side verification of newly materialized posting chunks. Reused chunks skip serialization, upload and owner-side read-back, but the fresh reader still validates every payload. Removing central posting copying does not guarantee faster saving against S3; compare owner materialization, coordinator metadata validation and publication separately. Failed or uncertain storage operations fail the save unless exact immutable read-back or exact descriptor equality resolves the response. External process and machine shutdown guards remain necessary; synchronous adapter calls do not impose a whole-save deadline.

The high-level save captures the previous latest descriptor before owner materialization, freezes that predecessor for the live coordinator save attempt, and conditionally replaces it after all owners succeed. Replaying an older completed attempt cannot roll the latest generation backward. Use a fresh namespace for each campaign; subsequent generations within that campaign share the namespace. The optional writer flag `--save-unchanged-generation` saves the same sealed state a second time, reports its profile under `repeat_save`, and validates both first and repeat generations in separate reader processes. Its time is excluded from the first `construction_and_save_seconds` comparison. The flag requires `--storage-config`. This change does not make a saved reader generation resumable as a writer; the 2M construction checkpoint preference remains a separate requirement.


## Cache invalidation and work accounting

An owner retains one cache of posting references for the same export root, durable namespace and format. Each entry pins the immutable routing image and keeps a weak reference to its resident record. Reuse requires both identities to match the current sealed snapshot. The cache does not retain posting bytes or add per-vector membership state. It becomes current only after the owner manifest succeeds; a later combined publication failure leaves both old generations and those newly durable chunks valid.

Every production posting-content mutation changes the routing image under the same node lock: append and pair transfers advance its revision through copy-on-write; local bulk transfers advance source/destination revisions; batched owner transfers install newly prepared routing images after editing postings. Reserving vector capacity does not change payload contents. Splits, merges and ownership installation create replacement resident records or routing images. Parent-only routing changes conservatively invalidate the posting cache. The pinned old routing image forces copy-on-write to create a distinct allocation even when no navigation reader is active. Replaced resident allocations cannot match a retired cache entry even with equal routing revisions or an identical routing Arc. Removing a leaf omits it from the next cache and manifest.

Newly materialized posting objects use content-derived names containing the full serialized leaf ID and vector IDs. A conservative invalidation that produces identical bytes retains the same immutable reference after exact replay verification. Such invalidations can still serialize and attempt an upload; the optimization promises to skip unaffected chunks, not to infer that every metadata-only change leaves posting bytes equal. Ownership moves conservatively materialize on the receiving owner because its resident identity is new. Existing typed blockfile records use narrower IDs, so this change reuses the existing full-width immutable leaf chunks instead of migrating key/value types.

The owner profile reports `posting_blocks_reused`, `posting_blocks_materialized` and `posting_vectors_scanned`. The last value intentionally exposes the full membership validation scan. Metadata manifests are regenerated per owner and generation. Total save CPU/memory is not proportional to changed blocks: the membership scan uses transient duplicate-detection state, and routing/reference metadata is rebuilt. The changed posting bytes copied, serialized and uploaded are proportional to the affected cached chunks, subject to conservative invalidation.
