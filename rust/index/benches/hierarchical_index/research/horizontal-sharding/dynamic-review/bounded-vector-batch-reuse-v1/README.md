# Reuse decoded blocks within a bounded fetch

The vector module keeps one reader for consecutive handles from the same immutable root inside an existing fetch of at most 16 handles. It drops that reader when the root changes or the fetch ends. Requests keep their original order. The owner adapter still resolves immutable handles under live authority and releases its ID and actor guards before storage I/O. Current-version checks, transfer, registration, navigation, and reference scheduling are unchanged.

New append roots explicitly use the existing 16 KiB block-write policy, which their immutable forks retain. The fixed FP vector format and admitted dimensions keep ordinary single rows below the writer's half-block exception. Reader admission additionally checks the actual padded decoded-block accounting after every get against 256 KiB. Four fetch contexts therefore admit at most 1 MiB of these accounted pins. These are admission limits, not maximum allocations: a corrupt or oversized block can decode transiently before rejection. Root indexes, encoded backing buffers, provider caches, allocator overhead, cache-owned vectors, and caller arrays remain separate. The public reader API exposes no root-index byte size, so that metric is null and explicitly unavailable.

The failure test deliberately sets a 1 KiB admission budget. It observes 9,664 decoded bytes before rejecting the request, drops its reader, retains no result or vector-cache entry, and marks provider traffic incomplete. This adverse transient is preserved. The implementation does not claim a hard pre-decode allocation bound or total RSS bound.

## Fixed-request result

The fixed test derives handles from actual owner authority, stages three immutable roots, and moves 16 IDs to another owner with a new membership version. Both policies read exactly the same ordered handles and immutable bytes with fresh equal 32-vector owner caches and no provider cache. The reference policy opens a reader per missed vector; the reuse policy keeps a reader only within each consecutive same-root run. A test-only reference switch changes no production query policy.

For the 80-request contiguous tape, reader opens and successful block decodes decrease from 80 to 5, and encoded storage-origin returns decrease from 964,128 to 60,258 bytes. Both policies copy exactly 10,240 vector bytes and observe a maximum of 12,416 accounted pin bytes in a reader. Returned vector bits and order match exactly, including negative zero. The test records actual counters rather than an elapsed-time ratio.

The 16-request alternating-root control shows no benefit: both policies open and decode 16 blocks and return 190,368 encoded bytes. Both copy 2,048 vector bytes and observe 11,008 accounted pin bytes. The implementation does not infer that reuse helps arbitrary access sequences. Encoded data-block returns exclude root-index traffic and are not physical device I/O or network bytes.

The accepted connected construction tests remain in the focused test run. They cover 1,024 vectors, two owners, real movement followed by split/NPA/merge work, exact ordered split-input decision replay, current membership and saved codes/vector bits, old readers, transfer during a paused fetch, lock progress, first-registration retry, and unsupported capacity/integrity failures. Independently stochastic construction outcomes are not compared as a speed or full-tree parity result.

## Scope and remaining work

The source delta is confined to the existing vector module and its focused adapter tests. The module uses the existing writer block-size option, reader, observation counters, and provider. No new transport, storage format, query policy, infrastructure, cloud package, or remote experiment is introduced. All other accepted source pins and the staged index remain unchanged.

The improvement is deliberately limited to one bounded batch. Scalar callers, including the current split-input access path, can still reopen readers between calls. The vector cache, root-index metadata, global routing and ID directories, full joined posting/version publication, u32 identities, and prototype phase limits remain separate constraints. The fixed test completes this access-tuning cycle; it does not authorize another optimization or benchmark.

The old 16K quality and query-cost evidence applies to its original saved trees, original Code4 overlays, and original binaries. This local construction implementation does not rebuild or query those snapshots. Its exact vector/code/current checks establish a local semantics bridge, not measured quality, latency, between-build equivalence, remote ownership, or a 100B throughput forecast. The original 95% quality target remains unmet.
