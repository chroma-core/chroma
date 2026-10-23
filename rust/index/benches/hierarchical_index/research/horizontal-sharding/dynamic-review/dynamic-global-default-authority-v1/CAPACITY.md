# Capacity inputs before a matched run

The new limits remove test-sized API barriers; they do not prove that two million vectors fit a resource budget. The implementation still uses one central routing and location directory, a shared local storage/cache provider, complete owner bootstrap and saves, and serialized owner commands. A matched run must measure those costs and its actual duration.

## Explicit default limits

| Item | Admission limit | Meaning |
|---|---:|---|
| Total vector ID prefix | 2,000,000 | Configurable up to the admitted u32 namespace; not a 100B global ID scheme. |
| Stored routing nodes | 100,000 | Configurable up to 1,000,000; checked after saved metadata reads. |
| Available owner actors | 128 | Caller selects the actual pool; actors are local threads, not machines. |
| Physical rows in one leaf | 262,144 | Allows deferred balancing but bounds the affected-leaf interface; not a decoder allocation bound. |
| Physical rows in one owner | 4,000,000 | Counts stale as well as current rows; this is per owner, not an aggregate process-memory guarantee. |
| Storage batch | 8,192 rows | Several batches can register before one balance cut. |
| Dimension | 4,096 | The actual cohort's dimension remains a separate input. |
| Retained trace | 100,000 events | Trace is off by default. Overflow invalidates the operation/profile; this is not a byte or preallocation bound. |
| Cached vector readers per actor | 16 roots | Does not bound the shared provider cache or decoded buffers. |
| Navigation depth | 128 | Structural balance depth and round limits remain the reference's four and 100. |

The local structural test uses a smaller explicit limit set: 4,096 vector IDs and nodes, four actors, 4,096 physical rows per leaf, 8,192 physical rows per owner, 128 rows per storage write, and a 100,000-event trace. It exceeds the former 128-vector restriction and uses no full-state preparation copies. It does not exercise the two-million-row limit.

## Conditional byte estimates

At 768 dimensions, the existing one-bit code has a 16-byte header plus 96 padded packed bytes, or 112 code bytes. Adding a u32 posting ID and a byte version gives 117 logical payload bytes per physical posting row. This arithmetic follows the current concrete code format; it excludes Arrow, allocator, container capacity, map entries, root indexes, cache buffers, and saved-generation retention.

| Assumption | Logical payload |
|---|---:|
| 2M current posting rows, no stale copies | 234 MB |
| 4M physical posting rows in total | 468 MB |
| Raw 2M × 768 FP32 vectors in saved storage | 6.144 GB |
| One 8,192-row FP32 input batch | 25.17 MB |
| A 100,000-row affected FP32 leaf | 307.2 MB |
| Maximum admitted 262,144-row affected FP32 leaf | 805.3 MB |
| 2,000 resident centroid payloads | 6.144 MB |
| Maximum 100,000 resident centroid payloads | 307.2 MB |

The per-owner row ceiling is deliberately not a whole-process ceiling. Taking all 128 owners to the default 4M physical-row maximum allows 512M physical rows, or 59.904 GB of posting payload alone. That is not a recommended configuration or a measured requirement. A real two-million-vector run must set per-owner physical limits and provider/cache budgets for its actual available resources and observe aggregate highwater; this prototype provides no RSS guarantee.

Current-version metadata has a logical minimum of 29 bytes per ID when counting the ID, version, leaf ID, UUID, and vector key fields, or 58 MB at 2M IDs. The central ID-location directory adds at least 32 MB of logical fields on a 64-bit owner-index representation. Rust padding, BTree node allocation, spare capacity, duplicate transient values, and cache objects add to both figures. These are not measured resident sizes.

## Material algorithmic changes and remaining work

A direct batch does not allocate whole-owner or coordinator rollback copies. It writes one vector root per storage chunk; a 100,000-row reference balance interval can use 13 chunks at the default 8,192-row chunk bound without forcing 13 balance cuts. This reduces root-creation count from per-row to per-storage-batch. It does not establish encoded-byte savings, because the copy-on-write provider's root and block writes still need measurement.

Placement examines the selected posting's IDs and performs directory point updates, rather than sweeping all global IDs on every leaf move. Selecting a candidate transfer still scans the overloaded owner's branch rows. Owner-count requests and the available actor pool are explicit costs. An indivisible branch or insufficient destination capacity can leave the soft placement threshold unsatisfied; the implementation reports it and preserves tree semantics rather than inventing a split.

Outer balancing still performs a leaf-metadata scan, and saving still writes all current owner state. Clustering reads an affected leaf's full vectors, which can dominate memory at a large deferred balance interval. Root and block decoding and provider retention remain outside the owned-payload estimates. The 100-round reference stop can leave work pending; retained round-limit events must accompany any build report. The local test's small elapsed time is not a projection of a real build.

The future matched experiment must use the same roughly 2M corpus prefix, metric, query set, ground truth, recall target, search work and rerank/cache policy for both treatments. It must choose common balance intervals, record actual default parameters and random-build provenance, measure build duration, and retain saved census plus ownership/quality diagnostics. No data download, cloud launch, Linux run, or controller execution occurs here.
