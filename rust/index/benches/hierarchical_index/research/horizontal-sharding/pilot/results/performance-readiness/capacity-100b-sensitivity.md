This table is an arithmetic sensitivity analysis, not a measured 100B-row result. The retained 10M-row epoch uses 128 owners and 768-dimensional vectors. Its complete owned checkpoints occupy 4445.705 logical bytes per row in aggregate, including private checkpoint objects; reachable objects occupy 3361.602 bytes per row. Larger owners may have different checkpoint growth and memory behavior.

| Assumed rows per owner | Owners for 100B rows | Flat-root float32 centroid payload | One owner's full checkpoint, linear model | Aggregate explicit u64 ID maps |
|---|---:|---:|---:|---:|
| 1,000,000 | 100,000 | 307.20 MB | 4.446 GB | 800 GB |
| 10,000,000 | 10,000 | 30.72 MB | 44.457 GB | 800 GB |

Sharing immutable objects avoids repeated copies after bootstrap; it preserves the need to store new objects and retained generations. Resource admission bounds a run rather than reducing its footprint. Sharing each query once per adapter batch reduces serialized request duplication for the measured fixture; it does not establish the same reduction at 100B rows. Bounded selection reduces sorting work while preserving candidate scoring. Lazy routing remains a proposed measurement, with no claimed memory or quality improvement yet.

The root values count centroid payload only. They exclude child references, decoded objects, JSON and allocator overhead. The owner checkpoint values extrapolate the measured 10M layout without claiming linear growth. The map values assume eight explicit bytes per row. No row in this table implies a latency, throughput, recall or machine-count guarantee.
