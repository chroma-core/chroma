# Preparing the 1M and 100M real prefixes

The next progression is a FineWeb 100k index gate, then 1M integration, then 100M when correctness and capacity fit. The following numbers project bytes from the measured first 100k export. They are not runtime, price, build-speedup, or index-footprint forecasts.

## Measured preparation boundaries

The 100k export writes 307,200,000 vector bytes, a 15,977,780-byte row-to-UUID JSONL sidecar, and a 12,660,736-byte SQLite UUID index. It fetches 295,997,376 distinct source payload bytes in 39 range requests. The first source file contains 1,090,356 rows and occupies 5,039,721,972 bytes in full. The export reads more projected row-group data than the exact requested prefix needs; subsequent rows in that file can amortize that overhead. The ratio from this prefix is therefore a planning bound rather than a corpus-wide compression measurement.

| Artifact or boundary | Measured 100k | Byte projection at 1M | Byte projection at 100M |
| --- | ---: | ---: | ---: |
| Dense float32 corpus | 0.3072 GB | 3.072 GB | 307.2 GB |
| Owner vector inputs, before indexing | 0.3072 GB, arithmetic | 3.072 GB | 307.2 GB |
| Explicit unsigned-64-bit ownership maps | 0.0008 GB, arithmetic | 0.008 GB | 0.8 GB |
| UUID sidecar plus lookup index | 0.02864 GB | 0.2864 GB | 28.64 GB |
| Retained prepared corpus, owner inputs, maps, UUID files | 0.64384 GB, arithmetic | 6.4384 GB | 643.84 GB |
| Projected source payload if retained as range cache | 0.2960 GB | 2.9600 GB | 296.00 GB |

GB means decimal billion bytes. Integer/string widths, source compression, row groups, SQLite index growth, and duplicate ranges can change the non-vector ratios. The vector and unsigned-ID-map terms are exact arithmetic for the chosen precision. Index blocks, temporary balancing output, replicas, old generations, filesystem overhead, query data, and exact references are additional. The measured process writes more bytes than the final files because SQLite journals and intermediate writes exist; final artifact size is not physical write traffic.

The driver has a 100 GiB root volume and cannot retain a 100M dense corpus, much less both dense and owner copies. Worker NVMe provides the preparation capacity. Two approximately 3.5 TiB worker devices can accommodate these raw prepared-byte terms, but their measured index and transient peaks must establish headroom. Four/eight-worker serving comparisons also need enough fixed ownership slots and the fair utilization policy described in the benchmark plan.

## Streaming and distributed preparation path

1. **Freeze the source prefix.** Pin the manifest and read source file footers to establish each required file's row count. Record the exact final row boundary, projected-column schema, object identity, and source ordering before starting ingestion. Raise per-run byte/time limits deliberately from the measured 1M profile rather than disabling them.
2. **Write bounded contiguous chunks on worker NVMe.** Stream only the required embedding and UUID columns. Preserve float16 values in float32 and keep integer global IDs tied to source row order. Each completed chunk records its source range, count, vector hash, UUID hash, and a completion marker. Keep the bounded range reader and account for decompressed Arrow buffers. A long-run resume protocol must first verify completed chunk hashes and exact next source position; the current exporter safely rejects partial output rather than resuming it.
3. **Sample across the complete prefix.** The initial 100M root must use a representative seeded sample across that prefix, rather than silently reusing a root sampled only from its first 100k rows. A mergeable deterministic priority sample keyed by seed and global ID permits distributed chunks to contribute uniformly. Preserve sampled row IDs, sample-vector hashes, model settings, and the final model hash.
4. **Route from verified local chunks.** Freeze the model, route every row with those same centroids, and write per-owner fragments carrying explicit global IDs. Merge fragments in canonical source-range order before the lower writer assigns local row positions. This preserves repeatable maps even when preparation jobs finish in another order. Do not change ownership to hide skew during placement scaling.
5. **Publish preparation metadata after verification.** Check chunk ranges for complete source coverage, UUID uniqueness, vector/map counts, per-owner hashes, and global ownership. Preserve the dense prepared chunks, owner inputs, maps, UUID mapping, and provenance needed by exact scoring and reproducibility. Measure disk use through the handoff to indexing.
6. **Purge only this run's reconstructible source cache.** After output hashes and provenance are safely retained, the run may remove only the partial range-cache paths it creates and lists explicitly. Those source ranges can be fetched again from pinned objects. Do not delete prepared vectors, maps, checkpoints, shared caches, another run's files, or the active benchmark's data. A sparse cache's apparent file size is not its allocated physical bytes.

The 1M exporter run measures whether range overhead, conversion, and SQLite UUID indexing remain practical. The 100k single-exporter runtime is not linearly extrapolated into a 100M completion promise. If the UUID index or decoder is the bottleneck, distribute chunk decoding while keeping a complete disk-backed uniqueness check and canonical merge. Index construction begins only after the shared model and preparation coverage are committed.

## Reference and capacity gate

Freeze calibration/test IDs before tuning and regenerate queries with the pinned tested runtime. The 100k and 1M exact references score their complete respective prefixes with one-minus-inner-product and stable integer IDs. The publisher's full-corpus answers remain unsuitable for these prefixes. Preserve query, corpus, UUID mapping, and reference hashes together.

At 100M and 200 pilot queries, the reference contains 20 billion vector comparisons. At 100M and the publishable 11,000-query calibration/test pool, it contains 1.1 trillion comparisons. Measure a moderate run and budget a suitable reference producer before the larger query pool. Exact references and preparation must use separate compute or an idle timing slot. Build and serving measurements also require a per-host saturation sweep; the current lightly threaded task graph is a mechanics/timing point, not capacity evidence.
