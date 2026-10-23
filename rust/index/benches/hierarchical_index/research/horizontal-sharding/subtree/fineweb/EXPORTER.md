# FineWeb prefix exporter

The exporter preserves the publisher's float16 dense values exactly when converting them to float32. It does not normalize vectors or generate query embeddings. The pinned source manifest records all 10,148 corpus files in lexicographic order, their sizes, publisher hashes, immutable URLs, and the source metadata hash. The schema comes from the publisher's pinned dataset documentation and is checked again against each parquet file before export: a list of 768 float16 dense values and a string document ID.

The implementation reads only dense embeddings and document IDs through seekable HTTP range requests. Each response must honor the exact requested range and object size; changing ETags, truncated reads, ignored ranges, or oversized parquet reads fail explicitly. Individual reads default to 256 MiB and network ranges to 8 MiB. Arrow can retain decompressed column and row-group buffers in addition to those read buffers, so measure process RSS before selecting a large prefix. Column decoding uses one thread. The exporter never falls back to downloading the whole 5.040 GB first source file when range support fails.

The output contains dense vectors, a row-to-UUID JSONL sidecar, and a disk-backed SQLite UUID-to-row index that rejects duplicate source UUIDs. Original UUID strings are preserved exactly. Global integer IDs follow the pinned file and row order. Source/file positions, fetched-range hashes, norm range, output hashes, settings, library versions, and preparation wall time are recorded. Publisher whole-file hashes remain publisher metadata; range-only reads do not locally verify the whole file. Query provenance remains explicitly absent and belongs to a separate regeneration artifact.

```sh
python3 -m tools.hierarchical_sharding.fineweb_prepare manifest --metadata fineweb-files-api.json --output source-manifest.json
python3 -m tools.hierarchical_sharding.fineweb_prepare export --manifest source-manifest.json --output fineweb-prefix-1000 --count 1000
```

The metadata-only manifest is already saved here. No real corpus bytes are fetched by creating it. Run the small real prefix only after the shared-tree mechanics gate clears and the timed workers are idle. Compare output vectors with an independent decoder and check the norm range before enabling one-minus-inner-product recall measurements. Recompute exact references for every exported prefix; full-corpus answer filtering is insufficient.

Five local tests pass with actual float16 parquet fixtures. They cover exact value preservation, UUID/row order across files and batches, duplicate UUID rejection, schema rejection, projected parquet decoding through the range reader, range limits, ETag changes, and immutable revision/order checks. These fixture tests are not a completed real FineWeb export. Interrupted exports preserve partial files without a completed provenance manifest. Resuming partial output is unsupported; safe retries require a fresh directory, and a large-ingest resume protocol remains a separate gate.

## Completed real 100k validation

The real prefix completes within explicit 512 MiB source-payload and 180-second limits. Internal preparation takes 43.074 seconds; process wall time is 43.30 seconds. Fetched source payload is 295,997,376 bytes, peak resident memory is 736,860 KiB, and user/system CPU time is 28.19/1.90 seconds. These counters belong to the exporter process and verified response-body boundary, not to physical device or application query I/O.

Polars 2.0.0 independently decodes the cached projected parquet ranges and matches every float16-to-float32 value and all 100,000 original UUIDs. The source norms lie between 0.9997556 and 1.0002457. A fresh cache-only retry takes 29.324 seconds including comparison hashes and reproduces all three output hashes without network reads. Reusing an existing output directory fails before mutation. Resume remains unsupported; these checks establish safe rejection and deterministic fresh retry.

The driver retains the complete output and partial range cache on its root EBS. Small provenance, timing, independent-decoder, and retry reports are also preserved in this directory. The partial cache is a sparse local file containing fetched ranges, not a complete source shard and not a locally verified whole-file copy. Query regeneration and exact prefix answers remain separate gates.
