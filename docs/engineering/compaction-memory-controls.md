# Compaction memory controls

Compaction builds a new index while the existing index remains readable. A worker can reduce memory retained during that work by copying numeric values out of consumed input blocks and uploading completed output blocks before the entire index is built. These controls act independently and default to disabled so a dedicated worker can compare each change against the same binary's baseline behavior.

The input reader copies the unsigned integer keys and floating point values used by sparse compaction. It reads one source block at a time and releases its local reference after copying the values. It does not insert these blocks into the reader's map of borrowed values or evict blocks that another caller still references. The shared block cache retains its existing policy, and the values for the current sparse dimension remain in memory while that dimension is processed (`BlockfileReader::get_prefix_owned`).

The ordered output writer uploads a batch when the number of completed blocks reaches its configured upload concurrency. Each upload owns its block and drops that reference when the upload finishes. The writer keeps block identifiers and entry counts for final index metadata instead of retaining uploaded blocks. The final flush publishes the root, the file that describes the completed index, only after all required block uploads succeed. A failed or cancelled upload prevents the writer and its clones from committing. An interrupted attempt can leave unpublished output objects in storage, as an interrupted final flush can (`ArrowOrderedBlockfileWriter`).

## Configure the dedicated worker

Set either or both fields in the worker's existing block-manager configuration. These fields do not change the index format or require a migration. The output control applies to all ordered blockfile writers created by that provider; the input control applies to numeric prefix reads used by sparse compaction.

```yaml
compaction_service:
  blockfile_provider:
    arrow:
      block_manager_config:
        use_owned_prefix_reads: true
        flush_completed_blocks: true
        num_concurrent_block_flushes: 40
```

The equivalent environment overrides for the manual worker are:

```text
CHROMA_COMPACTION_SERVICE__BLOCKFILE_PROVIDER__ARROW__BLOCK_MANAGER_CONFIG__USE_OWNED_PREFIX_READS=true
CHROMA_COMPACTION_SERVICE__BLOCKFILE_PROVIDER__ARROW__BLOCK_MANAGER_CONFIG__FLUSH_COMPLETED_BLOCKS=true
```

The batch limit bounds retained completed output blocks, rather than total job memory. Input values for one dimension, the current mutable output block, cache entries, other index structures, and temporary serialization buffers can still occupy memory. Sequential batch uploads also apply backpressure to index construction and can change completion time.

## Compare attempts

Use one dedicated worker with one active collection, the production cache mount, and the collection disabled on ordinary workers. Keep its image, cache configuration, job concurrency, and profiling settings fixed. Compare baseline, input-only, output-only, and both controls when comparable input is available. A successful live attempt advances the collection, so subsequent attempts are not equivalent replays unless their input is separately preserved.

Compare peak container memory, growth within sparse-index processing, sampled live allocations originating in input-block loads and completed-output buffers, completion time, and correctness of the published index. Successful publication and stable allocation counts matter; an out-of-memory termination is not required to evaluate the controls. Preserve profiles before, during, and after each relevant stage. The controls do not add stage instrumentation; use the existing diagnostic instrumentation when its build is available.

The incident evidence and operational experiment record are centralized in [Compactor OOMs and backlog — October 12–13, 2026](https://app.notion.com/p/3f858a6d819181baa0f7d4423de75ca5).
