# Building independent shards

The build driver partitions a dense vector corpus into stable logical shards, then runs one compaction worker per host at a time. Different hosts run concurrently. Successful jobs produce shard manifests; the separate collection publisher validates the complete set before making the generation available to the query coordinator.

## Prepare the inputs

Export the chosen dataset revision to little-endian row-major float32 vectors. Record the metric and any normalization before export. The initial corpus has global row IDs starting at zero. To append, export only the new suffix and supply its original first row ID. The preparation command records checksums, dimensions, original IDs, and per-shard row counts.

```sh
python3 -m tools.hierarchical_sharding.build partition \
  --source /data/corpus.f32 --output /data/partitions-1 \
  --dimension 1024 --shard-count 8 \
  --source-id dataset-name-and-revision
```

Preparation requires a new output directory and at least one input row per logical shard. Failed preparation leaves a partial directory without a completed input manifest; use a new directory for retry. The reference partitioner keeps a small buffer but copies the source into separate shard files, so include its read/write time and disk use in source-to-ready build accounting. At billion-vector scale, benchmark this step and replace it with a distributed or vectorized converter if it becomes the bottleneck.

Copy each shard input file to the node that will build it. Verify its checksum there. Keep logical shard count fixed when changing physical placement. Use canonical hostnames and paths in job files; SSH aliases for the same host cannot be detected automatically.

## Build the worker

Compile the shard bench executable from the pinned source revision. The executable supports `build` and `read` subcommands. The following command compiles without running a benchmark:

```sh
cargo bench -p chroma-index --bench hierarchical_spann_shard --no-run
```

Use the executable path printed by Cargo, including its suffix. Compile once per architecture and copy the same binary to each worker. Record its checksum and compiler version. A debug build is suitable for correctness checks; performance runs require the same optimized build profile on every machine.

## Launch a build generation

Create a JSON array with one entry per shard. Values for `vectors`, `row_count`, `global_id_start`, and `id_stride` come from the preparation manifest. Paths are interpreted on the destination host. The generation and source identity must match across all jobs.

```json
[
  {
    "host": "worker-1",
    "binary": "/opt/sharding/hierarchical_spann_shard",
    "vectors": "/data/shard-0.f32",
    "checkpoint_dir": "/data/index-0",
    "shard_id": "0",
    "source_id": "dataset-name-and-revision",
    "dimension": 1024,
    "row_count": 125000,
    "global_id_start": 0,
    "id_stride": 8,
    "generation": 1,
    "metric": "l2",
    "threads": 32
  }
]
```

The example shows only one of eight required jobs. A real collection must include every shard, and its publisher must receive the intended total vector count independently of the workers' reported counts. Use `host: "local"` for local process tests.

```sh
python3 -m tools.hierarchical_sharding.build run \
  --jobs jobs.json --log-dir /results/build-generation-1 \
  --max-hosts 8 --timeout 86400
```

The driver saves per-job stdout/stderr, status, start time, duration, and total build wall time. It does not publish a collection automatically. A nonzero worker exit keeps the run incomplete. A timed-out SSH command can leave a remote worker alive; subsequent jobs on that host are blocked until the operator checks the remote process and lock. Killing the local SSH client does not prove the remote build stopped.

The worker takes a directory lock to exclude competing writers. A crashed worker can leave `build.lock`; remove it only after verifying that its owner is no longer running. A failed build can leave unreferenced immutable blocks. Preserve published blocks, and perform cleanup separately from the experiment.

## Append and publish

Prepare the new corpus suffix with `--start-id` equal to the previous total corpus size. Keep the shard count, source identity, metric, dimensions, and construction configuration fixed. Each append job sets `resume: true`, increases `generation`, and uses the previous shard's `global_id_end` as `global_id_start`. Its row count is the number of new rows in the supplied file, not the target total.

Internal build checkpoints remain private until the supplied file completes. A failed append preserves the previous published generation. Retry from that generation using the same complete append input after reconciling any stale process and lock; private intermediate work is not a resumable public snapshot.

Copy the completed immutable shard manifests to the coordinator and follow [the publication and serving instructions](TRANSPORT.md). Save a distinct collection manifest per generation. Keep old manifests and blocks while readers use them. To keep the prior generation online during compaction, run its reader on a separate process and endpoint; start the new reader on another endpoint, publish its collection manifest, and retire the old endpoint only after requests drain.

## Measurement limits

The worker exposes several construction controls, including balance threads, branching factor, split/merge thresholds, and rows per internal checkpoint. The batch driver accepts these as optional job fields and saves the complete job specification alongside results. Insertions in this first wrapper are serial while balancing can use several threads. Calibrate this wrapper against the existing optimized benchmark before using its build times as a statement about the index's best achievable throughput.

The [benchmark plan](BENCHMARK.md) requires host CPU, memory, storage, and network counters in addition to these job logs. The driver does not collect those counters or provision machines. Use dedicated workers and collect counters over the entire build and publication interval.
