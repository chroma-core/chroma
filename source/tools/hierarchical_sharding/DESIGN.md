# Hierarchical SPANN sharding and horizontal scaling

Distribute a collection across independently compacted hierarchical indexes, then measure how much additional data and query traffic each added machine supports. The first milestone searches every shard and establishes distributed correctness. The second milestone adds spatial routing so a query can search selected shards. Each milestone needs its own recall and cost measurements before it supports a scale claim.

This is a research implementation plan for the [blog requirements](https://app.notion.com/p/trychroma/Requirements-3f958a6d8191802e8ac8f3171fee9b00). The source baseline is `ab03657249240a8732109058286efb8ce31e1faf`. The implementation branch is `codex/hierarchical-spann-sharding`. Production log consumption, updates and deletions under sustained load, distributed ownership changes, and automatic recovery belong to subsequent integration work.

## The pieces

- A logical shard owns a stable subset of vector IDs and one hierarchical tree. Logical shards remain fixed while experiments change the number of physical machines.
- A compaction worker adds vectors, balances its tree, and writes immutable index blocks. It publishes a shard snapshot only after those blocks are complete.
- A query worker opens one committed shard snapshot and retains only that shard's navigation data. It loads selected posting lists and original vectors for reranking as needed.
- A coordinator sends a query to the required workers and combines their exact reranked candidates into the global nearest results.
- A collection manifest identifies the complete set of shard snapshots, their ownership, dimensions, distance metric, and one collection generation. A generation is a named immutable version of the whole collection.
- A benchmark driver generates requests on a separate machine and records end-to-end response times, failures, and recall against exact reference answers.

## Build and publish a collection

1. **Assign every vector to one stable shard.** Use the original dataset row number as the global ID. With S logical shards, shard s owns IDs for which `id % S == s`. Its tree uses a local ID, and the worker translates it to `s + S * local_id`. This keeps the existing 32-bit tree IDs while allowing 64-bit collection IDs. Reject any shard that exhausts its local ID space.
2. **Prepare each shard's input independently.** Produce little-endian, row-major float32 files in dataset order, recording source revision, dimension, metric, row counts, and checksums. Time source conversion and distribution separately from construction and include them in the total time to a queryable collection. Do not have every worker download or retain the entire corpus.
3. **Build shards concurrently.** Give each shard a separate storage namespace and a single writer. Workers execute insertion, balancing, commit, flush, and reopening checks independently. Bound the number of concurrent shard builds on a host so memory use stays within the measured budget.
4. **Publish completed shard snapshots.** A worker writes a new immutable snapshot manifest after all referenced blocks exist. A failed or retried build cannot overwrite a previously published generation. Reopening must succeed before publication. Small shards up to 100,000 vectors also verify every reachable ID; large shards require a separate streaming integrity audit before headline benchmarks because the current full audit retains all IDs in memory.
5. **Publish the complete collection.** The coordinator checks that every shard belongs to the same intended generation, uses the same configuration, and collectively covers the expected dataset prefix exactly once. It writes the collection manifest atomically. Readers continue using the previous complete generation until the new one is ready.
6. **Compact the next batch independently.** Partition the next dataset suffix using the same ownership rule. Each writer reopens its previous shard snapshot, adds only its new rows, balances, and publishes a new snapshot. Measure time until the entire new collection generation is queryable, including the slowest shard and publication.

The persisted collection manifest anchors reproducibility and read consistency. A reader must use the exact snapshot named by its request; a worker that has another snapshot returns an error. A single publisher is sufficient for the benchmark. Multiple competing publishers require compare-and-swap or a fenced lease before production use.

## Search a committed collection

1. **Pin one collection generation.** The coordinator validates query dimensions, finite numeric values, result count, search settings, and the chosen manifest before sending work.
2. **Search every logical shard concurrently.** Each worker searches its own hierarchical tree and reranks candidates with original vectors using the same metric. The first milestone uses all shards; it makes no claim that routing eliminates network work.
3. **Require complete responses.** The coordinator checks request identity, generation, shard identity, ID ownership, and finite distances. A timeout, missing shard, or malformed response fails the request. Partial answers must not appear as successful benchmark results.
4. **Merge the nearest candidates.** Each shard returns up to k exact reranked candidates; the coordinator orders their union by distance and then global ID and retains k. This merge is exact relative to the returned candidates. Approximate shard search can still omit true neighbors, so collection recall must be measured against the complete corpus.
5. **Record the whole request cost.** Record end-to-end latency at the driver, worker search time, coordinator work, shards contacted, serialized network bytes, and errors. Keep index payload bytes, physical storage traffic, and retained memory as separate measurements.

Query workers open only their own shard navigation metadata, which reduces per-worker memory when data ownership is spread across machines. Each worker still materializes its entire shard's navigation structure. This milestone does not implement arbitrary navigation fragments loaded on demand, shared caches, or one globally balanced tree spanning machines.

Use 32 logical shards as the initial placement experiment. Assign them evenly across 1, 2, 4, and 8 physical workers, building one shard at a time per worker and running one persistent reader per shard. At 1B vectors this produces approximately 31.25M vectors per tree, reducing the risk that one writer exceeds a machine's memory. Set coordinator concurrency explicitly and record any fanout waves. A separate 8/16/32-shard experiment can determine whether the extra fanout is worth the smaller trees; do not mix that change into the primary machine-count sweep.

## Add routing after the baseline passes

Spatial routing is the next implementation stage because searching every shard makes total query work grow with shard count. Train a small coarse partitioner on a fixed representative sample, assign vectors to coarse partitions, and build a hierarchical tree for each partition. Treat the partitioner, assignment, and shard snapshots as one immutable generation. A coordinator scores the routing centroids and sends a query to the selected partitions.

Compare routed search with exhaustive search over exactly the same spatial shards. Sweep the number of selected partitions together with the internal beam and reranking budgets. Report recall, machines contacted, total vectors scored, network traffic, and tail latency. Separately compare spatial partitioning with the modulo baseline so improvements from placement are not confused with improvements from routing.

Skew must be measured before choosing the number of spatial partitions. Use more logical partitions than physical machines and place them by measured memory, build time, and query work. Keep oversized partitions visible in the results; do not hide them by reporting only mean shard size. A fixed partitioner routes appended vectors consistently. Repartitioning, movement between machines, and cross-partition neighbor replication require a subsequent protocol for ownership and deduplication.

A shared coarse router plus independent trees is a two-stage distributed index. Calling it a single distributed hierarchical tree requires an additional implementation in which upper routing nodes reference remotely owned subtrees and a query preserves its search budget across machine boundaries. That design must specify global candidate selection, split publication, subtree movement, and version consistency. The blog should use the name matching the demonstrated mechanism.

## Implementation sequence and acceptance gates

| Stage | Deliverable | Gate before proceeding |
| --- | --- | --- |
| 1 | A persistent shard worker builds, appends, saves, reopens, and searches an index. | Small real-index tests preserve IDs and answers across reopen and append. Invalid inputs and failed publication preserve the prior snapshot. |
| 2 | A distributed coordinator builds separate shards and merges network query responses. | Two separate worker processes pass end-to-end tests, including stale generation, failed worker, duplicate ownership, and wrong dimensions. A two-host run establishes that the same protocol works over the network. |
| 3 | A benchmark runner captures recall, latency, errors, and resource evidence. | A pilot produces complete raw records and reproducible summaries; every headline metric has a defined collection method. Missing physical I/O counters are marked unavailable, not replaced with payload estimates. |
| 4 | Fixed logical shards run on 1, 2, 4, and 8 physical workers. | The same corpus, snapshots, search settings, cache conditions, and concurrency are compared. Repeat at matched recall when changing topology or corpus size. |
| 5 | A coarse spatial router selects shards. | At matched recall, total work and tail latency improve over exhaustive search of the same spatial partitioning. |
| 6 | A larger real corpus tests the forecast. | Measured storage, memory, exact-reference cost, build time, and budget support the run. Record the prediction before running it. |

The initial implementation assignments are the Rust shard worker, the Python distributed coordinator, and the benchmark runner. Each assignment uses a GPT-6.1 Sol subagent. Integration includes a cross-component protocol review and real-index smoke test; isolated unit tests do not establish distributed index correctness.

## Benchmark questions

The [benchmark plan](BENCHMARK.md) defines the run matrix and result format. The central questions are whether more machines reduce collection publication time, whether they increase query throughput at a chosen tail latency and recall, and whether a larger collection can maintain useful recall within measured per-node resources.

For construction, compare a fixed corpus and fixed shard layout on 1, 2, 4, and 8 machines. Report total publication time and speedup `T(1) / T(p)`, efficiency `T(1) / (p * T(p))`, per-shard completion times, aggregate compute, and storage traffic. Run both fresh builds and a fixed-size append. Include input preparation, final flush, and manifest publication in separate phase measurements and in the total.

For reads, separate capacity scaling from topology changes. First place the same immutable shards on different machine counts. Then vary data size with a fixed amount of data per node. Finally sweep logical shard count and spatial routing as separate experiments. For each experiment, report fixed-setting recall and the cost to reach 90% and 95% recall where achievable. Use top-100 results, held-out queries, repeated trials, and enough requests for meaningful p95 and p99 estimates.

For failures, interrupt one compactor before publication and one query worker during a load run. Verify that the old collection generation remains readable, incomplete new generations remain unpublished, and failed queries appear in the results. Measure recovery and retry costs explicitly. These are research protocol checks, not evidence of production availability.

## Datasets for larger runs

Use existing public embeddings for scale and search-quality claims. The [dataset section in the shared plan](https://app.notion.com/p/trychroma/Hierarchical-SPANN-sharding-and-horizontal-scaling-3f958a6d8191814c86c2ff70ddcf426c) selects [Qdrant FineWeb-10B](https://huggingface.co/datasets/Qdrant/FineWeb-10B) as the primary high-dimensional text workload. Its published corpus contains approximately 10.07 billion 768-dimensional embeddings stored as float16. Pin a revision and use controlled 100M, 250M, 500M, and 1B prefixes after a small ingestion and correctness check. Synthetic vectors serve only mechanics tests.

FineWeb's supplied exact references cover its complete corpus, and its query embeddings must be regenerated using the documented procedure. Validate that procedure before relying on those references. Recompute exact answers for every indexed subset: filtering full-corpus top-1000 results does not generally recover a subset's top-100. Preserve the source-to-float32 conversion, normalization, metric, query provenance, and license requirements in the run manifest.

Use [DEEP1B and Text-to-Image-1B](https://big-ann-benchmarks.com/neurips21.html) as established lower-dimensional comparisons with supplied queries and references. The shared plan also identifies approximately 250M multilingual Wikipedia embeddings as an intermediate 1024-dimensional experiment. These are different workloads; keep their results separate from the controlled FineWeb size sweep.

At one billion vectors, the writer's float32 representation requires 3.072 TB for FineWeb's 768 dimensions, compared with 384 GB for DEEP1B's 96 dimensions, before index overhead and copies. Choose the largest validated real run after the correctness, convergence, resource, and exact-reference gates pass.

## EC2 resources and scale gates

Request a fleet that can run 1, 2, 4, and 8 identical worker machines plus a separate load driver in the same availability zone. The first two workers are sufficient for correctness and pilot measurements; expand to eight only after the pilot establishes per-node memory, disk, network, and runtime needs. Reuse the workers for build and read experiments rather than requiring separate fleets simultaneously. Keep the ongoing 100M benchmark and its source snapshots isolated.

Use two r6id.16xlarge workers for the first multi-host pilot, with a path to eight identical workers for the primary scale sweep. This matches the existing reference CPU architecture and gives each worker 64 vCPUs, 512 GiB RAM, two 1.9 TB NVMe devices, and advertised 25 Gbps networking according to [AWS specifications](https://aws.amazon.com/ec2/instance-types/r6i/). Choose a separate driver with at least 8 vCPUs and 32 GiB RAM, then verify that it stays below CPU and network saturation at the highest offered load. These are initial experimental allocations; peak writer memory, steady reader memory, and retained snapshots determine whether the larger run fits. No instance price or live availability is assumed.

At 1024 dimensions, uncompressed float32 vectors occupy 409.6 GB for 100M vectors and 4.096 TB for 1B vectors, before source copies, index blocks, temporary files, replicas, or snapshots. At eight equal workers, the 1B raw-vector share is 512 GB per worker. These are decimal byte counts, not disk-capacity estimates. Index storage may include original vectors again; measure each retained copy and the peak overlap between generations before requesting disks.

The available source notes identify a 100M-vector MS MARCO dataset prefix. A 1B claim requires a real, identified corpus of that size and exact reference answers for its held-out queries. Repeating vectors can test transport and storage mechanics but cannot establish billion-vector search quality. Extrapolate only after recording the assumptions about dimension, distribution, recall, storage, and cache reuse.

Before provisioning or using additional machines, resolve the authorized account and region, access method, instance inventory, storage persistence requirements, and spending or runtime limit. Use the Cops-managed AWS configuration and verify the selected account. An immutable object-storage copy should protect expensive completed snapshots if workers use instance-local disks. Record snapshot upload and download costs separately from local-NVMe query results.

The [Index Memory Optimizations chat](codex://threads/01a0d6c5-eb50-7d21-889a-359926993a01) inspected Lyon on October 14, 2026. It reports sandbox account `079819354890`, region `us-east-1`, availability zone `us-east-1f`, and instance `i-0f0b5dcb6888ad43d`. The MS MARCO pipeline holds `/mnt/data/chroma-benchmark.lock` while building toward 99M. It uses approximately 350 GiB RAM. Root disk free space is approximately 1.2 GiB, so source builds and temporary data belong in an isolated NVMe directory after acquiring the benchmark lock. These are time-specific observations and must be checked again before running. The existing job must finish before this pilot uses Lyon; additional idle workers can run independently.

## Risks that can invalidate a scale claim

- All-shard fanout can improve latency while increasing total work. Report aggregate CPU, bytes, and cost as well as per-machine values.
- Changing shard count changes tree construction and search budgets. Keep logical shards fixed for the primary machine-count sweep.
- Each shard independently returning k results multiplies candidate and reranking work. Record aggregate budgets and measure matched recall instead of treating identical per-shard settings as equal work.
- Independent writers can have uneven completion times. The slowest shard determines publication time.
- Process-local materialized postings and block pins can outlive the nominal block-cache budget. Measure resident memory through the entire run and define cache clearing precisely.
- A restarted application can still read from the operating-system file cache. Label that condition separately from a storage-cold or object-storage run.
- Reusing a hundred queries can produce warm-cache behavior without supporting a broad p99 claim. Separate exact-reference query preparation, warm-up, calibration, and timed test requests.
- Incremental append compaction does not establish update, deletion, log replay, or merge-on-read semantics. Keep those claims outside this milestone.

## Source references

The existing writer commits and forks block files in `rust/index/benches/hierarchical_index/writer/persistence.rs`. The reader loads navigation metadata in `rust/index/benches/hierarchical_index/reader/persistance.rs` and performs lazy candidate scoring and exact reranking in `rust/index/benches/hierarchical_index/reader/reader.rs`. The new shard executable wraps these components; the distributed orchestration and benchmark files live beside this plan.
