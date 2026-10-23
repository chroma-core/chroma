# Distributed hierarchical SPANN benchmark

The benchmark measures whether adding machines makes a disk-backed vector index build and search more data at useful recall, latency, and total cost. The first implementation assigns vectors to independent logical shards and searches every shard; it provides a distributed baseline. Each reader loads its shard's entire navigation tree into memory. This baseline does not demonstrate distributed tree navigation, selective shard routing, or a shared mutable tree.

## Questions and acceptance criteria

The primary result is recall at 100: the fraction of the exact nearest 100 vectors returned for each query. Report complete recall versus latency, bytes fetched, and vectors scored curves, with 90% and 95% recall targets when calibration reaches them. An unattainable target remains explicitly unattainable. Report top-10 only as a secondary result. Record the distance definition, normalization, tie rule, and all reader and writer parameters alongside every result.

Two experiments answer different questions. A fixed-setting size sweep holds search effort and writer settings constant as the corpus grows, exposing degradation rather than retuning it away. A matched-recall comparison selects the fastest setting meeting each target on calibration queries, freezes that setting, and evaluates once on held-out test queries. Select independently per corpus size and architecture, disclose the setting and calibration cost, and report test recall even when it falls below target. Never select a setting using test results.

The correctness gate requires tiny-corpus exact results at exhaustive settings, disjoint ownership, unique global IDs, and identical answers after reopen. Build, append, interrupted build, missing shard, timeout, stale generation, and publication failure must be exercised. A failed shard must fail the query visibly; incomplete answers cannot count as successful recall samples. Queries must observe one published generation throughout a request.

## Corpus, queries, and exact answers

Use the first 100M rows of `CohereLabs/msmarco-v2-embed-multilingual-v3`, revision `aaaddf8cd10328d4ccd5755832c2cc27773ceef6`, with 1024 dimensions and full-precision float32 stored vectors. Save dataset file hashes, source row order, prepared-vector hashes, distance definitions, source commit, build profile, compiler version, machine image, and complete manifests. Existing checkpoint notes live at `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/msmarco-100m-checkpoints/README.md`; those checkpoints use a different layout and cannot silently become distributed shard checkpoints.

Prepare queries independently of indexed rows whenever a compatible real query embedding set exists. Otherwise reserve source rows before indexing, exclude them from the corpus, and describe the resulting corpus as a held-out subset rather than exactly the first 100M indexed rows. Freeze a query pool, record its provenance and hashes, and split it with seed 1729 into distinct calibration and test sets. Repeated queries add request samples, not independent query diversity. The existing 100-query cache supports a pilot; it does not establish a defensible p99 or broad recall estimate.

Target at least 10,000 distinct timed test queries and three independently ordered repetitions per configuration, plus a distinct calibration pool of at least 1,000 queries. Increase these counts if confidence intervals remain wide. Produce exact references for both pools at every measured corpus size and metric. Record reference-generation wall time and compute cost separately. At 100M vectors and 10,000 queries, exact scoring requires 10^12 vector comparisons; budget this gate explicitly before promising the full matrix.

Compute exact top-100 independently on every logical shard using float64 distance accumulation, then merge the shard results using global IDs. Every shard contributes its exact local top min(100, shard population); this is sufficient for the global top-100. Use ascending distance with global ID as a deterministic tie break. For inner product, use one minus inner product before merging; for cosine, specify normalization and zero-vector behavior. Validate the merged reference against a single exhaustive scan on the tiny corpus. The merge utility trusts shard exactness and candidate completeness; store shard population and reference producer logs as evidence.

The `exact-shard` utility provides an independent tiny-corpus oracle by streaming little-endian, row-major float32 files and accumulating in Python float64 arithmetic. It keeps one vector row and top-k heaps for the supplied queries; memory grows with query count times k. It reports squared L2, one minus inner product, or one minus cosine similarity, and rejects zero vectors for cosine. It supports modulo IDs through offset and stride. Its Python distance loop is deliberately unsuitable for full-corpus production references. Before the 10M gate, implement or select a blocked CPU/GPU exact scorer, validate it against this oracle for all metrics, record its accumulation precision and tie policy, and budget its full-corpus runs. No full-corpus reference engine is supplied here.

Use a fixed count of logical shards and stable modulo ownership: a vector's global ID modulo the logical shard count selects its shard. Adding physical nodes changes only shard placement. Appends preserve ownership and use global dataset IDs. Materializing interleaved rows into contiguous shard input files is a measured preparation step, including source reads, output writes, CPU, temporary storage, checksums, and retries. Keep that input preparation outside isolated index-build time and inside separately reported end-to-end ingest time.

## Machine and storage budget

Start with two identical workers and a separate load driver for correctness and calibration, then expand to eight workers for the full sweep. The existing worker class is `r6id.16xlarge`, with 64 vCPU, 512 GiB RAM, and local NVMe; availability, actual devices, region, placement, and network bandwidth remain provisioning inputs. Verify disk capacity and filesystem configuration from each worker before selecting corpus sizes. Record instance price from the actual region and purchase model at run time; this plan makes no price quote. No cloud resources are created by this tooling.

Raw float32 vector storage is `4 * N * 1024` bytes, before navigation, postings, replicas, block metadata, temporary compaction output, checkpoints, source files, or filesystem overhead.

| Indexed vectors | Raw vector bytes | Raw vector size |
| --- | ---: | ---: |
| 1M | 4,096,000,000 | 3.81 GiB |
| 10M | 40,960,000,000 | 38.15 GiB |
| 40M | 163,840,000,000 | 152.59 GiB |
| 100M | 409,600,000,000 | 381.47 GiB |
| 1B, forecast only | 4,096,000,000,000 | 3.73 TiB |

For every run, measure final checkpoint bytes and peak live disk bytes, including source data, prepared partitions, temporary blocks, retained generations, and rebuild copies. Do not infer disk needs from raw vectors alone. Sum navigation memory across loaded readers and processes, accounting for duplicate trees if multiple readers serve the same shard. A 100M corpus spread over eight 512 GiB workers may fit entirely in memory; name warm-memory results accurately and retain a cold-device experiment to support disk claims.

## Experimental matrix

Run strong scaling at 1, 2, 4, and 8 physical workers with exactly the same corpus, logical shards, partition files, reader settings, and writer settings. Choose a logical shard count divisible by eight, for example 32, and freeze it for this comparison. Assign equal vector counts to workers, record placement, and report throughput speedup, latency, scaling efficiency, and aggregate resources. One-worker completion is required to claim speedup over one worker; otherwise mark that comparison unavailable.

Run weak scaling with fixed vectors per physical worker and the same logical shards per worker. This experiment changes total corpus size and logical shard count together; state that coupling. Use a separate shard-count experiment at fixed corpus and fixed physical hardware to isolate tree size, fanout, memory duplication, merge work, and scheduling effects. Compare a single unsharded index on the same total hardware where feasible, and disclose when its architecture cannot use the extra machines.

Run fixed corpus-size points at 1M, 10M, 40M, and 100M. Use a 1M pilot to validate correctness, observability, bottlenecks, reference production, and run duration. Advance to 10M/40M only after the pilot passes; advance to 100M only after measured memory and peak disk use fit with headroom on every worker. Produce a 1B feasibility forecast only after 100M, based on measured bytes/vector, navigation RAM growth, compaction throughput, reference cost, network, and observed scaling efficiency. The available real corpus supports up to 100M; a larger experiment requires a separately identified real dataset and provenance. Repeating vectors or extrapolating timing is not a measured billion-vector result.

Evaluate concurrency 1, 2, 4, 8, 16, 32, and higher only while the driver and workers can sustain it. Record per-worker concurrent requests and coordinator shard fanout limits. The supplied runner is explicitly closed-loop: each client submits its next request after the preceding response. Its latency distribution describes completed requests under that model, and does not measure queueing under an independent arrival process. For a service capacity claim, add an open-loop generator with scheduled arrivals, queue delay, dropped requests, and coordinated-omission-aware latency, then locate the maximum arrival rate that meets the stated latency and error thresholds.

Separate build and read phases first. Add a concurrent append-and-read experiment after correctness passes; record search generation, freshness lag, publication duration, and latency disruption during publication. Do not label a run online compaction if queries pause for the whole append.

## Cache regimes and repetitions

Name every cache regime precisely. `warm-steady-state` runs a disclosed untimed query sequence until worker counters stabilize, then measures without restarting. `reader-cold-os-warm` restarts readers and clears application caches while leaving the operating-system page cache intact. `device-cold` requires an explicitly controlled operating-system cache eviction or fresh isolated worker before the named first-read batch. Record the actual procedure, privileges, affected machines, and verification counters. The runner's regime label does not perform cache manipulation.

Report first-query and first-pass behavior separately from sustained cold results. A batch after one cache eviction warms during measurement, so call it a cold-start batch, not uniformly cold reads. If every query requires cold caches, reset and verify between queries and account for the excluded reset time. NVMe firmware caches can remain warm; state that limitation. Never flush a shared worker's operating-system caches during someone else's benchmark.

Run at least three repetitions with independent recorded ordering seeds, alternate or randomize configuration order, and preserve the complete run history. Compute recall intervals by resampling distinct queries, and latency/throughput intervals by repetition or time block; individual requests sharing caches and machines are not independent samples. Report sample counts and interval method alongside p50, p95, and p99. The runner emits point estimates; confidence intervals require postprocessing the retained records.

## Read measurements

Record end-to-end latency from submission through coordinator merge, including connection setup according to a disclosed connection policy. Report latency for all outcomes and separately for successes, plus error and timeout counts, offered and successful throughput, and delivered recall that counts failed requests as zero. A request that times out is right-censored at its deadline; its actual completion latency is unknown. The driver preserves observed failure latency and flags calls that return after the deadline. The backend must enforce total deadlines and cancel abandoned work; a driver thread cannot forcibly stop a stalled backend call.

For every query, retain query ID, generation, settings identifier, start time, repetition, status, error, latency, returned IDs, recall, per-shard durations, and worker counters where available. The transport may preserve additional counters inside the result object. Missing counters mean unavailable, never zero. Reject a headline I/O-efficiency result until all workers emit the required counters.

Keep three measurements distinct. Physical bytes fetched describe storage reads at a named boundary, measured using application block-fetch bytes and corroborated by device read-byte counters; operating-system cache hits and read amplification mean those boundaries differ. Logical vectors scored count distance evaluations, including repeated evaluations when applicable. Retained memory is resident memory after the request or at steady state, with peak RSS and application cache capacity reported separately. None of these is a substitute for another.

Collect per-node CPU utilization and CPU seconds, peak and steady RSS, page faults, device read/write bytes and operations, network bytes and packets, queueing, shard stragglers, and coordinator CPU/RSS. Use dedicated worker processes or cgroups to attribute shared-machine counters, and synchronize clocks. Preserve raw counter time series rather than summing overlapping RSS samples incorrectly. Aggregate peak memory means peak of simultaneous cluster totals; also show each node's peak.

## Compaction and build measurements

Measure a full build from prepared partitions to a verified published generation. Also report source-to-ready end-to-end time including preparation, index work, manifest publication, and reopen verification. Fix writer parameters, threads per physical worker, cache sizes, and total concurrency for each comparison. Disclose whether thread counts are fixed per machine or per shard. Report each shard's start/end time, input/output vectors, CPU seconds, peak RSS, read/write bytes, checkpoint bytes, retries, and failure reason.

Inspect every worker's stderr for a balance-round limit warning. Such a run can publish a searchable snapshot but does not establish completed balancing; exclude it from successful compaction speedup claims until its remaining work is measured and resolved. A zero process exit alone is insufficient for that claim.

Measure a fixed-size append of 1M global vectors at each feasible base size, with the same source tail, ownership rule, writer configuration, and durability/publication boundary. Base sizes 39M→40M and 99M→100M match existing source milestones, but require newly built distributed checkpoints. Record partition preparation, queue wait, per-shard compaction, slowest shard, publication, and reopen separately. The critical path ends only after the new generation is published and readable; summing shard durations is not wall time. CPU and storage/network work sum across nodes, while wall time follows the critical path. Record load imbalance, retries and their resource cost, failed attempts, and time spent waiting for the last shard.

Inject one worker failure before publishing and confirm readers retain the complete prior generation. Retry the failed shard, publish once all shards validate, reopen, and check exact tiny-corpus results and ownership. Report both successful-path and recovery-path compaction performance. Do not discard failed attempts from throughput or cost accounting.

## Cost and reporting

Report cost per node and aggregate cluster cost, including the coordinator, load driver, provisioned idle time, build workers, reference generation, storage, data preparation, and chargeable network. Show build dollars, append dollars, hourly serving dollars, and dollars per million successful queries at each offered load. Also show dollars per million requests meeting both the recall and latency objective when those objectives are defined. A cluster's cost is the sum across machines; dividing latency by a worker's hourly price is not an aggregate cost measure. Keep one-time preparation/reference cost separate from recurring serving cost, with both visible.

A defensible blog includes dataset/revision, exact corpus and query counts, hardware and topology, code/settings/manifests, raw result links, failures, uncertainty, recall curves, fixed-setting and matched-recall comparisons, warm/cold procedures, per-node and total resource/cost figures, and the navigation-memory limitation. Label forecasts as forecasts. Select the largest completed run that passes correctness and measurement gates as the headline scale.

## Runner usage

Run the standard-library utility from the repository root. Query input is JSONL with `query_id` and `vector`; reference input is JSONL with `query_id` and exact `ids`. Exact shard files additionally carry ascending `distances`. Query IDs must be unique strings. Hash every input and save CLI arguments, environment, settings, generation, and source revision in a run manifest alongside outputs.

```sh
python3 -m tools.hierarchical_sharding.benchmark split --queries queries.jsonl --calibration-count 1000 --test-count 10000 --output-prefix heldout --seed 1729
python3 -m tools.hierarchical_sharding.benchmark exact-shard --vectors tiny-shard-0.f32 --queries heldout.test.jsonl --dimension 1024 --id-offset 0 --id-stride 32 --k 100 --metric l2 --output exact-0.jsonl
python3 -m tools.hierarchical_sharding.benchmark merge-exact --shards exact-0.jsonl exact-1.jsonl --k 100 --output exact-global.jsonl
python3 -m tools.hierarchical_sharding.benchmark run --manifest cluster.json --queries heldout.test.jsonl --reference exact-global.jsonl --output-prefix run-001 --k 100 --concurrency 8 --repetitions 3 --seed 1729 --timeout 30 --regime warm-steady-state --query-settings '{"beam_min":10,"beam_max":256,"rerank_factor":10}'
python3 -m unittest tools.hierarchical_sharding.test_benchmark -v
```

The runner writes `.queries.jsonl` and `.summary.json`. Use separate invocations and recorded seeds for independent repetitions when preparing confidence intervals. It executes requests against the existing published cluster and does not provision machines, generate large-corpus exact references, warm caches, flush caches, collect host counters, or perform compaction. Those operations require the explicit procedures above and separate orchestration; a successful runner invocation alone does not complete this benchmark plan. The abbreviated merge example names two files; a real merge must include every shard file.

## Bounded CPU reference and isolated real pilot

The blocked reference utility scores float32 corpus rows using float64 NumPy matrix multiplication and retains only each query's best candidates between blocks. Its working buffers are bounded by corpus-block rows times dimension plus query-block rows times corpus-block rows, in addition to query and top-k state. The memory-mapped source can still occupy the operating-system page cache. Configure BLAS threads explicitly and keep reference generation separate from timed index work. Squared L2 uses the norm/dot identity with negative rounding residue clipped to zero; cosine divides by float64 norms and rejects zero vectors. This accumulation can differ in its final bits from the direct scalar oracle. The tests compare candidate IDs and numerical distances on tiny fixtures, including global-ID ties.

The preparation utility downloads only the required public parquet files directly into a new, empty run directory. It pins the required source revision, reads the same `emb` column and ascending file/row order as the repository loader, and rejects null, nonfinite, zero, and wrong-dimensional vectors. The first million rows form the corpus. The following 200 rows are excluded from the corpus and split into 100 calibration and 100 test rows. These are held-out document embeddings, which support a mechanics and recall pilot but do not establish search-query quality or a defensible p99 estimate. The provenance file preserves source and output SHA-256 hashes, download URLs, source norm range, row ranges, normalization, query counts, and preparation wall time. Source parquet files remain on disk and count toward peak preparation storage.

```sh
python3 -m tools.hierarchical_sharding.prepare_real --output /mnt/sharding/real1m --count 1000000 --calibration 100 --test 100 --normalize
OPENBLAS_NUM_THREADS=8 python3 -m tools.hierarchical_sharding.exact_blocked --vectors /mnt/sharding/real1m/corpus.f32 --queries /mnt/sharding/real1m/queries.test.jsonl --dimension 1024 --metric cosine --k 100 --output /mnt/sharding/real1m/test.exact.jsonl
python3 -m tools.hierarchical_sharding.host_metrics --pid 12345 --duration 3600 --interval 1 --output worker.host.jsonl
```

The host sampler preserves Linux CPU, disk-device, interface, and optional selected-process counters as a time series. Disk bytes are physical sectors reported by the operating system, while process `rchar` and `wchar` count logical system-call traffic. Host counters include unrelated work; device hierarchies and virtual interfaces can overlap, so select the physical device and interface before computing deltas. Process resident memory and high-water memory are KiB; CPU usage is cumulative seconds. Missing process data is marked exited or permission denied. These counters do not supply application block-fetch bytes, vectors scored, queue time, or navigation memory; those remain separate instrumentation requirements. A sampler interval is not an exact simultaneous cluster peak.

The search reranking parameter is a multiplier of the requested result count. A factor of 10 with k equal to 100 configures a budget of 1,000 candidates per logical shard, or 2,000 across two shards. The legacy `rerank` name is an alias for this factor and is not a candidate count. The runner records the explicit factor and configured per-shard and aggregate budgets; it reports an unavailable value if the caller leaves the factor implicit. These budgets do not measure actual vectors scored, which can be smaller and require separate worker counters.
