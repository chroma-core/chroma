# Hierarchical SPANN GPU POC: measured runs

## CPU versus GPU summary

The tables compare the same mathematical workload on each GPU and the corresponding CPU baseline. All vectors have 1,024 float32 dimensions. Inputs and outputs remain in the memory of the device being measured; GPU times exclude CPU-to-GPU transfers. Speedup is CPU time divided by GPU time. The million-vector rows provide the paired H100/B200 comparison; the larger rows measure the H100 workload near its throughput plateau. A dash means that batch size has no B200 measurement.

### Batch latency

| Calculation | Vectors | CPU baseline for H100 | H100 | H100 speedup | CPU on B200 host | B200 | B200 speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Dot products against 2 centers | 8,000,000 | 738.22 ms (23 threads) | 14.02 ms | 52.7× | — | — | — |
| Dot products against 128 centers | 8,000,000 | 2,659.61 ms (23 threads) | 41.69 ms | 63.8× | — | — | — |
| Dot products against 1,024 centers | 8,000,000 | 44,737.71 ms (14 threads) | 327.42 ms | 136.6× | — | — | — |
| Dot products against 4,096 centers | 2,000,000 | 8,986.58 ms (14 threads) | 327.05 ms | 27.5× | — | — | — |
| Average two labeled groups | 8,000,000 | 383.34 ms (14 threads) | 10.84 ms | 35.4× | — | — | — |
| One two-means assignment and update | 8,000,000 | 1,458.47 ms (14 threads) | 25.44 ms | 57.3× | — | — | — |
| Produce 1-bit codes and four headers | 8,000,000 | 1,048.19 ms (14 threads) | 19.53 ms | 53.7× | — | — | — |
| Dot products against 2 centers | 1,000,000 | 85.37 ms (14 threads) | 1.76 ms | 48.6× | 65.58 ms (14 threads) | 1.40 ms | 46.7× |
| Dot products against 128 centers | 1,000,000 | 524.17 ms (14 threads) | 5.16 ms | 101.7× | 565.83 ms (14 threads) | 4.21 ms | 134.3× |
| Average two labeled groups | 1,000,000 | 140.84 ms (14 threads) | 1.73 ms | 81.5× | 169.29 ms (14 threads) | 2.08 ms | 81.4× |
| One two-means assignment and update | 1,000,000 | 340.92 ms (14 threads) | 3.62 ms | 94.1× | 265.08 ms (14 threads) | 3.63 ms | 73.1× |
| Produce 1-bit codes and four headers | 1,000,000 | 125.10 ms (14 threads) | 2.67 ms | 46.9× | 206.09 ms (14 threads) | 2.40 ms | 85.7× |

### Throughput

Throughput is the primary measure of how much work each device completes. All rates below are **millions of vectors per second**, calculated from the full-precision recorded batch size and median operation time. They use the same CPU baselines and GPU kernels as the latency table, with transfers excluded. Each distance row processes every vector against the stated number of centers; rates across different center counts represent different amounts of work.

| Calculation | Vectors per batch | CPU baseline for H100 | H100 | CPU on B200 host | B200 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Dot products against 2 centers | 8,000,000 | 10.837 | 570.663 | — | — |
| Dot products against 128 centers | 8,000,000 | 3.008 | 191.895 | — | — |
| Dot products against 1,024 centers | 8,000,000 | 0.179 | 24.434 | — | — |
| Dot products against 4,096 centers | 2,000,000 | 0.223 | 6.115 | — | — |
| Average two labeled groups | 8,000,000 | 20.869 | 738.121 | — | — |
| One two-means assignment and update | 8,000,000 | 5.485 | 314.465 | — | — |
| Produce 1-bit codes and four headers | 8,000,000 | 7.632 | 409.721 | — | — |
| Dot products against 2 centers | 1,000,000 | 11.714 | 569.736 | 15.248 | 712.170 |
| Dot products against 128 centers | 1,000,000 | 1.908 | 193.967 | 1.767 | 237.274 |
| Average two labeled groups | 1,000,000 | 7.100 | 578.779 | 5.907 | 480.969 |
| One two-means assignment and update | 1,000,000 | 2.933 | 275.872 | 3.772 | 275.576 |
| Produce 1-bit codes and four headers | 1,000,000 | 7.994 | 374.732 | 4.852 | 415.874 |

At a fixed batch size, GPU throughput divided by CPU throughput equals the speedup in the latency table. The larger-batch rates come from sustained warmed repetitions; the million-vector rates come from five warmed runs. These derived rates do not measure concurrent requests or an indexing pipeline that includes transfers. The 1,024-center CPU timing variability also applies to its throughput figure.

### Measurement details


The larger-batch CPU model is **Intel(R) Xeon(R) Platinum 8470**. The table uses the fastest captured median for each calculation. Two- and 128-center matrix products have both 14- and 23-thread measurements; the other calculations use the completed 14-thread pass. The captured timing records are retained in [the CPU report](cpu-ceiling-runpod.json).

The CPU host metadata is recaptured after restart on the same host. The container CPU quota is `2210000 100000` and its recovery-time CPU affinity is recorded. The existing H100 timings are in [the matrix sweep](saturation-h100.json) and [the streaming kernels](optimized-h100.json). All Runpod reports in this comparison have the same public input fixture hash.

The larger-batch CPU run is on a different Runpod host from the H100 GPU sweep, whose CPU is an Intel Xeon Platinum 8480+. The CPU run uses `NUMPY_MADVISE_HUGEPAGE=0`; this disables the large-page allocation hint described in [NumPy’s memory-policy documentation](https://numpy.org/doc/2.0/reference/global_state.html). Input allocation is outside the timed measurements.

The remaining 23-thread results and the full original CPU JSON are unavailable after the connection interruption and pod stop. Progress into the second pass confirms every 14-thread correctness assertion passed, but their exact error values are not captured.

The 8-million-vector, 1,024-center CPU case varies from a minimum of 19.37 seconds to a median of 44.74 seconds; its 136.6× ratio uses that median and should be read with this variability. The million-vector H100 and B200 rows each use their own host's CPU measurements; those CPU models are unrecorded.

CPU matrix products use OpenBLAS. CPU centroid sums and quantization use compiled OpenMP C++ with parallel workers. The centroid reference accumulates worker sums in double precision and returns float32 centers; the GPU accumulates in float32.

The GPU variants use CuPy matrix products, a streaming centroid reduction, and the faster verified quantization variant at this batch size. These are optimized implementations of the same formulas, with different reduction orders.

CPU timings use warmed wall time; GPU timings use warmed CUDA-event blocks. The larger-batch runs last at least two seconds with at least three timing samples. The million-vector runs use five warmed repetitions.

CPU distances are checked against the public fixture on 256 rows with error below 1e-5; centroid errors must stay below 1e-5, quantized code bytes must match on sampled first and last rows, and quantization header errors must stay below 1e-4.

The CPU memory-copy control uses NumPy’s single-threaded copy and is not a CPU bandwidth ceiling. These results establish component gains under this host allocation, rather than a peak result for the entire physical CPU server or an end-to-end indexing gain.

## SF Compute H100 component measurements

Large batches show the H100's potential when vectors already live on the GPU. The input contains 100,000 captured Wikipedia vectors of 1,024 dimensions, repeated to form larger batches. Repetition preserves the vector values and arithmetic workload; it is not a new million-vector dataset. CPU distance and centroid calculations use 14-thread OpenBLAS, while 1-bit quantization uses a fused 14-thread OpenMP C++ reference. The GPU uses CuPy matrix operations and a fused CUDA quantization kernel. Each time below is the median of five warmed runs on the same H100 node. The complete 65,536-, 262,144-, and 1,000,000-vector results are in `/home/dev/hspann-results/component-kernels-h100-final.json` on the parked node.

| Component, 1,000,000 vectors | CPU | GPU with inputs resident | CPU / resident GPU | GPU including transfer |
| --- | ---: | ---: | ---: | ---: |
| Distance to 2 centers | 109.0 ms | 2.13 ms | 51.1× | 426.9 ms |
| Distance to 128 centers | 231.1 ms | 6.44 ms | 35.9× | 625.2 ms |
| Average two labeled groups | 161.9 ms | 1.81 ms | 89.6× | 475.1 ms |
| One two-means assignment and update | 279.1 ms | 4.11 ms | 68.0× | 479.8 ms |
| Produce 1-bit codes and headers | 141.3 ms | 3.32 ms | 42.5× | 519.2 ms |

The GPU-resident figures are operation times measured with CUDA events. The transfer figures include copying vectors from CPU memory and returning results; that transfer costs more than the CPU calculation in every one-million-vector case. The distance outputs match CPU within 7.75e-7 on 256 checked rows. Two-means labels and all quantized code bytes match across each full batch; centroid errors stay below 1.1e-7 and the largest quantization header difference is 2.9e-5. The C++ quantizer follows the Rust code format and formulas but is an independent CPU implementation. These measurements establish component speedups, not complete index speedups.

The CPU baselines ran on the same SF Compute H100 host reserved for GPU experiments. Each run used 14 insertion workers, one balancing worker, full-precision writer navigation and nearest-posting assignment, a single checkpoint, and validation before commit and after reopen. Recall used 1,000 sampled data vectors as queries and exact nearest neighbors at k=100. The manifests and full logs remain under `/home/dev/hspann-results` on the stopped node `hspann-gpu-poc-01`; the mission is [hierarchical-spann-gpu-poc](https://autoresearch.sfcompute.com/missions/hierarchical-spann-gpu-poc).

| Dataset | Vectors | Posting validation | Index build | Recall at tau=2, rerank=8 |
| --- | ---: | --- | ---: | ---: |
| Wikipedia English | 300,000 | 300,000 of 300,000 before and after commit | 36.58 s | 99.28% |
| MS MARCO v2 | 300,000 | 300,000 of 300,000 before and after commit | 36.02 s | 99.50% |
| Wikipedia English | 1,000,000 | 1,000,000 of 1,000,000 before and after commit | about 2.0 min | 98.05% |
| MS MARCO v2 | 1,000,000 | 1,000,000 of 1,000,000 before and after commit | about 2.0 min | 97.55% |

The one-million-vector reader's warm query latency was 82.8 ms for Wikipedia and 82.5 ms for MS MARCO at tau=2 and rerank=8. About 56 ms per query was spent in its distance-scoring phase. The exact-neighbor calculation took about 1.3 minutes for Wikipedia and 1.2 minutes for MS MARCO after the benchmark parallelized queries and selected only the top 100 distances.

The writer split replay used a captured Wikipedia leaf with 100,000 vectors of 1,024 dimensions. A vectorized NumPy two-means took 0.947 s; the CuPy H100 prototype took 0.898 s including transfers, a 1.05× speedup. Labels agreed for every vector, and the largest center-coordinate difference was 6.5e-7. This probe shares deterministic initial seed pairs across its CPU and GPU versions, but it does not reproduce Rust's seed selection or measure the complete writer.

## Writer assignment distance replay

The one-million-vector Wikipedia build takes about two minutes. It inserts vectors in 9.86 s, balances the tree in about 1.4 minutes with one balancing worker, loads postings in 17.63 s, and commits in 7.79 s. The writer reports about 256 ms per split in reassignment within the split's own group, but most of that time includes navigation, posting updates, and recursive balancing. Timers around the full-precision distance calculations in the actual build measure only 566 ms for group decisions and 3.18 s for neighboring-group decisions across all splits. All one million IDs remain reachable before and after reopening the index. The instrumented run skips recall evaluation; the earlier baseline above supplies the recall result.

The GPU replay uses real vectors from a captured 100,000-vector Wikipedia split. It reconstructs group centers and labels with the shared-seed two-means replay, then compares the decision to reassign each vector. The timings are medians of nine runs. CPU is a single-threaded batched NumPy calculation; GPU-resident keeps inputs on the H100, while GPU-with-transfer sends the vectors and returns the decisions for each run. Both CPU and GPU decisions agree with direct squared-distance comparisons for every tested vector. These centers are reconstructed for the probe and are not the exact centers selected by Rust's random initialization.

| Vectors per decision batch | CPU batched | GPU resident | GPU with transfer | CPU / GPU with transfer |
| --- | ---: | ---: | ---: | ---: |
| 2,048 | 0.768 ms | 0.295 ms | 0.985 ms | 0.78× |
| 4,096 | 1.503 ms | 0.302 ms | 1.459 ms | 1.03× |
| 100,000 | 57.289 ms | 0.525 ms | 41.070 ms | 1.39× |

Even perfect elimination of all 3.75 s of nearest-posting assignment distance math can save only about 3% of the roughly two-minute build, or at most about 1.03× overall speedup. This bound assumes the measured distance work lies on the single balancing worker's path and ignores any GPU overhead. Most split groups contain around 2,000–2,500 vectors, where the transfer-inclusive GPU replay is no faster than batched CPU math. The unusually large 100,000-vector group appears only at the start of this run. The GPU probe measures one group-decision substep; it does not replace the writer or measure a full GPU index build.

The reader scoring probe used synthetic records in the same 144-byte 1-bit code layout and the Euclidean distance formula used by the reader. It scored 133,100 records in a median 1.82 ms and 178,900 records in 2.37 ms, including host-to-GPU and GPU-to-host transfers. The kernel itself took about 0.05 ms. Scores matched a CPU formula on 256 sampled records within 8e-5 absolute error. This probe excludes tree search, disk reads, and score integration; it establishes a promising scoring ceiling, not an end-to-end reader speedup.

## Real reader scoring replay

The real-data replay opens each saved one-million-vector index and captures the first exact-neighbor query's selected posting codes, per-leaf query quantization, and CPU scores. It uses tau=2 and a rerank factor of 8, then checks GPU scores and candidate sets against those exact CPU scores. The CPU column times the capture path's scoring loop on one thread; the GPU columns are medians of nine replays and include transfers, allocation, and kernel launches. The batched GPU path combines all selected leaves into one launch.

| Dataset | Selected leaves | Codes | CPU scoring | GPU per leaf | GPU batched | CPU / batched GPU | Top-800 overlap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Wikipedia English | 128 | 186,863 | 12.074 ms | 7.636 ms | 2.149 ms | 5.62× | 800 / 800 |
| MS MARCO v2 | 128 | 184,378 | 11.899 ms | 8.011 ms | 2.121 ms | 5.61× | 800 / 800 |

Both GPU paths match all top 100 and top 800 IDs on both captured queries. The largest absolute score difference from the Rust reader is 3.58e-7. The CPU capture and GPU replay use the same stored codes and quantized query values; the Python replay prepares a flat code buffer and leaf lookup outside the timed GPU section. This preparation, the Rust-to-GPU integration cost, tree navigation, posting loads, deduplication, and vector reranking still need measurement in one reader search. The 5.6× figure describes this bounded scoring replay only; it is not a full-query speedup or a recall result across all queries.

The saved fixtures are `/home/dev/hspann-results/reader-wikipedia-1m-q0.bin` and `/home/dev/hspann-results/reader-ms-marco-1m-q0.bin` on the stopped node. The matching CPU logs and GPU JSON reports are in the same directory. The one-million-vector indexes and exact-neighbor query caches are unchanged, so another replay can use the same input.

## Paired reader search

The benchmark reader can now send selected leaf codes directly to a CUDA scoring library. A CPU and GPU run opens the same saved one-million-vector index, reads the same first 100 exact-neighbor queries, uses tau=2 and rerank=8, and records the ordered top-100 IDs. The table reports average warm search latency per query, including tree navigation, scoring, candidate selection, and vector reranking. Each run first makes a cold pass that loads postings and vectors; only the second pass is used for this latency comparison. The thread count is the number of concurrent recall queries, not the writer's insertion workers.

| Dataset | Concurrent queries | CPU warm query | GPU warm query | CPU / GPU | Recall@100, both |
| --- | ---: | ---: | ---: | ---: | ---: |
| Wikipedia English | 1 | 15.8 ms | 9.4 ms | 1.68× | 98.11% |
| Wikipedia English | 4 | 16.9 ms | 12.3 ms | 1.37× | 98.11% |
| Wikipedia English | 8 | 17.2 ms | 17.6 ms | 0.98× | 98.11% |
| Wikipedia English | 32 | 81.2 ms | 190.3 ms | 0.43× | 98.11% |
| MS MARCO v2 | 1 | 16.3 ms | 10.3 ms | 1.58× | 97.15% |
| MS MARCO v2 | 32 | 74.4 ms | 190.8 ms | 0.39× | 97.15% |

Every paired query returns the same top-100 ID set on both datasets at one and 32 concurrent queries. Wikipedia's ordered result lists also match exactly. One MS MARCO query has six rank positions in a different order, with the same 100 IDs. The intermediate four- and eight-query Wikipedia runs report the same aggregate recall, but they did not save individual result lists.

Direct transfer matters for latency. Copying about 25 MB of leaf codes into one CPU buffer cost 13–30 ms per query and made the integrated GPU reader slower than CPU even with one query. Passing pointers to the loaded leaves and keeping those leaves locked through the CUDA transfer cuts CPU preparation to about 0.3 ms; host-to-GPU copies then take about 4.2 ms and kernel plus return copy about 0.4 ms for a typical query. At 32 concurrent queries, competing transfers and GPU calls raise the distance-scoring phase to about 179 ms per query, compared with about 49–59 ms on CPU. The current CUDA bridge therefore improves isolated query latency but does not support the benchmark's usual 32-query concurrency well. GPU query batching or concurrency control needs a separate trial before considering this path for production.

The CUDA bridge is optional benchmark code, activated with `HSPANN_GPU_LIBRARY`. It allocates reusable device buffers per calling thread and loads a separately compiled library. It does not change the production reader. The paired logs and result-ID files are under `/home/dev/hspann-results/paired-*` on the parked node; source and reproduction steps are in this branch.

## Paired Runpod GPU component comparison

The B200 and H100 ran the same five isolated component calculations on the same public Wikipedia embedding fixture. The fixture contains 100,000 distinct 1,024-dimensional vectors, repeated to make 65,536-, 262,144-, and 1,000,000-vector batches. Both machines produced SHA-256 `14964906edc574bdfd51a0bcd91dade3044306d136ade84aad5bf549da853829` for the fixture. Each timing is the median of five warmed runs. The table compares GPU-resident times for the largest batch; it excludes host-to-device and device-to-host copies.

The consolidated CPU/GPU timing table appears at the top of this report.


The B200 improves distance calculations by 22–25% and quantization by 11% over the H100. The H100 is faster for the two-centroid average, and one two-means iteration is effectively tied. These are kernel measurements from the same CuPy and CUDA code on different Runpod hosts; the local CPU ratios use each host's own 14-thread CPU baseline and should not be compared across hosts. The B200 host had 24 vCPUs and CUDA 13.2, while the H100 host had 26 vCPUs and CUDA 13.0. Both used the same CUDA 12.8 PyTorch image.

Transfers remain the limiting factor for a call that starts with vectors in CPU memory. For the million-vector cases, every GPU time including input and output transfers exceeded the matching host CPU time. The largest B200 transfer-inclusive time was 1.318 s for distance to 128 centers versus 0.566 s on its CPU. The resident result therefore shows arithmetic potential, not an end-to-end indexing speedup.

Both runs matched CPU labels and quantized code bytes. Distance errors on the first 256 checked rows were at most 7.75e-7, and quantization header differences were at most 2.77e-5. Complete reports are [B200](component-kernels-b200.json) and [H100](component-kernels-h100-runpod.json); the fixture generator and benchmark source are in this directory. The pods were stopped after the results were captured. Runpod billing had not posted when checked, and stopped pod disks continue to incur storage charges until the pods are deleted.

## Correctness limit

Two 150,000-vector checkpoints can lose hundreds of IDs from the first checkpoint during the second batch. The failure occurs with one insertion worker and one balancing worker, and eagerly loading saved postings and vectors only sometimes prevents it. The missing IDs retain version metadata and embeddings but have no posting in any leaf. The baseline wrapper therefore uses one checkpoint and rejects a run if posting validation fails. The multi-checkpoint writer path needs a separate fix before it can support a paired GPU comparison.

## Cost and artifacts

The SF Compute H100 node is stopped and its GPU billing has ended. Automatic top-up is off. Source, benchmark wrappers, and probes are on `codex/hierarchical-spann-gpu-poc`. Full logs, manifests, dataset hashes, and fixtures remain on the node's parked disk. The component measurements are in `component-kernels-h100-final.json`; earlier writer measurements are in `npa-timing-wikipedia-1m.log` and `npa-self-replay-wikipedia.json`. Automatic approval review rejected exporting the earlier evidence directory to external object storage because the user had not specifically authorized that payload and destination.

## Remaining bounds

The paired Runpod comparison isolates arithmetic on two GPU architectures with identical input. The writer's group-assignment distance math has an approximately 3% whole-build ceiling even with perfect acceleration, so the component speedups above do not imply similar full-build gains. Writer tree navigation is the larger distance-scoring activity: its logged distance loop accounts for about 91% of navigation time, but that loop also fetches mutable tree nodes. A separate capture and batching trial would establish how much of it a GPU could actually save.

## H100 throughput ceilings and host CPU

The larger H100 sweep measures sustained GPU-resident throughput through eight million 1,024-dimensional vectors. The input repeats the same public 100,000-vector fixture. Each measurement uses warmed repeated timing blocks for at least two seconds. Sixteen million vectors exceed this run's device-memory allowance and are skipped.

The host CPU is an Intel Xeon Platinum 8480+. The full `lscpu --json` output is saved in both reports. It describes the physical host: two sockets, 56 cores per socket, and 224 logical CPUs. Runpod advertises 28 vCPUs for this pod; the captured container CPU quota is `2380000 100000`, equivalent to 23.8 CPU cores of scheduled time. These GPU-resident sweeps do not establish a new CPU speedup ratio. The earlier paired hosts' CPU models remain unrecorded.

| Calculation | Sustained throughput | Interpretation |
| --- | ---: | --- |
| Device memory copy | About 3.03 TB/s | A control for attainable streaming memory throughput. |
| Distance to two centers | About 570 million vectors/s | About 2.34 TB/s of algorithm input traffic. |
| Distance to 128–4,096 centers | About 50–51 trillion FP32 operations/s | Larger center counts reach a similar arithmetic plateau. |
| Streaming two-centroid average, eight million vectors | 738 million vectors/s | About 3.04 TB/s of algorithm traffic. |
| Streaming two-means iteration, eight million vectors | 314 million vectors/s | Includes group assignment and centroid update. |
| Streaming quantization, eight million vectors | 410 million vectors/s | About 1.74 TB/s of algorithm traffic. |

The original centroid calculation drops from roughly 631 million vectors/s at two million vectors to 58 million vectors/s at four and eight million. A tiled streaming sum removes that drop and approaches the copy control. This demonstrates an implementation limit in the original reduction. Quantization improves from roughly 378 to 410 million vectors/s with contiguous loads; it remains below the copy control.

Effective bandwidth counts the bytes required by the algorithm and divides them by elapsed time; it is not a measurement from hardware memory counters. Sampled GPU utilization reports the fraction of time the device is active. A value of 100% alone does not demonstrate peak throughput. The reports include sampled utilization, clocks, power, and temperature. Sampled quantized code bytes match CPU output, and centroid errors remain below 1e-5.

The captured GPU reports are [the baseline sweep](saturation-h100.json) and [the streaming variants](optimized-h100.json). The additional warp quantization variant and ten-second verification remain unmeasured because the stopped H100 host has no free GPU for restart. Runpod reports no Secure Cloud B200 capacity, so the larger B200 sweep remains pending. All experiment pods are stopped.
