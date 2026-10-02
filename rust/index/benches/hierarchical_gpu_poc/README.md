# Hierarchical SPANN GPU proof of concept

## Isolated component kernels on H100

The component benchmark measures the arithmetic used by the index at sizes large enough to keep an H100 busy. It starts with a captured 100,000-vector Wikipedia split, then repeats those real 1,024-dimensional vectors to make batches of 65,536, 262,144, and 1,000,000. Repetition preserves vector values and dimension but does not simulate independent data points. It measures distances to 2 and 128 centers, averages two labeled groups, performs one two-means assignment and update, and produces 1-bit codes with their four header values.

CPU distance and centroid calculations use NumPy backed by OpenBLAS with 14 threads. CPU quantization uses a fused C++ loop with OpenMP across 14 threads, matching the 1-bit layout and header formulas. The GPU uses CuPy matrix operations and a fused CUDA quantization kernel. The resident GPU column times operations with inputs already on the device using CUDA events. The transfer column times copying inputs to the GPU, computation, and copying the output to CPU memory. Each number is the median of five warmed runs. Distances are checked on 256 rows, while quantized codes, clustering labels, and centroids are checked across all rows. The C++ reference mirrors the Rust quantizer's arithmetic but is not the Rust function itself.

```sh
g++ -O3 -march=native -fopenmp -shared -fPIC -std=c++17 rust/index/benches/hierarchical_gpu_poc/quantize_cpu.cpp -o /home/dev/hspann-results/libquantize_cpu.so
OMP_NUM_THREADS=14 OPENBLAS_NUM_THREADS=14 /home/dev/hf-venv/bin/python rust/index/benches/hierarchical_gpu_poc/component_kernels.py /home/dev/hspann-results/split-capture-wikipedia-01/split-fixtures/split-0000.bin --sizes 65536 262144 1000000 --repetitions 5 --cpu-quant-library /home/dev/hspann-results/libquantize_cpu.so --output /home/dev/hspann-results/component-kernels-h100-final.json
```

The captured fixture and JSON report remain on `hspann-gpu-poc-01` under `/home/dev/hspann-results`. The component script can run on another CUDA GPU for the next architecture comparison with the same fixture and CPU thread settings.

See [measured runs](RESULTS.md) for the 300,000-vector and one-million-vector baselines, GPU probes, correctness limit, and spend.

The reader scoring replay opens an existing saved index, loads an exact-neighbor query cache, and captures one query's selected posting codes and CPU scores. `replay_reader_score.py` checks the same codes on the GPU with one launch per leaf and with all leaves in one launch. The fixture remains on the benchmark host. The GPU timing includes transfers and launches, while preparation of the flat batched input happens before timing.

```bash
(cd rust && cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- --dataset wikipedia-en --checkpoint 1 --checkpoint-size 1000000 --threads 14 --balance-threads 1 --write-navigation fp --fp-npa --num-queries 1 --save-dir /results/index-wikipedia --resume --ground-truth-cache /results/gt-wikipedia.bin --capture-reader-score /results/reader-query.bin --recall-tau-values 2.0 --recall-rerank-vectors 8 --compute-gt-clusters false)
python3 rust/index/benches/hierarchical_gpu_poc/replay_reader_score.py /results/reader-query.bin --repetitions 9
```

The paired search uses the same saved index and query cache for both runs. Compile the optional CUDA library on the H100 host, then run the benchmark once without `HSPANN_GPU_LIBRARY` and once with it. `--recall-results-path` saves ordered IDs so the two runs can be compared directly. Set `--recall-threads` to the desired number of concurrent queries; the measured run used 100 queries, tau=2, and rerank=8.

```bash
nvcc -O3 -std=c++17 -arch=sm_90 -shared -Xcompiler -fPIC rust/index/benches/hierarchical_gpu_poc/gpu_score.cu -o /results/libhspann_gpu_score.so
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- --dataset wikipedia-en --checkpoint 1 --checkpoint-size 1000000 --threads 14 --balance-threads 1 --write-navigation fp --fp-npa --num-queries 100 --recall-threads 1 --save-dir /results/index-wikipedia --resume --ground-truth-cache /results/gt-wikipedia.bin --recall-results-path /results/reader-cpu.tsv --recall-tau-values 2.0 --recall-rerank-vectors 8 --compute-gt-clusters false
HSPANN_GPU_LIBRARY=/results/libhspann_gpu_score.so cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- --dataset wikipedia-en --checkpoint 1 --checkpoint-size 1000000 --threads 14 --balance-threads 1 --write-navigation fp --fp-npa --num-queries 100 --recall-threads 1 --save-dir /results/index-wikipedia --resume --ground-truth-cache /results/gt-wikipedia.bin --recall-results-path /results/reader-gpu.tsv --recall-tau-values 2.0 --recall-rerank-vectors 8 --compute-gt-clusters false
cmp /results/reader-cpu.tsv /results/reader-gpu.tsv
```

For MS MARCO, use `--dataset ms-marco` and its matching index and query cache. A different order for equal-distance results can make `cmp` fail even when both top-100 ID sets and recall match; compare sets per query before interpreting a difference.

The CPU baseline measures the current writer on the same host as the GPU trials. Each run saves the exact command, source revision, dependency lockfile hash, ordered dataset file hashes, host details, and full benchmark log in a new output directory.

The writer distance probe times only the full-precision comparisons used to decide whether vectors need reassignment after a split. It runs a fresh single-checkpoint build with 14 insertion workers, one balancing worker, and no recall queries. The replay uses an existing captured split fixture, reconstructs group centers from those vectors, and times batched CPU and GPU decisions. It checks the GPU decisions against the CPU decisions and a direct distance calculation.

```bash
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized -- --dataset wikipedia-en --checkpoint 1 --checkpoint-size 1000000 --threads 14 --balance-threads 1 --write-navigation fp --fp-npa --num-queries 0 --save-dir /results/index-wikipedia-npa-timing --verify-valid-postings --compute-gt-clusters false
python3 rust/index/benches/hierarchical_gpu_poc/replay_npa.py /results/split-fixtures/split-0000.bin --sizes 2048 4096 100000 --repetitions 9
```

The wrapper runs one 300,000-vector checkpoint by default. It uses 14 insertion workers and one balancing worker, full-precision writer navigation, full-precision nearest-posting assignment, and posting validation. Two Wikipedia runs at this size kept all 300,000 IDs reachable before commit and after reopen.

Multi-checkpoint runs currently fail the posting validation gate: hundreds of IDs from the first checkpoint disappear during the next batch. Eagerly loading saved postings and vectors before the next batch sometimes helps but does not fix the loss. The POC uses one checkpoint for paired CPU and GPU trials until this writer bug is fixed. On MS MARCO at 150,000 vectors, balancing with 14 workers lost 302 reachable IDs; balancing with one worker kept all 150,000 IDs.

The wrapper requests up to 1,000 recall queries. The benchmark needs either a ground-truth file or `--brute-force-gt true` to evaluate them. Read the log's query count before treating recall as an acceptance result.

## Run a CPU baseline

Commit the source first. Stage the dataset shards and exact-neighbor reference on the benchmark host, then list every shard the benchmark will read in load order. The shard paths are recorded and hashed; the benchmark itself resolves files through the Hugging Face cache, so verify that the listed files are the same cache objects before accepting a result.

```bash
python3 rust/index/benches/hierarchical_gpu_poc/run.py \
  --output-dir /results/wikipedia-cpu-01 \
  --dataset wikipedia-en \
  --threads 14 \
  --shard /data/en/0000.parquet \
  --shard /data/en/0001.parquet \
  --shard /data/en/0002.parquet \
  -- --brute-force-gt true --compute-gt-clusters false
```

Use `--dataset ms-marco` for MS MARCO v2. Add benchmark flags after `--`, for example `-- --brute-force-gt true --compute-gt-clusters false`. The output directory must be new. A failed run retains its log and exit code in `manifest.json`.

The wrapper is a baseline capture tool. It does not allocate GPU nodes, prepare ground truth, or prove that a supplied shard path matches the file resolved by the benchmark. Those checks remain part of dataset staging before paired CPU and GPU trials.

## Capture split inputs

Use a separate run with `--capture-splits 16` to save up to 16 real oversized leaves under `split-fixtures/`. Capture writes files during balancing and changes initialization to a recorded seed, so its timings are not a CPU baseline. The normal writer path remains unchanged when capture is off.

Each binary fixture starts with the eight bytes `HSPNSPL1`, followed by little-endian `u32` point count, `u32` dimension, `u64` seed, `u32` leaf ID, and `u32` tree depth. Each point then contains a `u32` vector ID, `u32` version, and `dimension` little-endian `f32` coordinates. The seed reproduces the CPU split's four initialization trials through `split_seeded`; replay code must preserve point order and the recorded distance function from the run manifest.
