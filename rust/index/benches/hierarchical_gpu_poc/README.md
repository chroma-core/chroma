# Hierarchical SPANN GPU proof of concept

The first experiment measures the current CPU writer on the same host that will run GPU trials. Each run saves the exact command, source revision, dependency lockfile hash, ordered dataset file hashes, host details, and full benchmark log in a new output directory.

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
