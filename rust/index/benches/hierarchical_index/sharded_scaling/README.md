# Sharded hierarchical SPANN: scaling experiments

These experiments ask whether a hierarchical SPANN index can serve one large corpus from many machines without losing recall or efficiency as the corpus grows. They grow one index on the FineWeb corpus from 1M to 100M vectors, measure how much search work a fixed recall needs at each size, and serve the 100M index from 1, 2, 4 and 8 machines.

## The pieces

- **The index:** a tree of routing nodes over leaves. Each leaf holds a posting list of 1-bit vector codes; full vectors for exact rerank live in a separate vector store. The writer and reader are the `two_machine_spann` and `two_machine_reader` benches in `rust/index/benches`, with the shared code in `hierarchical_index/distributed`.
- **Parts:** a reader splits the index into M parts when it opens it, by subtree, so each machine scans the leaves of one part (`--parts M`).
- **The query machine:** navigates the routing tree for a query, scans its own part, sends one scan request to each other part's machine, and merges the results.
- **The results:** `RESULTS.md`, with every run's full table beside its analysis.

## What is here

- `RESULTS.md`: the results, newest sections last. The sections from "Phase 0 of the 100B scaling study" on cover these experiments: the 1M-to-100M growth ladder, the routing stand-in for a 100B-vector tree, read-time placement, the machine-count experiments with one query machine, and the last section, where every machine coordinates queries.
- `configs/`: the search configurations each run swept (beam, rerank depth, k).
- `data/`: derived results the text cites: matched-recall frontiers, fan-out by placement, level-cap sweeps, build pass times and charts.
- `routing-standin/`: reports of the routing stand-in, which measures navigation at the depth and width of trees with up to 70M leaves.
- `tools/`: the drivers and analysis scripts that produced the results. `fleet.py` drives the machine-count experiments, `table.py` renders run tables, `fleet_compare.py` and `spread_analyze.py` compare runs, and `frontier.py` and `fanout.py` produce the matched-recall and fan-out analyses.

The scripts are a record of how the runs were driven, not a ready-to-run harness. They drive specific EC2 machines over SSH and read artifacts from a private S3 bucket. Account IDs, bucket names, machine IDs and local paths are replaced by placeholders such as `<artifact-bucket>`, and they import a lifecycle helper that is not included. Raw per-query records (several GB) are not included; every run's summary table is in `RESULTS.md`.

## Commit hashes in RESULTS.md

`RESULTS.md` and the scripts cite commits by the hashes they had when the runs were built, and the binaries' file names carry those hashes (for example `fleet-reader-8410e90190ce`). The commits were then republished with a new author email, which changed every hash but not the code. This table maps each cited hash to the published commit.

| Cited hash | Published commit | Change |
| --- | --- | --- |
| `4bdd5dc5d` | `a53971a1f` | [ENH](index): Stream large owner replies |
| `46affe999` | `9d144e307` | [ENH](index): Add saved-reader bench mode |
| `cdc549d20` | `c019fc2fe` | [BUG](index): Bound cold-pass scans and timeouts |
| `41878daf1` | `9bf826283` | [ENH](index): Save and verify owner vector stores |
| `e3ad811b5` | `c354aaf86` | [ENH](index): Add per-level and optimal recall |
| `a573782a0` | `9bac8130b` | [PERF](index): Skip hopeless candidates in scans |
| `77f48f7e9` | `8df18a676` | [PERF](index): Binary scan payloads and RPC timing |
| `0a191cc13` | `00a965476` | [PERF](index): Rerank from cached blocks directly |
| `bee07765d` | `2fa086140` | Merge branch 'perf-store' into reader-bench |
| `6f278224b` | `fc8f63d8e` | [BENCH](index): Create the tree dump's directory |
| `ae5be2555` | `888e13878` | [BENCH](index): Exact-centroid leaf ranking diagnostic |
| `8ff625165` | `5833ad213` | [BENCH](index): Per-level beam floors and distance-ratio filters in bench configs |
| `1633b24ad` | `01c4dabeb` | [BENCH](index): Validate from the corpus file when the reader has no cache allowance |
| `7979ebc6b` | `b1d7a2fda` | [BENCH](index): Restore the reference writer's balancing depth limit |
| `e2dd37908` | `70f75c7df` | [BENCH](index): Let split children inherit their split's balancing depth |
| `c9d0f4823` | `df50e9a1b` | [BENCH](index): Keep the balancing depth for structural parent splits |
| `eed153a67` | `26921ee70` | [BENCH](index): Read-time subtree placement in the reader |
| `4c9c32f3c` | `f39904949` | [BENCH](index): Tests for read-time subtree placement |
| `28b783049` | `d53b496ac` | [BENCH](index): Allow single-owner saves that never place |
| `c1eef3ca5` | `bfdb327a4` | [BENCH](index): Reuse the corpus hash receipt for writer input |
| `775ba85d5` | `990385746` | [BENCH](index): Save the owner's current root for an unplaced generation |
| `53a26ad03` | `32f6f69c6` | [BENCH](index): Configurable save operation timeout |
| `8df9c5d0b` | `1ef0a44c1` | [BENCH](index): Write and verify vector stores in parallel |
| `997b011be` | `370162e3f` | [BENCH](index): Default to 16 vector store writers and retry failed segments |
| `fa803339b` | `cde6585ba` | [BENCH](index): Retry vector block loads and remote scans |
| `df4932e9f` | `1d05b1f65` | [BENCH](index): Persistent remote scan workers and parallel local navigation |
| `8410e9019` | `522790dc3` | [BENCH](index): Navigate in parallel only when cores are idle |
| `8366bca9a` | `a543fd917` | [BENCH](index): Load pass, pass barrier and per-query finish times |
