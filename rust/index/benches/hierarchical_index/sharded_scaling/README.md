# Sharded hierarchical SPANN: scaling experiments

These experiments ask whether a hierarchical SPANN index can serve one large corpus from many machines without losing recall or efficiency as the corpus grows. They grow one index on the FineWeb corpus from 1M to 100M vectors, measure how much search work a fixed recall needs at each size, and serve the 100M index from 1, 2, 4 and 8 machines.

## The pieces

- **The index:** a tree of routing nodes over leaves. Each leaf holds a posting list of 1-bit vector codes; full vectors for exact rerank live in a separate vector store. The writer and reader are the `two_machine_spann` and `two_machine_reader` benches in `rust/index/benches`, with the shared code in `hierarchical_index/distributed`.
- **Parts:** a reader splits the index into M parts when it opens it, by subtree, so each machine scans the leaves of one part (`--parts M`).
- **The query machine:** navigates the routing tree for a query, scans its own part, sends one scan request to each other part's machine, and merges the results.
- **The results:** `RESULTS.md`, with every run's full table beside its analysis.

## What is here

- `RESULTS.md`: the results, newest sections last. The sections from "Phase 0 of the 100B scaling study" on cover these experiments: the 1M-to-100M growth ladder, the routing stand-in for a 100B-vector tree, read-time placement, and the machine-count experiments.
- `configs/`: the search configurations each run swept (beam, rerank depth, k).
- `data/`: derived results the text cites: matched-recall frontiers, fan-out by placement, level-cap sweeps, build pass times and charts.
- `routing-standin/`: reports of the routing stand-in, which measures navigation at the depth and width of trees with up to 70M leaves.
- `scripts/`: the drivers and analysis scripts that produced the results. `fleet.py` drives the machine-count experiments, `table.py` renders run tables, `fleet_compare.py` and `spread_analyze.py` compare runs, and `frontier.py` and `fanout.py` produce the matched-recall and fan-out analyses.

The scripts are a record of how the runs were driven, not a ready-to-run harness. They drive specific EC2 machines over SSH and read artifacts from a private S3 bucket. Account IDs, bucket names, machine IDs and local paths are replaced by placeholders such as `<artifact-bucket>`, and they import a lifecycle helper that is not included. Raw per-query records (several GB) are not included; every run's summary table is in `RESULTS.md`.
