# Sandbox Linux pilot

This pilot checks whether two independent index workers can build and serve one complete collection from a separate request driver. All machines are new sandbox instances in account `079819354890`; the existing Lyon benchmark machine is excluded.

The actual two-host correctness gate passes. It builds two tiny shards, checks exact IDs and distances, appends a second generation, verifies that old endpoints retain their original generation, and rejects missing shards, stale shards, and incomplete publication. Its local raw evidence is in `results/driver/two-host-smoke/`.

The Linux executable uses Rust 1.92.0 and the optimized Cargo bench profile. `compiler.txt` records the compiler; `binary.json` records the executable checksum. `initial-build-source.json` records the source archive used for compilation and the repair of its omitted tracked directory symlink. `source.tar.gz` is a later complete snapshot, including the measurement utilities and tracked symlinks; `source-manifest.json` records its checksum. No commits or GitHub writes are required.

Synthetic vectors are independent standard-normal float32 samples with 1,024 dimensions and seed 1729. Queries follow the corpus in the generator stream and are excluded from indexing. Every synthetic result must retain the **SYNTHETIC** label. These vectors validate mechanics and cannot establish performance on a real embedding corpus.

The 100,000-vector synthetic build completes across two hosts in 13.44 seconds with two fixed logical shards and 32 balance threads per worker. Continuous host counters are unavailable for this first build, so this duration is preliminary mechanics evidence. Its construction configuration remains in `synthetic100k-jobs.json`. The one-million-vector synthetic build collects host and process counters through `measured_worker.py`.

The real-data pilot uses separate calibration and held-out test queries. `query_pilot.py` first measures three search settings on calibration queries, selects settings meeting the named recall targets when attainable, then measures held-out queries. The measured cache regime is a warm batch after a calibration query pass. Small query pools support pilot observations; they do not support a broad p99 or service-capacity claim.

The fleet shutdown guard preserves `/mnt/spann/results` to root storage before its deadline. Checkpoints and prepared data outside that directory require explicit preservation before stopping the machines.
