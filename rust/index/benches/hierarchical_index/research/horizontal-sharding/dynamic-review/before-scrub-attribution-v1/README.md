# Attribute before-scrub preparation

This diagnostic measures one existing construction path without removing or reordering work. It uses the accepted lazy owner-evaluation policy and the same retained 8,192-vector prefix. It adds only aggregate counters and two nonoverlapping coordinator timing spans. The reserve, parallel-add, parallel-balance, and re-shard direction remains future work.

The first span starts before preparing the `before_scrub` trace. It includes the Physical request, copying physical ID/version pairs, one authoritative Reference query per physical row, building the version list and JSON, and recording or discarding that trace. It ends before the semantic scrub begins. The second span covers only `rows_owned(neighbor, true)`, including the owner request, actual current-version scrub, and returned live references. It excludes center copying before that call and the subsequent neighbor-begin trace and eligibility evaluations. These spans do not overlap each other; both are nested within balance time. They include counter and clock overhead and are not CPU fractions.

Counters record trace preparations, Physical requests and rows, attempted and successful Reference queries, semantic scrub calls, and returned live rows. The native output exposes cumulative snapshots after each logical balance cut. Subtract consecutive snapshots for per-cut values. Addition uses the existing register-only path, so this workload performs the measured work during balancing. The observer owns no per-ID collection or unbounded map, never resets during work, and defaults off.

The fixed-decision local test compares the same construction tape with profiling off and on. Actor-command counts, topology, locations, event counts, saved postings, codes, versions, full vector bits, and complete saved membership remain equal. It verifies that every physical row produces a successful Reference query and that Physical-request and semantic-scrub call counts match. Separate local fake subprocess tests check the profile echo, accounting rejection, single-arm scope, and unchanged time limits. An actual local native entry-point check uses synthetic 64-by-32 data; it is correctness evidence only.

## Execute the bounded diagnostic

The plan runs one lazy authoritative arm on 8,192 retained 768-dimensional vectors with cuts at rows 4,096 and 8,192, full-precision navigation and neighbor predicates, and the existing settings. It permits at most 30 seconds for that native child and 30 seconds of diagnostic compute. Input preparation and the offline build are separately charged. Metadata is bounded to 50 MiB after execution; saved-store bytes are reported separately. This package makes no physical-I/O, large-working-set, speedup, or cross-host claim.

Use the existing offline build recipe against the pinned source and dependency cache. It verifies 935 regular source files and the safe relative IDL symlink, requires 20 GiB free, uses two build jobs, and retains the exact optimized compiler artifact. It starts no machine and downloads nothing.

```sh
python3 compile-native.py --source /mnt/spann/before-scrub-attribution-v1/source --target /mnt/spann/hardware-scorer-v1/target --toolchain /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin --output /mnt/spann/before-scrub-attribution-v1/build
```

Fill `before-scrub-spec.template.json` with the actual binary descriptor and the externally fixed deadline. The template binds the retained restored input and its receipt. Execute the accepted single arm only after the coordinator supplies the resource and guard admission:

```sh
python3 run-before-scrub.py --spec /mnt/spann/before-scrub-attribution-v1/before-scrub-spec.json
```

Local ARM development-profile results do not establish Linux execution or performance. Previous epochs and failures remain unchanged. No cloud start, upload, or execution occurs during this preparation.
