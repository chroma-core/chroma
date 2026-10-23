# Tiny build-speed diagnostic

This package compares construction mechanisms on the same retained prefix. It runs four reference combinations of serial or parallel addition and balancing, followed by one single-owner authoritative build. Every cell uses 8,192 existing 768-dimensional vectors, explicit full-precision navigation and neighbor predicates, and balance cuts at rows 4,096 and 8,192. The parent input is the already restored immutable two-million-row canonical file; no corpus transfer or new data is required.

The native command records addition and balancing separately. Authoritative addition includes vector-batch creation, commit, flush, routing, and actor registration; it is not a registration-only timer. Existing reference counters split navigation, registration, split, merge, reassignment, and scrub work. The authoritative vector-read timer covers reader lookup/open, point retrieval, owned copying, and pin clearing. Diagnostic observation overhead is included and is not independently quantified. The reference threading combinations have independent stochastic trees, so saved tree equality is not assumed; each output must have exactly one current membership for every expected ID.

Vector-reader counters measure actual point requests, pin hits, provider obtains/hits/misses, successful encoded callbacks, and storage-origin Arrow decode attempts/successes. They exclude root opening and provider-cache reconstruction. Separate successful LocalStorage return counters include root and block paths. Both are cumulative snapshots, and some time spans overlap. They are neither physical storage traffic nor CPU fractions. A fresh provider after writes can have warm blocks. The tiny working set can fit the cache and cannot reproduce a large working-set failure by itself. The local test explicitly demonstrates both warm reuse and real decoding after a test-only provider clear; the experiment does not clear the build cache.

The harness limits each direct child to 30 seconds and all five children to 120 seconds, also respecting an externally supplied absolute deadline. Build time is separate. It stops on the first failed or timed-out cell and retains its output. It records evidence size and rejects results exceeding 50 MiB; this is an observed output check rather than a filesystem quota. Native profile maps have a 256-block limit per reader; retired reader snapshots are capped, and overflow invalidates the diagnostic. Input prefix memory is bounded to 25,165,824 raw bytes plus its decoded vector representation. Provider caches and the operating-system cache remain separate from those bounds. The source remains local actor execution, with no cross-host performance claim.

## Build and execution

Extract the source archive into a fresh directory, preserving `rust/types/idl -> ../../idl`. Use the existing offline toolchain and dependency cache. The recipe verifies all source hashes and the relative symlink, requires 20 GiB free, builds with two jobs and optimization level 3, and retains the exact emitted compiler artifact. It does not download dependencies or start resources.

```sh
python3 compile-native.py --source /mnt/spann/build-speed-diagnostic-v1/source --target /mnt/spann/hardware-scorer-v1/target --toolchain /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin --output /mnt/spann/build-speed-diagnostic-v1/build
```

Fill `diagnostic-spec.template.json` with that actual binary descriptor, a fresh output directory, and the externally fixed diagnostic deadline. The template already binds the retained canonical file and successful restore receipt. Its default eight configured threads match the existing eight-vCPU machine; a different approved machine requires an explicitly reviewed thread value. Neither this package nor its local tests authorize a machine start, upload, or execution window.

```sh
python3 run-diagnostic.py --spec /mnt/spann/build-speed-diagnostic-v1/diagnostic-spec.json
```

Local evidence includes an observer-off/on exact-result test, warm-provider and cold-decode checks, the five native commands on synthetic 64-by-32 data with saved membership census, and fake subprocess harness checks. These ARM development-profile runs prove local correctness only. The initial zero-decode expectation and temporary-path alias assertion failures are retained. No real corpus is present in the local evidence, and no Linux experiment or speed claim is made.
