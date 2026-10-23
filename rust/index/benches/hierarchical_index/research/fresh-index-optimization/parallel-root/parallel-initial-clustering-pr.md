## Summary

A new index begins with one posting list. Splitting it chooses two group centers, called centroids, repeatedly scores every vector against those centers, and recomputes their means. The tree exposes independent balancing jobs only after this first split creates child leaves.

This experiment evaluates the first root split’s independent point distances concurrently. Distance totals and centroid accumulation keep the input order and serial floating-point arithmetic. Later splits use the existing serial routine while ordinary balance workers process independent leaves.

The worker pool is created only for that first split and drops before child splitting and neighbor reassignment. Its size is the smaller of the requested initial-split cap and the balancing phase’s worker budget. Reopened writers never claim this startup operation. If creating the pool fails, the operation reports the failure and uses the serial routine.

## Comparison setting

The default remains serial. Enable the experiment with `--initial-split-threads 32` or `--initial-split-threads 64`; the existing `--balance-threads` value still bounds the actual worker count. Dataset, metric, posting order, versions, and checkpoint boundaries retain their existing behavior.

This PR is independent of the earlier-startup-balancing experiment. Its existing startup cadence remains 100k-vector add batches through the first 1M vectors.

## Validation

A seeded core comparison checks exact centroid bits, labels, versions, and group order for L2, inner product, and cosine distances using 1, 2, and 4 workers. It covers empty, singleton, small, coincident, and mixed inputs, verifies finite centers and complete vector coverage, and checks the existing minimum partition size.

Both hierarchical writer/navigation/persistence harnesses pass: 65 test executions. New tests cover the worker cap, one-time root claim, full reachable-vector preservation through a fresh parallel build, and disabling the claim after reopening. The actual benchmark target compiles. Changed-file formatting and diff checks pass.

## Performance status

Benchmarks are pending. No EC2 work or timing runs are performed for this experiment. Pool startup cost, the remaining serial accumulation/quantization work, full build time, memory, and recall need comparison before claiming an improvement. The existing random seed selection remains random; exact serial/parallel equivalence is checked with identical seeds in tests.

## Code reference

The shared split utility gains a caller-owned pool entry point while retaining its serial entry point (`rust/index/src/spann/utils.rs`). Rayon moves from a development dependency to a regular dependency for that helper. Startup claiming and worker budgeting live in the prototype writer; the cap is exposed in its benchmark configuration and CLI.
