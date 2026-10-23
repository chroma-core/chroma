# Clustering uses the same kernel in both measured implementations

Both implementations call the same full-precision two-way clustering kernel. There is no different optimized reference kernel, iteration limit, sampling rate, centroid accumulation method or within-split worker pool to copy into the distributed path. The complete clustering source file is byte-identical in the frozen measured-reference source and the profiled distributed source. This rules out a source-level kernel mismatch; it does not establish equal clustering work or equal elapsed time.

The early nine-second sample places substantial running CPU in clustering distance calculations and centroid accumulation. It does not cover the entire balance or time blocked on synchronization. The 49.60% distance row and 40.16% split row are instruction-location shares; caller percentages must not be added again. No end-to-end attribution follows from their sum.

## What the common kernel does

A split receives an ordered list of shared full-vector handles and returns two centers and two groups. Both paths pass L2 distance and full-precision f32 vectors. Vector IDs and the unused version field are carried through the groups; they do not enter clustering decisions.

| Property | Both measured paths |
| --- | --- |
| Initialization | Four randomly chosen center pairs; each pair is evaluated against every input vector. |
| Main iteration | At most 128 iterations, with two full-vector distances per input per iteration. No row sampling occurs. |
| Centroid updates | Two newly allocated coordinate-sum arrays per iteration, one pass over all vectors, then division by group counts. Both use the same indexed addition loops; this audit does not infer whether the compiler vectorizes every loop. |
| Stopping | Relative center movement below f32 epsilon, or four iterations without objective improvement. |
| Minimum group size | Each group receives at least one quarter of the input rows, with a possible additional centroid-accumulation pass after moving rows. |
| Parallelism | Each call is serial. Multiple independent leaf jobs can call the kernel concurrently. A 32-worker setting does not make the initial one-leaf split use 32 cores. |
| Randomness | Both use the same `thread_rng` initialization and rand 0.8.5 dependency. Seeds and concurrent job ordering are uncontrolled in the retained builds. |

The nearby constants for three initialization iterations, 100 main iterations and sampled k-means belong to a separate routine. The writer and distributed leaf splitter call `spann::utils::split`, whose actual loop limits are four initial candidate pairs and 128 main iterations. They do not call that sampled routine.

For N input rows and I completed main iterations, this kernel performs 8N initialization distance calls and approximately 2NI assignment distance calls, plus up to three center-distance checks per iteration. Centroid accumulation also scales with N × dimensions × I. Equal row count or committed split count therefore does not imply equal clustering work when inputs, seeds, convergence or failed preparations differ.

## Consequential differences around that kernel

The distributed wrapper creates a u64 ID lookup and temporary u32 slot labels, then maps output slots back to persistent IDs. The reference passes its existing u32 IDs directly. Both preserve embedding order and use `Arc<[f32]>` handles, so slot labeling itself does not change clustering. The distributed wrapper adds allocations and linear ID-mapping passes; there is no extra full-vector coordinate copy inside that conversion. Both copy the returned centers into node-owned arrays.

The reference keeps its source leaf claimed while it collects vectors, clusters and prepares replacement codes. The distributed splitter releases the source node read lock after taking its image and ID snapshot, performs clustering and code preparation, then attempts protected installation. A changed dependency can reject the prepared split and enqueue another attempt. That is a concrete route to more clustering calls or input-row work per committed split. Its frequency and cost in the measured run are unknown. Source: distributed `construction.rs:344–386` and measured-reference `writer/writer.rs:1234–1265`.

The reference retains returned embedding handles for posting-code preparation. The distributed wrapper discards those handles when extracting IDs and fetches vector handles again in `fresh_leaf`. This is additional wrapper/vector-access work, not a different clustering kernel. Both generate one-bit posting codes outside the kernel.

The measured reference assigns eligible leaf jobs from a joined round's list and discovers new leaf work after workers join. The distributed scheduler can run newly queued jobs immediately. Consequently the sequences of split inputs, concurrent membership changes and random draws can differ even with identical vectors and construction settings. This audit does not equate their independently constructed trees.

The distributed `distances` counter excludes clustering. Its increments occur in owned routing-child scoring and root-leaf navigation, while clustering and neighbor eligibility call the metric without updating that counter. The common kernel has no access to the owner's atomic counter. The 7,469,370 reported navigation distances therefore cannot normalize clustering CPU. The reference already records aggregate split sizes and a clustering timer, but the distributed committed-split count alone does not count rejected clustering preparations or iterations.

## Smallest next diagnostic

Use one fixed prebuilt state with two neighboring oversized leaves, each containing 2,049 of the same saved 4,098 vectors at 768 dimensions. This is the minimum two-leaf condition above the configured 2,048 split threshold. Keep L2, branching 100, merge threshold 512, input order, old centroids and 32 configured workers fixed; report that only two split jobs initially exist. No size sweep or larger index is needed.

Count every clustering invocation, input rows, dimensions, chosen initialization pairs, actual iterations, distance calls and centroid-addition work. Bind each invocation to its source snapshot and report whether its prepared split commits or is rejected and retried. Separate kernel preparation from wrapper lookup/code generation and installation. Compare clustering calls and row-iterations per committed split between the paths. For strict decision parity, a diagnostic entry point can supply identical four initialization pairs to the common kernel without changing its arithmetic; otherwise record seeds and do not treat different iteration counts as implementation speed differences.

The first decision is whether extra attempted work explains a discrepancy. If work and initialization match, compare wrapper cost and compiled kernel CPU before changing algorithms. Since the kernel source is already identical, benchmarking two supposedly different clustering kernels is not justified by this audit. This is a proposal only; no source change or benchmark runs here.

## Exact provenance and references

The measured reference is the retained `single_shard_attribution` executable with SHA256 `6ff2767b809716df3994a5b5a76a38d7fcc267f69f4336cbc56a04d41ba18885`. The 100K reference preflight in `operations/window-epoch2/100k-baseline-reference/run-receipts.json` and the 3M reference preflight in `operations/window-epoch5/3m-reference/run-receipts.json` identify that same binary. Its `reference.binary.json`, `reference.cargo-artifact.json` and `build-reference.command.json` receipts bind it to `/home/ubuntu/spann-single-shard-3m-attribution-v1/epoch1/reference`, the ordinary writer branch, release optimization level 3 and no owner cfg. These receipts are retained under `single-shard-3m-attribution-v1/operations/window-epoch2/evidence-extra/run/`.

The reference frozen archive hash independently checks as `6814ae6f0bf7259641a7884c10f02bbccd91e5c33dadb99132ded627f68bc137`. Its archive contents, file manifest, recorded remote source paths and current matched-reference checkout agree on the writer hash `725b9a88bff395b5bb07b2ed801889b30443c9796886a64b6acd7cf3f277abea`. This is the inspected reference writer; the sibling single-shard checkout's writer hash is `96ee830f…` and is not substituted for it.

The epoch11 preflight identifies profiled distributed binary SHA256 `57f6baa4b09749dfede42247a3bf728748151c2df1a3b9ae5754877541f27de1`, matching `operations/window-epoch9/linux-build.json`. That build binds to the epoch9 source archive manifest, release optimization level 3 and `distributed_owner` bench cfg. Both build receipts use Rust 1.92.0 and `-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1`. The cfg selects the benchmark path, not another clustering implementation. Cargo manifest differences add the owner-lock feature and benchmark support; the clustering dependency and distance source match. Lockfiles differ, so arbitrary binary identity is not claimed.

Both frozen source manifests and archived source contents identify `rust/index/src/spann/utils.rs` as SHA256 `6a70657ddc7c742ca080003254f5658cfb8822b98756d93702fcabc7dad3065d`. Both identify the distance reexports as `d52a2c1e89d0c54f976843d540bec935383009717cad5b11fc31a19d5d788580` and AVX512 implementation as `500776e1ce7541c5c9f50729e3b17ef29308ddfde13b449f4e4fa36fdf817e6e`. The profile observes that AVX512 Euclidean implementation in the distributed executable. Identical source and build settings support kernel parity; this audit does not compare machine-code bytes or claim a matched reference CPU profile.

Local roots are `/private/tmp/chroma-hierarchical-spann-sharding/matched-reference` and `/private/tmp/chroma-hierarchical-spann-sharding/single-shard`. Source references are `rust/index/src/spann/utils.rs:618–806`, distributed `construction.rs:294–337`, measured-reference `writer/writer.rs:1230–1300` and `:1174–1207`, and distributed `engine.rs:688` and `:1576` for counter increments. Campaign evidence is under `/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/`; the profile and its coverage limits are retained in `two-machine-v1/operations/window-epoch11/100k-profile/` and `local-tests/OWNER-LOCAL-PROFILE-RESULT.md`.
