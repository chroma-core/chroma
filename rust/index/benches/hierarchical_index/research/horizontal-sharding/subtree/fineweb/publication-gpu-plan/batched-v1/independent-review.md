**The interval math appears sound, but I found two concrete source defects. CUDA correctness and speed remain unestablished.** This review authorizes no hardware launch.

All seven canonical files match their frozen archive entries and manifest hashes. The archive, PLAN.md and actual CPU-test log hashes also match. The four preserved numeric dependencies additionally match the producer hashes in both completed 1M and 10M GPU integration reports.

**Source findings**

1. **P1 — A chunk iterator can produce empty references while reporting complete corpus coverage.** In [reference_batched_engine.py:26](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/reference_batched_engine.py:26), preflight consumes `chunks`; line 38 iterates it again. A generator is exhausted before scoring. Lines 79–80 then return empty answers with `corpus_rows` taken from preflight.

   I reproduced this entirely in memory: four declared rows, k=2, zero scorer calls, zero full-score checks, empty IDs/distances, and reported `corpus_rows=4`. List inputs work. Materialize or explicitly reject one-shot iterators, count rows actually scanned, and require k output entries and the expected full-score-check count before returning.

2. **P2 — The NumPy filter retains entire sorted matrices behind its top-k slices.** [reference_batched_filter.py:16](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/reference_batched_filter.py:16) returns a view of the full sorted allocation. Every query batch therefore retains storage proportional to corpus-block size rather than k.

   For 32 queries, 32,768 rows and k=100, I measured 25,600 logical bytes backed by 8,388,608 bytes. Across 11,000 queries, comparable backing storage approaches 2.9 GB. This contradicts the bounded persistent-state requirement for the CPU backend. Copy the retained slice into compact storage. The CUDA `topk` implementation does not have this particular NumPy-view defect.

Neither finding demonstrates a mathematical error in candidate pruning.

**Numerical assessment**

The running threshold is conservative for each query. Every current upper endpoint enters the kth-upper calculation before selection. That threshold is at least the kth exact distance of the processed prefix, which is at least the final corpus kth distance. Thus a row whose lower endpoint exceeds the threshold cannot enter the final top-k. Inclusive comparison preserves cutoff ties; prefixes smaller than k retain everything.

The vectorized error calculation preserves the scalar device engine’s norm-product bound. Swapping the query norm and maximum source norm preserves their product and retains the existing inflation, overflow rejection and underflow allowance. Relative to `reference_gpu.py`, the block-maximum norm can produce looser bounds than individual vector norms; that can increase candidate retention without compromising completeness.

Endpoints, errors and threshold state remain float64. The source rejects nonfinite and subnormal inputs, checks norm-product overflow before scoring, and rejects nonfinite scores. Exact reranking uses the preserved float64 `einsum(..., optimize=False)` calculation and orders equal distances by ascending global ID. This is the preserved numerical definition of “exact,” rather than arbitrary-precision arithmetic.

Candidate retention is uncapped. Coordinate validation checks range, uniqueness and row-major ordering. Source mappings close in `finally`, and retained vector materialization processes one query at a time.

**Evidence and limits**

The hash-verified log records 12 new tests and 32 combined passes. I independently ran 14 existing tests that require no file writes; all passed. I also ran 45 in-memory full-oracle cases covering query counts 1/3/31/32/33, multiple corpus-block sizes, chunk boundaries and k values through the complete corpus size. All matched exact IDs and float64 distances.

I did not rerun the filesystem-writing tests under this read-only request. No CUDA execution, GPU/cloud commands, instance launches or file edits occurred.

**Unexecuted hardware checks and required gates**

The CUDA adapter at [reference_batched_engine.py:85](/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/reference_batched_engine.py:85) configures the preserved scorer and runs batched endpoint conformance. It does **not** itself execute the preserved scoring pathology suite, admit memory, verify input hashes or preserve failure reports. Those remain explicit responsibilities of the future wrapper.

Before any separately authorized hardware pilot:

1. **Freeze the complete executable job.** Address the findings or explicitly enforce their safe preconditions, then freeze and review the resulting source. Bind the wrapper, all executed numeric methods, input/output helpers, runtime lock and oracle implementation. Verify actual imported paths. The current callable checks sizes and continuity, not content hashes.

2. **Bind every input before CUDA initialization.** Verify source manifests, chunk hashes/counts, the byte-identical 1M prefix of 10M, both 200-query references, frozen embedding bytes and text identities. Require exact coverage and disjoint 200 integration, 1,000 calibration and 10,000 test identities.

3. **Require fresh operational admission.** Verify sandbox identity through the Cops-managed configuration, current budget/prices, compatible runtime and independent resource availability. Use the planned single new L4, encrypted root ≥150 GiB, free scratch ≥64 GiB and RSS cap of 16 GiB. Enforce the absolute 60-minute deadline with an independent termination guard and 300-second preservation reserve. Staging/install must fit 900 seconds; initialization/probes 180 seconds; known-answer comparisons 600 seconds. No retries extend the deadline. Preserve the 34-GiB unique-payload and separate 34-GiB retry ceilings, with request/retry accounting.

4. **Establish actual CUDA arithmetic.** Run the preserved strict-FP32 pathology probes through the actual batched scorer path, including the internal low-precision adversary. Require exact endpoint-bit, running-upper and selected-coordinate agreement. Exercise batches 1/3/32, production dimensions, corpus/query tails, cancellation, adjacent cutoffs, all ties and **different error bounds within one batch**. The current conformance histories use equal errors across queries within each step.

5. **Establish workspace empirically.** The recomputed allowance is **2,742,754,304 bytes**, requiring at least **5,485,508,608 free device bytes** under the half-free-memory rule. This is an allowance, not a vendor-workspace bound. Record allocator and physical-device peaks plus host RSS, including all-tie behavior. Preserve and abort on OOM; never trim candidates.

6. **Require complete numerical acceptance.** Compare both engines against all 200 independent answers on both corpora. Require identical ordered IDs and finite distances. Make the distance criterion explicit: the preserved launcher accepts absolute error ≤`1e-12`, whereas PLAN.md says every distance must match. Require bitwise equality between scalar and batched outputs using the same rerank method; document any oracle tolerance separately. Full-score checks must cover exactly 3,000,000 and 30,000,000 scores per corresponding scan, including rejected rows.

7. **Admit larger pools only from measured complete costs.** Measure the 32/128/256-query pilots under identical cache policy. Reserve independent full-prefix float64 oracles for at least three queries per published pool. Use the larger measured per-query rate with the 1.5 margin, adding validation, oracle, output/archive and preservation costs. Publish only complete, hash-bound pools; restore evidence before termination and verify root-volume deletion.

**Performance remains a hypothesis.** Batching reduces per-query filter dispatch and coordinate-transfer calls, but float64 top-k, synchronization, uncapped coordinates and CPU reranking may dominate. The preserved scoring method synchronizes, so `score_wall_s` includes query upload and scoring overhead. Filter timing includes completion and coordinate transfer; source preparation, full validation and reranking have separate boundaries. Hashing, initialization, query setup and publication costs still need wrapper-level timing. Neither kernel-only measurements nor CPU passes establish an end-to-end improvement.