# Continued scale runs

This log records completed runs from the continuation requested on October 14, 2026. Each result identifies the dataset, fixed tree or code revision, configuration, quality, timing, failures, and the next decision. Preparation and correctness runs are separate from measured index performance.

The next epoch uses one million real FineWeb vectors and 128 fixed logical subtrees. It validates the streaming loader and full publication audit before any larger ingestion. A separate profile measures the current read bottleneck on preserved snapshots. Dataset preparation and profiling must not overlap timed worker runs unless resource isolation is explicit.

Fleet restoration is in progress in sandbox account 079819354890; production and the existing Lyon machine are excluded. The previous completed pilot and its limitations remain in RESULTS.md.

## Run 1: Reader instrumentation validation

All 20 transport and subtree-coordinator tests pass. Optional metrics separate reader queue wait, pipe round trip, serialization, and JSON decoding. A two-caller subprocess test exposes serialized queue wait. Search budgets and Rust search code are unchanged. The initial restricted invocation could not bind loopback sockets; the permitted-loopback rerun passes. This is a correctness validation and provides no throughput claim.

## Run 2: Sandbox restoration

Only the three preserved sandbox instances restart. Instance metadata verifies account 079819354890 and the expected IDs. Worker0 verifies 2,255 restored files; worker1 verifies 2,219. Each fresh NVMe filesystem has approximately 3.55 TB free, and root EBS preservation remains intact. The new guard deadline is October 15 at 6:26 a.m. America/Toronto (10:26 UTC). Compute resumes at $10.0608/hour with no new fleet capacity. FineWeb preparation uses worker0 while the read profile uses worker1; no timed comparison shares their resources.

The run-1 instrumentation command and source hashes are preserved in `pilot/results/read-profile-instrumentation/manifest.json`, with its test output in `tests.log` in the same directory.

## Run 3: Larger query-pool freeze

The pinned source yields 11,000 distinct query texts, split into 1,000 calibration queries and 10,000 test queries, excluding all 200 pilot queries. The full publisher query-text checksum passes. Recovery takes 61.435 seconds and fetches 4,101,532 source payload bytes under 16 MiB and 180-second limits, with no errors. Artifacts are in `subtree/fineweb/query-pools11000-v1`. Embeddings and exact answers for these pools are not yet produced.

## Run 4: FineWeb100k single-client read attribution

The unchanged audited-v3 index uses global beam 64 and candidate budget 1,000. A single-client 15-second window completes 112 queries with zero failures, 7.40 queries/s, and 132.9 ms mean coordinator latency. Mean Rust expansion, selected-leaf scoring, and reranking times are 0.036, 1.732, and 0.508 ms; corresponding pipe round trips are 1.18, 5.38, and 1.86 ms. Reader queue wait is negligible at this concurrency. The resource sampler fails at startup, so complete CPU and RSS measurements are unavailable for this run. Request journals and operation timings remain preserved. These measurements motivate adapter-overhead profiling; they do not yet justify adding reader replicas.

## Run 5: FineWeb100k four-client read attribution

The same audited-v3 tree and B64/M1000 settings complete 176 queries with zero failures at four clients: 11.56 queries/s and 341.4 ms mean latency. Mean reader queue waits are 0.017 ms for expansion, 0.044 ms for leaf scoring, and 0.015 ms for reranking. Corresponding Rust times are 0.035, 1.762, and 0.494 ms. Queue wait does not explain the plateau at this concurrency; HTTP scheduling and repeated serialization remain the leading profile targets.

## Run 6: FineWeb100k eight-client read attribution

The same audited-v3 tree and B64/M1000 settings complete 179 queries with zero failures at eight clients: 11.60 queries/s and 681.8 ms mean latency. Mean reader queue waits remain below 0.084 ms; Rust operation means remain 0.035/1.771/0.478 ms for expansion/scoring/reranking. Throughput is nearly flat relative to four clients. At one client, summed sequential HTTP phases account for 113.2 of 132.9 ms; at four clients, they account for 319.2 of 341.4 ms. The sum of per-phase maximum Rust times is approximately 5 ms. Adapter processing and serialization are the next measured targets; reader replication is not justified by these profiles.

## Run 7: HTTP-phase instrumentation validation

All 20 targeted tests pass for the second measurement-only instrumentation revision. It adds server JSON decode, batch execution, and response encode timing headers, together with coordinator request-build, header-wait, response-read, and decode/validation timings. Direct and HTTP deterministic search fields remain identical. Evidence is preserved under `pilot/results/read-profile-instrumentation-v2`. No optimization or Rust rebuild is included in this revision.

## Run 8: Scale orchestration validation

All 14 targeted tests pass for durable per-owner journals, CPU affinity/runtime limits, stale snapshot/map/audit rejection, and retaining the old global pointer on failed verification. Both current hosts expose one NUMA node and 32 physical cores with SMT siblings separated by 32 logical CPU IDs. The eight-vCPU allocation uses logical CPUs 0–3 and 32–35; the sixteen-vCPU allocation uses 0–7 and 32–39. These are controlled allocations on existing hardware, not measurements of smaller instance types. Input staging and setup are outside construction timing and remain separately accounted.

The corrected sampler for runs 5 and 6 shows 1.50/1.51 total process cores and 0.94/0.95 adapter cores at four/eight clients, with sampled concurrent RSS of 582.6/598.7 MiB. This supports a same-host multi-adapter comparison while preserving exactly one reader per subtree.

## Run 9: Detailed eight-client HTTP attribution

The unchanged FineWeb100k tree completes 179 successful queries with zero failures at 11.58 queries/s and 679.1 ms mean latency. Sequential HTTP phases average 656.6 ms per query. Server timings include 552.7 ms batch execution, 9.3 ms request JSON decode, and 18.2 ms response encoding. Coordinator request building takes 17.6 ms and response decoding/validation 12.6 ms. Approximately 1.126 MB returns per query. Batch elapsed time includes scheduling and overlapping requests, so it must not be interpreted as summed Rust CPU time. The next controlled comparison changes only adapter process placement on the same physical host.

## Run 10: Actual metadata-only publication probe

An audited FineWeb snapshot passes remote publication verification in 0.626 seconds during an isolated profile gap. Verification reopens the pinned generation, checks snapshot and mapping SHA-256 hashes, full-audit bindings, root, source, count, and metric. Only the manifest and map are copied to the driver and checked by digest; no index blocks are copied. This validates the larger-run publication path without making a throughput claim.

## Run 11: Paired adapter baseline, one client

The paired comparison starts with one adapter process and sixteen readers on one physical host, retaining the FineWeb100k tree and B64/M1000 policy. One client completes 112 successful queries with zero failures, 7.43 queries/s, and 132.2 ms mean latency. Additional HTTP attribution remains enabled. This reproduces the earlier single-client baseline before changing process placement.

## Run 12: Real FineWeb1M streaming preparation

Preparation completes in 348.232 seconds with 1,479,720,257 fetched source payload bytes and no errors. The epoch freezes 128 subtrees using a 32,768-row whole-prefix sample and twelve clustering iterations. A 0.142-second ownership audit confirms all 1,000,000 global IDs occur exactly once. The first 100k vector chunk matches the independently decoded earlier prefix hash. Owner sizes range from 2,155 to 19,020 rows, an 8.83-times largest/smallest ratio and a largest owner 2.43 times the mean. The input manifest SHA-256 is `39c6919bf45dab908448208a8d7b6a308ac31de39a7baefeeb87a3a339e71535`. Fresh full-prefix reference scoring is a separate run.

## Run 13: Paired adapter baseline, four clients

The unchanged single-adapter placement completes 175 successful queries with zero failures, 11.49 queries/s, and 344.0 ms mean latency at four clients. This reproduces the earlier four-client result before switching adapter placement.

The new FineWeb1M calibration target is fixed at 95% recall@100, with 90% retained as a secondary outcome. Its grid and selection rule will be saved before calibration; held-out results will not select the policy.

## Runs 14 and 15: FineWeb1M exact reference answers

The 100-query calibration reference completes in 9.241 seconds, and the independent 100-query test reference completes in 9.235 seconds. Both use eight BLAS threads, float64 inner-product scoring over the full million-vector prefix, stable top-100 merging across verified chunks, and pinned query hashes. Both pass with no errors. Output SHA-256 values are `79ba06750becaa16c4dfc29e27e49870d3cee7019fe67b9565f6b02131045197` for calibration and `820fee9c3de3e85e9d73970867e0d3d0b8d0bdce68e3296c30a2f3c7fc857bdb` for test. Combined preparation, ownership audit, and both references take 369.543 seconds.

## Run 16: Paired adapter baseline, eight clients

The single-adapter placement completes 179 successful queries with zero failures at 11.55 queries/s and 684.2 ms mean latency. Mean reader queue waits stay below 0.142 ms. This run includes the complete resource window for the paired comparison and preserves the earlier confirmation runs separately.

## Run 17: Four adapters on one host, one client

The paired four-adapter placement retains the same physical host, sixteen total readers, audited-v3 binary, FineWeb100k tree, and B64/M1000 policy. One client completes 188 queries with zero failures, 12.44 queries/s, and 78.0 ms mean latency. The paired one-adapter baseline is 7.43 queries/s and 132.2 ms. The preliminary throughput ratio is 1.67; complete result and frontier parity remains a separate validation gate after the remaining windows.

## Run 18: Four adapters on one host, four clients

The unchanged paired four-adapter placement completes 482 queries with zero failures at 31.84 queries/s and 122.2 ms mean latency. The paired one-adapter result is 11.49 queries/s and 344.0 ms. This preliminary 2.77-times throughput ratio uses the same physical host and sixteen readers; final parity and resource aggregation remain separate gates.

## Run 19: Four adapters on one host, eight clients

The unchanged paired four-adapter placement completes 582 queries with zero failures at 38.32 queries/s and 204.0 ms mean latency. The paired one-adapter result is 11.55 queries/s and 684.2 ms. This 15-second window yields a preliminary 3.32-times throughput ratio on the same physical machine, with sixteen readers total. Full trace parity and aggregate CPU/RSS checks follow before this configuration becomes the next measurement baseline.

## Run 20: Independent FineWeb1M reference crosscheck

Three queries are independently scored against all one million vectors with NumPy float64 einsum without optimization, separately constructed global IDs, and a full distance/ID lexicographic top-100 sort. All IDs match the chunked reference scorer; maximum distance difference is 1.11e-16. The check takes 7.932 seconds. Evidence is in `subtree/fineweb/fineweb-stream1m-reports-v1/independent-reference-validation.json`. Preparation and reference processes are ended before timed build work starts.

## Run 21: Adapter parity and resource validation

All 100 distinct queries match the preserved audited-v3 baseline across every profile epoch in IDs, distances, frontiers, selected candidates, reached leaves, and global candidate counts. At four clients, four adapters average 4.70 total process cores and 2.99 adapter cores, versus 1.48 total and 0.93 adapter cores for one adapter. Sampled RSS peaks are 637.6 versus 599.8 MiB across twenty versus seventeen processes. The final four-adapter eight-client window lacks CPU/RSS evidence because the sampler expires before it; its query and timing journals are complete. Adjacent samples do not substitute for that missing window.

## Run 22: Parallel ingestion and reference-bound helper tests

Six new local tests pass for bounded per-file parallel ingestion and conservative reference-candidate bounds. Parallel ingestion crosschecks parquet row counts, assigns canonical offsets, enforces a single writer per file, applies shared byte/time/output limits, verifies resumable parts and UUID uniqueness, and publishes atomically. Tiny parallel and serial outputs and whole-prefix models match. The candidate helper assumes IEEE FP32 without TF32, retains all candidates whose conservative intervals can enter the top-k, and reranks in float64. This is CPU conformance evidence, not a GPU engine or a measured large-corpus run. A genuine real multi-file prefix must cross the first file's 1,090,356-row boundary.

## Run 23: Fixed-graph input staging

The frozen FineWeb1M 128-subtree vectors and maps, totaling 3.080 GB, transfer from worker0 to worker1 in 10.35 seconds. Both workers retain identical inputs. The driver receives metadata and query references only. Transfer is outside construction timing. The following allocation sweep uses audited-v3, deterministic largest-first placement, and durable owner journals on declared eight- and sixteen-vCPU allocations.

## Run 24: Expanded data-preparation validation

All 23 relevant local tests pass. Bounded per-file ingestion verifies stable source ETags, actual parquet row counts and schema, canonical IDs, verified completed-file resume, retry caps, and final UUID uniqueness. Source/request and explicit stage timers apply to future runs; earlier ingestion attribution remains timestamp-derived. Sources and hashes are preserved under `preparation-source-v2`. No 100M or GPU run is included.

## Run 25: Structured adapter admission validation

All 21 targeted transport tests pass. The new regression fills a single admission slot, receives explicit JSON HTTP 503 with `owner_busy`, then verifies capacity after release. The first invocation fails because its new test omits a JSON import; the corrected run and both logs are preserved in `pilot/results/read-profile-admission`. This reliability revision is separate from the frozen performance baseline and preserves fail-closed query behavior.

## Run 26: FineWeb1M build, one host, eight CPUs, four jobs

The frozen 128-subtree FineWeb1M graph uses one host, eight logical CPUs spanning four SMT physical cores, four concurrent owner jobs, one balancing thread and two runtime threads per owner, and audited-v3. Construction takes 28.89 seconds; verification through atomic publication takes 36.59 seconds total. Child CPU totals 69.00 seconds, or 29.9% of the declared allocation over construction. Maximum individual-owner RSS is 140.3 MiB and is not aggregate RSS. All 128 owners succeed, converge, and pass snapshot/map/full-audit remote reopen verification. There are no balance warnings or request failures.

## Run 27: Independent conservative-reference review

Two hundred randomized cases preserve every exact top-k candidate when the supplied error intervals are valid, and the three existing helper tests pass. The reviewer confirms the kth-upper-bound/lower-bound retention rule and inclusive tie handling. Reproducers identify missing checks for approximate-score shape and fractional IDs. A denormals-as-zero example, outside the documented IEEE contract, demonstrates that a future GPU engine must enforce its arithmetic assumptions rather than rely on finite-input checks. Norm inflation also needs a dimension-specific justification or supported-dimension restriction. Evidence and source hash are preserved under `pilot/results/reference-bound-independent-review`. GPU exact-reference use remains gated on repairs and conformance.

## Run 28: FineWeb1M build, one host, eight CPUs, eight jobs

Construction takes 19.82 seconds and completes through publication in 27.39 seconds. Child CPU totals 106.34 seconds, or 67.1% of the eight-CPU allocation; maximum individual-owner RSS is 139.8 MiB. The graph and audited-v3 binary remain fixed. All 128 owners succeed, converge, and pass snapshot/map/full-audit/reopen checks with no balance warnings.

## Run 29: FineWeb1M build, one host, eight CPUs, sixteen jobs

Construction takes 18.38 seconds and completes through publication in 25.71 seconds. Child CPU totals 127.68 seconds, or 86.9% of the eight-CPU allocation; maximum individual-owner RSS is 140.0 MiB. All 128 owners converge and pass atomic publication and pinned audit/reopen checks with zero failures or balance warnings. Gains narrow relative to eight jobs on the same allocation.

## Run 30: FineWeb1M build, one host, sixteen CPUs, eight jobs

The sixteen-logical-CPU allocation spans eight SMT physical cores. Eight concurrent owner jobs complete construction in 15.69 seconds and publication in 23.02 seconds. Child CPU totals 75.19 seconds, or 30.0% of the allocation; maximum individual-owner RSS is 140.4 MiB. All 128 owners converge and pass snapshot/map/audit/reopen publication gates, with zero failures or balance warnings.

## Run 31: FineWeb1M build, one host, sixteen CPUs, sixteen jobs

Construction takes 11.05 seconds and completes through publication in 18.37 seconds. Child CPU totals 112.99 seconds, or 63.9% allocation utilization; maximum individual-owner RSS is 139.9 MiB. All 128 owners converge and publish after pinned audit/reopen verification, with no failures or warnings.

## Run 32: FineWeb1M build, one host, sixteen CPUs, thirty-two jobs

Construction takes 11.42 seconds and completes through publication in 18.75 seconds. Child CPU totals 149.57 seconds, or 81.9% utilization; maximum individual-owner RSS is 139.4 MiB. Every owner converges and passes publication gates. This single measured point uses more CPU without improving wall time relative to sixteen jobs. The matching two-host arms freeze sixteen jobs per host for both CPU allocations.

## Run 33: Independent reference-helper repair verification

All four helper tests pass, all three previous counterexamples now reject, and 200 randomized valid-interval cases still preserve exact top-k inclusion. Evidence is in `pilot/results/reference-bound-independent-review/fixed-tests.log` and `fixed-review.json`. A future accelerated engine still needs an explicit overflow and arithmetic-mode gate. No GPU reference result is claimed.

## Run 34: Retained-posting metrics transport validation

All 22 targeted transport tests pass. The optional `profile_memory` request flag is forwarded explicitly, and a worker that omits requested metrics is rejected. Evidence is in `pilot/results/read-memory-profile-python`. Rust metrics compilation and the actual-index smoke are pending a separate resource slot; the active construction binary remains unchanged.

## Run 35: Final CPU reference-helper gate

All four helper tests pass after the conservative overflow gate is added. The helper rejects nonfinite bounds, unsafe norm products, subnormal inputs, invalid IDs, and invalid shapes, and rounds bounds outward. Independent review has no remaining blocking finding under the stated IEEE FP32 contract. A future GPU implementation must enforce that contract and the gates before every chunk; CPU helper tests do not certify GPU execution.

## Run 36: FineWeb1M build, two hosts, eight CPUs and sixteen jobs each

The same 128-subtree graph constructs in 10.10 seconds versus 18.38 seconds on one matching allocation, a measured 1.82-times speedup. Through publication takes 16.97 seconds. Total child CPU is 128.19 seconds, or 79.3% of the combined allocation; maximum individual-owner RSS is 139.8 MiB. Deterministic placement assigns 499,763 and 500,237 rows to the hosts. All owners converge and pass snapshot/map/audit/reopen checks with no failures or warnings.

## Run 37: FineWeb1M build, two hosts, sixteen CPUs and sixteen jobs each

Construction takes 6.49 seconds versus 11.05 seconds on one matching allocation, a measured 1.70-times speedup. Through publication takes 12.99 seconds. Total child CPU is 119.23 seconds, or 57.4% of the combined allocation; maximum individual-owner RSS is 139.2 MiB. The same balanced row placement passes every owner convergence and publication gate. The construction matrix is complete. These are CPU-allocation comparisons on 64-vCPU hosts, not smaller-instance cost measurements.

## Runs 38 and 39: Linux ingestion helpers and real source preflight

All four parallel-ingestion helper tests pass on Linux in 0.901 seconds. The actual two-file metadata/cap preflight passes in 0.764 seconds after 131,072 source payload bytes. Intersecting compressed columns and bounded metadata/retry allowances determine resume and serial-tail caps of 397,410,304 and 382,730,240 bytes. The following fault-injection gate uses a 3 GiB initial source cap, 900-second stage limit, 12 GiB output limit, and 1,800-second hard timeout. Its intentional second-file failure is a recovery test, not an observed reliability failure.

## Run 40: Four-adapter eight-client resource-complete repeat

The unchanged four-adapter/sixteen-reader placement completes 589 queries with zero failures, 38.81 queries/s, and 201.3 ms mean latency. Preflight verifies advancing sampler timestamps and exactly twenty serving processes. Sampling covers completion and stops only in final cleanup. This targeted repeat closes the earlier missing resource window and agrees with the previous 38.32 queries/s result. CPU/RSS aggregation is retained separately from the request journal.

## Run 41: Resource-complete repeat aggregation and parity

The run-40 sampler brackets the full query window. Serving processes average 5.48 CPU cores, including 3.62 adapter cores, and peak at 648.3 MiB sampled concurrent RSS across exactly twenty processes. All 81 distinct queries encountered in this repeat match their preserved full-trace hashes; the full paired grid already validates all 100. Evidence is in `pilot/results/read-profile-repeat-v2/resources-and-parity.json`. This closes the missing resource window without replacing the original measurements.

## Run 42: Wire-byte accounting and request-limit validation

All 23 targeted transport tests pass. The next Python epoch records actual request and response bytes for every owner HTTP batch and rejects requests above the existing 4 MiB limit before network I/O. The 64 MiB response cap and query deadlines remain unchanged. Evidence is in `pilot/results/read-wire-bytes-python`; the separate Rust memory-metrics build is still pending.

## Run 43: Optional-metrics build fails on incomplete restored source

Compilation fails before linking because the restored source tree lacks the audited-v3 audit module, so the new bench cannot resolve AuditLimits or audit_reachable_ids. The immutable audited-v3 binary remains present and unchanged; measured results are not invalidated by this source-restoration mismatch. The failed attempt is preserved. Recovery restores the complete tested source overlay, verifies its identity, reapplies only the separate metrics revision, and reruns compilation and validation.

## Run 44: Sandbox GPU feasibility preflight

The preflight verifies account 079819354890, a 768-vCPU GPU quota with no current G/VT usage, and g6.2xlarge offerings in five availability zones; an offering is not a capacity guarantee. Official price is $0.9776/hour, or $0.4888 for thirty minutes of compute before storage. The proposed Amazon-owned compatible AMI is ami-0a24e16849196176a (owner 898082745236), with a 75 GB gp3 root. The runtime package set is hash-locked around torch 2.5.1+cu124 and NumPy 2.2.6; host driver, patch version, CUDA settings, arithmetic conformance, and the independent engine review remain launch/run gates. Only verified FineWeb1M data and the existing 200 queries/references are in scope. No GPU instance is launched by this preflight.

## Run 45: Private sandbox artifact store

The archive bucket `spann-artifacts-079819354890-20261014-ecfcca4d` is provisioned and verified in the sandbox account. All public access is blocked; bucket ownership enforcement, encryption, TLS-only access, versioning, task tags, and incomplete-multipart cleanup are enabled. No artifact expiration or deletion permission is granted. An exact-bucket instance role is attached only to the three previously unprofiled benchmark instances, and the driver verifies its normal role identity. Official storage pricing is $0.023 per GB-month for the first 50 TB, approximately $23 per decimal TB-month excluding requests. Source/checkpoint/binary restore validation remains pending before 100M ingestion.

## Run 46: Independent GPU-engine review

The reviewer confirms the streaming running-kth-upper-bound logic, uncapped interval retention, float64 reranking, and stable distance/ID merge. The engine checks every corpus-block score for its first three queries. Before launch, three repairs are required: bind final chunk count to the prepared prefix count, add aligned positive-input probes that detect hidden TF32/FP16 arithmetic returning FP32 tensors, and preserve initialization failures in report.json. GPU launch remains held pending those repairs and recheck.

## Run 47: Corrected optional-memory reader build and index gates

The optimized Rust 1.92 build passes after complete source restoration. All ten enabled release integrity tests pass, with one read-only diagnostic ignored. Baseline, tree-and-memory-metrics, and full-audit smoke tests pass. The metrics smoke checks zero retained entries before scoring, retained leaf/entry/payload counters afterward, zero raw posting/vector pins after cleanup, and identical candidates with profiling enabled or disabled. Binary SHA-256 is `7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158`; corrected complete source archive SHA-256 is `7478d191c62dc3b03a60c7c40b924f56922aff0cf7e0b1b5543c7a20dd15845f`. Earlier measured binaries remain preserved separately.

## Runs 48 and 49: Real two-file fault and resume stages

The intentional-fault stage completes the first source file in 403.887 seconds, reserving 1,613,623,128 source payload bytes, then reaches the injected second-file fault as expected. This is a successful fault-injection boundary, not an unexpected ingestion failure. Verified resume completes 1.1 million rows in 21.625 seconds with 148,097,335 payload bytes and 3,699,636,542 owned output bytes. The first-file receipt remains unchanged and is reused. Serial-tail, UUID, and whole-prefix sample validation remain a separate final gate.

## Run 50: Real 1.1M canonical recovery validation

Final validation takes 52.825 seconds and fetches 281,803,598 suffix-source bytes. All 1.1 million global IDs are ordered and unique. First-million vector and UUID hashes match the serial run; the new 100k tail matches a separately decoded serial tail; the 32,768-row whole-prefix sample is byte-identical. The entire metadata/fault/recovery/validation sequence takes 479.106 seconds with no unexpected errors. This validates real multi-file recovery and canonical content, not parallel throughput under two simultaneously successful file downloads.

## Run 51: Final independent GPU-engine CPU gate

All eight bound/engine CPU tests pass. Additional independent scans cover blocks smaller than k and all cutoff ties, checking all scores for three queries. An aligned positive-input probe rejects hidden FP16 arithmetic returning FP32 output; short prefixes reject before CUDA initialization; initialization failures preserve a report. No blocking code-review finding remains for the bounded pilot under its strict runtime/input gates. Evidence is in `pilot/results/reference-gpu-independent-review`. Actual CUDA arithmetic and performance remain unverified until the GPU run.

## Run 52: Real ingestion resource attribution

The 1.1M fault/recovery gate consumes 405.32 process CPU seconds over 479.11 seconds and peaks at 2,162,820 KiB RSS. Successful first-file HTTP requests sum to 63.76 seconds within a 403.89-second stage; resume HTTP requests sum to 6.68 seconds. Network is not the sole critical path. Existing logs do not separate decode, per-value Python conversion, and serialization, so that attribution remains open. The fault design runs one successful file at a time and provides no parallel-throughput result.

## Run 53: Large-body overload regression

A focused local check finds that the new admission path returns JSON 503 for 17-KB requests but resets connections for 1-MiB and 4-MiB requests when it closes sockets with unread bodies. The attempted cases preserve this failure evidence. The planned repair drains the already-sent valid body under explicit byte/time limits without JSON decoding, then sends the structured rejection. Wide-query overload accounting remains gated on the corrected regression.

## Run 54: Checked columnar decoder microbenchmark

A local conversion-only benchmark repeats 512 real FineWeb rows sixteen times, converting 8,192 rows and 25.17 MB per repetition without network I/O. The scalar path takes 0.844/0.839/0.840 seconds; the checked Arrow primitive-buffer path takes 0.00770/0.00716/0.00723 seconds. Every vector value and UUID position matches. This isolates removal of per-scalar Python materialization and is not a full ingestion-speed claim. Versions and Darwin RSS units are preserved in `columnar-decoder-local-microbenchmark.json`; end-to-end Linux validation is pending.

## Run 55: Bounded large-body admission repair

All 24 targeted transport tests pass. Rejected valid one- and four-MiB bodies receive structured HTTP 503 responses, and incomplete uploads are bounded by the drain deadline. Rejection processing caps header state at 16 KiB, body size at four MiB, drain wait at 250 ms, and response-write time at 50 ms. The original resets and corrected regression evidence remain in `pilot/results/read-large-body-admission`. The final Python transport epoch is handed to execution for deployment before wide-query calibration.

## Run 56: Columnar decoder ingestion tests

All 16 ingestion tests pass for the separate decoder epoch. It validates variable/fixed list offsets, parent/child nulls, primitive type and dimensions, preserves exact float16-to-float32 values, and records separate read/decode, primitive-conversion, and emit/hash timers. A known-million-row end-to-end hash check is pending. The actual first two files contain 2,156,499 rows and 3,190,918,351 compressed projected-column bytes; proposed genuine concurrency-one/two timing runs use identical source order and global IDs without injected faults.

## Run 57: GPU launch capacity failure

The g6.2xlarge launch fails with InsufficientInstanceCapacity in us-east-1f. An exact idempotent retry confirms no instance is created. AWS indicates offerings in other zones; execution will inspect account-owned subnets in the same sandbox VPC and retain the same instance type, security group, role, and thirty-minute guard. This attempt has no running GPU instance or CUDA result.

## Run 58: Linux columnar decoder preflight

Three Linux tests pass for Arrow offsets, null handling, and exact bit preservation. The separate known-million-row validation starts on isolated worker0 with a 2 GiB requested-source cap, 8 GiB output limit, 600-second internal limit, and 900-second hard timeout. It will compare all ten vector and UUID chunk hashes with the original verified output and separate request, read/decode, conversion, and emit timings. No end-to-end speed result is available from this preflight alone.

## Run 59: Bounded sandbox GPU launch

Capacity attempts in us-east-1a, us-east-1b, and us-east-1c fail without orphan instances. Launch succeeds in us-east-1d for one g6.2xlarge instance, i-0aa5faca97d41442b, in sandbox account 079819354890. NVIDIA L4 and driver 595.91.07 are verified. Compute costs $0.9776/hour. The absolute stop deadline is 2026-10-14 23:32:47 UTC, or 7:32:47 p.m. Toronto time. Verified million-row chunks and 200 queries/references are staged for runtime and numeric validation; launch alone establishes no arithmetic or throughput result.

## Run 60: Independent allocation-summary review

Independent review of all eight million-row allocation summaries confirms job counts, one-million-row totals, CPU fractions, and affinity consistency. Raw snapshot manifests, reopen responses, and byte inventories are not yet independently reviewed, so this is summary validation rather than independent publication proof. Evidence is in fineweb1m-128-allocation/independent-summary-review.json.

## Run 61: Million-row columnar ingestion validation

The isolated run completes in 100.808 seconds overall and 100.739 seconds for ingestion, with 1,479,851,329 requested payload bytes and 3,363,486,506 output bytes. All ten vector and UUID chunk hashes exactly match the original million-row dataset. It consumes 45.699 process CPU seconds and peaks at 971,736 KiB RSS. The file stage takes 89.907 seconds: read/decode including HTTP takes 77.892 seconds, primitive conversion takes 2.129 seconds, and emit/hash takes 9.648 seconds. Request/range hashing totals 59.801 seconds over 192 requests. No errors occur. Remaining read/decode timing combines network and native decompression; this does not yet establish multi-file parallel throughput.

## Run 62: Durable artifact recovery gate

Restricted instance-role writes and version-pinned restores verify the complete audited-v3 source archive, executable, and an actual 5,002-row checkpoint. All 21 checkpoint files and 2,000 source files pass verification. Multipart CRC32 transport verification and full SHA-256 verification cover the checkpoint. The restored executable reopens the restored generation and returns identical root/query IDs and distances. An initial unsupported-operation query attempt is retained separately; the corrected supported query passes. Evidence is in pilot/results/artifact-restore-v1/report.json. Larger ingestion still requires measured storage capacity.

## Run 63: GPU runtime startup prerequisite failure

Startup stops before package installation or scoring because the base image lacks python3.10-venv. Python 3.10.12 and driver evidence are preserved. A bounded 180-second prerequisite repair and retry retain the original 23:32:47 UTC stop deadline. This is a runtime setup failure; no numerical result exists.

## Run 64: GPU package-installer compatibility failure

The runtime retry stops before scoring because the bundled pip lacks the report option used to preserve installation provenance. Both setup failure logs are retained. Recovery bootstraps hash-pinned pip 24.3.1 from official PyPI and retries the same 23-package lock under the unchanged 23:32:47 UTC stop deadline. No numerical result exists. The paired two-file CPU ingestion tests run independently on a separate worker after staging releases it.

## Run 65: Linux read-epoch validation

All 36 Linux tests pass for the complete read Python epoch and orchestration. Million-vector calibration has not started; these tests establish readiness rather than measured recall or throughput.

## Run 66: First million-vector checkpoint archive

The selected tree snapshot half on worker1 is privately archived with a version-bound S3 descriptor and SHA-256 verification. The artifact contains 1,683,752,960 bytes and archives in 23.86 seconds outside measurements. The remaining tree half and complete epoch metadata still require archival. At 23:09:30 UTC, the three active CPU nodes have accumulated approximately $7.36 in compute since the 22:25:37 restart, excluding storage and transfer; their combined rate is $10.0608/hour. The bounded GPU compute allowance is below $0.489 at $0.9776/hour.

## Run 67: Independent full publication review

All eight construction cases pass independent review of 1,024 snapshot/map/audit bindings and eight million mapping rows. Each case covers IDs zero through 999,999 exactly once. Recorded root responses match immutable manifests, all audits cover expected live IDs, and model/ownership/configuration bindings agree. Owner sizes range from 2,155 to 19,020 rows; the largest is 2.43 times the mean. Every job sets checkpoint_rows to its complete owner row count, so these runs measure a single final flush rather than repeated private checkpoints. Audits show zero duplicate current copies and unreachable nodes; stale physical posting entries range from 2.23% to 3.12%. Raw block bytes remain unmeasured. Evidence is in pilot/results/fineweb1m-publication-metadata/independent-publication-review.json.

## Run 68: GPU exact-reference numeric pilot

All top-100 IDs match the saved float64 references for 200 queries over the full million-row corpus. Maximum reranked distance difference is 2.22e-16. All 3,000,000 full-score envelope checks and four adversarial cases pass. Scan wall time is 12.742 seconds, including host conformance overhead; synchronized GPU scoring takes 0.172 seconds, host-to-device transfer takes 0.293 seconds, and CPU reranking takes 0.269 seconds. The uncapped interval filter retains 357–450 candidates per query. Integrity hashing precedes the scan, so file caches are warm; runtime installation is excluded. The verified runtime uses driver 595.91.07, torch 2.5.1+cu124, and NumPy 2.2.6, with TF32 disabled and workspace settings pinned. This validates the million-row pilot, not a cold end-to-end or billion-row performance claim.

## Run 69: Single-worker two-file ingestion

The canonical first two files contain 2,156,499 rows. The single-worker case completes in 221.451 seconds, requests 3,191,311,567 source bytes, writes 7,251,431,144 output bytes, and consumes 98.184 CPU seconds. The matching two-worker run and complete paired content comparison remain pending.

## Run 70: Two-worker ingestion and paired content validation

Two workers process the same canonical 2,156,499 rows in 122.648 seconds versus 221.451 seconds with one worker, a 1.806-times elapsed improvement in this single paired observation. Every vector and UUID chunk hash matches. Both runs request 3,191,311,567 source bytes and emit approximately 7.251 GB. Process CPU is 98.79 seconds versus 98.18 seconds; peak process RSS is approximately 1.89 GB versus 0.98 GB. Overlapping HTTP waits explain the useful concurrency after columnar conversion; a process pool is not presently justified. These are engineering measurements without repeated-run confidence bounds.

## Run 71: GPU reference check after clearing file cache

The scan passes after clearing the operating system's file cache, taking 13.786 seconds with 3.075 GB of process-accounted physical reads. All 200 query reference IDs and three million full-score checks pass. GPU events record 0.176 seconds of scoring, 0.292 seconds of host-to-device transfer, and 0.131 seconds of device-to-host transfer; CPU reranking takes 0.265 seconds. This separates storage-read effects from the earlier warm-cache run. Runtime installation and prior source staging remain outside the scan boundary. Results are being preserved before GPU shutdown.

## Run 72: Independent local GPU answer comparison

A separate local comparison verifies all 200 unique query IDs and all 20,000 ordered result IDs against the saved CPU references. Maximum distance difference is 2.220446049250313e-16. Input/output hashes are recorded in subtree/fineweb/gpu-pilot-result-v1/parent-independent-comparison.json. This independently checks saved answers; it does not rescore the corpus.

## Run 73: Complete GPU evidence preservation and independent review

The complete 2.28 MB evidence archive is local, including query/reference arrays, input hashes, exact outputs, code, driver XML, package hashes, both setup failures, and both scans. Independent review recomputes all 200 ID matches and distance differences for each scan, verifies input/output bindings and package wheel hashes, and confirms driver 595.91.07. Evidence is in gpu-independent-review.json. GPU work is released for immediate archive and termination; shutdown confirmation is still pending.

## Run 74: Read-only storage diagnostic validation

The separate diagnostic compiles and passes all four release tests. Tests cover retained-generation roots, shared-block accounting, immutable head bindings, map hashes, missing/corrupt objects, resource limits, and symlink rejection. Sentinel payloads verify that it reads root metadata without decoding data blocks. Binary SHA-256 is a6827c10288e54cc994b34d8ff644b1d85b0e3d5f0b9675e7bd3434a443c9174; source archive SHA-256 is 34d7ebfdc05b97428e96fd467132ac5c712463690d7862e27caac91c8b4582f2. Actual index storage inventory remains pending. The measured read and construction executables remain separate and immutable.

## Run 75: GPU archive restore and complete selected-tree snapshot archive

The 2,281,658-byte GPU evidence archive is version-pinned in the private sandbox bucket and passes checksum-verified restoration; the same archive is local. A fresh account/tag check precedes exact-ID termination submission for i-0aa5faca97d41442b. Terminal API state remains pending. Both selected million-vector snapshot halves finish archival before resumed timing; no duplicate benchmark jobs start after interruption.

## Capacity review: Repeated private checkpoints

Each internal batch balances, commits, flushes, and reopens. Only the final batch runs the complete reachable-ID audit and publishes a manifest. Resume starts at the last published head, so an interrupted invocation rebuilds its incomplete suffix and can leave physical objects from private checkpoints. Numeric larger-run caps must account for private checkpoints, retained generations, failure residue, staging, and scratch. The read-only diagnostic measures object reachability separately from logical live rows; it authorizes no cleanup. Review evidence is in pilot/results/storage-audit-v1/review.md.

## Archive approval review: Prepared input payload

Automatic approval review rejects a command uploading approximately 6.1 GB of prepared vectors, maps, and ownership metadata to the private sandbox artifact bucket, citing insufficient explicit authorization evidence for the payload and destination. The payload derives from the pinned public FineWeb corpus. Execution is preparing provenance and verified account/bucket evidence for a review retry; it does not bypass the rejection. Snapshot and GPU archives are already preserved. Read calibration and read-only storage checks remain independent.

## Compaction harness review

Core persistence forks blockfiles on commit and uses generation-specific no-clobber ID maps, allowing persistent generation-one readers to stay pinned. The existing initial-build orchestrator and one-owner append check do not establish the proposed multi-owner 100k append. A dedicated harness must verify starting heads, every old-map prefix, target ownership/counts, and old-reader IDs and distances through global publication. Failed generation publication can leave immutable maps or manifests that block a same-generation retry; the harness will preserve that evidence and fail closed, with any retry starting from a fresh verified parent copy.

## Archive approval review: Evidence-based retry accepted

The retry is accepted after narrowing the payload to manifest-listed public FineWeb vectors/UUIDs and derived numerical maps/model, checking every SHA-256 and size before upload to the exact verified private sandbox bucket. No alternative route bypasses the rejection. The input archive is running; both snapshot halves and the separate memory-metrics executable/full-source provenance are already archived.

## Run 76: Multi-owner compaction harness tests

All six tests pass for subtree_compaction.py. The harness verifies generation-one heads and map prefixes, complete dense target coverage, frozen routing boundaries, fresh clone paths, and absence of orphan target-generation artifacts. Persistent generation-one SSH readers remain on those cloned stores through compare-and-swap publication and compare IDs, distances, navigation levels, selected candidates, and leaves. Independent reopening checks original snapshot/map hashes. Failures are journaled and retries require fresh clones. An untimed real-executable smoke remains pending before timed compaction.

## Run 77: Independent million-vector storage inventory

All 128 owner bindings verify against the published tree. Current, retained-union, and physical data-store logical bytes are each 3,351,329,366 across 3,047 files; allocated bytes total 3,358,134,272. All owned checkpoint files including maps/manifests total 3,359,959,458 logical bytes. No unknown paths or objects outside the enumerated retained roots occur. Each owner has only generation one and one flush, so a physical/current ratio of 1.0 does not establish repeated-checkpoint capacity. Evidence is in pilot/results/storage-audit-v1/independent-inventory-review.json with reproducible aggregate.py.

## Run 78: Complete selected million-vector input archive

The approved public input archive completes in 86.17 seconds and contains 6,316,615,680 bytes. Manifest-listed vectors, UUIDs, and owner inputs verify before upload. Both snapshot halves and binary/source/query/publication provenance are also privately archived, completing selected-epoch preservation. Read setup transfers identical cohost snapshots and starts fixed adapters outside timed calibration. GPU termination remains shutting-down for four minutes; execution requests force and skip-guest-shutdown for that exact already-released instance after evidence preservation. Terminal-state confirmation remains pending.

## Run 79: Compaction reader-overlap gate

All seven focused harness tests pass after adding an explicit interval check: at least one old-reader request starts and one response completes while writer tasks are active. Before/after-only traces fail this gate. Timing separates construction, publication-only, and through-publication. Real-executable smoke remains pending. A proposed 250 GB checkpoint stop budget for 10M is an execution limit rather than a demonstrated footprint bound; checkpoint multiplicity and tree-height effects remain unmeasured.

## Run 80: Ten-million-row preparation helper tests

Seven local tests pass for verified full-file seed reuse and actual-Parquet metadata planning. The 10M epoch may reuse the canonical first 2,156,499 verified rows without downloading them again; global order and values remain bound to the pinned source. No bulk 10M preparation starts during measured reads.

## Ten-million-row execution guard review

The existing run_jobs helper records outcomes but launches subprocess.run without owning remote process groups and has no disk/RSS monitor. Its timeout can leave remote writers alive. Execution is checking whether an independent run-level supervisor already supplies those controls before adding a new guard. The required guard must own writer process groups, enforce scoped storage/free-space/RSS margins, and preserve terminal reconciliation evidence.

## Run 81: Read setup import failure

The first read setup attempt fails on a Python import before transfers or adapter startup. Execution corrects PYTHONPATH and restarts setup, preserving the failure. No timed read result is produced. The GPU leaves running at 23:18:06 UTC but remains in AWS shutting-down state after force/skip-guest-shutdown; terminal state remains pending while CPU work proceeds.

## Run 82: Resource guard local tests

Five local tests pass for the separate resource_guard.py supervisor. It owns explicitly launched process groups, binds PID/start-time/session identity, monitors scoped bytes, free space, and RSS, and sends TERM/KILL only to verified owned groups. It checks survivors and preserves partial artifacts and journals. Unverifiable groups fail closed for reconciliation. Sampling is not a hard quota; margins must cover growth and abort latency. The Linux process-group test remains pending before 10M execution.

## Run 83: Pinned read placement setup

One-host and two-host setup passes pinned-root verification after 109.84 seconds outside timing, including 64 exact cohost snapshot copies. Frozen million-vector calibration starts on the separate memory-v4 executable with four adapters and sixteen logical CPUs per host. The grid is B64/128/256/512/1024 by global candidate budget 1,000/4,000, with 95% primary and 90% secondary recall targets over 100 calibration queries. Per-query deadline is thirty seconds, each configuration is bounded to 600 seconds, and failed/partial journals remain explicit. No calibration configuration has completed yet.

## Run 84: GPU termination confirmed

AWS API confirms i-0aa5faca97d41442b is terminated before its 23:32:47 UTC guard deadline. It leaves running at 23:18:06 UTC after launch at 23:02:47 UTC, approximately fifteen minutes nineteen seconds and $0.250 estimated compute at the verified hourly rate, excluding storage and network. Evidence remains local and version-pinned/checksum-restored in private S3. Fleet evidence is in fleet/gpu-numeric-pilot. Only the three authorized CPU benchmark nodes remain active.

## Runs 85 and 86: Preparation test invocation and corrected validation

The first test-discovery invocation uses the wrong package root and fails import before executing tests. The corrected invocation passes all 21 FineWeb tests, covering columnar offsets/nulls, verified-prefix reuse, canonical ordering, and metadata caps. Delta preparation remains outside the active measured calibration slot. The separate optional GPU device-filter proof is design work; the tested exact-reference engine remains unchanged.

## Run 87: Million-vector calibration B64/M1000

All 100 calibration queries complete without failure. Recall@100 is 80.05%, mean latency is 134.59 ms, and configuration wall time is 14.76 seconds. Both the primary 95% and secondary 90% targets are unmet. Resource, contribution, and request/response-byte traces are preserved.

## Run 88: Million-vector calibration B64/M4000

All 100 calibration queries complete without failure. Recall@100 remains 80.05%, mean latency is 232.13 ms, and configuration wall time is 25.23 seconds. Raising the candidate budget does not recover the branches pruned at this beam width. Wider-beam calibration continues without held-out query inspection.

## Run 89: Optional reference-filter proof and CPU baseline gate

Independent review accepts the uncapped inclusive candidate rule under its explicit numeric contract. Required implementation gates update the threshold from every current-block upper endpoint before filtering, retain exact float64 threshold/selection, bypass filtering for full-score validation queries, and check device nextafter around cancellation and adjacent cutoffs. All eight existing bound/engine CPU tests pass. This approves a separate implementation review rather than certifying unimplemented CUDA behavior. The tested engine remains unchanged.

## Run 90: Million-vector calibration B128/M1000

All 100 calibration queries complete with zero failures. Recall@100 is 86.28%, mean latency is 225.96 ms, and configuration wall time is 24.24 seconds. Both frozen recall targets remain unmet. The held-out pool remains unopened.

## Run 91: Optional device-filter component CPU tests

Five CPU tests pass for the separate reference_device_filter.py component: early prefixes smaller than k, cutoff ties, current-block threshold updates, random blocked exact retention, and invalid-contract/endpoint checks. Independent implementation review is pending. It is not integrated into a scan loop and has no GPU result.

## Run 92: Million-vector calibration B128/M4000

All 100 calibration queries complete without failures. Recall@100 is 86.29%, mean latency is 419.84 ms, and configuration wall time is 45.10 seconds. The larger candidate budget gains 0.01 percentage point over M1000 at this beam width while both recall targets remain unmet. Calibration proceeds to wider beams without inspecting held-out data.

## Run 93: Independent device-filter retention checks

The five component CPU tests and 2,000 independent variable-bound streaming/tie-order cases pass without a retained-candidate or outward-rounding finding. Runtime conformance still needs cross-backend combine/top-k/running-threshold histories, beyond endpoint/comparison checks. Caller gates must enforce valid vector ranges, complete corpus size at least k, stable global IDs, and full-score checks including rejected rows. The tested scan engine remains unchanged.

## Run 94: Million-vector calibration B256/M1000

All 100 queries complete without failure. Recall@100 is 92.74%, mean latency is 314.83 ms, and configuration wall time is 33.67 seconds. The secondary 90% target passes; the primary 95% target remains unmet. The entire frozen grid completes before selection or held-out inspection.

## Run 95: Separate device-filter scan CPU tests

Nine CPU tests pass with a fake backend for the separate optional scan. Input range/overflow gates run before scoring; ordinary queries transfer candidate indices only, while full-score check queries bypass filtering. Ties remain uncapped. Device conformance includes complete running top-k histories and bitwise float64 thresholds. CUDA deployment and actual hardware validation remain pending; reference_gpu.py stays unchanged.

## Run 96: Independent optional-scan CPU review

Independent execution passes all nine component/scan CPU tests. Review confirms current-block threshold ordering, uncapped cutoff ties, stable float64/global-ID ranking, range gates, and rejected-row full-score checks. Complete running-threshold conformance histories close the prior component finding. Deployment still requires a strict CUDA wrapper, complete prepared-row preflight before CUDA initialization, actual-device numeric probes, and independent reference comparison. No GPU launch occurs.

## Run 97: Optional CUDA-wrapper CPU gates

The optional reference modules pass eleven CPU tests, including preserved failure reports before CUDA initialization and declared corpus-count mismatch rejection. A separate CUDA wrapper preflights the complete corpus count, probes endpoint histories/adversarial arithmetic, and compares every output with independent references. Bootstrap accounts for the verified image prerequisites and pinned package installer. Hardware deployment remains pending the 10M gate and coordinated slot. The pinned 11,000-query embedding plan is in subtree/fineweb/QUERY-11000-PLAN.md; that pool remains text-only.

## Run 98: Independent optional CUDA-wrapper review

Eleven CPU tests pass independently and bootstrap shell syntax verifies. Review finds no numeric or retention blocker: source hash/count preflight precedes CUDA, arithmetic and streaming endpoint probes precede scanning, full-score checks include rejected rows, and every query compares with preserved references. Two failure-report hardenings are requested: install the EXIT trap before bootstrap probes and guard runtime-metadata collection against masking the original error. Endpoint/top-k/selection work remains within wall time without separate attribution. No GPU launches.

## Run 99: Million-vector calibration B256/M4000

All 100 queries complete without failure. Recall@100 is 92.80%, mean latency is 702.49 ms, and configuration wall time is 74.97 seconds. The secondary target passes; the primary target remains unmet.

## Run 100: Million-vector calibration B512/M1000

All 100 queries complete without failure. Recall@100 is 97.85%, mean latency is 420.53 ms, and configuration wall time is 44.93 seconds. This setting meets the primary 95% target. Selection still waits for the complete frozen grid, and held-out results remain uninspected.

## Run 101: Million-vector calibration B512/M4000

All 100 queries complete without failure. Recall@100 is 97.94%, mean latency is 1,144.81 ms, and configuration wall time is 121.45 seconds. The primary recall target passes.

## Run 102: Million-vector calibration B1024/M1000

All 100 queries complete without failure. Recall@100 is 99.89%, mean latency is 456.64 ms, and configuration wall time is 48.78 seconds. The primary recall target passes. B512/M1000 remains the provisional fastest qualifying setting; selection waits for the final frozen grid point B1024/M4000.

## Run 103: Final local reference-helper validation

All nineteen reference tests pass after wrapper failure-report hardening: eleven optional-device tests plus eight existing bound/engine tests. Bootstrap shell syntax passes. Source hashes and results are preserved in subtree/fineweb/device-filter-local-validation.json. No hardware launch occurs; final million-vector calibration and held-out parity precede the isolated delta/metadata slot.

## Run 104: Million-vector calibration B1024/M4000

All 100 queries complete without failure. Recall@100 is 100.00%, mean latency is 1,379.43 ms, and configuration wall time is 146.10 seconds. All ten frozen configurations complete with zero failed settings.

## Frozen million-vector policy selection

The declared lowest-mean-latency qualifying rule selects global B512/M1000 at 97.85% calibration recall and 420.53 ms mean latency. It meets the primary 95% target. Selection is saved before held-out scoring. Readers restart for the one-host/two-host held-out comparison, clearing application caches while the operating system file cache remains uncontrolled.

## Run 105: Independent complete calibration review

Independent recomputation verifies all 1,000 query records, four input hashes, and tree/binary bindings. B512/M1000 is exactly the lowest-mean-latency setting meeting 95% recall: 97.85% at 420.534 ms. Selected-setting HTTP JSON bodies average 8.006 MB per query; B1024/M4000 averages 33.842 MB. Maximum individual owner response is 4.281 MB, below the 64 MiB response cap. Across 128 owners, retained-entry cache maxima total one million entries, 117,000,000 payload bytes, and 119,359,296 bytes including centroids, with no raw block pins. These maxima are not simultaneous RSS. Calibration/test sets are disjoint and hashes match; chronological selection-before-held-out verification awaits held-out timestamps. Evidence is in pilot/results/fineweb1m-calibration-independent-review.json.

## Load-driver interpretation review

Current and deployed benchmark sources use ProcessPoolExecutor with spawn and one coordinator process per logical client. Concurrent owner requests use threads within each client, so clients have separate Python interpreters while each client still serializes JSON decoding and candidate pooling within its interpreter. Selected queries return about 8 MB of bodies and 116k contributions. The eight-vCPU driver can cap aggregate throughput; readiness barriers, disjoint complete journals, and driver CPU/run-queue/RSS/network measurements are required alongside worker utilization. No extra multiprocess rewrite is needed.

## Run 106: One-host held-out million-vector read

All 100 held-out queries complete without failure at frozen global B512/M1000. Recall@100 is 97.88%, mean latency is 496.94 ms, and wall time is 52.68 seconds. Application readers restart before testing; operating system file cache remains warm/uncontrolled.

## Run 107: Two-host held-out million-vector read and complete parity

All 100 queries complete without failure with 97.88% recall@100, 422.55 ms mean latency, and 45.21-second wall time. All 100 traces exactly match the one-host placement in IDs, distances, complete frontiers, reached leaves, selected candidates, and contribution counts. The saved selection hash (34785445...) predates both tests. Mean latency decreases about 15% in this single paired observation; capacity, repetition, and tail-latency claims remain unsupported. Prep slots release before the load epoch.

## Run 108: Canonical 100k append preparation

The zero-network run passes in 6.466 seconds, writing 626,146,817 additional bytes with 24.866 CPU seconds and 792,968 KiB peak RSS. Full target coverage contains exactly 1.1 million global IDs. All 128 owners receive rows; every old map prefix and the frozen routing model remain unchanged. Delta values match the independently verified canonical suffix. Target manifest SHA-256 is aa9bcbc44a3006ab8933ccfc7d27a26033b29c5853c5b18e9afc12739816e64c.

## Runs 109 and 110: Full 1.1M append reference answers

Fresh exact scoring of the appended rows merges with pinned million-row references, producing complete 1.1M references for the 100-query calibration and 100-query test pools. Scoring takes 1.235 and 1.234 seconds respectively. Full-prefix references permit append quality checks without filtering answers from a larger corpus. The separate capped metadata-only 10M preflight starts afterward.

## Run 111: Required 10M metadata preflight

The capped metadata-only preflight passes in 4.119 seconds using 720,896 payload bytes. Eleven actual-Parquet files provide the canonical ten-million-row prefix; projected compressed dense/UUID columns total 14,800,596,084 bytes versus 50,336,878,891 whole-file bytes. One file has only 30,720 rows, so verified metadata determines global offsets. Reusing the complete first two files saves 3,190,918,351 projected source bytes, leaving approximately 11.61 GB before metadata/retries. Worker0 releases to execution; no bulk 10M ingestion or index job starts in this gate.

## Results-download approval review

Automatic review rejects a broad case-directory archive that could include large indexes on the low-disk Mac. Execution replaces it with explicit configuration/sampler/report file whitelists and compressed-size caps before download. No index or source corpus is copied locally. Remote held-out evidence and privately archived snapshots remain preserved; benchmark execution is unaffected.

## Runs 112 and 113: Local socket restriction and Linux integration

The Mac test attempt cannot bind the multiprocessing manager Unix socket under its sandbox. Required idle Linux validation passes all nineteen tests in 0.80 seconds, including a real two-client spawned readiness barrier, complete failed-client journal accounting, compaction harness gates, and process-group shutdown of a TERM-resistant child while preserving partial files. No timed read overlaps these tests. The Linux guard gate passes before larger execution.

## Run 114: Whitelisted evidence transfer and full Python suite

Explicit whitelist/cap archives transfer 15.38 MB from the driver, 6.99 MB from worker0, and 0.319 MB from worker1, including held-out records, process samples/configurations, delta metadata/maps/references, and 10M preflight. No index or corpus is downloaded. Driver full Python discovery passes all ninety tests in 10.21 seconds. A generic manager-fork warning leads to explicit spawn for the load manager; its focused Linux rerun remains pending.

## Run 115: Ten-million-row job configuration gate

The zero-network CLI/config gate and two unit tests pass. Validation covers valid settings, disk/source caps, concurrency type, paths, and sample size. Planned preparation limits are 32 GiB requested source including one retry, 96 GiB ingest logical output, 192 GiB total new logical preparation output including bounded incomplete routing attempts, 16 GiB RSS, one-hour ingestion, and two-hour overall timeout. It requires at least 704 GiB starting allocated free space and retains a 512 GiB floor. Logical path bytes, unique-inode allocated bytes, newly allocated bytes excluding seed inodes, and retained attempt paths are reported separately. These limits cover preparation only; periodic index checkpoints require their own guard. Bulk execution remains queued after measured 1M work.

## Run 116: Independent held-out chronology and parity review

All 200 raw held-out query records verify: both placements have 97.88% recall, zero failed records, and all 100 complete deterministic search signatures match. Mean query time recomputes to 496.941 ms and 422.554 ms. Every test query starts after saved selection timestamp 1792020945; the first starts at 1792020998.578. All four query/reference hashes match the frozen plan. Evidence is in pilot/results/fineweb1m-heldout-independent-review.json; driver archive SHA-256 is 12784d2469250c9dfb0131f05b4864c2ebdd014712da19d80792126217026d8d. These sequential streams establish latency and consistency, not server capacity.

## Run 117: Worker-local supervisor tests

Three local tests pass for the separate host supervisor. On each worker it owns actual local writer argv/process groups through the resource guard, pins declared CPU affinity, records durable identities, enforces a renewable worker-clock lease and absolute deadline, and reconciles journaled writer groups before terminal success. Driver abort markers and expired leases trigger cleanup. The real Linux lease-expiry/process-group test remains pending. Missing or unreconciled terminal evidence blocks publication and retry.

## Ten-million-row preparation memory review

Parent review finds that retaining all ten-million-row source memmaps through the full bitwise audit can leave approximately 30.72 GB of mapped pages resident and trip the planned 16 GiB RSS guard despite bounded temporary arrays. Preparation must release mapping pages or use bounded open/close lifetimes, including reference-check stages. The independent reference probe must also use the job-pinned test-query path and verify query IDs rather than a hardcoded location. These fixes precede bulk launch.

## Run 118: Ten-million-row mapping lifecycle validation

Five mapping/reference tests pass, including closure on exceptions. The 10M runner no longer retains all corpus mappings: owner audits use bounded reads and open/copy/close one source mapping, independent probes use bounded reads, and exact_blocked closes its mapping in finally. A separate Darwin check touches one GiB through sixty-four sixteen-MiB mappings with only 33.60 MB peak RSS growth. Independent reference checks use the frozen job query path and explicit query-ID binding. Evidence is in subtree/fineweb/mapping-lifecycle-local-validation.json.

## Independent held-out resource review

Serving groups average about 2.01 CPU cores and peak 4.80 GiB summed RSS on one host. Two hosts use about 2.34 cores total, with independently sampled per-host peaks summing to 4.90 GiB. Driver busy CPU averages 0.74 and 0.89 cores. Stable process counts are 132 for one-host serving and 68 per host for two-host serving; inactive placements are excluded. Complete sampler windows bracket both sequential query streams. Evidence is in pilot/results/fineweb1m-heldout-resource-independent-review.json.

## Dedicated 10M preparation scheduling

A separate sandbox preparation node is planned so public-data ingestion and exact scoring can run independently of the existing workers' timed 1M load/compaction cases. Readiness owns only the new node; Linux retains ownership of the existing fleet. Official instance specs/price, account identity, restricted role/security group, resource limits, and a bounded stop deadline precede bulk work. No new node is launched by this scheduling decision alone.

## Run 119: Linux worker-supervisor lease gate

All four Linux tests pass in 12.19 seconds, including expired-lease cleanup of actual local child process groups and cross-run lease protection. The supervisor execution gate passes before larger index work.

## Run 120: Final load-harness Linux gate

All six Linux tests pass with explicitly spawned manager and client processes. The existing workers and driver are quiet before the one-/two-host load sweep starts at the unchanged frozen search policy. Complete outcome journals and driver/serving process-group resource measurements are enabled. No wire-format or search optimization is deployed.

## Run 121: Dedicated preparation-node preflight

Sandbox account 079819354890, the existing restricted artifact role/security group/SSH configuration, and Canonical Ubuntu image ami-0b6c6ebed2801a5cb (owner 099720109477) verify. The selected i4i.4xlarge provides sixteen vCPUs, 128 GiB RAM, and 3.75 TB local NVMe at the verified official price of $1.373/hour. Three-hour compute ceiling is $4.119; a forty-GiB gp3 root adds approximately $0.013 over that interval at $0.08/GiB-month, with archive/requests/transfer additional. The hard deadline preserves bounded small proof files to EBS and stops unconditionally; completed prepared inputs archive privately before the deadline. Fresh public source reads avoid timed-worker network traffic. No launch is claimed by preflight alone.

## Run 122: One-host million-vector load, one client

At frozen B512/M1000, the fifteen-second offered window records thirty successes, zero errors, 1.97 queries/second, and 500.26 ms mean latency. The completion denominator is 15.20 seconds. All clients pass pinned-root readiness before the shared start; journals contain zero invalid records. Resource samples are preserved; no saturated-capacity claim is made.

## Run 123: One-host million-vector load, two clients

Two independent coordinator processes record fifty-five successes, zero errors, 3.58 queries/second, and 547.12 ms mean latency. Completion window is 15.37 seconds for fifteen seconds offered load. Readiness passes and all journals validate. Driver/worker CPU, RSS, and leader-thread context-switch samples await independent interpretation.

## Run 124: Dedicated 10M preparation-node launch

Instance i-0d606301de7f8b4ae launches in sandbox account 079819354890 as i4i.4xlarge at $1.373/hour, public address 32.192.255.176 and private address 172.31.64.212. AWS confirms the restricted artifact profile. The fixed absolute stop deadline is 2026-10-15 02:49:40 UTC, or October 14 at 10:49:40 p.m. Toronto time. The forty-GiB root preserves bounded small proofs; 3.75-TB NVMe holds reconstructible scratch. Bootstrap, timer/IMDS/storage/runtime verification precede bulk work. Existing benchmark nodes remain untouched.

## Run 125: Preparation-node deadline and retry contract checks

The absolute stop timer verifies at 02:49:40 UTC with one-second accuracy. The initial relative timer permits systemd scheduling slack; an exact earlier deadline corrects that without extending the deadline. The fresh-download job validates full 14.8-GB projected reads within its 32-GiB requested-source cap. Attempt reports retain prior runs in immutable directories with an atomic latest pointer. The unsupported spatial-stage deadline is removed; enforced ingestion/overall limits remain explicit. Index admission requires a final passing attempt plus the matching prepared routing hash. Runtime bootstrap continues; no bulk work starts yet.

## Run 126: 1-host million-vector load, 4 clients

The fifteen-second offered-load window completes with 80 successes, zero errors, zero invalid journal records, 5.15 queries/second, and 763.32 ms mean latency at frozen B512/M1000. Each client has an independent coordinator process. Resource attribution remains pending; this is a pilot throughput observation.

## Run 127: 1-host million-vector load, 8 clients

The fifteen-second offered-load window completes with 97 successes, zero errors, zero invalid journal records, 6.17 queries/second, and 1265.07 ms mean latency at frozen B512/M1000. Each client has an independent coordinator process. Resource attribution remains pending; this is a pilot throughput observation.

## Run 128: 1-host million-vector load, 16 clients

The fifteen-second offered-load window completes with 110 successes, zero errors, zero invalid journal records, 6.72 queries/second, and 2282.30 ms mean latency at frozen B512/M1000. Each client has an independent coordinator process. Resource attribution remains pending; this is a pilot throughput observation.

## Run 129: 2-host million-vector load, 1 clients

The fifteen-second offered-load window completes with 35 successes, zero errors, zero invalid journal records, 2.32 queries/second, and 424.38 ms mean latency at frozen B512/M1000. Each client has an independent coordinator process. Resource attribution remains pending; this is a pilot throughput observation.

## Run 130: 2-host million-vector load, 2 clients

The fifteen-second offered-load window completes with 65 successes, zero errors, zero invalid journal records, 4.26 queries/second, and 459.28 ms mean latency at frozen B512/M1000. Each client has an independent coordinator process. Resource attribution remains pending; this is a pilot throughput observation.

Sixteen clients oversubscribe the eight-vCPU driver. No host-capacity claim follows from that point without driver/worker resource evidence. Two-host four/eight/sixteen-client cases remain pending.

## Run 131: Guarded host-batch bridge tests

Four control-plane tests pass for the new guarded_host_batch.py driver bridge. It launches hash-bound host-local supervisors independently of SSH, renews worker-clock leases, aborts peers on host/control failure, and requires successful command outcomes plus reconciled terminal reports before publication. A real Linux two-supervisor peer-failure test remains pending. Persistent old-reader sessions disable SSH multiplexing, and resource guards report allocated/sparse bytes; these changes belong to future epochs and do not alter the active timed load cases.

## Run 132: Dedicated-node Linux preparation gate

All thirteen tests pass on the actual preparation node, covering allocated-aware process-group resource control, mapping lifecycle, job caps, and immutable attempt reports. IMDS and STS independently bind account 079819354890, instance i-0d606301de7f8b4ae, and the restricted artifact role. Mounted scratch is 3.4 TiB; root is forty GiB. The absolute 02:49:40 UTC stop timer verifies. Fresh metadata preflight starts before bulk execution.

## Run 133: Combined guarded-construction local revision

Twenty-one tests pass locally for the combined guarded-host-batch revision, with three Linux-only process-group integrations skipped and still required. Coverage includes persistent-reader SSH multiplexing disablement and sparse/hard-link allocated-byte accounting. Source files and hashes are preserved under pilot/results/guarded-host-batch-v1/source-manifest.json. Earlier Linux results certify their separately preserved revisions rather than this combined source by implication.

## Run 134: Two-host million-vector load, 4 clients

The fifteen-second offered window completes with 108 successes, zero errors/invalid journals, 6.99 queries/second, and 558.95 ms mean latency at frozen B512/M1000.

## Run 135: Two-host million-vector load, 8 clients

The fifteen-second offered window completes with 139 successes, zero errors/invalid journals, 8.88 queries/second, and 877.73 ms mean latency at frozen B512/M1000.

## Run 136: Two-host million-vector load, 16 clients

The fifteen-second offered window completes with 158 successes, zero errors/invalid journals, 9.90 queries/second, and 1560.23 ms mean latency at frozen B512/M1000.

The complete ten-window sweep records 877 successful requests with zero request/client failures. At sixteen clients, two-host throughput is 1.47 times one-host throughput (9.90 versus 6.72 queries/second). Single-window measurements and possible driver/adapter limits prevent saturated host-capacity or tail-latency claims. Resource attribution remains pending. The measured slot releases before the first fresh-clone real compaction integration.

## Run 137: Final combined guard local tests

Twenty-two local tests pass, with three Linux-only integrations still pending for that combined construction revision. Live inventory tolerates file rename/deletion during scans to avoid false aborts while writers publish temporary objects. Allocated/sparse and logical accounting remain distinct. Measured read/load epochs stay unchanged.

## Run 138: Final dedicated-node Linux gate

All fourteen Linux preparation tests pass with the race-fixed resource guard SHA-256 42255b784bbaddf56b5d46ea297ff1076f9691f67369804701801e82d8491210. Bounded source/query/runtime/install/proof files are preserved on root EBS.

## Run 139: Fresh-node canonical metadata repeat

Metadata preflight repeats successfully in 4.426 seconds with 720,896 payload bytes and the same 14,800,596,084-byte compressed-column projection. All dedicated-node launch gates pass. Guarded fresh 10M preparation starts with a 32-GiB source limit, 192-GiB logical preparation limit, 16-GiB RSS limit, 512-GiB allocated-free floor, and two-hour process guard. The unconditional machine stop remains 02:49:40 UTC. No completed bulk 10M result is claimed yet.

## Load resource-counter scope

Process context-switch counters cover leader/main threads only, not all Rust/Python worker threads. Independent load review labels them accordingly. Driver CPU interpretation uses bracketing host counters because short-lived spawned clients may exit between process samples; stable worker-serving PID groups support direct CPU/RSS attribution.

## Guard interruption-path review

Review of the preserved host-supervisor test finds that an external SIGTERM during the resource-monitor sleep can enter ThreadPoolExecutor shutdown before the outer exception handler aborts children. The host supervisor fallback bounds that path for its recorded writer groups, explaining the previous test pass. A directly launched preparation guard should not rely on that fallback. A new exception boundary and deterministic interruption regression are being prepared for a separate future revision. The active prep guard is not modified or restarted; its internal checks and absolute machine stop remain in place.

## Run 140: External-interruption cleanup regression

Eight local tests pass after moving the abort exception boundary inside the executor context. The deterministic regression verifies that external interruption cleans children before executor waiting. Review confirms active preparation internal resource/deadline failures already invoke abort before context exit, so the running job remains valid without restart. A real Linux SIGTERM-during-sleep gate requires cleanup within three seconds and remains pending for the future revision. Absolute machine stop is unchanged.

## Run 141: First real compaction integration preflight failure

The explicit one-host integration uses fresh hashed generation-one clones, 128 owners, canonical 100k delta, audited-v3 executable, sixteen logical CPUs, sixteen concurrent jobs, one balancing thread, and two runtime threads. It fails before any writer starts because staging assumes owners/<id>/ paths rather than the delta manifest artifact paths. The harness rejects the missing map and preserves generation one. Evidence is in fineweb1m-append100k-integration-h1-v1. A corrected retry must use a new case and fresh clones, preserving this failed attempt. All twelve previous HTTP adapter groups are identity-checked and stopped after load.

## Run 142: Independent complete load/resource review

All ten readiness barriers and 877 outcomes verify with zero failed/invalid records; all encountered trace hashes match held-out results across 35 distinct queries. Sixteen-client throughput recomputes to 6.715 queries/second on one host and 9.903 on two, with 2.282- and 1.560-second means. The two-host driver averages about 7.44 of eight vCPUs versus 3.56 for one host; leader-thread involuntary context switches are approximately 2,062/second versus 205/second. Active worker CPU is 6.78 versus 9.96 cores. Adapter CPU is 3.83 of four interpreter cores on one host versus 5.65 of eight on two. Active serving peak RSS sums are 5.48 versus 5.43 GiB; driver peaks are 3.14 versus 3.17 GiB. The driver constrains the two-host point. Evidence is in pilot/results/fineweb1m-load-independent-review.json. A separate verified 32-vCPU driver is planned for paired repetition without disturbing current compaction or changing worker/search policy.

## Run 143: Second compaction integration SSH preflight failure

The fresh-clone v2 attempt fails before writers at persistent-reader SSH startup. An eight-thread constructor pool does not bound authentication because constructors return before authentication completes, allowing 128 handshakes to overlap. Closed connections are preserved in v2 evidence; generation one is unchanged.

## Run 144: Linux guard interruption cleanup

All nineteen tests pass for the separate updated Linux guard suite, including actual SIGTERM process-group cleanup in 0.164 seconds. This validates the repaired external-interruption path without modifying the active prep guard.

## Run 145: Larger load-driver launch

Sandbox instance i-05c41c0090a257a06 launches as m6i.8xlarge with thirty-two vCPUs and 128 GiB RAM, public address 100.48.66.121 and private address 172.31.64.169. Account, Canonical image, private security group/subnet, and restricted artifact profile verify. Official price is $1.536/hour; the three-hour compute cap is $4.608 before storage/network. Absolute stop is 2026-10-15 03:01:58 UTC, or October 14 at 11:01:58 p.m. Toronto time. Existing instances and deadlines remain unchanged; bootstrap/readback precede work.

## Run 146: Bounded SSH authentication startup tests

Nine focused tests pass after holding each of eight startup slots through a pinned describe_root round trip. Shard/generation/root/count/map identity verifies, every startup response is retained, queued launches cancel on failure, and all readers close including late successful futures. A 128-reader mixed-success/failure regression verifies concurrency bounds and complete cleanup. Linux receives this revision for a fresh v3 integration case.

## Run 147: Larger-driver bootstrap package failure

The optional apt awscli package is unavailable on the selected Ubuntu image. Execution preserves the setup failure and installs only the needed Python environment and boto3/NumPy dependencies. No load runs start. Instance identity and the absolute 03:01:58 UTC stop timer verify.

## Run 148: Successful real 1M-to-1.1M compaction integration

The v3 integration passes on fresh one-host generation-one clones with 128 owners, sixteen logical CPUs/concurrent jobs, one balancing thread, and two runtime threads. Canonical 100k appended IDs produce audited generation-two snapshots in 8.960 seconds; global publication alone takes 0.473 seconds, and elapsed time through publication is 13.858 seconds. All 128 owners converge and pass remote reopen/audit. Fourteen persistent old-reader checks preserve complete IDs/distances/frontiers through writer activity and publication. Every generation-one snapshot/map reopens unchanged afterward. New tree SHA begins dc217e93. This is an explicit integration success after two separately preserved pre-writer failures; alternating paired repetitions remain pending.

## Local disk cleanup completed

At the user’s request, seven explicit Git-ignored Rust build-cache paths are removed after confirming no active compiler processes. The cleanup reclaims 59.98 GiB and raises free space from approximately 1.7 GiB to 61.65 GiB. Git status verifies unchanged before/after for every source checkout, including dirty worktrees; no branch, source file, or uncommitted change is removed. Paths, heads, source statuses, and free-space measurements are recorded in local-cache-cleanup.json.

## Run 149: Real ten-million-row ingestion

Ingestion passes in 676.928 seconds, requesting 14,802,824,308 public-source bytes and producing 33,629,228,179 owned logical output bytes. Complete canonical coverage contains ten million rows; the unique UUID index occupies 1,277,358,080 bytes. Receipt SHA-256 is 873c627438357b2baca899cc0a919c9993162b62448c4ccdd84091a7658d3b58. No errors or retries are reported at the boundary. Sampled RSS is approximately 0.60 GB and free allocated space is 3.47 TB. Whole-prefix model/routing/audit/reference stages continue separately; the complete 10M preparation is not yet declared passed.

## Run 150: Compaction pair one, two-host arm

The first paired arm passes on two hosts with sixteen logical CPUs and sixteen concurrent jobs per host, the same 128-owner generation-one graph, and canonical 100k appended rows. Construction plus audited snapshots takes 4.342 seconds, publication alone takes 0.439 seconds, and elapsed time through publication is 10.175 seconds. Staging and complete clone hashing take 8.847 seconds outside timing. All 128 owners converge and pass remote reopen/audit. Ten pinned old-reader checks preserve full results, and all generation-one snapshots/maps reopen unchanged. New tree SHA begins d9aff50a. Resource aggregation and the matched one-host arm remain pending; the earlier one-host integration is a separately labeled attempt.

## Run 151: Ten-million-row whole-prefix model

The model stage passes in 31.327 seconds using 65,536 deterministic sample rows from the complete prefix. Frozen 128-slot model SHA-256 is e644f2af720d6952b9105668773809c7f6719c287aefb3fdaf686efb02016a81; sample SHA-256 is b030f78c0513c14b5edc617f92cd2eb3bfddd05c817860d57ca845a8877be86c.

## Run 152: Compaction pair one, one-host arm

The matched one-host arm passes with sixteen logical CPUs/concurrent jobs and the identical canonical mutation graph. Construction plus audited snapshots takes 5.779 seconds, publication alone 0.441 seconds, and through-publication 10.702 seconds. Verified clone staging takes 8.289 seconds outside timing. All 128 owners converge and pass audit/reopen. Eleven pinned old-reader checks stay identical, and generation-one snapshots/maps reopen unchanged. Pair-one writer-phase ratio is 1.331 times (5.779/4.342); aggregate interpretation waits for all three alternating pairs. All jobs exit zero. Child CPU totals are 30.073 seconds one-host and 23.525 seconds two-host; maximum individual-owner RSS is 161.21 and 163.46 MiB. The first two-host arm lacks a complete aggregate-process sample window, so its per-owner maximum is not presented as aggregate RSS. The separate integration uses 25.745 child CPU seconds and maximum individual-owner RSS of 163.03 MiB.

## Run 153: Ten-million-row routing

Routing passes in 113.766 seconds. All 128 owners are nonempty, with 27,583–170,731 rows and mean 78,125; the largest is 2.185 times the mean. Prepared logical data totals approximately 64.63 GB, sampled RSS is 0.84 GB, and free allocated space is 3.44 TB. Complete ownership/bitwise/sample/UUID audits and reference scoring remain admission gates; no index is launched yet.

## Run 154: Compaction pair two, one-host arm

The one-host arm passes with 6.031-second construction/audited snapshots, 0.491-second publication, and 10.968 seconds through publication. Staging takes 8.341 seconds outside timing. All 128 writers exit zero and converge, complete 1.1M coverage and old-generation reopen checks pass, and eleven pinned old-reader checks match. Child CPU totals 27.721 seconds; maximum individual-owner RSS is 163.21 MiB. The matched two-host arm remains pending.

## Compaction work and publication interpretation

The fixed parent snapshots and mutation task graph do not guarantee identical internal balancing operations: the shared split utility uses thread-local random initialization. Repetitions retain operation counters where available; these are engineering observations rather than exact deterministic-work comparisons or confidence intervals. Read-placement parity must compare the same selected immutable generation-two tree across hosts. Current prepublication host inspection runs serially across hosts, contributing an orchestration cost that a separate parallel-verification epoch may evaluate after the baseline pairs.

## Run 155: Compaction pair two, two-host arm

The two-host arm passes correctness but is slower than its matched one-host arm: construction/audited snapshots takes 8.427 seconds versus 6.031 seconds, publication 0.666 seconds, and through-publication 14.169 seconds. Verified staging takes 8.891 seconds outside timing. All 128 jobs exit zero, converge, and pass audit/reopen. Thirteen pinned old-reader checks match, and generation-one snapshots/maps reopen unchanged. Child CPU totals 24.493 seconds; maximum individual-owner RSS is 161.22 MiB. The slowdown remains included. Pair three begins two-host then one-host; small tasks, orchestration, and randomized internal balance work constrain interpretation.

## Pair-two slowdown attribution

Critical owner12 spends 8.407 seconds in the driver job but only 0.466 wall/0.464 CPU seconds in its actual Rust wrapper; wrapper start is delayed 7.921 seconds after driver job start. Slowest actual Rust owner91 takes 1.660 seconds versus 1.596 seconds in pair-one two-host. Launch-inclusive results remain unchanged; the observed slowdown is attributed to launch orchestration rather than a long Rust balance loop. Audited-v3 emits no split/merge/reassignment action totals, so they remain unavailable. A separate guarded worker-local pool epoch can evaluate removal of per-owner SSH launches after the baseline matrix.

## Run 156: Ten-million-row validation-script dtype failure

The first attempt stops in the initial bitwise routing audit after ingestion, model fitting, and routing pass. Subtracting signed NumPy starts from unsigned global IDs produces floating-point indices, which NumPy rejects. No source corruption or resource limit occurs. The guard reconciles the writer group, approximately 64.63 GB of outputs remains retained, and the original attempt report is preserved.

## Run 157: Checked integer-offset regression

All six mapping/reference tests pass after introducing checked integer offsets. Retry reuses and re-hashes completed ingestion without new public downloads, refits the deterministic model, and reroutes while retaining prior owner data as owners-incomplete-0. Approximately 145 seconds of local recomputation avoids untested stage-resume logic; the machine deadline remains fixed.

## Run 158: Compaction pair three, two-host arm

The two-host arm passes in 5.354 seconds for construction/audited snapshots, 0.643 seconds for publication, and 11.056 seconds through publication. Staging takes 8.821 seconds outside timing. All 128 jobs exit zero and converge, complete new-generation audit/publication and old-generation reopen checks pass, and ten pinned old-reader checks match. Writer CPU totals 28.492 seconds; maximum individual-owner RSS is 161.14 MiB. The final matched one-host arm remains pending.

## Run 159: Compaction pair three, one-host arm

The final arm passes with 5.640-second construction/audited snapshots, 0.428-second publication, and 10.557 seconds through publication. Staging takes 8.277 seconds outside timing. All 128 jobs exit zero and converge, audits and old-generation reopens pass, and eleven pinned old-reader checks match. Writer CPU totals 26.001 seconds; maximum individual-owner RSS is 160.96 MiB. All six measured arms pass. Pair writer-phase ratios are 1.331, 0.716, and 1.053 times; this matrix provides no consistent end-to-end compaction speedup under per-owner SSH launch. The measured launch outlier remains included.

## Run 160: Ten-million-row retry Linux gate

All nineteen Linux tests pass, including integer-offset bounds, mapping closure, and repaired external-signal cleanup. Actual SIGTERM child cleanup takes 0.114 seconds. Full routing-membership recomputation runs on every 4,096-row owner block alongside bitwise source equality. Both source epochs and failed attempt zero are preserved on EBS. A no-download retry starts under the same resource limits and unchanged machine deadline, retaining the previous owner directory.

## Run 161: Opt-in adapter candidate-reduction tests

Thirty-two focused adapter/transport tests pass, including the unchanged default path and actual local HTTP parity. The opt-in adapter reduces its union of subtree candidates to global M after validating every candidate against checksum-bound read-only maps, minimum-distance deduplication, and global-ID tie ordering. Mixed queries/budgets/metrics and wrong pinned generations reject before reduction. Global frontiers, selected candidates, exact results, and pre-reduction work counts match baseline. Transmitted rows are recorded separately as wire_candidate_contributions; owner summaries distinguish live unique scored, raw subtree contributions, and deduplicated owner contributions. Physical posting-score count remains unavailable. No Rust change or deployment occurs.

## Run 162: Actual two-host peer-abort gate

The isolated gate passes in 3.845 seconds. Independent worker-local supervisors launch an intentional exit-three command and sleeping peer. Failure triggers peer abort; both terminal reports reconcile and every recorded writer PID/process group is absent. publication_allowed is false. No benchmark data is accessed. Evidence is in pilot/results/guarded-peer-fixture-v1. The unchanged four-adapter generation-one endpoints restart for the larger-driver baseline, with untimed hundred-query warmups before thirty measured windows.

## Runs 163 and 164: Independent adapter test invocation and review

The first local independent test attempt lacks loopback socket permission and names a nonexistent test module; the corrected loopback-enabled invocation passes nineteen owner-reduction/HTTP tests. The top-M/global-ID tie proof is sound and Rust preserves cutoff ties before external-ID selection. Review requests two stricter malformed-response checks: cross-check live_unique_candidates_scored against available response sums, and reject boolean count/generation fields rather than accepting Python equality with integer one. Valid ranking is unaffected; the optional epoch remains undeployed and the baseline is unchanged.

## Run 165: Ten-million-row ingestion reuse validation

The retry verifies and reuses completed ingestion in 30.604 seconds with zero new public-source download. Prior attempt reports and owner outputs remain preserved.

## Run 166: Deterministic ten-million-row model rerun

Refitting takes 31.497 seconds and produces exactly the same model SHA-256 e644f2af720d6952b9105668773809c7f6719c287aefb3fdaf686efb02016a81 as attempt zero. Routing continues under the unchanged limits and machine deadline.

## Runs 167 and 168: Larger-driver untimed warmups

Both explicit hundred-query warmups pass before the 32-vCPU-driver baseline. Three alternating paired repetitions at one/two/four/eight/sixteen clients begin with the same generation-one tree, global B512/M1000, memory-v4 executable, four adapters per host, and sixteen logical CPUs per host. Worker/driver samplers cover all measured windows. Optional candidate reduction and worker-local compaction pools remain undeployed.

## Run 169: Post-append storage diagnostic collection

The selected 1.1M generation-two tree completes all 128 owner diagnostics successfully, sixty-four owners per host, with zero diagnostic failures. Byte aggregation and preservation remain pending; no numeric amplification claim is made yet.

## Run 170: Larger-driver repetition one, one host/one client

Thirty requests succeed with zero request/client failures, 1.981 queries/second, 498.36 ms mean latency, and a 15.143-second completion window. The serving tree/policy/allocation remain unchanged; the driver now has thirty-two vCPUs. Readiness and process/host samples are preserved.

## Run 171: Independent repaired adapter validation

Twenty-one owner/transport/HTTP tests pass independently. The scoring summary equals available per-response sums, and boolean generation/count/budget/version fields reject. No remaining proof/accounting blocker is found for the opt-in reduction. Defaults stay disabled, pre-reduction logical contributions remain separate from wire counts, and physical posting-score count remains unavailable. Evidence is in subtree/owner-reduction-independent-review.json.

## Run 172: Larger-driver repetition one, one host/2 clients

All 55 requests succeed with zero request/client failures, 3.561 queries/second, 550.94 ms mean latency, and a 15.445-second completion window. The fifteen-second offered load, frozen search policy, worker allocation, complete journals, and resource sampling remain unchanged.

## Run 173: Larger-driver repetition one, one host/4 clients

All 87 requests succeed with zero request/client failures, 5.585 queries/second, 702.44 ms mean latency, and a 15.579-second completion window. The fifteen-second offered load, frozen search policy, worker allocation, complete journals, and resource sampling remain unchanged.

## Run 174: Larger-driver repetition one, one host/8 clients

All 102 requests succeed with zero request/client failures, 6.566 queries/second, 1192.93 ms mean latency, and a 15.534-second completion window. The fifteen-second offered load, frozen search policy, worker allocation, complete journals, and resource sampling remain unchanged.

## Complete six-arm compaction baseline aggregate

Paired writer-phase ratios are 1.3309, 0.7157, and 1.0533; geometric mean is 1.0011 times. Mean launch-inclusive construction is 5.816 seconds on one host versus 6.041 seconds on two. All sixty-six old-reader checks and every audit pass, but no aggregate compaction speedup is established for this SSH-based execution path. The known launch outlier and randomized internal work remain explicit. Evidence is in pilot/results/compaction-baseline-summary-v1.json. Guarded worker-local pools form a separate future epoch.

## Run 175: Larger-driver repetition one, one host/sixteen clients

All 113 requests succeed with zero errors, 6.892 queries/second, 2,252.99 ms mean latency, and 16.396-second completion window. Frozen search/worker allocation, full readiness, journals, and resource sampling remain unchanged.

## Run 176: Larger-driver repetition one, two hosts/one client

All thirty-five requests succeed with zero errors, 2.311 queries/second, 425.93 ms mean latency, and 15.143-second completion window. Remaining two-host concurrency settings continue in this repetition.

## Run 177: Optional large-reference memory admission tests

Twelve optional-device CPU tests pass with fixed thirty-two-query/32,768-row batching and one-query endpoint arrays, uploading each source block once. A conservative visible-allocation allowance plus doubled workspace and two-GiB reserve estimates 2,506,824,704 bytes for 11,000 queries at k100; admission requires at most half available GPU memory and rejects query batches above 256. This is an execution allowance rather than a vendor-workspace proof. Actual peak-memory and numerical hardware gates remain required; no GPU launches.

## Run 178: Optional guarded compaction execution tests

Twelve focused tests pass for exact host-local argv/config hashes, CPU allocation, resource-scope coverage, and owner mappings. The optional backend uses guarded worker-local pools, preserves per-owner logs/outcomes, and separates host writer spans from driver control/metadata collection. Post-build inspection and metadata transfer run in parallel across hosts, and a failed host blocks compare-and-swap publication. Default launcher remains unchanged. Linux receives the interface for a fresh optional epoch after the active larger-driver baseline.

## Run 179: Larger-driver repetition one, two hosts/2 clients

All 67 requests succeed with zero request/client failures, 4.396 queries/second, 444.93 ms mean latency, and a 15.240-second completion window for fifteen seconds offered load. Frozen tree/search/serving allocation and complete readiness/journals remain unchanged.

## Run 180: Larger-driver repetition one, two hosts/4 clients

All 116 requests succeed with zero request/client failures, 7.517 queries/second, 520.87 ms mean latency, and a 15.431-second completion window for fifteen seconds offered load. Frozen tree/search/serving allocation and complete readiness/journals remain unchanged.

## Run 181: Larger-driver repetition one, two hosts/8 clients

All 172 requests succeed with zero request/client failures, 11.121 queries/second, 699.57 ms mean latency, and a 15.466-second completion window for fifteen seconds offered load. Frozen tree/search/serving allocation and complete readiness/journals remain unchanged.

## Run 182: Larger-driver repetition one, two hosts/16 clients

All 196 requests succeed with zero request/client failures, 12.364 queries/second, 1263.49 ms mean latency, and a 15.853-second completion window for fifteen seconds offered load. Frozen tree/search/serving allocation and complete readiness/journals remain unchanged.

The first larger-driver paired repetition completes with zero failures. At sixteen clients, throughput ratio is 1.794 times (12.364 versus 6.892 queries/second). Resource attribution and two further alternating repetitions remain pending.

## Run 183: Larger-driver repetition two, two hosts/1 clients

All 36 requests succeed with zero request/client failures, 2.345 queries/second, 419.89 ms mean latency, and a 15.350-second completion window. Repetition two reverses placement order to check order effects; frozen tree/search/serving policy remains unchanged.

## Run 184: Larger-driver repetition two, two hosts/2 clients

All 65 requests succeed with zero request/client failures, 4.246 queries/second, 459.43 ms mean latency, and a 15.308-second completion window. Repetition two reverses placement order to check order effects; frozen tree/search/serving policy remains unchanged.

## Run 185: Larger-driver repetition two, two hosts/4 clients

All 114 requests succeed with zero request/client failures, 7.417 queries/second, 528.21 ms mean latency, and a 15.370-second completion window. Repetition two reverses placement order to check order effects; frozen tree/search/serving policy remains unchanged.

## Run 186: Complete ten-million-row ownership/value/membership audit

The retry audit passes in 156.036 seconds. All ten million global IDs occur exactly once, every routed vector matches canonical source float32 bits, and every owner assignment recomputes to the frozen nearest-centroid slot. The rerun model and owner counts match attempt zero; previous outputs remain retained.

## Run 187: Whole-prefix sample and UUID provenance validation

The deterministic whole-prefix sample and all 256 UUID provenance spots pass in 20.746 seconds. Fresh complete ten-million-row calibration and test references start afterward. Index admission still requires the complete passing preparation attempt and exact-reference checks.

## Run 188: Larger-driver repetition two, 2 host(s)/8 clients

All 170 requests succeed with zero request/client failures, 10.907 queries/second, 715.31 ms mean latency, and a 15.586-second completion window. The two-host sixteen-client point closely repeats the first repetition (12.331 versus 12.364 queries/second).

## Run 189: Larger-driver repetition two, 2 host(s)/16 clients

All 193 requests succeed with zero request/client failures, 12.331 queries/second, 1259.90 ms mean latency, and a 15.651-second completion window. The two-host sixteen-client point closely repeats the first repetition (12.331 versus 12.364 queries/second).

## Run 190: Larger-driver repetition two, 1 host(s)/1 clients

All 30 requests succeed with zero request/client failures, 1.977 queries/second, 499.48 ms mean latency, and a 15.176-second completion window. The two-host sixteen-client point closely repeats the first repetition (12.331 versus 12.364 queries/second).

## Run 191: Independent canonical first-million prefix check

The first ten chunks of the fresh ten-million-row preparation exactly match the original verified million-row vector and UUID-provenance hashes, global starts, and counts. Only the 38-KB receipt is transferred locally; no corpus is downloaded. Evidence is in fleet/prep10m/canonical-first1m-review.json.

## Fixed load-query schedule scope

Each client starts at its own index offset in the fixed hundred-query pool. Short measured windows encounter an overlapping prefix/subset, while warmup covers all hundred queries. The earlier independent review observes thirty-five distinct measured queries. Repetitions and the first optional protocol comparison preserve this schedule to isolate changes. Publication-scale work requires a separately frozen globally shuffled, client-strided schedule over the larger query pool; current points provide no broad distribution or tail-latency estimate.

## Run 192: Larger-driver repetition two, one host/2 clients

All 55 requests succeed with zero request/client failures, 3.566 queries/second, 550.10 ms mean latency, and a 15.422-second completion window. Frozen search/worker allocation and complete journals remain unchanged.

## Run 193: Larger-driver repetition two, one host/4 clients

All 80 requests succeed with zero request/client failures, 5.182 queries/second, 758.72 ms mean latency, and a 15.437-second completion window. Frozen search/worker allocation and complete journals remain unchanged.

## Run 194: Independent complete compaction coverage/pinning review

All seven successful cases pass independent review of 896 owner maps and audit/root bindings. Every owner retains its exact generation-one map prefix, each case covers all 1.1M global IDs exactly once, and old-reader signatures remain identical through publication and reopening. All six paired arms have matching complete starting clone SHA inventories; the separate initial integration records counts only. Summaries capture the monitor count before joining its final in-flight query and undercount by one per case. Raw journals are authoritative: paired arms contain seventy-two successful checks, and integration fifteen, with all results identical. Derived aggregate records those counts; original evidence remains unchanged. The future count-after-join fix passes a deterministic regression within thirteen compaction/backend tests. Evidence is in pilot/results/compaction-baseline-independent-review.json.

## Run 195: Larger-driver repetition 2, one host/8 clients

All 104 requests succeed with zero request/client failures, 6.657 queries/second, 1176.84 ms mean latency, and a 15.623-second completion window. Frozen search, serving allocation, readiness, journals, and resource sampling remain unchanged.

## Run 196: Larger-driver repetition 2, one host/16 clients

All 114 requests succeed with zero request/client failures, 6.952 queries/second, 2241.69 ms mean latency, and a 16.398-second completion window. Frozen search, serving allocation, readiness, journals, and resource sampling remain unchanged.

## Run 197: Larger-driver repetition 3, one host/1 clients

All 30 requests succeed with zero request/client failures, 1.989 queries/second, 496.37 ms mean latency, and a 15.082-second completion window. Frozen search, serving allocation, readiness, journals, and resource sampling remain unchanged.

## Run 198: Larger-driver repetition 3, one host/2 clients

All 56 requests succeed with zero request/client failures, 3.618 queries/second, 541.74 ms mean latency, and a 15.476-second completion window. Frozen search, serving allocation, readiness, journals, and resource sampling remain unchanged.

## Run 199: Full ten-million-row calibration exact references

Float64 blocked CPU scoring over all ten million rows passes for the pinned hundred calibration queries in 100.326 seconds, using stable distance/global-ID ordering. Test-pool references and three independent full-prefix checks remain pending; index admission is still blocked until the complete preparation attempt passes.

## Run 200: Larger-driver repetition three, one host/4 clients

All 85 requests succeed with zero request/client failures, 5.475 queries/second, 715.88 ms mean latency, and a 15.526-second completion window. All three one-host sixteen-client windows now fall between 6.892 and 6.952 queries/second. Final two-host sweep follows.

## Run 201: Larger-driver repetition three, one host/8 clients

All 105 requests succeed with zero request/client failures, 6.642 queries/second, 1180.76 ms mean latency, and a 15.809-second completion window. All three one-host sixteen-client windows now fall between 6.892 and 6.952 queries/second. Final two-host sweep follows.

## Run 202: Larger-driver repetition three, one host/16 clients

All 112 requests succeed with zero request/client failures, 6.942 queries/second, 2224.87 ms mean latency, and a 16.133-second completion window. All three one-host sixteen-client windows now fall between 6.892 and 6.952 queries/second. Final two-host sweep follows.

## Run 203: Larger-driver repetition three, two hosts/1 clients

All 36 requests succeed with zero request/client failures, 2.344 queries/second, 420.13 ms mean latency, and a 15.359-second completion window. Frozen tree/search/worker allocation and complete readiness/journals remain unchanged.

## Run 204: Larger-driver repetition three, two hosts/2 clients

All 65 requests succeed with zero request/client failures, 4.243 queries/second, 460.65 ms mean latency, and a 15.319-second completion window. Frozen tree/search/worker allocation and complete readiness/journals remain unchanged.

## Run 205: Larger-driver repetition three, two hosts/4 clients

All 114 requests succeed with zero request/client failures, 7.465 queries/second, 524.87 ms mean latency, and a 15.270-second completion window. Frozen tree/search/worker allocation and complete readiness/journals remain unchanged.

## Run 206: Larger-driver repetition three, two hosts/8 clients

All 173 requests succeed with zero request/client failures, 11.202 queries/second, 694.53 ms mean latency, and a 15.444-second completion window. Frozen tree/search/worker allocation and complete readiness/journals remain unchanged.

## Run 207: Larger-driver repetition three, two hosts/16 clients

All 196 requests succeed with zero request/client failures, 12.365 queries/second, 1258.58 ms mean latency, and a 15.851-second completion window. Frozen tree/search/worker allocation and complete readiness/journals remain unchanged.

All thirty larger-driver windows finish with zero request/client failures. Sixteen-client throughput ranges are 6.892–6.952 queries/second on one host and 12.331–12.365 on two. Ratio of the three-run mean throughputs is approximately 1.783 times. These are short warm-window repetitions over the limited fixed query schedule; independent resource attribution remains pending. Only recorded samplers/adapters stop before preservation and the separate guarded-pool compaction epoch.

## Run 208: Guarded worker-side compaction integration on two hosts

The separate execution backend passes on two hosts with the same 128-owner parents, 100,000-row append, audited-v3 binary, sixteen logical CPUs per host, and sixteen concurrent owner jobs per host. Host-local writer spans are 2.652 and 2.463 seconds. Driver launch, control, and reconciliation take 6.461 seconds, and log collection takes 1.451 seconds. Construction including control and logs takes 7.950 seconds; publication takes 0.627 seconds; elapsed time through publication is 11.383 seconds. These component intervals overlap and must not be summed as independent stages.

All 128 writers, full 1.1M-row audits, publication, and old-generation reopening pass. Thirteen old-reader checks match. Writers consume 26.522 CPU-seconds, and the largest individual owner process uses 161.16 MiB RSS; this is not an aggregate memory peak. Both host supervisors reconcile successfully. The matched one-host arm remains pending, and this integration alone supports no scaling claim.

## Run 209: Ten-million-row preparation completes

The final preparation attempt passes. Fresh exact references for all hundred test queries take 99.628 seconds; the hundred calibration references take 100.326 seconds. Three independent complete ten-million-row float64 elementwise scans with global sorting pass in 56.134 seconds. Complete ID coverage, bitwise vector equality, recomputed owner assignments, sample checks, and UUID provenance checks also pass. The resource guard exits cleanly.

This result certifies prepared inputs and references, not index construction or read capacity. A separate capped public-source spotcheck and authoritative archive/restore check follow before staging the build. Authoritative staging excludes failed routing copies while retaining their failure reports and hashes separately. The required scale progression remains 1M, then 10M, then 100M.

## Run 210: Independent larger-driver reconciliation and resource review

All thirty windows pass independent review: 2,906 successful requests, zero failed or invalid outcomes, matching held-out oracle trace hashes, and complete reconciliation of readiness barriers, client counts, throughput, and mean latency. Sixteen-client mean throughput is 6.9287 queries/second on one host and 12.3535 on two, a ratio of 1.783 times.

At sixteen clients, the driver uses 2.69 and 5.22 of its thirty-two CPU cores for one- and two-host cases. Main-thread involuntary context switches average 7.36 and 14.30 per second. Request adapters use 3.94 of four available interpreter cores on one host and 7.09 of eight on two; Rust readers use 3.01 and 5.39 cores. Resource samples bracket every window. The prior eight-vCPU driver saturation is absent, while adapter interpreters constrain the current path. Short windows and the limited overlapping query schedule still limit broader capacity and tail-latency claims.

## Run 211: Independent compacted-generation storage inventory

All 128 owners pass independent inventory. The live 1.1M-row generation references 3,687,020,238 logical bytes. The union of retained generation-one and generation-two objects is 4,370,367,792 logical bytes, equal to the complete physical object-store file inventory; allocated bytes are 4,380,311,552. There are zero unknown or unreferenced objects. Entire owned checkpoint directories consume 4,388,113,150 logical bytes. The original million-row live generation references 3,351,329,366 bytes.

The retained/current ratio is 1.1853 for this single append commit and two published generations. It does not bound repeated private checkpoints, input staging, clones, or peak scratch. No objects are deleted. Evidence is in pilot/results/gen2-storage-independent-review.json and review_gen2_storage.py.

## Run 212: Independent public-source spotcheck for ten million rows

Forty-eight rows across the first, middle, and last source files pass bitwise float16-to-float32 vector and UUID comparison against prepared data. The check takes 28.795 seconds, requests 444,094,339 public-source bytes, and peaks at 1,211,240 KiB RSS. It stays within its 1 GiB source and 180-second limits. Combined main ingestion and spotcheck source traffic is approximately 15.247 GB, below the 32 GiB requested-source allowance. Full authoritative archive and restore verification follow.

## Run 213: Matched guarded worker-side compaction on one host

The matched one-host arm passes with a 5.948-second host-local writer span, versus 2.652 seconds on the slower host in the two-host integration. One-host launch, control, and reconciliation take 13.454 seconds, and log collection takes 1.513 seconds. Construction including control and logs takes 15.004 seconds, publication 0.417 seconds, and elapsed time through publication 19.799 seconds. All 128 writers, coverage audits, publication checks, old-generation reopenings, and twenty-two old-reader checks pass. Writers consume 29.316 CPU-seconds; largest individual owner RSS is 162.98 MiB.

This single matched pair gives a 2.243-times reduction in the critical host writer span, a 1.887-times ratio for construction including control and logs, and a 1.739-times ratio through publication. Component intervals overlap and are not additive. Control overhead remains explicit. Repetitions follow while ten-million-row inputs are archived; this pair alone is not a confidence-bounded scaling estimate.

## Run 214: Initial-construction orchestrator source gate

Seventeen existing tests and three independent checks pass for the new guarded initial-construction orchestrator. Actual local arguments preserve a 10,000-row checkpoint interval, all 23,001 rows in the fixture, and final published generation one. Injected reopen and full-coverage failures block publication. Source review confirms exact host, argument, CPU, and resource-scope admission, all-host terminal reconciliation, pinned reopening, and complete global-map coverage before publication. Concrete ten-million-row host configurations and a live tiny periodic-checkpoint integration remain separate launch gates. Evidence is in pilot/results/guarded-initial-independent-review/.

## Run 215: Live guarded initial-build periodic-checkpoint smoke test

A live fixture builds 23,001 real public FineWeb rows across two owners with a 1,000-row private checkpoint interval. All twenty-four expected flushes are confirmed, and the final immutable global and lower generation is one. Full ownership, remote pinned reopening, audit, and compare-and-swap publication pass. Input verification takes 0.523 seconds and is excluded from construction timing. The host writer span is 1.123 seconds; construction including control and logs is 4.058 seconds; publication is 0.016 seconds; elapsed time through publication is 4.728 seconds. Five Linux builder tests also pass.

This validates the live execution path and repeated private flushing. Concrete ten-million-row resource/configuration admission remains a separate gate, and this small fixture does not establish large-run memory or storage capacity.

Independent smoke review confirms every global ID from zero through 23,000 appears exactly once, every map hash and reopened root binds, every host outcome reconciles, and publication matches the audited tree hash. No writer processes or resident writer memory remain. The guard records 269,376,519 unique-inode logical owned bytes and 270,016,512 allocated bytes across the complete owned scope, including inputs and private outputs. Those totals are not index-only bytes; private-object reachability inventory remains separate. The smoke uses a 1,000-row interval deliberately, while the required 10M configuration uses 10,000. Evidence is in pilot/results/guarded-initial-independent-review/fixture-review.json.

## Run 216: Guarded compaction pair two, one host

All 128 writers, host terminals, audits, publication checks, and old-generation reopenings pass. Twenty-two old-reader checks match. Host-local writer span is 5.677 seconds; launch/control is 13.728 seconds; log collection is 1.485 seconds; construction is 15.248 seconds; publication is 0.560 seconds; elapsed time through publication is 20.157 seconds. Writers consume 27.416 CPU-seconds, and maximum individual owner RSS is 163.56 MiB. The paired two-host arm follows; component intervals overlap and are not additive.

## Run 217: Guarded compaction pair two, two hosts

All 128 writers, audits, publication checks, and old-generation reopenings pass. Fifteen old-reader checks match. Host-local writer spans are 2.560 and 3.396 seconds; launch/control is 7.637 seconds; log collection is 1.417 seconds; construction is 9.089 seconds; publication is 0.423 seconds; elapsed time through publication is 12.338 seconds. Writers consume 29.307 CPU-seconds, and maximum individual owner RSS is 164.83 MiB. Pair two gives a 1.672-times ratio for critical host writer span and a 1.634-times ratio through publication. Component intervals overlap and are not additive. Future path-admission hardening is a separate source epoch; these frozen runs are unchanged.

## Run 218: Path-admission hardening source gate

Thirteen local backend and initial-builder tests pass for a separate pre-10M source epoch. Nonabsolute paths, lexical traversal, and symlink ancestry reject before remote writers start or outputs are created. A bounded read-only ancestry check runs on each owning host before its supervisor. Existing frozen configurations use canonical paths and are unaffected. The backend hash is e07ae768ecd22959b1c0df54294292ade1e54aad2468651ff89472fde17fea55. A Linux execution gate remains pending. Concurrent external renames remain outside the controlled-owned-directory contract. Evidence is preserved as guarded-path-admission-v1.

## Run 219: Ten-million-row driver metadata handoff

A 25.9 MB archive containing 141 metadata, map, query, and reference files uploads and passes an independent full version-pinned restore in 1.87 seconds. The driver handoff descriptor is fleet/prep10m/driver-handoff-v1.json. This enables placement and configuration review while the full authoritative data archive continues. It does not admit index construction before that complete archive/restore passes. No corpus transfers to the Mac or existing benchmark fleet in this step.

## Run 220: Guarded compaction pair three, two hosts

All 128 owner terminals, audits, publication checks, and old-generation reopenings pass. Fifteen pinned old-reader checks match. Host writer spans are 3.162 and 3.645 seconds; construction is 8.941 seconds; publication is 0.401 seconds; elapsed time through publication is 12.193 seconds. Writers consume 31.762 CPU-seconds, and maximum individual owner RSS is 160.59 MiB.

## Run 221: Guarded compaction pair three, one host

All 128 owner terminals, audits, publication checks, and old-generation reopenings pass. Twenty-four pinned old-reader checks match. Host writer span is 6.723 seconds; construction is 16.233 seconds; publication is 0.417 seconds; elapsed time through publication is 21.059 seconds. Writers consume 28.179 CPU-seconds, and maximum individual owner RSS is 161.05 MiB.

All six guarded arms complete without failures or outlier exclusions. The geometric mean paired ratio through publication is 1.6994 times. Mean one-host elapsed time is 20.3383 seconds, versus 11.9713 seconds on two hosts. These guarded results support scaling within their frozen execution path. Its absolute one-host time exceeds the earlier SSH path, and this is not an improvement over the fastest earlier absolute result. Independent six-arm reconciliation remains pending.

## Run 222: Driver-side restoration of ten-million-row metadata

The benchmark driver restores and hash-checks the version-pinned small handoff archive in 1.342 seconds, producing 89.726 MB of raw metadata and maps. Its 128 slots and ten million rows bind the prepared input hash beginning 739c0b52. This is a metadata handoff; full authoritative data archive and concrete construction admission remain separate gates.

## Run 223: Full authoritative ten-million-row archive and restore

The full authoritative archive passes in 566.329 seconds. It covers 662 paths and 646 unique content-addressed objects: 64,648,821,827 logical path bytes and 64,639,440,968 unique object bytes. Every unique object restores from its pinned version and matches its complete SHA digest. The external guard passes with zero residual child processes. Failed routing duplicates are excluded only after byte-equality validation, with aliases and failure evidence retained separately.

Small proofs and descriptors are fleet/prep10m/archive-manifest-v1.json, archive-manifest-object-v1.json, and archive-guard-v1.json. The worker-only staging catalog contains 259 files and approximately 30.803 GB; corpus copies do not pass through the Mac. Concrete construction configurations and worker staging must still pass before launch. The preparation machine keeps its unchanged 02:49:40 UTC stop deadline and can stop earlier after its remaining small proofs are durable.

## Run 224: Linux path and initial-builder gate

Thirteen Linux tests pass for the hardened path-admission and initial-builder source epoch. Concrete two-host ten-million-row configurations, frozen placement, prepared-input/model bindings, pre-staging filesystem inventory, and producer hashes are preserved in pilot/results/10m-config-admission-v1.tar.gz. Independent configuration review, actual staged input hashes, and final free-space checks remain required before launch. Only each host's assigned owner inputs restore from version-pinned objects; staging is outside construction timing. Test evidence is pilot/results/guarded-path-initial-linux-tests.log.

## Run 225: Dedicated preparation machine stops after verified handoff

AWS confirms dedicated sandbox instance i-0d606301de7f8b4ae is stopped at 00:45:53 UTC. Launch at 23:50:22 UTC gives a conservative 55.53-minute compute upper bound, approximately $1.271 at $1.373/hour, below the three-hour $4.119 compute cap. Its retained encrypted 40 GiB root costs $3.20/month until removed; archive storage and requests are separate. Every authoritative object and final small proof is version-bound archived and fully restore-verified before shutdown. No dedicated preparation or GPU compute remains active. Existing benchmark workers remain active and untouched by this stop. Evidence is fleet/prep10m/stopped-api.json and node.json.

## Run 226: Concrete ten-million-row configuration review

Independent review verifies all 128 exact local jobs, placement assignments, configuration hashes, owned scopes, and 10,000-row checkpoint arguments. Each host receives 64 owners, with 4,998,538 and 5,001,462 rows. Each uses sixteen logical CPUs, sixteen concurrent jobs, one balancer thread, and two runtime threads. Effective guard thresholds are 224 GiB owned logical bytes, 56 GiB aggregate writer RSS, and at least 640 GiB filesystem available space. Pre-staging free space is 3.434 and 3.520 TB.

The reviewed input scopes are not yet populated, so this review is conditional. Actual staged input and binary hashes, final owned-scope/free-space inventory, an unexpired absolute deadline, and hardened remote ancestry admission must all pass before launch. Evidence is pilot/results/guarded-initial-independent-review/10m-config-review.json. This review does not claim staging is complete.

## Run 227: Private-checkpoint smoke storage inventory

The live 23,001-row periodic-checkpoint fixture references 77.216 MB of current live objects but owns 194.710 MB of physical object files, a ratio of 2.522 times. Approximately 117.495 MB lies outside the final published manifest references. These are private-checkpoint leftovers, conservatively retained; no deletion or garbage collection occurs. This fixture completes twenty-four flushes across two owners and does not bound larger-run growth. Ten-million-row inventory and peak scratch must measure the same distinction directly.

## Ten-million-row two-host construction starts

The required 10M two-host run starts after authoritative archive/restore, independent configuration review, Linux hardening tests, assigned-object restore proofs, and fresh capacity checks. Each host has 64 owners and approximately five million rows, sixteen logical CPUs, sixteen concurrent jobs, one balancer thread, two runtime threads, and a 10,000-row private checkpoint interval. The frozen 128-slot model and prepared inputs are unchanged.

Each host restores 131 files: approximately 15.398 GB in 58.297 seconds and 15.407 GB in 54.294 seconds. Every object binds a pinned version and full hash. Staging and initial input/binary rehash are outside construction timing. Fresh available space is 3.419 and 3.504 TB. Effective guards stop at 224 GiB owned logical bytes, 56 GiB aggregate writer RSS, or 640 GiB filesystem available; host deadlines are approximately 01:44 UTC. Publication requires every job to reconcile and converge and every snapshot, map, and audit to reopen correctly. No completed construction result is claimed at launch.

## Run 228: Mixed-generation adapter reduction gate

All twenty-two focused owner-reduction and transport tests pass. A new positive fixture publishes global generation two while retaining an unchanged generation-one subtree beside a changed generation-two subtree. All four approximate distances tie at the cutoff. Enabling adapter reduction preserves exact result IDs and distances, full frontiers, leaves, selected global candidates, and logical work counts; wire contributions fall from four to two while logical contributions remain four. Production adapter code and frozen benchmark epochs are unchanged. One initial test invocation names a nonexistent third module; the corrected two-module invocation passes. Evidence and exact test source hash are in pilot/results/owner-reduction-mixed-generation-v1/review.json.

## Run 229: Bounded next-scale uniqueness preparation study

A local deterministic 100,000-UUID metadata fixture passes complete uniqueness and cross-run duplicate rejection for both SQLite and sorted-run merge approaches. Valid runs take 0.367 and 0.304 seconds respectively. Their output capabilities differ, so these are bounded metadata checks, not vector benchmarks or large-scale throughput estimates. No vectors, downloads, remote jobs, or pipeline changes occur.

The eleven verified 10M per-file receipts sum to 939.223 seconds wall time and 106.210 seconds emit/hash time across concurrent jobs, within a 676.928-second whole ingestion. Separate SQLite, dense decode, and data-request stage timings are unavailable, so existing evidence does not establish their bottleneck. The study records canonical file-prefix IDs, exact-string global uniqueness options, immutable interval/reducer receipts, bounded memory and retries, and future 100M then 1B gates. Evidence is subtree/fineweb/scale-preparation-review/.

## Run 230: First ten-million-row build stops before publication on log volume

All 128 writers complete successfully, and both host supervisors reconcile passed. The driver launch/control/writer phase takes 159.143 seconds. Bounded log collection then fails: audited-v3 emits approximately 1.533 GB and 1.619 GB of repeated navigation-mode stdout messages on the two hosts, exceeding the collector's 64 MiB per-host limit. No balance warning is reported. The controller correctly leaves the global tree unpublished.

Local audited snapshots, full raw worker logs, and the explicit failed attempt remain preserved. This is a demonstrated harness/log-volume failure, not a demonstrated index coverage failure or a completed published build. A separate streaming, checksummed collector epoch and publication-only verification of these immutable snapshots follow. No writer rerun, Rust change, discarded failure, or successful through-publication timing is claimed for this attempt.

Source inspection locates the repeated message in hierarchical_index/writer/writer.rs::navigate_with_policy: the condition prints for full-precision mode even though both branches use the full-precision implementation. The default writer selects that mode. This explains the message volume without demonstrating an unsupported arithmetic path. The audited-v3 executable remains unchanged for this experiment; any logging correction requires a separate source epoch.

## Run 231: Streaming log collector source gate

Sixteen local backend and initial-builder tests pass for a separate streaming collector epoch. It scans complete remote stdout and stderr read-only for SHA digests, byte and line counts, and warning/error-token occurrences while transferring only bounded 16 KiB head/tail excerpts. A nonconvergence phrase anywhere in a log propagates to the existing publication gate. Missing, symlinked, or changing logs reject, writers must be terminal and reconciled, and journals bind stable hashes. Backend SHA is 8450836ada852db6181c45ef716381c68ac3d9860626afbb637fd33f69e4dd8b. Evidence is pilot/results/streaming-log-collector-v2/.

Linux execution tests, recollection and verification of existing immutable outputs, and full raw-log archival remain pending. The failed timed attempt stays preserved; no writer rerun or Rust change occurs for this recovery.

## Run 232: Larger-query GPU preparation source gate

Thirty-two combined bound, engine, and publication-wrapper CPU tests pass, and the bootstrap passes shell syntax checking. Independent producer, certified-reference, and launcher review passes twelve focused CPU tests; these counts overlap and are not additive. Review fixes enforce exact shapes and finite distances, safe publisher paths, finite legacy embedding comparisons, actual eager float32 model state, and exact imported producer/chunk hash bindings. Evidence is pilot/results/publication-gpu-independent-review/review.json.

The pinned model/tokenizer/pooling/config bytes total 627,840,230, with 66,150 additional implementation bytes. The 627,906,380-byte total fits the 700 MiB download cap with 106,096,820 bytes headroom. File sizes and Git/LFS hashes bind publication-gpu-plan/model-download-manifest.json. Read-only sandbox account, image owner, quota, security group, and artifact-profile preflight passes. No GPU is launched by this source gate; concrete job hashes and an independent sixty-minute stop guard remain required before launch. Actual CUDA conformance, old 1M/10M reference comparisons, and measured whole-pipeline budget admission precede bulk embedding or new references.

## Run 233: Ten-million-row physical storage inventory

All 128 owner storage diagnostics pass. Finished owner snapshots reference 33,611,589,028 logical object bytes, equal to their retained public-owner manifest union. All physical object files consume 44,396,979,162 logical bytes and 44,475,817,984 allocated bytes, a physical/reference ratio of approximately 1.321. Entire owned checkpoint directories consume 44,477,613,684 logical bytes. The 10,785,390,134 bytes outside enumerated public-owner manifest references remain conservatively retained. Classification authorizes no deletion.

Both host terminals independently reconcile passed with 64 successful outcomes each. The global tree remains unpublished pending separate recovery verification. These final object counts exclude input staging and are not peak scratch or aggregate memory. Raw and summary evidence is pilot/results/fineweb10m-storage-small-v1.* and fineweb10m-storage-summary-v1.*.

## Run 234: Publication-only recovery helper source gate

Eighteen local tests pass for the final recovery helper source. It launches no writers or supervisors, binds the failed case and prepared-input digest, regenerates exact jobs/configuration, and compares newly polled successful terminal evidence with the saved evidence. Full-log nonconvergence, changed terminals, missing maps, invalid audits, failed reopenings, or incomplete global coverage block fresh-case publication. Recovery timing remains separate from the failed 159.143-second launch/control/writer phase. Source copies and tests are preserved in pilot/results/publication-recovery-v1/.

The actual read-only recovery starts with a separately frozen earlier helper and seventeen passing Linux tests, before the final mandatory prepared-digest argument is added. For that launched epoch, an independent external receipt verifies the complete admitted input SHA739c0b520b204c9ade837aa92a501c6ccd8c87d735248a5f395b0df39202cbcb. The later source is not substituted into the active run. Actual recovery and publication results remain pending.

## Run 235: Ten-million-row publication-only recovery passes

Recovery passes in 59.688 seconds without restarting any writer or supervisor. Full-log scans cover all 128 owners; freshly polled terminal records equal the original successful and reconciled identities. Every actual snapshot, map, and audit reopens, complete global ownership covers all ten million IDs, and atomic compare-and-swap publication succeeds. Published tree SHA is d73a550f577a383e51f899b5b105b446de3dac128e82ff93eb644233496638b7.

The original 159.143-second launch/control/writer attempt remains a failed log-collection run. Recovery is a separate verification/publication operation; adding those intervals does not produce a successful uninterrupted benchmark result. The actual earlier recovery source and its external complete-input SHA receipt remain separately preserved, rather than substituted with the later mandatory-argument source. A fresh matched one-host build follows with the same audited-v3 binary and 10,000-row checkpoint policy. Independent recovery reconciliation remains pending.

## Run 236: Independent ten-million-row storage reconciliation

Independent review passes all 128 generation-one owner stores and complete ten-million-row owner counts. Referenced and retained-union object bytes are 33,611,589,028; physical logical bytes are 44,396,979,162; allocated bytes are 44,475,817,984. Ratio is 1.320883. Exactly 13,083 objects totaling 10,785,390,134 bytes lie outside enumerated public-owner manifests. This is not deletion proof. Entire checkpoint-file logical bytes are 44,477,613,684. Inputs, clones, raw logs, and peak scratch are separate. Evidence is pilot/results/fineweb10m-storage-independent-review.json.

## Bounded larger-query GPU pilot starts provisioning

Exactly one new sandbox GPU instance is created: i-03ddedeaccb5cba8f, g6.2xlarge with an L4, in us-east-1d at private address 172.31.34.219. Fixed stop deadline is 2026-10-15 02:00:08 UTC, or October 14 at 10:00:08 p.m. Toronto time, one hour after the recorded start. Compute plus its encrypted 150 GiB gp3 root with configured 500 MiB/second throughput projects to approximately $1.015 for one hour, excluding archive storage and requests. Actual instance EBS baseline is 250 MB/second, so configured throughput is not assumed sustained performance.

On-node account, instance, stop guard, and device verification precede bulk staging. Canonical vector staging is 30.720 GB plus a 25.917 MB bundle under a 32 GiB/600-second cap. No owner vectors, index files, or timed-worker network traffic are used. Request, launch, and deadline evidence is subtree/fineweb/publication-gpu-plan/{request,launch,deadline}-v1.json. No completed CUDA or query-pool result is claimed at provisioning.

## Run 237: Independent ten-million-row publication recovery review

Independent review verifies the actual launched helper/backend hashes, external admitted-input receipt, original failure/configuration/report hashes, fresh terminal equality, all 128 manifest/map/audit/reopen bindings, and complete dense ten-million-ID coverage. The fresh published tree hash matches d73a550f577a383e51f899b5b105b446de3dac128e82ff93eb644233496638b7. Full-log metadata covers 3,151,924,976 bytes and 30,900,585 navigation warnings with no convergence flag. Recovery remains a separate 59.688-second operation. Complete raw-log S3 preservation is a separate pending step. Evidence is pilot/results/fineweb10m-recovery-independent-review.json and review_10m_recovery.py.

The original two-host actual worker-clock writer spans are 155.441 and 147.725 seconds. Recovery includes 52.483 seconds for full-log scanning/classification and approximately 7.2 seconds for remaining reopening, maps, and publication. Per-host clocks define local intervals; no synchronized cross-host interval is inferred.

## Matched ten-million-row one-host run starts

A fresh one-host case starts on worker zero with the same 128-slot graph, audited-v3 binary, sixteen logical CPUs, sixteen concurrent jobs, one balancer thread, two runtime threads, and 10,000-row private checkpoints. The streaming collector source epoch affects bounded receipt handling after writer completion. All 259 input files totaling 30,802,697,505 bytes restore or verify in 55.428 seconds outside construction timing. Final available space is 3,379,516,420,096 bytes; scoped allocated input bytes are 30,803,304,448. No completed one-host timing is claimed at launch.

## Run 238: New GPU on-node identity and cleanup gate

On-node metadata verifies the exact sandbox account, instance, and image. The L4 reports 23,034 MiB device memory and driver 595.91.07. The fixed 02:00:08 UTC stop timer is active before bulk work. A new owned directory uses the image's already-mounted NVMe with approximately 391 GiB free; no filesystem is formatted. Root EBS retains bounded source, deadline, and proof copies.

Twelve Linux artifact/guard tests pass in 0.734 seconds, including actual SIGTERM process-group cleanup in 0.114 seconds. The pipeline uses 64 GiB owned-file, 16 GiB RSS, and 64 GiB free-space limits, per-stage timeouts, and the independent fixed stop deadline. Canonical 30.72 GB S3 restoration starts under its own cap. No CUDA numerical, embedding, or full reference result is claimed at this gate.

## Run 239: GPU resource admission rejects runtime symlinks before work

The first pipeline guard admission rejects the broad owned scope because the Python environment contains symlinks. No child workload or corpus download starts in that failed admission. The original configuration and failure log remain preserved. The retry scopes the 64 GiB logical-byte cap to regular canonical inputs and package-download cache. Runtime and model-cache symlink directories are excluded from that logical counter; aggregate 16 GiB process RSS and the 64 GiB filesystem available-space floor remain active. Model and package downloads retain separate pinned byte caps. Installed-runtime bytes and root proof space require separate inventory; the corrected logical cap must not be described as whole-job storage accounting. The fixed 02:00:08 UTC stop deadline is unchanged.

## Run 240: Optional adapter transport Linux gate

Twenty-eight focused optional owner-reduction transport and benchmark tests pass on the idle 32-vCPU driver in 3.157 seconds, including new wire-candidate contribution journal fields. A local Mac invocation fails on socket permissions and is retained as an environment failure. The optional source lives in a separate directory and is not active on workers; frozen baseline source remains unchanged. The 10M calibration grid is frozen before calibration/test use: global beam 256, 512, 1024, or 2048 crossed with candidate budget 1000 or 4000, using the hundred calibration queries, a 95% primary target, 90% secondary target, and lowest qualifying mean latency within that grid. Held-out evaluation follows selection.

## Run 241: Separate logging-only binary gate

Eleven Rust release tests pass, with one diagnostic test ignored. The new split-tree regression verifies identical full-precision navigation results across the existing mode fallback and tests once-only unsupported-mode warning selection. A live index smoke emits zero stdout bytes and zero expected-full-precision warnings. The isolated binary compiles with SHA55ab58878efdbb4ddf217df386535e3dcf2ab9c0ef0f23b80689c4293c4cb830; full source archive SHA is fc7e62d9a8e7adc6f857f2a626c800badbfc8f0c860aef39a57098fc0d715d30. Evidence is pilot/results/logging-epoch-v5/.

The active matched ten-million-row runs keep audited-v3 unchanged. This logging-only epoch changes no navigation arithmetic or dispatch behavior. The new binary and source require private archive preservation before machine shutdown and separate use/provenance for future measurements.

## Run 242: Uninterrupted one-host ten-million-row build passes

All 128 owner jobs complete with zero failures or nonconvergence. Complete ten-million-row coverage, actual snapshot/map/audit reopening, and atomic publication pass. The frozen configuration uses audited-v3, sixteen logical CPUs, sixteen concurrent jobs, one balancer thread, two runtime threads, and a 10,000-row private checkpoint interval. Host-local writer span is 284.859 seconds. Driver construction takes 393.069 seconds, including 102.378 seconds of complete-log proof; elapsed time through publication is 401.981 seconds. Local publication/CAS takes 2.158 seconds. Initial prepared-input SHA preflight takes 27.536 seconds outside construction.

Published tree SHA is ca315aa87cec88629f4516c7c467a35a3c65ee1ebcf4a092763a712fea6518c7. A fresh uninterrupted two-host run starts under the same binary, checkpoint, and streaming-collector protocol. Earlier failed and recovered two-host evidence remains separate. No successful matched end-to-end ratio is claimed until that run passes.

## Run 243: Instrumented exporter and next-scale validation source gate

Thirteen local profile, scale-plan, parallel-ingestion, and stream tests pass in a distinct source epoch. Canonical vector and UUID chunk descriptors remain byte-identical; duplicate rejection and resume tests pass. New receipts expose file-thread CPU, HTTP/decode/conversion spans, serialization, atomic writes, chunk hashes, and serial SQLite parse/insert/commit/input-hash/database-hash/CPU timings. Inclusive overlaps and missing timers for reused receipts remain explicitly labeled.

The scale validator checks dense source-file and row-group coverage for a hundred-million-row plan beyond the earlier 32-file limit, including bounded requested bytes. A footer-only runner is prepared for an actual 128 MiB/five-minute/512-file preflight; no 100M vector ingestion or machine provisioning occurs in this source gate. Evidence is subtree/fineweb/scale-preparation-review/instrumentation-v1/.

## Run 244: Actual hundred-million-row source metadata preflight

Footer-only preflight passes for exactly 100,000,000 canonical rows across 101 source files in 56.255 seconds, requesting 6,619,136 metadata bytes. Selected dense-vector and UUID compressed columns project to 148,001,848,798 bytes. Complete selected Parquet files total 461,577,865,542 bytes and are not downloaded. The final selected file truncates at 1,078,232 of its 1,110,470 rows.

One complete retry with five percent request amplification projects 310,938,100,204 requested bytes, below the proposed 384 GiB source cap. Evidence is subtree/fineweb/scale-preparation-review/100m-footer-v1.json, SHAad8ba10dfa007db63f06f415de649c55d929a4f4e54a0ef06a3d25f9b2cb1357. No vector columns, 100M node provisioning, or bulk ingestion occur in this run. A bounded same-four-real-file two-versus-four concurrency pilot is planned, starting with two files and counting its fifteen-minute allowance against the fixed six-hour preparation-node deadline.

## Run 245: Ten-million-row final topology aggregation

Independent final-topology aggregation binds the recovered 10M manifests: 10,000,000 unique live IDs, 11,076,960 physical posting entries, 1,076,960 stale entries, zero duplicate-current entries, and zero unreachable nodes. There are 12,109 reachable leaves and 12,377 total stored/reachable nodes; maximum leaf size is 2,048. These are final topology counts, not split or merge action counts. Evidence is pilot/results/fineweb10m-final-topology-v1.json.

## Run 246: Fresh uninterrupted two-host ten-million-row build passes

All 128 owner jobs, complete ten-million-ID coverage, actual snapshot/map/audit reopening, and atomic publication pass with zero job or nonconvergence failures. The matched configuration retains audited-v3, 128 logical slots, sixteen logical CPUs and sixteen concurrent jobs per host, one balancer thread, two runtime threads, and 10,000-row private checkpoints. Host-local writer spans are 154.196 and 154.399 seconds. Construction is 212.377 seconds, including 52.580 seconds of complete-log proof. Elapsed time through publication is 219.556 seconds; local publication/CAS is 2.161 seconds. Initial input preflight takes 28.219 seconds outside construction.

Published tree SHA is 6fe60910493a4f875bca39b2f729b619d24ec2f0eb0298ccf0a77c3b46dd1659. The fresh matched pair gives a 1.845-times ratio in critical host writer span and 1.831-times ratio through publication. These are single paired measurements on larger machines with sixteen of sixty-four logical CPUs assigned to writers; internal balancing initialization is randomized. Earlier failed and recovered cases remain separate. Read-quality and same-immutable-tree placement checks follow.

Matched resource receipts record 3,936.776 aggregate child CPU-seconds and 902,120 KiB largest individual owner RSS on one host, versus 3,836.986 CPU-seconds and 956,532 KiB on two hosts. Both have 128 zero-exit owner jobs. The approximately 2.6% CPU-work difference is consistent with randomized internal balancing; fixed inputs do not establish identical internal work. Individual maximum RSS is not aggregate peak memory.

## Run 247: Independent guarded-compaction six-arm review

All six guarded arms pass independent review: 768 owner maps preserve exact generation-one prefixes, complete 1.1M global-ID coverage per case, frozen boundaries/model, and bound manifest/map/audit/reopen/publication hashes. Every host terminal reconciles, with 128 successful actual jobs per case. Clone inventories match their parents, and no convergence warnings occur. Raw and summary old-reader counts agree: 13, 22, 22, 15, 24, and 15 checks, totaling 111, with unchanged IDs, distances, and full traces. Evidence is pilot/results/guarded-compaction-independent-review.json and review_guarded_compaction.py.

The monitored reader uses one fixed old query. Journals establish overlap with the driver's construction interval, not independently proven overlap with each individual Rust mutation interval. Internal split work is randomized, and complete raw block payloads are not downloaded locally. These limits qualify concurrent-reader stress coverage without changing the verified final audits/publication results.

## Run 248: GPU source stage reaches timeout after file restoration

The single source attempt reaches its unchanged 600-second timeout, returning 124 after all 102 vector files and the small bundle restore, but before numerical or runtime work. No GPU arithmetic, embedding, or reference run starts. Cleanup records zero residual children and approximately 75.2 MB peak sampled source-group RSS. Job construction redundantly hashes the full 30.72 GB corpus twice, delaying its completion metadata beyond the stage deadline.

The failed attempt remains preserved. A separately reported bounded retry verifies and reuses the restored files once, requesting no further object payload unless a file is missing. The stage's 32 GiB cap denotes planned/restored object payload, not an unavailable all-request/retry network counter. Actual SDK retry transfer totals are unavailable. The fixed 02:00:08 UTC machine stop is unchanged.

## Run 249: Optional read-protocol parity setup stops on missing references

The first optional million-row parity attempt stops before any query because the 32-vCPU load driver has test embeddings but lacks the saved exact-reference file; prior load runs do not require it. This is a setup failure, not an observed search or protocol failure. The plan and failure log remain preserved. The saved exact answers stage by their existing SHA, and a fresh v2 output records the retry. Worker samplers start, while the coordinator sampler finds no active process after setup exits; no successful parity resource window is inferred from this attempt.

## Run 250: GPU staged-input verification retry passes

The separately reported retry passes in 46.167 seconds, SHA-verifying and reusing all 30,745,917,011 logical object bytes locally with zero new object payload. This follows the first 600-second timeout, which restored those files. Aggregate source-phase elapsed time is at least 646.167 seconds plus control/guard overhead; actual all-request/SDK-retry transfer totals remain unavailable. Concrete job SHA is 18aa5574846efc6f1017c3b3b9e13b42f609172ea39fe36b2391679d66b6d8ac.

Locked numerical runtime installation starts with foreground child ownership and separate package/EBS proof watchdogs. The corrected 64 GiB logical scope covers canonical inputs and package cache, with runtime/model bytes inventoried separately. The fixed 02:00:08 UTC machine deadline remains unchanged. No GPU numerical or embedding result is claimed yet.

## Run 251: Real million-row adapter reduction parity passes

All hundred held-out queries match the retained baseline's complete logical traces, result IDs/distances, recalls, and logical work counts under the unchanged memory-v4 binary, immutable tree, and global B512/M1000. Held-out recall remains 97.88%. Mean logical candidate contributions are 116,867.82 per query, while mean transmitted wire contributions are 4,000. Total response bodies across all phases average 638,758 bytes per query.

Mean query latency is 294.288 milliseconds in this parity window; it is not a matched performance comparison. The earlier missing-reference setup failure remains preserved. This is a separate optional Python protocol/source epoch, with baseline source unchanged. Same-immutable-tree ten-million-row placements and the frozen calibration grid follow.

## Run 252: Locked GPU runtime and deployed CPU gate pass

The locked numerical runtime installs all forty-four pinned packages, including torch 2.5.1+cu124, within its 600-second limit. Thirty-two deployed CPU tests pass on the actual GPU node's Python 3.10/NumPy 2.2.6 environment in 0.160 seconds. Exact prepared-source, chunk, and imported-module bindings are checked before the million-row CUDA integration gate. Installation and CPU test success are not CUDA numerical or reference-generation results. The fixed 02:00:08 UTC stop deadline remains unchanged.

## Run 253: Ten-million-row optional serving helper gate

Thirty-six focused Linux tests pass in 2.661 seconds for the optional owner-reduction transport, benchmark, and orchestration source epoch. Frozen baseline and million-row parity directories remain unchanged. The selected immutable 10M tree with SHA6fe60910493a4f875bca39b2f729b619d24ec2f0eb0298ccf0a77c3b46dd1659 copies and verifies for cohost placement outside read timing. The frozen setup uses memory-v4, four adapters and sixteen logical CPUs per host, memory profiling, and optional adapter reduction. Both placements query the same owner snapshots; independently built trees are not used for trace equality. Calibration and held-out results remain pending.

## Run 254: New GPU million-row exact-reference integration passes

All two hundred queries match independent CPU reference IDs and distances. Three million full-score checks, including rejected rows, and signed, tie, near-tie, and internal-precision probes pass. Warm scan is 10.758 seconds; synchronized scoring is 0.181 seconds; host-to-device transfer is 0.279 seconds; device-to-host transfer is 0.236 seconds; float64 reranking is 0.303 seconds. Retained candidates range from 357 to 450 per query. Measured device peak is 236.1 MB. CPU usage is 8.331 user and 2.439 system seconds, with 1.60 GB lifetime peak RSS.

Input validation takes 10.154 seconds, and initialization/probes take 3.981 seconds separately. Endpoint/top-k/normalization/loop remainder is not individually isolated; the small scoring fraction supports investigation but does not prove a specific overhead attribution. The actual ten-million-row integration follows. Evidence is subtree/fineweb/publication-gpu-plan/integration1m-report.json; the fixed 02:00:08 UTC stop deadline remains unchanged.

## Run 255: Same-tree ten-million-row read placements pass staging

All sixty-four remote owner snapshots copy and verify: 22,257,151,296 bytes, with 139.767 seconds transfer and 81.690 seconds source/target SHA inventory. Cohost readiness takes 245.389 seconds, and both placements are ready in 249.923 seconds, all outside query timing. The same selected tree SHA6fe60910493a4f875bca39b2f729b619d24ec2f0eb0298ccf0a77c3b46dd1659, memory-v4 binary, maps, and actual roots bind both four-adapter placements. The 32-vCPU driver verifies all 263 immutable metadata files and executable from a 59.49 MB compressed bundle.

Frozen calibration starts only after readiness; no held-out queries execute before saved selection. The declared cache policy uses fresh persistent application readers at startup, one calibration-query warmup per configuration, ascending frozen grid order, and warm/uncontrolled operating-system cache. Selection is the fastest observed qualifying mean within that grid, not a cache-neutral global optimum.

## Run 256: Ten-million-row GPU exact-reference integration passes

All two hundred query IDs and distances match fresh complete-10M CPU references. Thirty million full-score checks and all arithmetic pathologies pass. Warm scan is 113.304 seconds; synchronized scoring 1.681 seconds; host-to-device transfer 2.770 seconds; device-to-host transfer 1.792 seconds; float64 reranking 2.445 seconds. Device peak is 236.1 MB, and retained candidates range from 560 to 713 per query. CPU usage is 80.700 user plus 32.069 system seconds. Input validation is 137.786 seconds and initialization/probes 1.420 seconds separately. Endpoint, top-k, normalization, and loop remainder are not individually isolated. Evidence is subtree/fineweb/publication-gpu-plan/integration10m-report.json.

## Run 257: Complete frozen eleven-thousand-query embedding pool passes

All 11,000 frozen texts produce finite, unit-norm, 768-dimensional embeddings in 27.036 seconds including 9.205 seconds of model download/load. Actual tokenizer lengths range from four to fifty-eight. The 128-query CPU/GPU probe has maximum absolute difference 3.958e-7 and cosine agreement at least 0.999999999996; repeated GPU output is bit-identical. Sixteen legacy CPU probes have maximum absolute difference 1.080e-7. Batch-128 pilot device peak is 1.475 GB; the full producer peak is not separately recorded.

Outputs total 187,150,753 bytes, SHA c95d65ffbb08997a95a63edc54c6bc541f9167eb82bed951912cee5737762d4e, including text-format embeddings and immutable float32 batches. These are embeddings, not complete exact references. An independent three-query full-10M CPU oracle and thirty-two-query reference pilot follow. Full eleven-thousand-reference completion is not claimed. A separately reviewed thousand-query calibration fallback runs only if measured admission with runtime margin and preservation reserve fits the unchanged 02:00:08 UTC stop deadline.

## Run 258: Ten-million-row calibration B256/M1000

All hundred calibration queries succeed with zero failures. Recall@100 is 79.41%; mean latency is 288.447 milliseconds and median 261.211 milliseconds. Setting elapsed time is 33.037 seconds. This point misses both the 95% primary and 90% secondary targets. It uses the frozen two-host selected tree, memory-v4, optional adapter reduction, four adapters and sixteen logical CPUs per host. The next grid point follows; held-out queries remain unexecuted.

## Run 259: Ten-million-row calibration B256/M4000

All hundred queries pass with zero failures. Recall@100 is 79.43%, mean latency 333.889 milliseconds, median 329.564 milliseconds, and setting elapsed time 37.857 seconds. The point misses the 95% and 90% targets. The frozen tree, protocol, allocation, and cache policy remain unchanged.

## Run 260: Ten-million-row calibration B512/M1000

All hundred queries pass with zero failures. Recall@100 is 86.34%, mean latency 248.576 milliseconds, median 244.220 milliseconds, and setting elapsed time 28.801 seconds. The point misses the 95% and 90% targets. Its lower latency despite a wider beam is an observation under warmed persistent caches and fixed grid order, not a monotonic search-cost claim. Warming/order may contribute, but the cause is not isolated. Held-out queries remain unexecuted.

## Run 261: Ten-million-row calibration B512/M4000

All hundred queries pass with zero failures. Recall@100 is 86.36%, mean latency 383.362 milliseconds, median 377.122 milliseconds, and setting elapsed time 43.062 seconds. The point misses both recall targets. Frozen protocol and cache policy remain unchanged.

## Run 262: Ten-million-row calibration B1024/M1000

All hundred queries pass with zero failures. Recall@100 is 91.22%, mean latency 278.951 milliseconds, median 273.130 milliseconds, and setting elapsed time 32.211 seconds. This point meets the 90% secondary target and misses the 95% primary target. Held-out queries remain unexecuted while the remaining frozen grid runs.

## Run 263: New-query reference pilot passes; remaining production work is declined

The thirty-two-new-query complete-10M reference pilot passes numerical checks, thirty million full-score checks, and three independent whole-corpus CPU oracles. Oracle time is 68.638 seconds. Warm scan is 69.621 seconds; synchronized scoring is 0.353 seconds; host-to-device transfer 2.568 seconds; device-to-host transfer 0.376 seconds; float64 reranking 0.427 seconds. Torch peak is 239.1 MB.

Measured admission declines the full eleven-thousand-reference run. The separately reviewed thousand-query calibration fallback also fails conservative remaining-time admission: projected runtime plus input/oracle work, margin, and preservation reserve is 3,903 seconds versus 1,708 seconds remaining. This is a budget-admission decision, not a failed numerical run or proof that an optimized pipeline cannot fit. No bulk new-query references start or publish. Both the new thousand-query calibration and ten-thousand-query test references remain pending. Complete frozen embeddings, integrations, pilot, failures, and configurations archive before terminating only the new GPU node; its fixed deadline is unchanged.

## Run 264: Ten-million-row calibration B1024/M4000

All hundred queries pass with zero failures. Recall@100 is 91.24%, mean latency 454.538 milliseconds, median 451.827 milliseconds, and setting elapsed time 50.605 seconds. The point meets only the 90% secondary target.

## Run 265: Ten-million-row calibration B2048/M1000

All hundred queries pass with zero failures. Recall@100 is 95.45%, mean latency 324.469 milliseconds, median 320.768 milliseconds, and setting elapsed time 37.363 seconds. This point meets the 95% primary target. The last frozen grid setting follows; selection waits for all eight settings, with held-out queries still unexecuted.

## Run 266: Final ten-million-row calibration setting and frozen selection

B2048/M4000 completes all hundred queries without failure: recall@100 95.50%, mean latency 534.064 milliseconds, median 529.657 milliseconds, and setting elapsed time 59.293 seconds. All eight frozen configurations complete, totaling eight hundred successful calibration queries and zero transport failures.

The frozen lowest-observed-mean qualifying rule selects B2048/M1000 at 95.45% calibration recall and 324.469 milliseconds mean latency. Selection SHA is ae76af1a02203a5433c186c1d4411eeb8d53b6cc488293660fc79f8b8c85b5b0, saved at 1792028048.4532332 before any held-out execution. Both placements restart application readers; operating-system cache stays warm/uncontrolled. The same immutable selected tree and selected policy evaluate a hundred held-out queries per placement next.

## Held-out measurement interference identified before comparison

The first held-out pass starts after a recorded fresh-reader restart, but calibration-archive copying from the active 32-vCPU driver overlaps its first cohost queries. CPU and network copy work contaminates latency attribution. All v1 outcomes remain preserved as quality and interference evidence. After copying completes, a fresh-reader v2 pass repeats the unchanged selection and immutable tree without overlapping driver or worker I/O. No policy retuning occurs after held-out exposure, and the interfered pass is not used as a clean latency measurement. Completed outcomes are reported separately.

## Run 267: GPU result archive and complete embedding consistency pass

The result-only archive passes in 20.612 seconds: 357 files, 370,702,321 logical member bytes, and a 258,573,248-byte archive. Full pinned-version restoration and every member SHA pass. A separate full eleven-thousand-query text-format versus float32-batch, source-order, frozen-ID, and integration-disjointness check passes in 3.184 seconds. Norms range from 0.9999998966 to 1.0000000937. All six final small proof objects also upload and restore successfully.

Archive descriptor SHA is 63447c79995ff9e91233ffad43f72a3683dccbee4bba62a3e7ac79042f0b7651, version ZYD4f0vvpO4Vk3G7nDMmTmjviRQAmtaq. Evidence is subtree/fineweb/publication-gpu-plan/archive-report.json and final-proof-catalog.json. After rechecking sandbox account identity, termination is submitted only for i-03ddedeaccb5cba8f. Its state is shutting-down at this report; final terminated/root-deletion confirmation remains pending. No additional GPU work starts, and new calibration/test exact references remain incomplete.

## Run 268: Ten-million-row held-out quality and placement consistency pass

Both same-immutable-tree placements pass all hundred held-out queries with zero failures and 95.87% recall@100. All hundred complete logical traces, result IDs/distances, candidates, and other frozen trace fields match under unchanged B2048/M1000 and selected tree SHA6fe60910493a4f875bca39b2f729b619d24ec2f0eb0298ccf0a77c3b46dd1659. Selection precedes held-out exposure; no policy retuning occurs.

The v1 pass has known calibration-archive-copy overlap, so latency values are excluded from clean performance claims. It establishes correctness and quality, not uncontaminated latency. Final selected-tree storage inventory, a hash-bound quality/capacity receipt, and a fresh-reader v2 latency repeat follow. Earlier recovered-tree storage remains separate until the selected immutable tree's inventory is bound.

## Run 269: Independent ten-million-row calibration reconciliation

All eight hundred calibration records reconcile with exact references and saved means. B2048/M1000 is the lowest observed mean-latency configuration meeting 95%, with 95.45% recall and 324.469 milliseconds. Tree, calibration inputs, and selection hashes verify. Selected-setting mean response traffic is 1.583 MB and request traffic 5.183 MB per query. Logical candidate contributions average 105,265, while wire candidates average 8,000 across eight adapters. The widest setting averages 3.441 MB responses; maximum individual owner response is 288,010 bytes. Per-owner cache maxima sum to 1.168 GB payload and 1.195 GB owned entries, with zero block pins; this sum is not simultaneous RSS. Evidence is pilot/results/fineweb10m-calibration-independent-review.json and review_calibration_10m.py. Held-out chronology and disjointness review remain separate.

## Hundred-million-row prepared wrapper remains held before launch

Independent review finds three concrete blockers in the prepared runnable-v2 epoch: a whole-root scope includes Python-environment symlinks rejected by the tested resource guard; producer binding accepts an incomplete executed-module set; and the new six-hour clock lacks hash-bound actual restart evidence and reconciliation of the old preparation timer. Fifteen focused scale tests pass but do not establish those bootstrap properties. A repaired distinct epoch is required before any restart. No 100M node restart, vector ingestion, or active benchmark source change occurs. Existing sandbox authorization covers the fixes; no new user permission is required.

## Run 270: Selected ten-million-row quality and capacity qualification

The hash-bound qualification independently verifies every global map ID from zero through 9,999,999 exactly once; selected-tree store/manifest bindings; all two hundred held-out recalls; all hundred full-trace placement matches; selection-before-test chronology; and archived object hashes. The selected tree references approximately 33.616 GB current objects, with 44.376 GB physical objects and 44.457 GB total owned checkpoint files. Zero unknown paths are reported; this does not authorize deletion of private objects outside retained snapshot references. V1 latency stays excluded due to interference.

Receipt is pilot/results/fineweb10m-preparation-qualification-v1.json, SHA665d6c8fbe1d2e15746b07770506c071d594b1348e95bdc5f3718bb6135e244d. It qualifies materializing the bounded hundred-million-row preparation job, not bypassing independent runnable-source fixes, Linux/bootstrap validation, fresh instance/account checks, or the new fixed stop guard. The clean v2 held-out repeat starts separately.


## Run 271: Clean ten-million-row held-out placement comparison

The clean v2 pass succeeds on all two hundred queries, with 95.87% recall on both placements and all hundred complete logical traces matching exactly. The same selected immutable tree, memory-v4 and B2048/M1000 remain fixed. One-host mean latency is 453.934 milliseconds, median 432.585 milliseconds and wall time 51.128 seconds. Two-host mean is 367.420 milliseconds, median 347.055 milliseconds and wall time 42.088 seconds. Both restart application readers; operating-system cache is warm/uncontrolled. No archive transfer or worker preparation overlaps either query window. These are sequential passes, not load-capacity measurements. The interfered v1 evidence remains preserved and excluded from latency claims.

## Run 272: GPU termination and root deletion verified

AWS verifies i-03ddedeaccb5cba8f terminated, and its exact root volume vol-07aa662d3e3bcd354 returns InvalidVolume.NotFound. All evidence is fully archived/restored. Conservative compute-plus-root projection is below $0.78 using launch through terminal-verification wall time, excluding S3; actual billing may be lower. Proofs are subtree/fineweb/publication-gpu-plan/terminated-api.json, root-volume-after-termination.stderr and cleanup-and-cost.json. No GPU or retained GPU root remains.

## Run 273: Repaired hundred-million-row source admission passes

Frozen runnable-v3 SHA d4be814001b5caff847b35c9c78303d65d76548a42f67f703c57c443b6e3d14c passes independent source bindings and twenty-five CPU tests. The repaired source binds the actual 10M qualification receipt, complete producer-module identity, actual boot clock, old-timer reconciliation and strict runtime inventory. Actual Linux installed-venv/tiny workflow execution, fresh stopped-node identity/headroom and new fixed stop guard remain required before restart or bulk ingestion. Operational ownership transfers to linux_pilot under the existing sandbox authorization; readiness makes no further remote changes. The prep node remains stopped.


## Run 274: Hundred-million-row Linux preparation gate and receipt-only recovery

The actual Linux gate passes twenty-five producer tests, ten resource-guard tests and the guarded twenty-one-row preparation/archive/handoff workflow. The copied runtime, source and cache inventory contains 7,055 entries and 390.8 MB, without symlinks or surviving writer groups. Actual SIGTERM cleanup takes 0.114 seconds. The final receipt writer initially fails with KeyError('reconciled') because reconciliation is represented by successful outcomes and an empty unreconciled_children list. The failed receipt attempt remains preserved; a report-only correction succeeds without rerunning the workload. Evidence is pilot/results/prep100m-linux-gate-v3/prep100m-linux-gate-v3-small.tar.gz. The tiny archive fixture uses an in-memory store; real SDK/role/large-object restoration is separately established by the preserved 10M archive, rather than by this fixture.

The first remote execution request is automatically rejected because review identifies the task-owned old control driver as protected Lyon. Fresh Cops account, EC2 and instance identity evidence distinguishes sandbox i-00268d1e1dfea0357 from protected i-0f0b5dcb6888ad43d; the evidence-based same-target retry is approved. No unresolved approval block remains.

## Run 275: Dynamic sharding correctness milestones and retained failed fixture

The opt-in implementation passes fifteen Linux tests in 8.450 seconds, including nine fixtures using actual Rust readers, three planner tests and three static manifest compatibility tests. Peak process RSS is 29,464 KiB. Tests exercise whole-subtree migration, destination-only reopening after the source goes offline, old generation-specific HTTP readers, full global ID/vector preservation through split/merge/reassignment, direct/HTTP exact and approximate traces for each new topology, append compaction after a split, retained-generation admission, snapshot-bound five-block audits, failed/corrupt transfers and competing deployment publications. An earlier thirteen-test gate has twelve passing tests and one expected-error-type mismatch for a missing destination map. The failure remains preserved; normalizing that public admission error produces a separate passing gate. All source epochs and logs remain under dynamic-review/milestone-v*/.

The final passing source archive SHA is 13aad6da2e06a0686a04403dc4c55bf16ee10262671c42fb44525c682fffb1d6; its exact source hashes, command and output are dynamic-review/milestone-v5/{manifest.json,linux-gate.log,source.tar.gz}. The checked binary SHA is 7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158. These are tiny correctness fixtures, not performance or distributed deployment evidence. Remote transfer supervision/authentication and production consensus remain unimplemented. Astra medium code review is running against this frozen milestone. Generation-specific endpoint readiness has a further worker-identified admission gap; a separate follow-up epoch will require an actual HTTP root probe.

## Run 276: Logging-only executable and source preservation pass

Both the logging-only V5 executable and its full source upload privately and pass complete checksum-verified restores in 1.073 seconds. Executable SHA is 55ab58878efdbb4ddf217df386535e3dcf2ab9c0ef0f23b80689c4293c4cb830 and source SHA is fc7e62d9a8e7adc6f857f2a626c800badbfc8f0c860aef39a57098fc0d715d30. The exact bucket versions and restored-object checks are in pilot/results/logging-epoch-v5/archive-proof-v1.json. Existing V3 construction comparisons remain unchanged; full-node V5 construction will be a separate matched epoch.

## Hundred-million-row bounded preparation starts

After the Linux gate, 10M qualification receipt and fresh sandbox identity/root/profile checks pass, preparation instance i-0d606301de7f8b4ae restarts. Its actual boot establishes the independent absolute stop at October 15 08:01:30 UTC, exactly six hours later. The completed 10M transient timer is absent after reboot, and its retained root evidence remains untouched. Existing worker and 32-CPU driver deadlines stay unchanged. Immutable job SHA is 624f122e0a274b1e19ca76c6bba3aacda70a56c3a770fcfa93d68d020d79002f. The pipeline starts with 3.502 TB free on fresh instance storage and the fixed stop active. Its compute rate is $1.373/hour and six-hour ceiling is $8.238. This is a launch, not a completed preparation/index benchmark; full audit, exact references, archive and restore gates remain pending.


## Run 277: Post-compaction read setup failures remain separate

The first generation-two serving setup fails before any reader launches because compaction case metadata lacks host_order. A fresh derived read case uses deterministic sorted host order without changing the published tree. The second setup fails before reader launch because a generic worker source directory lacks checkpoint_inventory. A third setup binds the exact isolated optional source directory. Both failed setup logs remain preserved in pilot/results/fineweb1m1-gen2-quality-small-v1.tar.gz. These attempts contribute no query timing.

## Run 278: Published 1.1M generation-two read quality and placement consistency

Both placements pass all hundred held-out queries at unchanged B512/M1000 against fresh complete-1.1M references. Recall@100 is 97.41% on both, with zero failures and all hundred complete logical traces, IDs and distances equal. Selected immutable tree SHA is 04d57d52834fdd41b2065202775dcdd6769ed85e7721d8d8907174771951ea27. All twelve recorded adapter groups stop afterward. Cohost staging transfers 2.192 GB in 112.31 seconds, outside query timing. Exploratory mean latencies are 286.971 and 220.960 milliseconds, without resource profiling or load-capacity claims. Policy is unchanged after held-out exposure. Evidence is pilot/results/fineweb1m1-gen2-quality-small-v1.tar.gz, including summary and per-query journals.

## Run 279: Advertised endpoint readiness fixture passes

A distinct dynamic-v6 epoch passes sixteen Linux tests in 8.758 seconds, including ten actual Rust fixtures, with 29,684 KiB peak process RSS. When a deployment advertises an HTTP serving endpoint, readiness probes its pinned tree, root, generation and map as well as the local checkpoint. An unavailable endpoint blocks publication. Source SHA is 7174912b3fe3f2460e309be634df9e669fd6fb20122524def7d78f5977acce04; source, manifest and actual output are in dynamic-review/milestone-v6/. V5 remains frozen for its independent code review. No performance or remote orchestration claim follows from this fixture.

## Run 280: Astra medium working-transaction review completes

The read-only Astra medium review verifies all seven canonical v5 source hashes against the preserved archive and completes without modifying code or running tests. It resolves the earlier local-authority publication-fencing and stable-identity findings, and recognizes actual old-reader/query-budget coverage. Four source-derived bugs remain: a merged cosine centroid can cancel to zero and reject queries; file fsync does not make the replica directory hierarchy or routing model durable; journal recovery can misclassify a transaction after a descendant publishes; and the resource planner omits the enforced 128-subtree worker cap. The report also identifies metric, separate-process race, targeted lazy-vector corruption and repeated-append acceptance gaps. These are review findings rather than executed reproductions.

All findings are delivered to the Sol6.1 implementation owner for a separate v7 epoch with deterministic regressions. The worker also fixes planning helpers that still depend on original map paths after migration. Report is dynamic-review/02-working-transactions-review.md; the log verifies gpt-6-astra, medium effort, read-only sandbox and exit zero. Another independent review follows the fixed source and actual integration gates.


## Run 281: Independent full-node V5 construction admission

Independent local review admits sequential bounded execution of the staged one- and two-host full-node V5 construction arms. Both bind identical 128 tasks and ten million rows, checkpoint 10,000, balancer one, runtime two, CPU affinity 0–63 and up to sixty-four concurrent jobs per host. Executed core and inspection source closure hashes match; unrelated deployed preparation modules differ and remain explicitly excluded from identity claims. Exact sandbox instances, no relevant active processes, binary/wrapper hashes and effective resource thresholds pass. The receipt is pilot/results/fineweb10m-v5-allocation/independent-admission/admission.json. This is permission to execute under existing authorization, not a completed measurement. The operational agent encounters a transient model-capacity failure after staging; it is resumed with the same requested Sol6.1 model and must reconcile existing submissions before continuing.

## Run 282: Independent clean ten-million-row held-out and resource review

The independent review recomputes 95.87% recall from full ten-million-row exact references for both placements, verifies selection-before-test chronology, disjoint calibration/test IDs, all hundred complete trace matches, all 128 map/snapshot hashes and unique coverage of all ten million global IDs. Means remain 453.934 and 367.420 milliseconds. Sampled interior process CPU averages are 4.345 cores on the cohost placement and 2.569 plus 2.601 across the two hosts. Process RSS sums can double-count shared pages; both reader groups remain resident. These are one hundred sequential queries per placement with warm/uncontrolled OS cache, not p99 or saturation evidence. Unrelated host/network absence relies on timing-isolation declarations, and persisted vector blocks are not reread by this metadata review. V1 latency stays excluded. Receipt is pilot/results/independent-heldout-v2/review.json.

## Run 283: Dynamic-v7 regressions pass before fresh Astra review

All twenty-three Linux tests pass in 14.605 seconds, including fourteen real Rust-backed test methods across L2, inner product and cosine. Peak process RSS is 32,856 KiB. Regressions exercise zero-cosine-centroid rejection, checkpoint/child/ancestor fsync ordering and injected failure, definitive publication receipt and descendant ancestry reconciliation, and heterogeneous 129-subtree admission with the enforced 128-slot cap. Other new fixtures exercise a separate-process topology-versus-placement race, targeted lazy raw-vector block loss, source-offline migration followed by split planning/rebuild, normalization-policy binding and advertised HTTP readiness. The actual test log independently prints the V4 executable hash. Source SHA is 0850357977a389d97e6897f0d403a024f68fa4b183ab36a501741b1b5cad6fa1; source/manifest/actual log are dynamic-review/milestone-v7/. These tests are correctness evidence, not physical crash or scaling measurements.

A fresh ephemeral Astra medium read-only review runs against frozen v7; report/log paths are dynamic-review/03-integration-review.{md,log}. The Sol6.1 worker prepares the next real two-host migration demonstration without changing the reviewed source or launching overlapping jobs. Supervised cross-host transfer and serving lifecycle remain open implementation work.


## Run 284: Idle 32-CPU driver stops with retained evidence

AWS confirms task-owned i-05c41c0090a257a06 stopped at 02:26:34 UTC, before its unchanged 03:01:58 deadline. Its retained root vol-00968b03fdf62e854 preserves the entire working directory; the local preservation manifest hashes 1,062 evidence/source files totaling 3,209,266,805 bytes. Conservative launch-through-verification compute is at most $3.702, with retained root storage extra. Proofs are fleet/load-driver32/{stop-preservation-v1.json,stop-summary-v1.json,stopped-state-v1.txt}. Remaining active compute is approximately $11.4338/hour for the original three task nodes and isolated preparation node, excluding storage/network.

## Run 285: Full-node V5 two-host ten-million-row construction

All 128 owners converge with complete ten-million-ID coverage, map/snapshot audits, actual reopening and atomic global publication; no failures occur. Each host uses all sixty-four logical CPUs with at most sixty-four concurrent jobs, checkpoint 10,000, balancer one and runtime two. Host writer spans are 70.720 and 81.793 seconds; construction is 88.930 seconds and time through publication 95.899 seconds. Input verification takes 28.027 seconds separately. Child CPU is 4,595.16 seconds and largest individual owner RSS 999,896 KiB. Aggregate writer RSS peaks at 18.52 and 20.13 GB; owned logical scopes peak at 53.00 and 37.70 GB. Worker zero includes a full input replica while worker one holds assigned inputs. The new tree commits successfully. Raw publication/resource evidence is being packaged under pilot/results/fineweb10m-v5-allocation/; complete vector/checkpoint archives remain pending and must not be inferred from metadata preservation.

## Run 286: Full-node V5 one-host construction and matched pair

The sequential matched one-host arm passes all 128 owners, full ten-million-ID coverage, audits, actual reopening and publication with no failures. Writer span is 127.643 seconds, construction 136.262 seconds and time through publication 144.953 seconds; 27.426-second input verification is excluded. Child CPU is 5,249.66 seconds; largest individual owner RSS is 913,708 KiB. Aggregate writer RSS peaks at 23.59 GB and owned logical scope at 75.25 GB, within the admitted bounds.

This single V5 pair gives 1.560-times critical writer-span and 1.512-times elapsed scaling through publication. One-host child CPU is about 14.2% higher, and internal balancing work is randomized. Both arms use all sixty-four logical CPUs per host. Comparing absolute timings against V3 sixteen-CPU results changes the logging epoch as well as CPU allocation, so those comparisons do not isolate CPU effects. Independent final reconciliation and complete checkpoint archival follow outside performance windows. Workers are released for a separately admitted tiny dynamic correctness fixture.

## Run 287: Astra integration review clears four fixes and finds three gaps

Astra03 verifies all nine canonical files and archive SHA for v7, reads the retained 23-test output and binary hash, and completes read-only with exit zero. All four concrete prior findings are resolved. Three source-derived correctness gaps remain: initial snapshot admission does not establish canonical-vector correspondence; newly created publication/journal ancestor directories lack complete durability; and accepted relative artifact paths lose their base when readers reopen. Report is dynamic-review/03-integration-review.md with verified Astra/medium settings in its log. The worker accepts all findings and implements a new distinct source epoch, including mismatch rejection and expanded append/metric cases, before real two-host execution. This review is not an executed reproduction or scale result.


## Run 288: Full-node V5 pair independent reconciliation passes

The closed metadata archive SHA3f2cbe399b1a8d10c86460a98f14d435dd46406089d61a07fd40546c99594b0a passes independent reconciliation of both 128-owner/10M coverage cases, maps/snapshots/raw logs, snapshot-bound audits, remote-root reopen receipts, all successful wrappers and host terminals. Exact writer ratio is 1.560565 and through-publication ratio 1.511523. Child CPU is 5,249.662 versus 4,595.162 seconds; the two-host arm uses 12.47% less CPU relative to one host, or equivalently one host uses about 14.24% more relative to two. Randomized tree/audit work differs. Nominal one-second resource sampling has an observed gap as large as 4.578 seconds, so recorded peaks are sampled lower bounds. Receipt is pilot/results/independent-v5-completed/review.json. This archive contains metadata, not complete checkpoint blocks; remote reopen receipts are checked without independently reopening absent block stores.

## Run 289: Dynamic-v8 content and append gates pass

All twenty-nine Linux tests pass in 41.979 seconds, including eighteen real Rust-backed methods across L2, inner product and cosine. Peak process RSS is 49,124 KiB. New tests reject mismatched same-sized canonical corpora, unverified producers, legacy outputs and relative artifacts; exercise control-ancestor fsync failure and post-pointer reconciliation; and verify non-axis cosine normalization, immediate source-offline append after migration, repeated appends, old HTTP readers and direct/HTTP parity. Actual build provenance binds the admitted immutable writer/producer, sealed retained inputs, process return evidence, manifest and full output inventory. This assumes trusted worker execution and is not attestation against a compromised worker. Source SHA is ab31a2556d1c356b8e4d30c2c475fda04d558ec9aaa173b9a57dc97a3628ac29; evidence is dynamic-review/milestone-v8/. A fresh read-only Astra medium review runs against this frozen epoch under dynamic-review/04-content-admission-*.

## Complete checkpoint archival begins; worker-zero attempt fails closed

The independently reviewed archive helper and concrete configs pass admission after five actual Linux safety/roundtrip/failure tests on each worker. Per-host one-hour guards enforce the exact owned scopes. Coverage includes original selected 10M and both V5 stores, including private/unreferenced files; reconstructible cohost replicas are excluded. Pre-upload current S3 inventory is 668 objects totaling 75,203,377,499 bytes; older versions and incomplete multipart storage remain unmeasured. Evidence is fleet/s3-current-inventory-v1.json and pilot/results/checkpoint-archive-v1/.

The first worker-zero attempt fails during post-export source verification. Its guard reconciles all child groups and no object upload from the failed store is admitted. Worker one independently reaches upload; neither whole-store preservation nor restoration is complete at this report. The exact cause is under investigation and the failed source/logs remain retained before any retry. This failure is separate from successful index construction. The 100M ingestion remains active with 51 source files complete, approximately 159.8 GB owned bytes and 1.65 GB RSS; complete ingestion is not yet claimed.


## Run 290: Archive probe failure is isolated to an equal-distance tie

Reproduction returns global ID 9558722 or 9843466 from the auxiliary standalone legacy top-one search, both at distance 0.45406025648117065, while snapshot/map hashes, root and generation remain identical. This differs from the deterministic distributed global-frontier protocol used in the recorded read comparisons. Failed export and raw tie responses remain preserved in a compact proof archive, reported SHA prefix d80ef07c; no source or failed tar is deleted. A distinct deterministic pinned-ID exact-rerank/fetch probe revision passes six Linux tests and awaits narrow independent retry admission. Full-byte restoration and snapshot/map/audit validation remain required. Worker one has restored 15.89 of 22.29 GB at the reported sample, not a completed archive yet.

## Run 291: Astra04 closes three findings and identifies two further bugs

The read-only Astra medium review verifies all eleven frozen v8 files and source archive, then closes the three previous findings under the trusted immutable local-worker contract. Two medium source-derived bugs remain: compaction prefix validation reads obsolete candidate preparation map paths instead of destination-resolved maps; and failed pointer-directory synchronization can classify a visible pointer as durably published without establishing persistence. The worker accepts both and prepares v9 regressions. Report is dynamic-review/04-content-admission-review.md; actual model/effort and exit zero are preserved in its log. Mutable-path races still require enforced artifact ownership/immutability; build hashes alone are not protection against a compromised actor.

Separate unfinished cross-host components pass eight local transfer, reader-reservation/cleanup and authenticated-client admission tests. No two-host job runs. The bounded proof will mirror the tiny complete checkpoint stores on the driver for existing semantic validation while probing/querying the actual physical workers. This deliberately adds a full-copy verification path for a correctness demonstration and is not presented as a scalable migration design. Real execution remains held during archive jobs and pending frozen-source review, complete supervision/configuration admission and v9 local fixes.


## Run 292: Deterministic archive retry admission passes

Independent narrow review admits only the fresh worker-zero retry, helper SHA2ed37aff55744ae22b1f57b04b855d271c7f6abca9ee1e5a9453ab75f732a397. Six independent local tests and six preserved Linux tests pass. Explicit local ID zero exact rerank is bound to the first global-map entry; source/byte/snapshot/map/audit/root/restore checks remain mandatory. The retained failed 22.234 GB tar is included in the new guard scope; projected usage is 378.685 GB against the 481.036 GB effective threshold. The initial failing comparison values were not saved, so tie attribution relies on retained independent reproduction and the file-check failure position, rather than an exact replay of those missing responses. New probes persist responses before comparison. Receipt is pilot/results/checkpoint-archive-independent-review/retry-v2-admission.json. Worker zero starts a separate one-hour guarded attempt; worker one's original attempt stays unchanged.

## Run 293: Selected ten-million-row worker-one checkpoint preservation

The selected 6fe tree's worker-one half passes complete upload, pinned-version restore, every extracted member SHA, all snapshot/map/audit/root bindings and reopened source/restored query probes in 648.802 seconds. Its 64 owners contain 5,001,462 rows; 17,528 files total 22,257,151,296 source bytes. Durable tar size is 22,290,892,800 bytes, reported SHA prefix063134ce and version rwjHYhAovZDFuvIPmGYOg0bQDleveneh. Exact descriptor and full hashes remain in worker archive evidence; parent waits for local packaging. Worker zero remains an independent in-progress retry, so no full-tree archival completion is claimed. Worker one proceeds to its V5 store.

## Run 294: Local dynamic-v9 Linux gate passes

All forty-three tests pass on the isolated old control driver in 43.968 seconds, including nineteen actual Rust-backed methods; peak process RSS is 49,732 KiB. Both Astra04 regression fixes and the expanded producer-source bundle/private-input changes are included. Source archive SHA is 2ea4d6c45e79640cad5a314cf28301196b4441479d0a63e6d2d323fee1505e5d; exact archive and actual output are dynamic-review/v9-working/{tested-source.tar.gz,linux-gate.log}. The previous local run had nineteen real-index methods skipped without a configured binary and is not counted as this Linux success. Cross-host components are included but complete driver orchestration is unfinished; no actual two-host migration or remote admission follows. Newer cross-host edits remain separate from the frozen tested source.

Read-only component review finds a concurrent replica namespace/count reservation race. The worker adds one locked reservation table and targeted build-versus-import/count regressions before source freeze. SSH trust additionally requires independently authenticated control-plane key evidence rather than circular verification through the same connection. The separate batched GPU-reference engine implementation resumes with Sol6.1 readiness, CPU proofs only; no new GPU instance or reference pool completion is claimed.


## Run 295: Worker-one V5 checkpoint preservation and storage inventory

Worker one's two-host V5 store passes full upload, pinned-version restore, every-member SHA and reopened query parity in 644.512 seconds. Its 64 owners contain 5,001,462 rows; 17,548 source files total 22,293,089,588 bytes. Archive size is 22,326,865,920 bytes, reported SHA prefix20f268a9 and version d1toD9e_RxIPQZM43WAAbh.d0LHwDw9m. Both worker-one stores are preserved and its guard exits successfully with no surviving groups. Final owned/source scope is 178.34 GB and free space 3.291 TB. Proofs are pilot/results/checkpoint-archive-progress/worker1-{complete,guard-complete}-v1.json.

A separate 64-owner V5 storage diagnostic passes: current snapshot references and retained-public-manifest union are 16.831 GB, while physical objects total 22.253 GB. The other 5.422 GB remains retained, with zero unknown paths. This is one worker half, not the full tree. Evidence is pilot/results/fineweb10m-v5-storage-worker1-{v1.tar.gz,summary-v1.json}. No deletion is authorized.

## Run 296: Complete selected ten-million-row checkpoint preservation passes

Worker-zero's separate retry passes its 64 owners and 4,998,538 rows in 702.802 seconds. Together the verified halves preserve all 128 owners, ten million rows, 35,037 files and 44,457,046,028 source bytes. Combined durable archives total 44,524,503,040 bytes. Every member SHA and pinned snapshot/map/audit/root passes, with restored reader probes. The worker-zero archive SHA is 41caf3676551888df4cb6a885df9fd17854d0fdf1a1a6cf5702f2cbdab7c8eb4, version HlXzD5dZvF3QeOovD2CelFDq0qP4c5Kn; the worker-one descriptor is preserved alongside it. Aggregate proof is pilot/results/checkpoint-archive-progress/selected10m-complete.json. Worker-zero uses explicit pinned-ID rerank while worker-one's successful original probe uses standalone top-one search. These preservation probes establish retained/reopened bytes, not an independent nearest-neighbor accuracy measurement. The original tied-ID failure remains separate. Worker zero proceeds to the one-host V5 store; complete V5 preservation is still pending.

## Run 297: Separate batched reference engine CPU proof passes

Twelve new tests and thirty-two combined numerical tests pass in 0.236 seconds against full float64 oracles and the preserved scalar engine, including ties, signed errors, precision limits, subnormal/overflow handling and chunk/batch tails. All four prior numeric dependency hashes remain unchanged. New batched filter/engine/tests freeze as source SHA f5e5dd1043d9ab8a1896b9c2002998ca999d5fd03aff5496ef6a1d546f63920d, with evidence in subtree/fineweb/publication-gpu-plan/batched-v1/. The future plan budgets 2.743 GB device memory and a phased sixty-minute L4 gate. Independent review, actual CUDA validation and fresh resource admission remain pending; no GPU launches and no new exact-reference pool completion occur.

The 100M preparation has completed receipts for 78,193,020 rows across eighty source files, with 255.37 GB owned bytes and 1.26 GB RSS at the reported sample. This is partial ingestion; full routing, ID/value audits, exact references and archive gates remain mandatory under the unchanged 08:01:30 UTC stop.


## Run 298: Cross-host frozen-source operational review finds three blockers

Independent review verifies all 125 source hashes in crosshost-v9 SHA b0bcc549429bfaba1e3b97f377f3bfad12a49ba9e65792e83219a86ea748460b and passes eighteen local focused tests in 0.542 seconds. Remote execution stays blocked: the driver disk floor exceeds its actual disk; new readers can accept a backend pinned only to an unchanged tree hash after placement changes; and observed peer/lease failures lack an ongoing pre-publication health gate. Exact host-key nonce/control-plane evidence, tokens, transfer bounds and supervisor ownership pass source checks. The worker accepts the blockers and proceeds to a distinct repaired epoch; no actual two-host fixture runs. Receipt is dynamic-review/crosshost-v9/independent-review.json. Physical instance labels, durable per-step journals and explicit remote failure subcases remain requirements.

At 03:21 UTC the 100M ingestion has completed receipts for 94,605,360 rows across ninety-six files. This is ingestion progress only. Current S3 inventory is 681 objects and 188,782,457,040 current-version bytes; older versions, incomplete multipart uploads and requests remain unmeasured. Active compute remains approximately $11.4338/hour; no GPU or 32-CPU load driver runs.

The 1.1M 97.41% read-quality result binds baseline SSH compaction tree04d57d52834fdd41b2065202775dcdd6769ed85e7721d8d8907174771951ea27, not the later guarded-pool output. Both use the same append input but randomized construction differs. A guarded-pool-output quality gate remains separate if needed; no latency or quality claim is transferred between these trees.


## Run 299: Baseline generation-two worker-one preservation passes

The selected baseline SSH-compaction tree04d57d worker-one half passes full upload/restore, every-member SHA and explicit-map-ID rerank parity in 65.563 seconds. It contains 64 owners, 550,299 rows, 2,612 files and 2,196,147,394 source bytes. Durable tar is 2,201,128,960 bytes, reported SHA c2682b81…d427f and version SXjLNNaM1VrELRhJeFNPaaUTCHsUgnqh. Guard terminates successfully without unreconciled children. Local proofs are pilot/results/checkpoint-archive-gen2-v1/{worker-1-report,worker-1-guard-report}.json. Worker-zero's matching 549,701-row archive is independently admitted only after its active V5 archive terminal reconciles. This does not preserve the distinct guarded-pool output.

## Run 300: Batched numerical review finds two reproduced source bugs

The fresh read-only Astra medium review verifies the seven numeric source/dependency hashes and frozen evidence. Its in-memory reproductions show a one-shot chunk iterator consumed in preflight can return empty references while reporting complete coverage, and a NumPy top-k slice retains the complete sorted matrix backing allocation. The interval threshold math appears conservative, inclusive ties remain uncapped and forty-five independent in-memory full-oracle cases pass. These numerical positives do not excuse the iterator/memory bugs. Report is subtree/fineweb/publication-gpu-plan/batched-v1/independent-review.md, with model/effort and exit zero in its log. Sol6.1 readiness owns distinct v2 fixes, cardinality/coverage/storage regressions and the complete bounded executable wrapper before hardware review/admission. No CUDA performance or new references are complete.

## Hundred-million-row file emission is not completed ingestion

At 03:27:28 UTC all 101 complete-file markers total exactly 100,000,000 rows. The authoritative preparation report still has running status, empty completed stages and no ingestion receipt. Global publication/UUID checks remain pending; no successful ingestion or index run is claimed. The fixed 08:01:30 UTC stop stays unchanged.

Automatic approval review rejects V10 source transfer to the old control driver before execution, stating source export is not specifically authorized. The worker preserves that rejection and gathers exact sandbox instance/account evidence and bounded source/secret inventory for an evidence-based same-target retry. No alternate route bypasses review and no V10 remote test starts from that attempt. User authorization already covers sandbox implementation/tests; unresolved rejection status will be reported until review resolves it.


## Run 301: Complete one-host V5 ten-million-row preservation passes

All 128 owners, ten million rows, 35,062 files and 44,450,602,620 source bytes pass complete upload/restore/member SHA and pinned explicit-ID query parity in 1,440.876 seconds. Archive size is 44,518,092,800 bytes, reported SHA c61a599f…5c452 and version taLXruolYD3gGhyE.9wCkpVrnEVEHWxa. Proof is pilot/results/checkpoint-archive-progress/v5-onehost-complete-v2.json. Worker zero proceeds to its final two-host V5 half; complete two-host V5 preservation remains pending. Original failure raw-log preservation starts separately on worker one under its admitted fifteen-minute guard.


## User scale ceiling: hold the 250M progression

Lyon explicitly instructs not to run the next 250M progression. The operational worker confirms no 250M ingestion, preparation, indexing or benchmark job is active or staged for submission. Current authorized 100M work, archival and dynamic implementation/tests continue under unchanged deadlines. No 250M or larger prefix launches without new explicit user authorization. All workers receive this constraint.


## User experiment policy: information yield and faster iteration

Lyon changes the campaign objective from automatic scale progression to the smallest high-quality experiment that resolves each question. The goal is a sharded implementation with a credible measured path to 100B or more vectors. Every new experiment must name its hypothesis, minimum representative size, metric, stopping rule and resulting decision. Large runs require a failure mode that smaller tests cannot reveal. Current 100M downstream work and the queued 10M append are held for information-yield assessment; current bounded preservation continues. Tiny correctness and representative subtree/working-set, fan-out, network, migration and compaction tests take priority. Sol6.1 workers receive the new policy. The 250M prohibition remains in force.


## Plan clarification: production fleet separation and query strategy comparison

Lyon authorizes continued distributed correctness and architectural work, while explicitly deferring execution of the query strategy experiment. CURRENT-PLAN.md records the comparison of globally coordinated per-level traversal with independent subtree searches at matched recall, including storage reads, latency, network and synchronization costs. Existing separate compactor and query fleets provide the ingestion/read separation; a new live-reader migration service is unnecessary. Indexed vectors have single ownership and are not replicated. Current top-one upper insertion routing is an explicit semantic gap; the target write search explores candidate subtrees before choosing one destination leaf. Existing Sol6.1 workers resume local correctness/design, independent Astra medium review, and bounded lifecycle reconciliation. The prior source-export rejection remains unresolved and no transfer retry is authorized by this general continuation. No query benchmark or larger progression is launched.


## Explicit authentication and source-transfer authorization

Lyon responds “logged in. confirmed” to the request for one Cops AWS login and permission to upload the 223 KB test-source archive to the task-owned sandbox driver. This supplies the specific human authorization requested by automatic approval review. Linux owns fresh account, exact-instance, capacity and unchanged-deadline verification; the implementation worker owns any reviewed source upload. Fresh Astra review has preliminary failure-propagation and test-callback findings, so remote runtime admission waits for repairs and independent review. No upload or remote test success is claimed by this authorization entry. Query-strategy experiments and larger progression remain deferred.


## Authorization to proceed and discard unfinished 100M preparation

Lyon approves all stated next steps: finish independent correctness review and minimal distributed checks, integrate global write-beam insertion with the Rust writer, prove distributed maintenance, and address measured architectural limits. The query-strategy comparison remains deferred until ready; larger progression remains prohibited. Lyon also accepts the explicit recommendation to discard unfinished reconstructible 100M input and stop its preparation node. The lifecycle owner will verify preserved timing/provenance evidence and record the unadmitted raw payload and partial SQLite as intentionally excluded from retained roots before stopping the exact sandbox preparation instance. This entry records authorization, not completion. Original benchmark workers and driver are already stopped; any minimal runtime restart requires reviewed code, fresh identities and independent fixed guards. Independent V12 review identifies a remaining child-health post-poll failure-latch window; the implementation worker repairs it in a new epoch before remote admission.


## Independent V14 repair review closes source findings

The independently verified V14 archive SHA c52bb4bb5f804eaa2938b6f6f6a83ab0a737c545fc3872b6dedb994463a587f6 passes its three-file delta review. Thirteen focused local checks pass; two Linux-only process checks remain pending. The source adds a final failure-latch check, preserves process-group ownership before reaping the exited leader, and records placement intent before transfer. These source checks close the Astra06 findings but do not establish Linux cleanup or remote Rust execution. The next admitted activity is a minimal driver-only correctness gate with fresh identities, capacity and independently enforced stop guards. Receipt: dynamic-review/crosshost-v14/independent-delta-review.json.


## Overnight continuation enabled; V14 transfer rejected

Lyon asks to ensure work continues overnight. The existing ten-minute follow-up is updated and ACTIVE, with explicit instructions to redispatch completed agents, continue local implementation around remote blockers, and preserve small-experiment limits. A newly restarted driver-only gate receives fresh identity and independent 05:35:27 UTC stop guards, but automatic approval review rejects the exact V14 source upload again. No transfer or runtime occurs. The reason states insufficiently specific archive/destination authorization despite prior confirmation. No alternate route is attempted; Linux is instructed to stop the idle driver immediately. Local Rust implementation and independent review continue.


## Explicit driver upload authorization broadened

Lyon explicitly states “you may upload anything you want to the driver” in response to the exact V14 archive and sandbox driver destination question. The implementation and lifecycle workers receive this new authorization for ordinary approval-reviewed task-source uploads to i-00268d1e1dfea0357. Fresh identity/address, reviewed hashes, independent bounded stop guards and a single runtime controller remain required. The previous rejected attempt uploaded nothing and its driver is confirmed stopped; any new attempt is recorded separately. Local native Rust validation now reports seven focused distributed-write tests passed with zero skips; this is separate from the pending Linux process tests and frozen V14 runtime.


## Run 302: Native Rust write integration tests pass

The new native Darwin write-interface epoch passes seven focused Rust tests with zero failures or skips in 0.01 seconds after a separate 1m53s lean offline build. Tests cover cross-owner leaf selection and single posting ownership, unequal-distance direct traversal parity, mixed-depth/root-leaf cases, cross-owner ties, global policy widths, registration failure atomicity, and actual blockfile flush/reopen with explicit global-ID scalar distances and the unchanged old snapshot. Independent review verifies all eighteen frozen files and the preserved test receipt, but does not independently rerun the tests. Archive SHA is 4e45828e835297482029a0f190302495ae4833c93ca3705f6cc8328008110532; receipt is dynamic-review/07-rust-write-v2-local-review.json. This proves the stated in-process interface contract, not a remote compactor protocol or production fencing. Frozen upper-centroid routing, durable global ownership, and complete publication integration remain pending.

A separate native shard CLI smoke builds eight real vectors and passes publication audit, reopen and scalar-distance checks in 0.0505 seconds. Its binary SHA is 745c00160558cda786a2f19ec3ebf2e465a839e9acb209a0162a63cd1c22a449. The initial missing-argument CLI failure is retained separately. Two frozen V14 methods reject this new binary before writes because it is not an admitted writer epoch; the worker does not bypass the allowlist. Independent review of a distinct native epoch is pending. Evidence is dynamic-review/write-interface-v2/native-worker-smoke.

## Run 303: Remote retry and Astra invocation remain approval-blocked

The second driver-only attempt uploads zero bytes and executes no tests: automatic approval review rejects the exact V14 archive upload despite Lyon’s explicit broad driver authorization. No alternate route is attempted. The driver is API-confirmed stopped at 05:24:26 UTC, with all four task nodes stopped and root disks retained. This attempt costs at most $0.010431 compute; both short attempts total at most $0.022754. Receipt is fleet/v14-driver-gate/stop-confirmation2.json. Independent Astra medium review of private Rust source and native evidence is also rejected before launch; no actual Astra execution is claimed. Local independent source review and implementation continue. The automation remains active because useful local work remains.

The preparation node is independently API-confirmed stopped at 05:02:54 UTC after the approved exclusion of 323,584,942,888 raw bytes and 3,721,658,368 observed partial-SQLite bytes from retention. Its 5,669,766-byte timing/provenance archive is verified and root EBS retained. Receipt is fleet/prep100m/intentional-disposition/stop-confirmation.json. Running campaign compute is zero; retained EBS and S3 storage remain billable.


## Run 304: Two local Rust-backed agents pass boundary correctness

A separately admitted native writer epoch, SHA 745c00160558cda786a2f19ec3ebf2e465a839e9acb209a0162a63cd1c22a449, passes two focused local real-index tests in 5.087 seconds: stale placement backend rejection and authenticated two-agent transfer with split/merge/reassignment and scalar/direct-versus-transport checks. Source freeze V16 SHA is 9031bcd8967439d594c3512e83b187a13572771952d533d71d2b985e857a0ccf. The preceding native attempt exposes two test-harness defects (Darwin temporary-path canonicalization and cleanup of a stateless backend); its two failures remain separate in crosshost-v15-native/loopback-tests.log. Success is crosshost-v15-native/loopback-v16-tests.log. The worker separately reports two actual Rust reconciliation/health methods passing in 0.960 seconds. This is a local two-agent correctness result, not physical two-host or Linux process acceptance.

## Run 305: Global write routing and ownership ledger native tests pass

Rust V3 passes eleven focused native tests with zero failures/skips in 0.01 seconds after a 2.69-second incremental build. Frozen source SHA is 9072fa3815e12a3d7ed3d67d592c11514a743b6a2c598049b5e65b15ab3da8be. The new boundary adapter uses committed upper routing centroids with writer distance arithmetic; typed root/expansion/selected-leaf tasks and a bounded dense-prefix ledger reserve single ownership. The ledger appends a mapping only after registration succeeds. Tests cover duplicate/stale/missing-local/budget rejection and persisted reopen. Independent review is pending; external task transport and complete saved-deployment publication remain integration work. The 100,000-row ledger bound is a correctness prototype limit, not 100B capacity evidence. Receipt: dynamic-review/write-interface-v3/manifest.json. No cloud or query-strategy experiment runs.


## V3 and V16 independent review qualifications

Independent V3 source review finds that boundary routing first scores an unused actual lower-root centroid, so finite valid upper-alias routing can fail when that unused distance overflows. The worker fixes the new epoch to score only the committed alias and adds a regression. The candidate publication composer also rebuilds lower trees and therefore can discard the globally selected leaf. It remains unadmitted and is being replaced by direct fork/flush of the registered writer snapshots. No end-to-end publication success is claimed. Review: dynamic-review/write-interface-v3/independent-review.json.

The V16 reviewer verifies all 128 frozen source files and the two-method 5.087-second success log. The local scenario uses fixture identities and omits the control entries/health supervision that would test leases and separate-process races. Its temporary raw scenario journals are not retained, so the preserved pass log supports narrower review than a complete trace archive. Two separately reported methods at 0.960 seconds lack a complete independently inspected raw log and remain worker-reported. These qualifications apply to Run 304; Linux process and physical-host acceptance remain pending. Review: dynamic-review/crosshost-v16-native/independent-review.json. Future integration fixtures will retain bounded journals and receipts.


## Run 306: Selected leaves survive private commit and reopen

The frozen global-write-v4 native task passes a sixteen-parent-row plus two-new-row fixture across two owners. It registers globally selected leaves, forks and flushes the exact private writer state without rebuilding or balancing, then reopens and audits all eighteen live IDs and FP32 vector values. The independently admitted older reader verifies both new IDs in their exact selected saved leaves. The preserved manifest distinguishes 0.866 seconds for the complete unittest from 0.652305834 seconds for the private commit; these are nested timings, not separate performance comparisons. The original global deployment pointer remains unchanged. The complete bounded fixture retains checkpoint objects, spec, maps, logs, receipts and membership responses under dynamic-review/global-write-v4/runtime/direct-private-fork.

Frozen source SHA is d701935b83eae7f21a15e6535c96e30919293aafed0dca6f8bfa3ee17ee4d740; native compactor SHA is d26ae940c9e0d918fde681e0fc7b9e2a6b16bcda0c2355bcdd534ccbd1a1cc12. Twelve focused Rust tests also pass, including the alias-distance overflow regression. Exact source/binary and composer admission is pending; registries remain empty until review. Full deployment publication, failed flush and stale-CAS tests are not yet executed. Any flush failure discards the private writer because commit clears its dirty state before flush completes. No remote/Linux/physical-host acceptance is claimed.


## Run 307: Complete local saved publication and stale-parent tests pass

The independently admitted d26 native compactor passes three real local composer tests in 1.751 seconds: complete eighteen-row saved-deployment publication with unchanged old snapshot, partial-owner orphan-generation rejection without publication or same-attempt retry, and stale-parent publication rejection. A separate 0.511-second check uses the independently admitted older reader to verify both appended IDs in their exact selected leaves after final publication. Full bounded fixtures and responses are retained under dynamic-review/global-write-v5/runtime-retry1 and runtime-retry2. An initial artifact-descriptor call failure occurs before Rust execution and is retained separately. The orphan-generation test is not an actual flush-I/O failure injection.

Independent review notes that the general reader audit allows duplicate current posting entries, so the compactor needs a strict zero-duplicate policy to establish single storage ownership. A new separately frozen source revision enforces this for parent and changed owners without changing the general reader audit. Its persisted duplicate regression and an eighteen-row direct task pass; independent admission and a minimal composer check remain pending. Source SHA is 9cf1ac38a6ffa444fb11e6e799fd5393bd735f241424981307398fab4847b768; binary SHA is ff46b44e63a37e67a90c10d17975263813471803176a6c47d14f83ad80edabfa. The worker proceeds to actual flush-I/O failure coverage before bounded-state replacement. No remote host, Linux process or scale claim is added.


## Run 308: Actual first-flush failure and fresh-parent retry pass

The strict single-copy native compactor passes four real publication tests with zero skips in 1.909 seconds. Independent review verifies all 154 frozen files and reconciles publication, two final selected-leaf bindings through the older reader, stale-parent rejection, partial-owner orphan rejection, and actual first-blockfile flush failure. A filesystem permission fault produces PermissionDenied, no eligible generation-two manifest, and a failed-unpublished journal. Complete parent inventories and the published pointer remain unchanged. Discarding the private writer and retrying from a fresh parent clone publishes seventeen IDs successfully. Source SHA is 41657c7784cdde26ed53a09a52b6f7373d6ed9be23e7a08a1ad51043dfe88f41; review is dynamic-review/global-write-v6/independent-runtime-review.json. This tests the first flush component only, not later-component partial flush, Linux process cleanup, physical hosts, or remote compactor transport.

## Run 309: Standalone ownership pages pass six local tests

A new immutable fixed-size ownership-page component passes six local tests in 0.031 seconds, covering partial-tail reuse, new-ID range uniqueness, changed-page verification, old-root reads, partial-write failure/fresh retry and stale-root rejection. Frozen source SHA is 32a28bec9d101f4bdff01461b5980fa8f9ec5841df653698d23fc93b2d7f7356. Independent review is pending. The component remains separate from Rust flat maps and complete deployment publication; the 100,000-row bound and full root descriptor list remain prototype limitations. The next work integrates its root into the existing deployment compare-and-swap, rather than creating a second independent publication authority. Evidence: dynamic-review/ownership-pages-v1/manifest.json. No cloud or query-strategy experiment runs.


## Run 310: Ownership pages join complete saved-deployment publication

Three real native tests pass in 1.767 seconds and a separate repeated-append test passes in 0.829 seconds. One complete deployment compare-and-swap publishes the saved index roots and ownership-page root together. Tests cover old-root queries, page-write failure after private index flush with unchanged published state and fresh-parent retry, wrong local page/flat-map binding, and stale deployment rejection. Repeated append preserves both earlier readable ownership roots. Source SHA is b751691728674da56cf4c21525cfde67c90040c72ad3d59e7d06c2f29ec8ca47; retained raw fixtures and logs are under dynamic-review/ownership-pages-v2. Independent integration review is pending; standalone V1 already passes six independently rerun tests.

The changed-page verifier reads 16 bytes against 144 total page bytes and reuses four of six page descriptors. This is not an overall append-I/O reduction: the independent flat-map oracle also reads all pages, and root metadata processing remains linear in descriptors. The 100,000-row bound remains. Paged topology replacement fails closed pending its explicit replacement contract; the next implementation addresses that gap before hierarchical metadata. No cloud, query-strategy or larger run occurs.


## Run 311: Paged topology replacement passes local real-index tests

Three real native tests pass in 2.323 seconds for split, merge and reassignment with complete ownership-page replacement inside the existing saved-deployment CAS. They cover exhaustive sixteen-ID scalar results, root ancestry, pinned old roots, exact placement-root preservation, partial replacement-page failure after lower-tree builds with no publication and a fresh retry, and stale publication rejection. A focused validation regression rejects wrong-parent, missing, duplicate and out-of-range IDs before page writes. Frozen source SHA is a2624d3c9556136553a5de7ee5b8bb16c295dcd08e06491dca875305ea5c7545; raw evidence is under dynamic-review/ownership-pages-v3. Independent review is pending. Replacement currently rewrites all ownership pages; the helper rejects unbound cross-worker execution. The 100,000-row cap and full-map/canonical audit remain.

Independent review of the preceding append integration passes actual complete-record validation on nineteen-row and retained eighteen-row deployments, as well as six component tests and retained failure evidence. Receipt is dynamic-review/ownership-pages-v2/independent-complete-cas-review.json. It confirms that full verification, metadata work and page fsync remain linear; the changed-page byte count alone does not measure overall performance. The next component introduces authenticated hierarchical descriptors with bounded-path lookup, retaining the full small-fixture oracle. No cloud or query-strategy experiment runs.


## Run 312: Hierarchical ownership descriptor component passes

Six standalone component tests pass in 0.559 seconds for authenticated fixed-fanout owner/page paths, bounded metadata caching, exact-parent append proofs, corruption/stale-root rejection, and partial writes with fresh retries. Frozen V1 SHA is 7b7de5ec4cb2f6af0fd38d25759c259cd07a0e0a3fbfa7990ebb86400937f320. Retained counters show cold lookup reading thirteen metadata nodes at both four and sixty-four pages. The sixty-four-page append verifies twenty-three metadata nodes and eight page bytes versus thirty-one metadata nodes for a full audit; the four-page append needs twenty-five versus thirteen, so small cases show no benefit. Cache accounting excludes process RSS and temporary copies. These component counters do not establish end-to-end index performance.

Self-review catches Boolean/integer equality ambiguity in changed-node metadata. A distinct V2 canonical-byte equality fix passes one malformed-node regression in 0.021 seconds; source SHA is 1329e7c0107b3a44abc8570c5f8ddbbd362229645e87ff82ac0309509f666f86. V1 timing remains separate. Independent review and complete deployment integration are pending; the 100,000-row cap, bounded owner directory and full flat-map audit remain.

Independent topology review verifies actual split/merge/reassignment and placement records, but finds missing explicit duplicate-current-posting rejection in rebuilt snapshots. A narrow pages-v4 revision adds the gate and earlier cross-worker preflight, with a focused regression passing in 0.226 seconds. SHA is 5977981f4f722fa4c28ee85da4dfa024f58ff64323518d02044af29a7ad2065d; independent delta review is pending. No cloud or query-strategy experiment runs.


## Run 313: Hierarchical ownership joins atomic index publication

Three native integration tests pass without skips in 2.514 seconds for one deployment CAS publishing saved-index, flat-page and trie roots together. They cover repeated append with older pinned roots, exact placement-root preservation, partial descriptor-node failure with no publication and a fresh-parent retry, and stale complete publication rejection. Source SHA is 9974abc70bebb7ea2ca8c901c4da355ab9ce41486dc5b1b54a87de42e8e34254. A separate test-only revision SHA baba6fd960052555786453a79becc435e88c77739bf1639a8ed069f776a0a1bb passes a trie data-page write fault in 0.508 seconds; implementation is unchanged. Raw fixtures are under dynamic-review/ownership-trie-v3 and v4. Independent integration review is pending.

The first two-row append proof reads forty-four metadata nodes (20,042 bytes) and sixteen page bytes; the next one-row proof reads twenty-six metadata nodes (12,337 bytes) and twenty-four page bytes. Full flat-map/page validation and reachable-file durability still traverse all data, so these are proof counters rather than end-to-end improvement. The cap remains 100,000 rows and trie topology replacement remains fail-closed pending its contract. Independent preceding component review passes fourteen tests plus twenty-four appends and 276 lookups against the full oracle; the topology duplicate-posting gate also passes review. Next work closes trie topology compatibility and replaces the actual query mapping hot path with pinned bounded-cache lookup.


## Run 314: Trie topology and actual coordinator mapping pass

Three native topology tests pass in 3.321 seconds under source SHA 95b2dbf6e71ebef9e1ff6975e8a33a9746c205b6eddd5b2d3fe79ab366303640, covering split/merge/reassignment with complete sixteen-ID/value/page/trie validation, old roots, exact placement preservation, partial-write failure/fresh retry and stale publication. Independent review is pending.

A distinct coordinator-mapping source epoch SHA a0df94eb9579d0b805f469b84d03679aed398b2a0ad9df7aa35e26abad7cd1f2 passes one real test covering three queries with complete logical trace parity against the flat-map oracle. Its 0.411-second duration measures the test, not query latency. The actual coordinator uses a pinned trie and opens zero flat-map mmaps; the fixture blocks coordinator flat-file access. Retained counters across queries and a repeat are 110 lookups, twenty-two metadata reads/parses, 1,408 cache hits and 3,520 page bytes. Peak cache charge is 53,445 Python-owned bytes plus 2,683 pinned-root bytes, within thirty-two-node/256-KiB cache bounds. These exclude process RSS, allocator overhead and transient copies.

Rust still reads its flat map when producing returned global IDs, so only coordinator mapping is replaced. The next narrow opt-in response mode returns local IDs, with the coordinator mapping and validating all candidates before global tie ordering/trimming. No search budget or query strategy changes are authorized by this protocol work. Full startup validation, flat-map oracle and the 100,000-row cap remain. Evidence: dynamic-review/ownership-trie-v5 and v6. All cloud nodes remain last-confirmed stopped.


## Local-ID reader protocol source admission

The opt-in local-ID-only Rust reader compiles in 5.60 seconds and passes four focused protocol tests in 0.059 seconds; independent review reruns all four in 0.055 seconds and verifies 164 frozen files plus binary SHA 32557f7941a76377d7731abcb7319343743cdbd02c5a8b61c296acc4b7ff4491. Source SHA is c4bcb386ef59834763720165b82454cd6f2b7a4d6c8d79a6fce2ddbb0fc8bcb8. The mode skips Rust flat-map access, returns boundary ties, and requires the coordinator to validate/map every row before global-ID tie trimming. Unsupported owner reduction and external backends reject before use; default global mode is unchanged. Independent admission covers only two bounded local reader fixtures: exact query-trace parity with ties and private-copy reads with the flat map absent. Actual runtime evidence remains pending. Receipt: dynamic-review/local-id-v1/independent-source-native-admission.json.

Independent trie topology/coordinator review passes fourteen component tests and validates retained topology records and all three actual query traces; coordinator zero-flat-mapping evidence is reconciled. Rust flat-map removal remains pending the new protocol runtime tests. No cloud work or query-strategy benchmark runs.


## Run 315: Actual local query path works without runtime flat-map access

Two native fixtures pass in 1.237 seconds and independent review reconciles their retained evidence. Three logical query traces match the older global-ID oracle, including boundary ties. A private copied worker snapshot with its flat-map file removed still opens, expands, scores and reranks in local-ID mode; default global mode fails opening the same copy as expected. The worker returns all cutoff ties, and the coordinator maps every candidate before stable global-ID trimming. Receipt: dynamic-review/local-id-v1/independent-native-runtime-review.json. This isolates the query runtime; complete deployment startup validation still requires saved flat maps and full page/trie oracles. No latency, remote transport or owner-reduction claim follows.

## Run 316: Remove duplicate reader-startup trie audit

Two focused native tests pass in 0.527 seconds, with independent source/evidence review. The constructor reuses only the typed receipt from its own mandatory complete validation, not a caller-supplied or persistent trust cache. Every later constructor validates again and rejects changed page data before starting workers. Source SHA is 4fe5b39d3c36b4bdfe051726b6361e0b789827f397d5c009d2602dd2b083ffd0. On the same sixteen-row fixture, trie metadata reads decrease from forty-four to twenty-two, metadata bytes from 18,406 to 9,203 and page bytes from 256 to 128. Single uncontrolled constructor timings increase from 0.0208 to 0.0312 seconds; no latency improvement is claimed. Full startup validation and durability remain linear. Receipt: dynamic-review/startup-validation-v1/independent-review.json.

Implementation and independent review proceed to measured-load compactor placement with explicit observation units, hysteresis, cooldown and retained/in-flight resource reservations. No new cloud or query-strategy experiment runs.


## Run 317: Measured-load subtree balancing passes local fixtures

The opt-in balancing policy passes four decision tests and three real native fixtures in 1.074 seconds. It estimates redistribution using observed user-plus-system CPU time over a shared measurement window, chooses one beneficial whole-subtree move with hysteresis/cooldown, and rechecks deployment and resource inventories before copy and publication. Tests include inventory change after copy blocking publication and a stale deployment blocking copy. Source V1 SHA is 6983243ec63e483b71e1a952ee727c9fb52bca6ee45e9eb24a8d9ad086a44d9a.

Self-review adds a retained-source reservation so omitted inactive sources remain charged. V2 SHA f8f2ea76eea8c56bf749927b96bd201124f719d20f1a021bb66bed36c8f636f4 passes two focused tests in 0.466 seconds, including actual local snapshot copy/publication with query parity and the catalog-omission regression. Independent review is pending. Source/destination physical-artifact overlap is distinguished from process memory/jobs; reservations remain charged until a future explicit reconciliation hook. This is local functional evidence using recorded observations, not production telemetry, future capacity, network performance or physical-host scaling. Default planner and 100,000-row cap remain unchanged. Evidence: dynamic-review/measured-balance-v1 and v2.

The next local profile checks whether actual retained posting arrays grow with visited leaves independently of a configured memory bound. It will distinguish owned arrays, provider cache and RSS rather than claiming a full-memory limit from one counter. No cloud or query-strategy experiment runs.


## Run 318: Balancing rechecks mutable planning inputs before publication

Independent review finds that V2 health checks revalidated resource inventory but omitted observation/policy/source hashes after readiness callbacks. V3 fixes that gap and pins parent/plan copies. One actual native method with two mutation cases passes in 0.620 seconds; independent review verifies both failed-unpublished journals, retained copies and reservations. Source SHA is c36356c6d9dac203d913d79d19cd980fb19cab88db5169a24510c490b1f893ad. Together with V2 accounting review this admits the bounded local recorded-window balancing contract, not production telemetry or future capacity. Receipt: dynamic-review/measured-balance-v3/independent-delta-review.json.

## Run 319: Posting-array retention profile and opt-in release pass

An actual eight-vector/two-leaf profile shows retained array payload growing from zero to 116 to 232 bytes as leaves are visited (132 and 264 bytes including centroids), while posting/vector block pins remain zero. A separately reviewed opt-in native reader releases materialized posting arrays after each success or error response; default retention remains unchanged. Nine operations in each mode produce eighteen retained raw responses with identical logical content, independently reconciled. Revisit/reload, malformed request and exact reranking are covered; eviction mode retains zero leaf arrays after each operation. Source SHA is 0e795a4fd9f9203cb912bc13ccae69abb7770b4bc86cbfe2107a44a1c831c2eb; binary SHA is 17c772244d8f9ac3110665aafcc41dd6874de66385f9ae820701771faa473487. Reader processes exit with bounded watchdog cleanup. Receipt: dynamic-review/posting-retention-v1/independent-native-runtime-review.json.

The bound covers inter-operation array payload only, not one operation’s peak, allocated vector capacity, provider cache, transient copies or RSS. No latency or complete coordinator-trace claim follows. The next gate integrates the opt-in policy into the existing coordinator and checks complete trace parity plus reload costs on a tiny fixture. All cloud nodes remain last-confirmed stopped.


## Run 320: Full coordinator posting-release parity passes

One native method passes four paired complete query traces in 0.545 seconds, with twenty-six worker operations per mode. Independent review verifies IDs, distances, global frontiers, leaves, selected candidates, budgets, logical contributions and wire counts. Repeated cutoff-tie and larger-budget cases are included. Optional eviction retains zero leaf arrays after every operation; default retention remains unchanged. Source SHA is 5e4f57791395ac718db5ad2c59fe0ef7212205b754c4117085bf7c5fa427091a; binary remains 17c772244d8f9ac3110665aafcc41dd6874de66385f9ae820701771faa473487. Receipt: dynamic-review/posting-retention-v3/independent-full-coordinator-review.json. A preceding invalid k=4/M=1 fixture is corrected before execution in a separate test-only revision; it is not a runtime failure.

Each repeated query rematerializes/releases five leaves and 544 payload bytes, with a maximum single-operation release of 280 bytes. These counts expose reload work, not disk I/O, decoding CPU, peak memory or RSS. No latency improvement is claimed. The readable implementation acceptance matrix now separates native proof from six remaining scale blockers. RESULTS.md no longer claims active GPU or benchmark machines. Next work examines sound incremental validation/durability rooted in exact audited immutable parents, retaining full-audit fallback and explicit local-file mutability assumptions.


## Run 321: Validation cost profile and same-call page reuse

A sixteen-row current deployment-validation diagnostic passes in 0.303 seconds and records 33,927 Python-returned read bytes across fifty-nine opens, including 256 ownership-page bytes for 128 payload bytes. Durability is measured separately at 9,728 returned bytes and 418 fsync calls including directory ancestry. These counters are not physical I/O. The validation report explicitly requires a lower reader to open; it is not an independent semantic audit of every lower block. Same-size, restored-mtime canonical corruption after an earlier audit is rejected, so local hashes/chmod/stat do not justify cross-call reuse.

The narrow V3 change reuses freshly audited owned page bytes only within that same validation call. Two component tests pass independently; one native method passes in 0.278 seconds, reducing page reads from 256 to 128 bytes on the identical fixture with the same validation result. A later corrupted page with restored size/mtime rejects before worker startup and before readiness/publication, leaving the pointer unchanged. Native evidence reconciliation is pending. Source SHA is 7413c95b90538c21aacf40639d0b0399b6a018e2909c96985889953765b184d4; evidence is dynamic-review/validation-pages-v3. All fsyncs, subsequent full validations and lower-reader requirements remain.

The initial unit report of two passes was incorrect: the retained run has one pass and one PermissionError while mutating a read-only page. A separate test-only correction deliberately changes permissions on its owned fixture before mutation; only the corrected run passes both methods. This failure and correction are retained separately. Further incremental admission needs a verified immutable/versioned provider contract and complete changed-object semantic coverage; no local trust flag is added.


## Storage contract review blocks cross-call validation reuse

Independent source inspection confirms copy-on-write UUID conventions but not an enforced immutable-version/retention contract at the current integration boundary. Block and root flush paths use default upsert; local storage does not enforce conditional put options; current reads lack pinned object-version identity. Saved version/GC references do not alone establish the required retained immutable-object proof. This is a statement about inspected APIs, not deployed storage permissions. Evidence is dynamic-review/immutable-provider-hook-inventory.json. Cross-call validation skipping remains disabled; complete validation and durability remain. The same-call page reuse result passes final independent reconciliation.

Implementation proceeds to a separate opt-in within-operation streaming path: one leaf at a time, authoritative version filtering, exact minimum-distance deduplication and cutoff ties, explicit allocation/candidate limits that fail the complete operation. Source and tiny semantic/resource tests precede native runtime admission. No new query path is yet enabled and no cloud or query-strategy benchmark runs.


## Run 322: Borrowed-posting streaming passes small query and budget checks

The opt-in streaming reader passes two Rust semantic tests and two Python protocol tests; independent source/native review admits a bounded replay. One native full-coordinator method passes in 0.900 seconds, covering three complete query-trace pairs including repeated cutoff ties, plus whole-request budget failure, cleanup and retry. It borrows one persisted posting at a time and retains no copied leaf arrays. Authoritative versions, minimum distance across duplicate current copies, and all cutoff ties remain intact. Source SHA is 4c36400dd8c2c116ae3b1dad4dd7519a77022a2f5522e9e372197bf5a71b1339; binary SHA is 91d19cd84f59fbe087ddcb726ce8799824816faac766118ff4c3fe541f872e41. Independent runtime reconciliation passes in dynamic-review/streaming-postings-v1/independent-native-runtime-review.json; it explicitly retains the empty-leaf gap and memory limits.

The tiny replay visits sixteen rows per query, at most four per borrowed leaf. Per owner it retains eight distinct candidate entries with HashMap capacity fourteen, corresponding to 112 bytes of key/value payload only. Provider Arrow/cache memory, hash-table overhead, final hits, temporary allocations and RSS are excluded. This is capped all-distinct candidate state, not memory proportional only to M. A cap of one rejects the whole operation; pins/arrays clear, retry matches and the published pointer stays unchanged. Self-review identifies an empty-physical-leaf case that needlessly requires a centroid; a separate narrow repair is pending and does not change the nonempty replay evidence. Artifacts: dynamic-review/streaming-postings-v1. No cloud or query-strategy benchmark runs.


## Run 323: Empty-leaf provenance repair passes independent review

Four scoped native Rust tests pass in 0.01 seconds for the streaming reader repair. A private list captured from persisted metadata at reader open identifies originally empty leaves; a later filtered live count cannot authorize a missing posting. The tests cover a valid empty leaf without a centroid, stale/deleted copies and duplicate-distance parity, and rejection when a nonempty leaf becomes empty in the loader cache before a missing-posting fault. Source SHA is ff9fc34e7dd50f9d4d52af145a2b5f4a7d19df137d8528ac0ad6bee074b021b4. Independent acceptance is dynamic-review/streaming-postings-v5/independent-empty-fix-review.json. V5 CLI replay remains pending; the earlier three full query traces belong only to V1.

Two exploratory test compilation failures and an unsupported explicit-empty-row serialization panic remain retained separately; the codec is unchanged. Review rejects the intermediate mutable-length check before admitting the original-metadata repair. No failed attempt is counted as a passing runtime. The next candidate collector receives design admission for isolated implementation and CPU proof tests: retain the nearest M distinct minima plus every cutoff tie, preserve exact distinct counts with a bounded bitmap, and fail the complete operation on capacity overflow. Every-prefix differential tests and explicit transient/output limits precede native activation. Worst-case ties remain linear.

Implementation and independent review agents are assigned the V5 CLI replay and candidate collector work. No cloud operation occurs. The last cloud confirmation remains 05:24:26 UTC with task nodes stopped; retained storage is the dated 213,244,141,107 S3 bytes and 440 GiB of root volumes, not a fresh inventory.


## Run 324: Repaired streaming CLI and isolated candidate collector pass

The repaired streaming binary passes one tied-M=1 full logical trace comparison on the retained sixteen-row deployment. In the same process, a candidate-limit error followed by a valid retry matches the default oracle, leaves zero posting arrays and pins, and preserves the published pointer. Binary SHA is 789873aa96046f713546f000ebedbfb35f170b1518bad67993dedfc1a767e790. Independent reconciliation is dynamic-review/streaming-postings-v5/independent-cli-replay-review.json. This is correctness evidence without a latency claim and remains separate from V1 timings.

The isolated candidate collector passes four CPU tests, independently repeated in 0.09 seconds after a 17.42-second build. Across twenty-four deterministic seeds and M values 1, 2, 7 and 32, 15,360 comparisons check every input prefix against the full-minimum oracle, including exact distinct counts, cutoff ties, signed zero and discarded IDs that later improve. Explicit bitmap, staging, retained-candidate and output limits fail the complete operation and latch failure. Source SHA is 20fcc5047d8c85c990016640417e5bb2a3148dbc89e5ff17ea9ff90c6e087867; independent receipt is dynamic-review/candidate-working-set-v1/independent-component-review.json.

The collector is test-only in this source epoch. Optional query integration is in progress and requires separate frozen-source review before runtime admission. Count limits do not bound allocator overhead or RSS; standard tree-container allocation failure may abort rather than return a structured resource error. Worst-case cutoff ties remain linear. No cloud activity or query-strategy benchmark occurs; lifecycle and storage observations remain unchanged.


## Run 325: Optional candidate collector matches integrated query traces

One native integration method passes in 0.673 seconds and independently reconciles three complete query-trace pairs, including repeated M=1 ties and M=16. A configured tie-limit failure returns no partial result; a valid retry in the same process matches every reference local ID, distance and exact distinct count for the same nodes. Arrays and pins are zero after failure and retry. The V3 test-only overlay strengthens the retry oracle; the V2 test is not executed and both use the same reviewed binary. Evidence is dynamic-review/candidate-working-set-v3/independent-native-runtime-review.json, with raw SHA 0867c61d0abfb6e9282de5d8ce74aef2aa05f02e285d0ef67a28daf2935b023d.

On the sixteen-row fixture at M=1, each owner still scores eight physical/current rows and counts eight distinct IDs. Peak candidate entries decrease from eight to two including staging; table capacity decreases from fourteen to three, corresponding to 112 versus 24 key/value payload bytes plus one bitmap byte. At M=16 both paths retain all eight IDs with capacity fourteen, as required. These observations exclude tree-container metadata, allocator overhead, output overlap, provider caches and RSS. Worst-case ties remain linear or fail configured limits. No latency or total-memory claim follows from test duration or payload counts. Defaults remain unchanged.

Both implementation and independent review agents continue with the smallest reader-memory diagnostic needed to identify the next limit, separating provider retention, decoded blocks, metadata, candidate/output state and process memory. No cloud calls, restarts, transfers or query-strategy experiments occur; dated lifecycle and storage receipts remain unchanged.


## Run 326: Provider retention diagnostic completes

Three native processes complete thirty-six phases on the same eight-row saved generation and its nine-row successor. All processes exit successfully, full normalized candidate sets and distinct counts agree across policies, saved-file inventories remain identical, and the cleanup receipt finds no native reader process. Independent review verifies every runtime hash in dynamic-review/reader-memory-v2/independent-runtime-review.json.

After all readers are dropped, the provider still caches nine known decoded blocks with 4,608 estimated buffer bytes and ten roots with 840 serialized metadata bytes. Explicit provider clearing removes those objects. Sampled maximum process RSS is 12,369,920, 12,288,000 and 12,255,232 bytes for retained arrays, released arrays and streaming respectively. These measurements include reporting allocations, shared buffers and allocator retention; they neither isolate policy savings nor capture transient decoding peaks. The tiny fixture does not fill the cache or rank large-scale bottlenecks.

The initial controller is held before execution for cleanup, failure-reporting, path-overlap and tie-normalization repairs. A separate synthetic cleanup attempt is blocked by process-sampling permissions and is not a passing test; the repaired admitted diagnostic completes under ordinary permissions. No cloud work occurs. Both agents proceed with optional existing-provider capacity/lifetime configuration and a minimal eviction/reload correctness proof. Defaults, retained artifacts and dated cloud lifecycle/storage receipts remain unchanged.


## Run 327: Optional provider cache diagnostic completes; protocol proof pending

The admitted provider-cache diagnostic completes three processes and forty-two phases with successful exits, identical complete candidate sets and exact reranking, and unchanged saved-file inventory. It includes old-generation access after new-generation cache churn. The default configuration retains nine known blocks and ten roots with nine recorded block-cache misses; the small single-shard target retains one block and one root with thirty-seven misses. Operation-boundary clearing retains zero known cache objects. These are cache API counts and retention observations, not physical I/O, latency or heap bounds. Independent runtime reconciliation and the separate full-coordinator error/retry fixture remain pending.

The source uses the existing cache implementation, whose oversized entries can exceed a shard's target and whose reader-held references survive eviction. Defaults remain unchanged. Two Rust policy tests and seven Python tests pass, with the seven Python tests repeated independently. An initial incorrect test invocation fails before running tests and is retained separately. Source SHA is 81cac2c5d5a61535ecb2419d3b6efa800ab5199ce9051acdcef5370b85cc29a2; evidence is dynamic-review/provider-cache-v1. No cloud operation occurs; both implementation and review agents remain active on completing the admitted proof.


## Run 328: Cache query correctness passes; watchdog signaling fails

Independent review accepts the forty-two-phase provider diagnostic and the protocol method's query evidence. The protocol method passes in 0.686 seconds: three full trace triplets, eighty-one operations and six ordinary error responses preserve all eighteen normalized repeated scoring/reranking result sets and counts. Defaults remain unchanged. Evidence is dynamic-review/provider-cache-v1/independent-protocol-runtime-review.json.

The external watchdog subsequently raises PermissionError while signaling its owned process group and produces no terminal guard receipt. This attempt remains unreconciled for forced cleanup; passing query assertions do not make it a clean guarded run. A later read-only inventory finds no matching native reader or Python wrapper. No signal retry or alternate termination route occurs. A separate reporting-only repair preserves denied-signal evidence and passes one independently repeated mock test in 0.003 seconds; it does not prove actual process cleanup.

Both agents continue with source review of admission before provider decoding and bounds on simultaneous pinned blocks. Cache targets constrain retained entries only after construction and cannot bound decoder allocations. The next design must identify checks that genuinely precede allocation and use a minimal oversized-input test. Existing compactor transport integration remains a separate unresolved seam. No cloud operation occurs, and the dated compute/storage records remain unchanged.


## Reviewed design: Pre-decode limits require storage integration

Independent inspection verifies twelve source hashes for the allocation-path audit in dynamic-review/provider-admission-audit-v1. The existing provider callback receives an already allocated encoded buffer, so checking its size cannot bound transport allocation. Block and root decoding use separate paths, and shared references require allocation-lifetime accounting. The current limit of thirty-two rerank futures also does not bound retained blocks. No code, runtime experiment or new memory guarantee is added by this audit.

The reviewed design requires byte-policy propagation through storage reads/shared fetches, bounded metadata preflight before decoder allocation, signed-length checks and reservation guards tied to shared allocation lifetimes. It records minimal oversized-input, cancellation and cleanup proofs. Implementation now prioritizes the existing compactor service and collection commit lifecycle. Its dispatcher executes local threads; it is not an existing remote subtree transport. Both agents are assigned a narrowly typed generation/owner/session integration and independent review before invasive changes. Physical transport and production publication wiring remain unproven.


## Run 329: First compactor registration adapter passes focused tests

Eight Python authority/replay tests pass in 0.016 seconds. One Rust bridge test compiles against actual collection and segment types and verifies context and unrelated segment-key preservation. This is a focused type bridge, not full worker compilation or production execution. Frozen source SHA is 0f0c5bbee192c5695922797ca65fa1613577eefa8bce7365f12f95d2d4851451 under dynamic-review/existing-compactor-v1. Independent source review and actual saved-leaf producer integration remain pending.

The adapter prepares a complete hierarchy bundle through the existing global-append path and leaves the local deployment pointer unchanged. Its target publication authority is the existing collection registration lifecycle. Actual interface review finds no commit-attempt terminality receipt and no version fence on log-offset repair; ambiguous outcomes must remain unknown, and registration cannot be automatically retried. Fake-authority terminal-rejection cases test an abstract contract, not a capability proved in production. Both agents continue with review and the smallest native preparation-to-bundle fixture. No cloud or external-system operation occurs.


## Run 330: Native preparation preserves selected leaves in registration bundle

One admitted native fixture passes in 0.455 seconds, growing a retained sixteen-row parent to eighteen rows. Independent reads verify both planned selected-leaf memberships exactly once. Review reconciles the complete saved record, sealed bundle hash and registration proposal, including version three, log position twelve, record count eighteen, all segments and unrelated keys. The parent deployment pointer remains unchanged. Repeated pending outcomes do not resubmit registration; the eventual committed observation is supplied by the fake authority, not SysDb. Evidence is dynamic-review/existing-compactor-v3/independent-native-runtime-review.json.

Ten focused Python tests pass independently. Review catches missing target count/size binding, and self-review catches an outer-versus-embedded collection-ID mismatch; the V4 projection repair and real-type regression pass. V4 source SHA is 74bd557548596d73953dc40ae7a819470764f9a0a73fa7760eaf504f8a1bc777. Its retained Rust log records a cached 0.29-second compilation and a test duration rounded to zero; a separately reported 1.01-second initial compilation is not substituted for that log.

The complete worker crate, actual registration service, log handling, authentication and cross-host transport remain unproven. Both agents continue with full worker compilation and the smallest opt-in connection to the existing registration path, using local interface tests before external execution. No cloud operation occurs; storage and lifecycle observations remain dated and unchanged.


## Run 331: Full worker compilation stops in baseline index dependency

The locked offline worker check stops before compiling the modified worker module with fourteen quantization API errors in its index dependency. Missing APIs include quantization helper functions, data_bits, rerank configuration fields and centroid quantization fields. The worker verifies the affected files have no local changes and the missing definitions are also absent in HEAD. Exact diagnostics and source hashes are retained in dynamic-review/existing-compactor-v5/full-worker-blocker.json and existing-compactor-v4/full-worker-check.log. This build is neither a successful worker check nor frozen V4 acceptance: V5 edits were made while dependencies compiled. Approximately 41 GiB of local disk remains free.

The V5 source connects the actual registration types to RegisterInput and adds two real-operator tests using existing local test backends. Source SHA is 446d393d7b53d74f8687359acf99a9c9149120630d673761dece720c51a06d70. These tests remain unexecuted because of the dependency failure. Existing test backends do not establish production atomic CAS or monotonic log semantics. Both agents continue source review and diagnosis, seeking a semantics-preserving repair or explicitly partial real-operator harness. No quantization defaults are invented and no feature is removed to claim a full build. No cloud operation occurs.


## Run 332: Actual registration operator passes partial harness

The V10 focused harness compiles the byte-unmodified registration and attached-function operators with real types and local test backends. Both admitted tests pass, with two other tests filtered out; compilation takes 2.24 seconds and test duration rounds to zero. The checks cover successful registration and metadata publication followed by a log-offset error. Source-end hashes remain unchanged. Evidence is dynamic-review/existing-compactor-v10/runtime-receipt.json; independent review reconciles the log, all 175 source-end hashes and unchanged operator sources. This does not compile or execute the full worker or its compaction-context wrapper, and test-backend behavior does not prove production atomicity.

Earlier attempts remain separate: V8 stops at lock resolution before compilation, a metadata query fails for cross-platform offline resolution, and V9 reaches the operator but fails on three test borrow errors without executing tests. V10 repairs only test observer borrowing. Frozen source SHA is 1a66ae476adaf03d8c3d73bc52f187cffa04f9d0fb2c436c70a1a8ceadbe9623. Local disk remains approximately 40 GiB free.

Baseline history contains the missing quantization definitions, but they belong to a broader change in bit formats, configuration and index dispatch. Restoring fields alone is not an established semantics-preserving repair. The full worker remains blocked, with no feature removal or guessed defaults. Next work targets the actual materialized-compaction input adapter under independent review. No cloud operation occurs.


## Run 333: Actual materializer input binding passes

Four Rust tests using the actual log materializer pass, with an 8.24-second compilation and test duration rounded to zero. Independent review verifies the log and all 181 source-end hashes. The component binds assigned offset IDs, user IDs, source log offsets and exact vector bits to the materializer's own captured input, including signed zero. Unsupported folded operations, hidden rows, mismatched source payloads, invalid ID ranges and overflow reject. A checked 512 MiB exported embedding-payload cap precedes copying; it is not a whole-operation memory bound. Source SHA is 4350e83bc8b7b1a9b26a107f546ca28f52183f511fe026ea62f2f6df89ad0f1a; evidence is dynamic-review/materialized-input-v2/independent-runtime-review.json.

V1 remains unexecuted: review identifies a raw-log/materializer pairing gap, repaired by a read-only accessor to the result's owned source before V2 admission. The passing tests perform zero writer or publication calls. Parent ownership remains an independently admitted input, and consumed private ID allocations are not rolled back. Full worker compilation remains blocked. Both agents now connect this actual exported input to the saved-leaf producer and complete registration bundle on the smallest local fixture. No cloud operation occurs.


## Run 334: Materializer-export adapter unit tests pass; native pipeline pending

Thirteen CPU unit tests pass in 0.039 seconds for the captured-export adapter. The V2 source freezes 184 files with archive SHA be30de927e346db436514ec2b3c7708a96df5a7d458e0b5e7fc6e195d84a0e19. Independent source review is active before the actual Rust export and native saved-leaf stages run. Neither native stage is yet claimed.

The adapter retains the original export and producer provenance, distinguishes the registered descriptor file hash from its semantic deployment hash, and binds the captured collection/log context and parent fence. The proposed fixture uses two actual raw logs, including negative zero, against the retained sixteen-row parent. It must consume the real Rust materializer export without a fabricated replacement or delegate fallback. Production context/authentication/transport and full worker compilation remain separate gaps. Evidence is under dynamic-review/materialized-pipeline-v1 and materialized-pipeline-v2. No cloud operation occurs.


## Run 335: Actual materializer export reaches saved leaves

The admitted Rust export test passes in 0.36 seconds after a 4.15-second compilation. A separate native adapter-to-writer fixture passes in 0.482 seconds, producing an eighteen-row bundle from the retained sixteen-row parent. Independent reconciliation verifies all 184 source-end hashes and the runtime/input/export/journal/bundle hashes. Assigned IDs sixteen and seventeen, log offsets eleven and twelve, and negative-zero vector bits survive the full local path; independent reads verify both selected saved-leaf memberships. The parent pointer remains unchanged and no second publication pointer is created. Evidence is dynamic-review/materialized-pipeline-v4/independent-runtime-review.json. The exact executed exporter binary is durably retained separately from the mutable build cache.

This fixture uses no parent record reader and a private allocator starting at sixteen, so it does not establish existing-user absence against a production record segment. Its collection authority remains local and fake; actual service/context wiring and the full worker build remain unresolved. Production materializer offsets are u32, requiring an explicit local-offset/global-ID namespace design before a 100B claim. Both agents continue with the smallest actual retained record-reader fixture to close the parent check gap. No cloud operation occurs.


## Parent-record source finding: Distinct ID namespaces require a mapping

The actual record writer uses one-based offsets: a sixteen-row parent occupies offsets one through sixteen, and the next add receives seventeen. The retained vector fixture uses global IDs zero through fifteen and expects sixteen next. The current adapter rejects this mismatch. Neither zero-based record seeding nor subtracting one silently is admitted.

The next reviewed fixture isolates real flushed/reopened parent-record checks, existing-user rejection and allocator behavior; positive saved-leaf integration remains blocked pending an explicit audited mapping. That mapping must bind exact record files, user identities, vector values and global ownership with complete coverage and uniqueness under the same publication authority. Both agents remain active. This is a source finding, not a completed runtime result or a 100B ID-capacity proof.


## Run 336: Real parent-record admission checks pass

Two bounded Rust tests pass and independently reconcile all 187 source-end hashes, logs and saved record files. The real record writer flushes sixteen rows; a fresh reader verifies every user, offset, vector bit and maximum offset. Seven materialized input windows reject before hierarchical writes, including existing-user operations, folded/duplicate inputs and fresh offsets incompatible with the current global namespace. New users receive offsets seventeen and eighteen, leaving the private allocator at nineteen. Rejection does not roll back allocations. The actual record test takes 0.05 seconds; initial and cached compilation timings remain separate. Evidence is dynamic-review/parent-record-v2/independent-runtime-review.json.

The explicit namespace design receives independent admission for bounded implementation. Initial global-to-user identity must come from bound shared source data, not equal-vector matching. Complete record enumeration and both lookup directions must establish coverage; existing assignments remain stable. Record, vector and mapping roots enter one complete registration bundle. Both agents now implement and review this bridge with permuted record order and equal-vector/different-user negative cases. Positive saved-leaf integration and production wiring remain pending. No cloud operation occurs.


## Run 337: Namespace component tests pass; captured-view binding remains separate

V3 compiles and passes three pure component tests with one filtered out; compilation takes 2.47 seconds and test duration rounds to zero. Independent review verifies all 189 source-end hashes and the log. V2 fails compilation on a slice-iterator expression before any tests; the one-line correction is preserved in a separate freeze. These pure tests do not prove actual stored-map coverage or publication.

Review holds V4 before execution to clarify parent binding. Construction provenance names the preceding deployment; it cannot equal the content hash of a current enclosing deployment that includes the namespace artifact without a hash cycle. The construction-only V5 interface explicitly checks its construction context. A reusable resolver must instead capture the namespace, record, vector and ownership roots from one current registered bundle and derive write authority from that view. Only the tiny construction fixture is admitted; its runtime evidence and latest narrow delta remain under review. No cloud operation occurs.


## Run 338: Real namespace mapping fixture passes after serialization repair

V7 passes one actual record-store test with four filtered out, a 2.22-second compilation and 0.08-second test duration. Independent review verifies all 189 source-end hashes and retained artifacts. The four-row parent uses permuted record offsets two, four, one and three; new materialized offsets five and six map explicitly to global IDs four and five. Equal-vector swapped users and an actual hidden reverse-map alias reject for their expected reasons. The default same-ID adapter still rejects. Evidence is dynamic-review/record-namespace-v7/independent-runtime-review.json.

V6 separately fails its valid-root test before audit or hierarchical writing because ordinary collection serialization omits runtime context fields. V7 explicitly records those fields and preserves strict checks; the failed attempt is retained, not counted as success. This milestone proves construction-time mapping only, with no hierarchical writer or publication call.

The next captured-view design is independently admitted for bounded implementation. Current bundle authority and construction metadata stay separate. Candidate record roots must replace the selected record segment files in the complete registration proposal alongside vector and namespace roots. Both agents continue implementation and review before a tiny integrated runtime fixture. No cloud operation occurs.


## Run 339: Captured-view contract and actual saved-record path pass focused tests

Three captured-view Python contract tests pass independently in 0.004 seconds. They test current-reference binding and poisoning with mocked producers; they do not establish an integrated record/vector proposal. The separate admitted saved-record fixture passes one actual Rust test with four filtered out, compiling in 5.09 seconds and running in 0.10 seconds. It consumes the real materializer result through the actual record writer, flushes a four-to-six-row candidate, reopens it and audits all three record maps and canonical vector bits. All 191 source-end hashes match. Runtime reconciliation is pending under dynamic-review/record-namespace-saved-v1.

Unsupported auxiliary record/Bloom files reject before writing. The old-reader assertion covers count and maximum offset only; full old-content and injected flush-failure proofs are not claimed. Vector writing and complete proposal eligibility remain pending. Both agents continue the composer that binds these saved record roots to actual vector leaves and namespace roots in one registration proposal. No cloud operation occurs.


## Run 340: Combined-proposal record export stage completes

The previous four-to-six saved-record fixture passes independent reconciliation. The next staged composer completes its separately admitted real sixteen-to-eighteen record-export test: one pass, five filtered out, 4.54 seconds compilation and 0.59 seconds test duration. All 193 source-end hashes match. The actual parent reader and materializer assign record offsets seventeen/eighteen mapped explicitly to global IDs sixteen/seventeen, and the actual record writer flushes candidate roots. The exact exporter binary is durably retained with source/input provenance under dynamic-review/record-namespace-composer-v1.

The Python vector composer and read-only record audits remain held for produced-binary review. This implementation supports the first constructed append only; repeated append or moved current-view authority is not established. The intended next fixture combines saved vector leaves with the actual candidate record roots in one complete proposal and preserves sidecar-failure evidence separately from a fresh retry. No publication, full-worker or production integration claim follows from this export stage. No cloud operation occurs.


## Run 341: Complete local record/vector/namespace proposal passes

The V2 composer method passes in 1.429 seconds and independently reconciles all 193 source-end hashes. One complete registration proposal contains the exact four candidate record roots in the selected record segment, the saved vector deployment and namespace mapping. Two actual record audits run before eligibility and submission; independent reads verify both new selected-leaf memberships. The old sixteen mapping entries remain unchanged, record offsets seventeen/eighteen map to global IDs sixteen/seventeen, and negative-zero bits survive. Evidence is dynamic-review/record-namespace-composer-v2/independent-runtime-review.json.

A real namespace-directory failure is retained as a separate poisoned attempt with orphaned private output and no eligible bundle. A fresh attempt succeeds and yields an eighteen-row captured view without creating a second publication pointer. The 1.429-second test duration includes these correctness scenarios and is not an uninterrupted construction benchmark. V1 stage two is unexecuted; V2 adds receipt revalidation while reusing the admitted exporter.

This proves the first constructed append with a fake collection authority. Repeated append, placement-preserved namespace reuse, actual record-flush I/O failure, full worker compilation and physical transport remain unproven. Both agents continue with a minimal repeated/current-view and placement-only integration design. No cloud operation occurs.


## Run 342: Repeated-view exporter and canonical hash check pass

The current-view V1 first stage completes one restricted canonical-hash unit test and one fresh actual initial export test; the exporter runs in 0.58 seconds. All 193 source-end hashes match. The new executable is durably retained for exact binary review before the full sequence. Evidence is dynamic-review/record-namespace-current-v1/stage1-runtime-receipt.json. The repeated append and placement sequence has not yet executed.

The new logical placement helper prepares a complete proposal through the same collection authority, preserving record/namespace/data roots, log position, count, size and unrelated metadata while advancing collection version and placement fence. It does not publish a local pointer or prove physical migration. Ambiguous registration remains unknown without blind retry. The next admitted scope is still pending produced-binary review; trusted snapshot capture and restricted canonical encoding do not establish production authentication or atomic assignment. No cloud operation occurs.


## Run 343: Repeated append through logical placement passes

One actual local sequence passes in 2.602 seconds and independently reconciles all 193 source-end hashes and retained artifacts. The index grows sixteen to eighteen rows, passes through a complete same-authority logical placement proposal, then grows to twenty rows. Placement preserves exact namespace, tree, record files, log position, count and size while advancing collection version four to five and placement fence two to three. The second actual materialization assigns record offsets nineteen/twenty to global IDs eighteen/nineteen; all eighteen prior mappings remain unchanged. Two actual record audits and both selected saved-leaf memberships verify. Evidence is dynamic-review/record-namespace-current-v1/independent-repeated-runtime-review.json.

A stale captured view rejects before input-directory/allocator creation, and a stale proposal rejects before the authority call. No second publication pointer is created. This is logical worker-label placement with a fake collection authority and trusted snapshot capture, not physical transfer or concurrent assignment authorization. Restricted semantic-hash encoding, the full-worker dependency blocker and production wiring gaps remain. Both agents now target the smallest actual partial record-flush failure and fresh-parent recovery proof. No cloud operation occurs.


## Run 344: Partial record-flush failure and fresh recovery pass

The V2 fault sequence passes one test in 2.573 seconds and independently reconciles all 193 source-end hashes and raw evidence. Actual local storage returns IsADirectory after 0.010632 seconds, leaving three successful other roots and seven orphan objects. The blocked root directory and every orphan remain intact. All old parent files remain unchanged; the failed attempt produces no export or eligible bundle and makes zero registration calls.

A separate process reaudits the unchanged parent and creates a fresh allocator and writer. Its export takes 0.595647 seconds, after which complete record/vector/namespace audits and two selected saved-leaf checks pass. Exactly one registration occurs after recovery, with no second pointer. Failure and recovery intervals measure separate operations and are not combined into an uninterrupted construction result. Evidence is dynamic-review/record-flush-fault-v2/independent-runtime-review.json.

V1 remains an overall failed attempt: its final assertion compared incompatible inventory field sets after the injected storage failure and recovery. V2 changes only that test projection and uses a fresh scope. The fault hook is test-only; normal calls remain unchanged. Both agents now integrate existing split/merge/reassignment preparation with the same collection registration authority and explicit namespace. Production atomicity, full worker compilation and physical worker execution remain unproven. No cloud operation occurs.


## Run 345: Topology submission contracts pass; native sequence pending

The V3 topology fixture compiles in 4.75 seconds, and two focused submission tests pass in 0.005 seconds. This compilation does not execute the native split/merge/reassignment sequence. Exact binary admission and runtime remain pending under dynamic-review/record-namespace-topology-v3.

Source review catches a retry gap: validation failure before registration leaves a candidate eligible. The repair adds a durable exclusive validation marker so the candidate cannot be reused even if writing its failure classification also fails. Already-submitted uncertain requests remain reconciliation-only. A separate test correction reads the fresh candidate intended for submission rather than discarded roots. The optional path uses topology preparation only, preserving record identity and complete proposal authority; the local publication function stays unused. Both agents continue exact-binary review and the smallest native sequence. No cloud operation occurs.


## Run 346: Same-authority split, merge and reassignment pass

One native fixture passes in 3.051 seconds with 195 independently reconciled source hashes. It prepares a fresh split, merges the resulting owners and reassigns an ID between owners. Each complete collection proposal preserves the exact identity mapping, canonical vectors and record files, while advancing collection version and deployment fence. Nine actual record audits and independent scalar-distance checks for all eighteen saved IDs after each operation pass. Evidence is dynamic-review/record-namespace-topology-v3/independent-runtime-review.json.

Failed preparation exposes no eligible proposal. Stale submission and authority advancement during audit consume their private candidates before RPC; a fresh candidate supplies the successful split. No second publication pointer is created. The fixture duration includes failure checks and is not an uninterrupted construction benchmark.

This proves local same-worker saved-tree behavior with a fake collection authority. Remote inputs and multi-worker merges remain rejected before construction. The implementation and review agents now address verified transfer placement and remote-source topology through the existing transport and the same collection authority. Full worker compilation and production atomic registration remain unresolved. No cloud operation occurs; retained storage and stopped-instance observations retain their existing dates.


## Run 347: Transport intent and readiness guards pass

The first mocked intent test passes in 0.002 seconds. The V2 pair of focused tests also passes in 0.002 seconds with 197 matching source-end hashes. The tests check that caller-chosen generation handles are owned before requests, unknown outcomes are not retried, and generation readiness rejects a second invocation before another request. Evidence is dynamic-review/record-namespace-transport-v2/unit-receipt.json and unit.log; independent exact-binary admission remains pending.

These are mocked contract tests, not native transport execution. The proposed eighteen-row fixture uses two local agents and a fake collection authority. Rebuilds consume canonical vector payloads; imported checkpoints verify source provenance rather than supply incremental block edits. Physical fleet behavior, production authentication and forced cleanup remain unproven. No cloud operation occurs.


## Run 348: Two-agent runtime stops at sandbox socket permission

The independently admitted eighteen-row transport fixture returns one error after 0.086 seconds: the first localhost listener bind raises PermissionError (errno 1). No HTTP request, Rust build, reader child or generation starts. Independent cleanup reconciliation finds no remaining server threads or owned generations. Source review and two mocked tests remain valid, but this attempt establishes no transport runtime proof. Evidence is dynamic-review/record-namespace-transport-v2/independent-blocked-runtime-review.json.

The implementation worker is assigned the ordinary automatic approval path for this exact localhost test, with fresh separate attempt evidence and unchanged source/binary verification. This socket permission request is distinct from the prior rejected cloud upload and process-group operations; those remain blocked. A rejection must stop this action without an alternate route. No cloud operation occurs.


## Run 349: Approved two-agent topology fixture passes

The exact reviewed localhost fixture passes in a fresh approved attempt in 3.431 seconds with 197 matching source hashes. Verified transfer placement, split, cross-agent merge and reassignment each produce complete proposals through the same fake collection authority. Nine actual record audits, full mirror inventories, exact owner-instance bindings and all eighteen remote vector IDs and scalar distances pass independent reconciliation. Corrupt transfer stops before upload, import or build and exposes no eligible proposal. Evidence is dynamic-review/record-namespace-transport-v2-approved-attempt/independent-runtime-review.json.

Normal cleanup stops both listener threads, leaves zero generations on both agents and waits for eleven owned reader processes. This establishes ordinary cleanup only. The original 0.086-second socket-permission failure remains separate; its timing is not combined with this successful correctness fixture.

The test uses two local agents, injected identities and a fake collection authority. Rebuilds consume canonical vectors; transferred checkpoints establish provenance rather than incremental block editing. Physical-host operation, production registration, full worker compilation and forced cleanup remain unproven. The next implementation and review assignment connects global write-beam selection and direct selected-leaf insertion to this transport and complete collection proposal. No cloud operation occurs.


## Run 350: Selected-leaf writes pass the current local integration test; memory inventory stops as insufficient

The current writer passes the complete two-agent loopback selected-leaf write fixture in 3.117 seconds with 229 reconciled source hashes. Two additions reach the same globally selected destination beyond the nearest upper subtree, preserve distinct IDs and saved leaf membership, and enter one complete twenty-row proposal. Two actual record audits pass; stale submission rejects before registration. Both writers exit successfully and both listener threads stop. Independent evidence is dynamic-review/remote-selected-write-v25/independent-success-runtime-review.json. This is a tiny local transport proof with injected identities and a fake collection authority, not physical-host or production publication proof.

The intervening failures and narrower successes remain separate evidence. Version ten fails its crossing assumption; version twelve establishes deliberately controlled geometry after a separately retained setup failure. Version thirteen's centroid diagnostic fails because it assumes eagerly loaded values; version fourteen reads persisted centroids successfully. Version fifteen's combined fixture passes in 4.699 seconds, but its invalid-leaf attempt remains unknown and is never reclassified. Version eighteen has seven passing offline outcome tests; version nineteen fails setup before selected writing; version twenty passes the actual precommit disposal fixture in 1.399 seconds, with both failed writers exiting 1 and no registration. Its outcome means discarded private state with unknown effects, not proof of no mutation.

Actual parser tests in version twenty-one expose acceptance of extra fields: six tests pass and one fails. Version twenty-two repairs that gap and passes seven Rust tests, then its CLI compilation fails on a missing import. Version twenty-three corrects that import and compiles successfully in 4.12 seconds. Version twenty-four passes three actual CLI sessions in 0.539 seconds covering identical replay, conflicting replay and unexpected end of input. These failures, archives, logs and independent receipts remain under their distinct remote-selected-write version directories. They are not combined into a performance benchmark.

Two read-only inventories finish successfully in 0.555663 seconds total, with all 316 source hashes and checkpoint inventories unchanged. Each retained owner has only one embedding block, so this fixture cannot measure memory growth across blocks. The experiment stops before instrumentation or corpus growth. Evidence is dynamic-review/rerank-block-inventory-v1/independent-runtime-review.json. The implementation and review agents now inspect repeated reads within one complete changed-subtree validation call; no optimization, cross-call trust or weakened file checks is admitted.

A fresh normally approved read-only GitHub identity request returns lyon-chroma, and both actual checkout Git identities match lyon-chroma <lyon@trychroma.com>. This resolves the local evidence-update blocker. No cloud operation occurs in this continuation. The last API instance observation remains 05:24:26 UTC, with four task nodes stopped; it is not a current observation. Retained storage remains the dated 04:42:32 UTC measurement of 213,244,141,107 bytes in 700 object versions plus 440 GiB of root volumes. The two earlier driver attempts retain their combined compute upper bound of $0.022754 and execute zero tests.


## Run 351: One validation call reveals repeated map reads

The first measurement attempt fails during temporary bitmap setup because its strict read wrapper rejects the standard library backing handle. The failure and unchanged fixture inventories remain under dynamic-review/validation-read-inventory-v1. A separately reviewed repair excludes only that temporary bitmap handle; it does not change validators or exempt fixture reads.

The second attempt passes in 0.238953 seconds overall, with 0.029839 seconds inside the measured validation call. Independent reconciliation verifies 317 source hashes, the exact unchanged validation report and full unchanged fixture inventories. The retained sixteen-row fixture produces 334 read events across 64 paths and 109,322 returned Python bytes. Its two 64-byte ID maps account for 896 returned bytes across fourteen opens. Evidence is dynamic-review/validation-read-inventory-v2/independent-runtime-review.json. These are returned Python bytes, with text counted as UTF-8; physical disk I/O, temporary bitmap/mmap activity, native reads, provider memory and process memory are excluded.

The next authorized implementation shares privately owned, checked map bytes between the page and trie comparisons within one validation call. The predicted reduction is 384 returned bytes on this fixture and remains unmeasured until the implementation passes review and the same measurement repeats. Initial full validation, independent reference checks, standalone validators and every later validation call retain their checks. No cross-call cache, snapshot reuse or larger dataset is authorized by this step. The existing implementation and independent review agents are reassigned.

Fresh read-only GitHub verification returns lyon-chroma and both actual checkout Git identities match. No cloud operation occurs. Instance and retained-storage observations retain their dates from Run 350; no fresh fleet status or compute measurement is claimed.


## Run 352: Within-call map reuse passes; focus returns to integration blockers

The implemented map reuse reduces returned map bytes from 896 to 512 and map opens from fourteen to eight on the same sixteen-row fixture. Total returned Python bytes fall from 109322 to 108938. Independent review verifies 318 source hashes, identical validation results and unchanged fixture inventories; only the two map paths change their read totals. Evidence is dynamic-review/validation-map-sharing-v2/independent-measurement-runtime-review.json. This establishes a small read reduction, not a latency, physical I/O or process-memory improvement.

Three focused component tests pass. The first copied-fixture test fails setup on a noncanonical macOS temporary path. A separate test-only path correction passes the freshness test in 0.069 seconds: later complete validation and both standalone validators reject same-size corruption even with the modification time restored. The failure remains preserved under version one. Initial full checks remain intact, and reuse is bounded to checked bytes within one invocation.

The next coherent milestone targets reliable full compilation and a reproducible Linux test environment, followed by a modest multi-block, multi-owner fixture. Implementation and independent review inspect the quantized dependency API mismatch and define acceptance criteria before changing code. Small read optimizations will not displace these integration gaps. Existing cloud approval, budget and retention limits remain binding; this scope creates no hardware authorization or scale progression.

Fresh GitHub and actual checkout Git identities match lyon-chroma. No cloud operation occurs; fleet and storage observations remain dated as recorded in Run 350.


### Run 352 continuation: full-build diagnosis and implementation release

Independent source inspection identifies internal API drift: the quantized caller expects helpers and configuration methods absent from the current supported schema and wrapper. An external dependency upgrade does not address this mismatch. The implementation assignment preserves the current None/FourBit schema, defaults and stored four-bit representation while aligning the caller and testing compatibility. Actual feature-enabled worker and service builds are required; no feature disabling is accepted as a repair. The reviewer examines the complete change, and the Linux pilot inspects existing local runtime availability without starting infrastructure. The representative fixture is bounded at 128 synthetic rows and must demonstrate multiple vector blocks per owner before memory conclusions. No new runtime result or cloud observation is claimed.


## Run 353: Actual worker and both service binaries build successfully

The coherent internal API repair passes independent full-build reconciliation. The feature-enabled worker library check and both compaction and query service builds succeed. Three compatibility tests, three actual USearch tests, one quantized persistence test and two actual worker registration tests pass. All 869 source hashes and build logs reconcile; exact service binary hashes are retained. Evidence is dynamic-review/worker-fourbit-build-v1/independent-complete-build-review.json. The repair preserves the current None/FourBit schema and stored representation without changing dependencies or disabling features. This closes the local compilation gap, not Linux execution or production registration proof.

The representative integration fixture now proceeds as one coherent test with 96 initial synthetic rows, two additions, two owners and 32 dimensions. The total remains capped at 128. Test-only small block targets must produce at least two actual vector blocks per owner before memory conclusions. The worker preserves admitted service binaries before further builds, and the reviewer reviews the complete fixture before execution.

Read-only Linux readiness finds installed Docker and OrbStack clients but no available daemon socket. Cached images remain unknown. The host compiler targets macOS and the observed Protobuf compiler differs from the repository Linux recipe. Evidence is pilot/results/local-linux-readiness/readiness.json. The next assigned environment step may start the existing local runtime through ordinary controls only after checking that unrelated workloads will not start; it permits no downloads, installations, builds or cloud operations. Linux reproducibility remains unproved.

Fresh GitHub and actual checkout Git identities match lyon-chroma. No cloud operation occurs in this continuation. The last cloud instance and storage observations retain their recorded dates and do not establish current fleet state.


### Run 353 continuation: representative test compiles; Linux startup remains held

The representative builder and record exporter compile successfully with 870 reconciled source hashes. Independent review requires explicit routing premises and a fresh private failure context targeting 100 rows from the final 98-row parent. The separately frozen test corrections address those findings; exact registry and staged runner admission remain before native integration execution. Compilation alone does not establish the multi-block or end-to-end result.

The local Linux runtime startup remains held because its documented start operation resumes previously running machines and persisted Kubernetes is enabled. No runtime or configuration changes occur. The pilot investigates documented selective startup read-only while local integration work continues. Evidence is pilot/results/local-linux-readiness/startup-readiness.json. Fresh GitHub and checkout identities match. No cloud calls occur; lifecycle and retained-storage observations remain dated.


## Run 354: Representative multi-block compaction and failure recovery pass

The complete local fixture passes in 3.768 seconds, or 5.845912 seconds including staged preparation, with 871 independently reconciled source hashes. Its two owners contain twenty and twenty-two actual embedding blocks both initially and after reopening saved outputs. Two additions produce a complete 98-row proposal from 96 source rows, including a destination beyond the nearest upper alias and an exact float32 routing tie. All old identity mappings and vector bytes remain exact. Full 98-ID checks after append, split, merge and reassignment pass, along with eight actual record audits and unchanged old pinned-reader results. Evidence is dynamic-review/representative-compaction-v3/independent-complete-runtime-review.json.

The deliberate record-flush failure produces the genuine operating-system directory error, retains three other roots and seven orphan objects, and makes no registration call. Fresh-process recovery prepares private 100-row record output from the final committed 98-row parent; it does not publish another complete append. All parent and failed-object hashes remain verified. Evidence totals 1,776,207 bytes across 1,072 files. This is local multi-owner saved-state proof with a fake authority, not Linux, physical deployment, production registration or a memory bound.

The next coherent milestone measures actual reader-held blocks on this retained multi-block fixture and tests bounded release only if the measured behavior warrants it. Exact results and pinned-reader correctness remain acceptance criteria. No corpus growth or query-strategy comparison is scheduled.

The Linux readiness review finds no documented selective startup that guarantees unrelated workloads stay stopped. Runtime and cached images remain unavailable for verification; the pilot is idle with prerequisites and offline recipe retained in pilot/results/local-linux-readiness/selective-start-and-closure.json. Useful local work continues. Fresh GitHub and checkout identities match. No cloud operation occurs, and the dated compute/storage observations remain unchanged.


## Run 355: Reader-held blocks fall to one with exact results preserved

The opt-in serial rerank path passes independent reconciliation on the retained multi-block fixture. Default reader-held peaks are fifteen blocks and 9984 estimated payload bytes for one owner, and sixteen blocks and 10624 bytes for the other. The serial release path peaks at one block and at most 1024 estimated payload bytes. All exact results, four post-fetch errors and subsequent retries, independent-reader results and fixture hashes match. Six owned reader processes terminate normally. Evidence is dynamic-review/rerank-held-pins-v2/independent-complete-runtime-review.json, with 873 reconciled source hashes and 64 responses. The wrapper takes 0.951424 seconds; this is not a latency comparison.

Three focused Rust tests, the reader build and enabled worker check pass. The initial three tests fail in shared Arrow fixture construction before rerank assertions because columns have different lengths. A separately reviewed balanced-tree test fixture passes; the original construction failure remains unresolved and retained under version one. No storage-code repair is claimed for that failure.

The measured limit applies to references retained in the reader map and estimated block payload. Provider caches, allocations before insertion, output vectors, duplicate tracking, shared-buffer accounting and total process memory remain outside the claim. Defaults remain unchanged. The serial path requires exclusive map ownership and ends vector borrows before release; panic, cancellation and process-death cleanup are unproved.

The next coherent milestone examines validation proportional to changed subtrees, including actual storage immutability guarantees and global ownership/publication checks. It must not reuse validation across calls for mutable files. The implementation and review pair are reassigned to source/design work before implementation. Fresh GitHub and checkout identities match. No cloud operation occurs; compute and retained-storage observations keep their existing dates.


## Run 356: POC scope correction

Lyon explicitly limits acceptance to a working proof of concept and measurements supporting credible theoretical scale to 100 billion vectors. Production integration, deployment, save failures and recovery, node failures, fleet resizing and similar hardening are excluded. Existing evidence remains retained. The current plan supersedes conflicting historical requirements.

The implementation and independent-review agents are reassigned to the POC scaling argument and a fixed-input experiment addressing whole-corpus append copying and validation. Storage immutability is an explicit experiment assumption, not a newly enforced production guarantee. Production provider and garbage-collection integration stop. The completed proportional-validation design remains architecture evidence only; no runtime is claimed.

Fresh GitHub and actual checkout author/committer identities match lyon-chroma. No cloud operation or new benchmark occurs in this scope update. Compute and storage observations remain dated.


## Run 357: POC validation build passes; measurement setup fails

The coherent fixed-input validation implementation passes two Python composition tests and three Rust segmented-input tests, and its native auditor builds. Independent review reconciles 879 source pins and admits one representative replay. The replay exits with a setup error after 0.429777 seconds, before any native audit or completed measurement phase. Standard-library temporary-directory discovery encounters the strict read-counting wrapper. The attempt remains retained under dynamic-review/poc-proportional-validation-v2. No validation speedup or byte reduction is established by this attempt.

The implementation agent prepares a narrow setup repair outside the measured region, with independent review and a fresh retained attempt. Whole-corpus writer copies and legacy full validation remain explicit separate costs. The evidence inventory separates measured ten-million-vector storage from conditional 100B extrapolation. Both agents remain active. Fresh GitHub and checkout identities match lyon-chroma; no cloud operation occurs and lifecycle/storage observations remain dated.


## Run 358: Changed-owner validation passes the representative POC replay

Independent review reconciles ten native saved-state audits, 879 source pins and all 1072 retained input hashes. Append validation returns 66660 encoded storage bytes versus 128916 for its complete-owner oracle; split validation returns 52760 versus 117442. Canonical reads are 6400 versus 12544 bytes and 6144 versus 12544 respectively. Unchanged owners receive no checkpoint payload reads in the incremental phases. The 126554-byte bootstrap and full setup reads are charged separately. The replay completes in 1.022006 seconds including its wrapper; this is not a latency comparison. Evidence is dynamic-review/poc-proportional-validation-v3/independent-complete-runtime-review.json.

The experiment assumes fixed immutable saved inputs and preserves exact ownership and saved vector checks. It establishes proportional changed-owner validation, not proportional whole-append work: actual writer canonical copies, all-owner clones, record scans and legacy metadata remain separate costs. Counters measure returned encoded bytes, not physical disk/network traffic or total memory. The original Python 3.9 setup failure remains retained; the successful replay pins Python 3.13.5 and initializes temporary-directory setup outside measured regions.

The implementation and review agents move to the next major task: integrate the segmented canonical view and selected-owner preparation into the actual writer, then run one representative experiment. Production hardening and minor optimizations remain excluded. Fresh GitHub and checkout identities match lyon-chroma. No cloud operation occurs; compute and storage observations retain their recorded dates.


## Run 359: Actual writer milestone and scoped POC completion

The actual selected-leaf writer passes independent review with 883 source pins, 1072 unchanged fixture files, exact routes and complete 98-ID ownership. Full canonical copying falls from 25088 bytes to zero, and unchanged-owner checkpoint copying falls from 85786 bytes to zero. The changed owner still copies 89807 bytes. Both native processes exit successfully; saved versions, postings, vector bits and selected leaves match. The baseline measures copy operations only, so no equal-work throughput improvement is claimed. Evidence is dynamic-review/poc-lazy-selected-write-v1/independent-complete-runtime-review.json.

Independent final review accepts the working POC and corrected conditional 100B model, verifies all six model-input file hashes, and requires no further experiment. Evidence is dynamic-review/100B-FINAL-SCOPED-ACCEPTANCE.json. The measured 10M layout extrapolates to 336.16 TB of live index objects or 444.57 TB of complete checkpoints at 100B; these are alternative accounting scopes, not sums. Extra input/record copies are separately charged. Global maps/version loops, partitioned record IDs, bounded routing quality and hardware/load assumptions remain explicit algorithmic prerequisites. The executable is not claimed to handle 100B today.

The corrected scoped POC objective is complete, and the recurring follow-up is paused under Lyon’s completion instruction. No production integration or further minor optimization is assigned. Fresh GitHub and checkout identities match lyon-chroma. No cloud operation occurs; last API fleet observations remain 05:24:26 UTC and retained-storage observations remain dated. All failed attempts and retained roots remain preserved.


## Run 360: Performance research phase begins

Lyon explicitly reopens the completed POC to improve build and query latency, cost and bytes read or transferred, with a competitive dimension as an aspiration. The existing implementation and independent-review agents receive major-milestone assignments. The linked literature-review chat supplies separate 40M Wikipedia observations that motivate routing, raw-vector access and maintenance research; those observations are not substituted for the current 10M measurements. No new performance result is claimed.

The recurring follow-up is active with the new objective and preserved operational constraints. Actual checkout Git identities and GitHub login match lyon-chroma. No cloud operation occurs; dated fleet and retained-storage observations remain unchanged. Prior completion evidence and failed attempts remain preserved.


## Run 361: Counted-parent append pair passes; final reconciliation pending

Two focused Rust tests and the benchmark build pass for 884 frozen source files. The actual paired 96-to-98-row append experiment passes under a separately audited immutable-parent assumption. Repeated old-version probes fall from 193 to zero, mapping reads fall from 768 to 384 bytes, and loaded old IDs fall from 96 to 48. Returned encoded parent bytes fall from 65390 across 31 fetches to 61610 across 29 fetches; probes are not equivalent to storage requests because of caching. Exact routes and both independent fifty-row saved-owner audits match. The selected-owner checkpoint copy remains 89807 bytes and canonical reads remain 6656 bytes. The entire paired controller takes 0.709861 seconds; no throughput improvement is claimed. Final independent reconciliation is pending. Evidence is dynamic-review/poc-counted-parent-write-v1/runtime-attempt1/result.json.

Primary-source research ranks major query and construction opportunities and explicitly rejects unsupported state-of-the-art comparisons. The next investigation checks actual refinement access patterns and retained real-data traces before selecting another implementation. The pilot performs a read-only local evidence inventory; no cloud operation, corpus download or runtime startup occurs. Fresh GitHub and checkout identities match lyon-chroma. Cloud compute and retained-storage observations remain dated; this experiment adds local artifacts only.

Independent complete reconciliation accepts the paired result, verifies all 884 source pins, 1072 unchanged fixture files, bootstrap hash, exact routes and four successful native exits. The review is dynamic-review/poc-counted-parent-write-v1/independent-complete-runtime-review.json. The next milestone remains evidence-driven: retained real-data traces have logical candidate IDs but lack physical block mappings and fetch/decode events, so their offline analysis can establish logical reuse and fanout only.


## Run 362: Query trace analysis guides a block-access measurement

Independent review accepts the separate counted-parent performance-model delta while preserving the baseline model. Its large operation-count reduction is mathematical extrapolation, not throughput evidence. Sixteen fixed held-out queries contain 16000 selected occurrences and 15915 distinct vector IDs, with no within-query duplicates. Exact refinement spans 449 owner calls across 122 owners. The local streaming analysis completes in 0.254 seconds at 47.74 MB peak memory. An initial unsupported address-space-limit attempt fails before trace analysis and remains retained. The trace lacks physical block mappings, storage fetch and decoding events; repeated logical IDs cannot establish redundant physical reads.

Source review identifies a possible concurrent cold-cache duplicate-fetch path, but its cost is unmeasured. The implementation and independent-review agents receive one measurement milestone on the retained multi-block fixture before any grouping optimization. The pilot receives a bounded offline stage/traffic analysis of the same sixteen queries. All three existing agents are reused. Fresh GitHub and actual checkout identities match lyon-chroma. No cloud operation occurs, and cloud compute/storage observations retain their dates. Evidence is dynamic-review/poc-counted-parent-write-v1/independent-refinement-and-model-delta-review.json and pilot/results/performance-readiness/fixed16-candidate-reuse-summary.json.


## Run 363: Refinement measurement finds no redundant decoding on the fixture

Five focused tests and the frozen reader build pass. The native measurement passes with identical exact results: thirty-two candidate IDs require fifteen or sixteen blocks per owner, and each fresh provider fetches and decodes each required block once. Serial release and default reads return the same 30054 or 32032 encoded bytes; warm repeats return zero storage bytes and perform zero storage-origin decodes. Additional serial provider lookups hit the cache. This rejects substantial duplicate decoding on this fixture and does not justify a grouping optimization. Final independent reconciliation is pending. Source versions one and two remain retained as unexecuted correction epochs; review catches an observer-forwarding omission and an incorrectly configured comparison test before version-three execution. Evidence is dynamic-review/refinement-block-access-v3.

The measured native children peak at 17530880 bytes of terminal-reported memory. The runner configures two runtime threads; this is not an operating-system CPU quota. Memory and output limits are terminal observations rather than hard instantaneous caps. Fresh providers do not imply cold operating-system caches, and the small fixture establishes mechanism rather than real-data query speed.

Independent analysis of the sixteen retained real-data queries finds mean elapsed time of 561.58 ms. Sequential dispatch lower bounds are 260.28 ms for posting scoring, 149.98 ms for exact refinement and 98.64 ms for routing. Overlapping worker durations are not exclusive time shares. Mean completed JSON bodies total 5.02 MB sent and 1.30 MB returned per query, excluding protocol overhead. The next investigations target posting scoring and repeated request content. No cloud operation occurs; identity checks match lyon-chroma and cloud lifecycle/storage observations retain their dates.

Independent review accepts the negative result after verifying all 885 source pins, 1072 unchanged files, ten exact responses and six terminal child statuses. The review is dynamic-review/refinement-block-access-v3/independent-complete-runtime-review.json. The duplicate-decoding hypothesis closes for this fixture; the next major target is selected from posting-scoring and request-payload evidence.


## Run 364: Shared-query batching selected for implementation

Independent analysis finds that repeated query-vector JSON accounts for most request bytes in the sixteen fixed real-data queries. Sharing identical values once per adapter batch models 4670202.75 fewer bytes per query, reducing 5019981.56 to 349778.81 bytes, or 93.03%. The component lengths and frozen query inputs are verified, but complete historical payloads are unavailable. This remains a serialization counterfactual rather than an executed network improvement. The implementation and review agents receive one coherent shared-query milestone with actual serialized-body accounting and full native query parity. The pilot supports pinned inputs and independent cost arithmetic.

Posting-scoring traces also show 1689 calls scanning 53.52 million unique candidates in total; 1674 calls exceed the 1000-result budget. Source inspection identifies full sorting before trimming as a subsequent algorithmic opportunity. No sorting-speed or latency claim is made, and it is not implemented concurrently with shared-query batching. All three agents are reused, fresh GitHub and checkout identities match lyon-chroma, and no cloud operation occurs. Existing retained-storage and fleet observations remain dated. Evidence is dynamic-review/poc-counted-parent-write-v1/independent-query-hoisting-opportunity-review.json and pilot/results/performance-readiness/fixed16-score-counter-distribution.json.


## Run 365: Shared-query batching passes; candidate selection begins

Five tests and independent complete reconciliation accept query sharing. Three full native query trace pairs preserve IDs, distances, selected candidates, reached leaves, budgets and snapshot bindings across sixty-four successful operations. Local request-body totals fall from 14461 to 12783 bytes, an 11.6% reduction. The actual new serializer applied to 256 explicitly reconstructed real-query batches reduces mean request bodies from 5014171.875 to 344657.125 bytes, a 93.126% reduction. The reconstructed envelopes use declared placeholder metadata and do not reproduce historical network packets. Adapter-to-Rust traffic and scoring work are unchanged; no latency, CPU, throughput or SOTA improvement is claimed. Evidence is dynamic-review/batch-query-sharing-v3/independent-complete-runtime-review.json, with 888 verified source pins and 1072 unchanged fixture files.

The first executed controller attempt fails during setup because the in-process transport replacement is not activated. Its placeholder hostname fails resolution before reader operations. That attempt remains retained under version two; version three activates the intended in-process path and passes. Version one remains an unexecuted source correction. No cloud or external network experiment occurs.

The next coherent milestone replaces full candidate sorting with cutoff selection followed by sorting retained candidates, preserving exact ordering and all cutoff ties. The worker and independent reviewer are assigned implementation, meaningful tests, paired kernel measurements and native query parity. The pilot verifies whether retained full score arrays exist so synthetic inputs cannot be mistaken for real-data measurements. Fresh GitHub and checkout identities match lyon-chroma; dated cloud lifecycle and retained-storage observations remain unchanged.


## Run 366: Candidate selection improves the isolated kernel with an explicit worst case

Eight focused tests, the native reader build and an optimized standalone build of the exact shared selection helper pass. Independent review accepts four full-query triplets comparing the retained reader, new full-sort baseline and new partition selection, with identical external results across 129 native operations. All 892 source pins and 1072 fixture files remain verified. The initial test compilation fails on an ambiguous float type before tests execute; version two supplies the explicit test type, preserving the failed epoch.

The optimized kernel benchmark uses observed candidate-count sizes and explicitly synthetic scores because full pre-trim real score arrays are not retained. Across 252 paired timing samples, the median-size spread case of 26136 candidates and a 1000-result budget improves from 718834 to 122292 nanoseconds, or 5.88 times. The tie-heavy case improves 1.93 times. The all-ties control is 1.90 times slower and retains every candidate. At the largest observed size, the synthetic spread kernel improves 8.31 times. These exclude score generation, cloning, deduplication and distance computation; they do not establish end-to-end query improvement or attribute the measured posting stage to sorting. The option remains explicit, with the all-ties regression preserved. Evidence is dynamic-review/posting-candidate-selection-v2/independent-complete-runtime-review.json.

The next assessment consolidates the major gains and remaining algorithmic barriers. The pilot checks local records for realistic end-to-end replay prerequisites without starting cloud resources or bypassing the prior source-upload rejection. Implementation and review agents assess a consequential next task rather than minor tuning. Fresh GitHub and actual checkout identities match lyon-chroma; no cloud operation occurs and dated compute/storage observations remain unchanged.


## Run 367: Integrated assessment selects checkpoint-copy removal

Independent integration assessment accepts the phase's improvements over its own baseline without an industry SOTA or end-to-end latency claim. Query sharing and selection compose through unchanged payload semantics and distinct policy fields; another tiny combined replay is unnecessary for the existing claims. The separately preserved phase assessment binds evidence and retains the original capacity model.

The next major implementation removes repeated selected-owner checkpoint copies by reusing existing immutable UUID objects in an experiment-owned store. Existing root-fork mechanics support this path; generation-specific sidecars and manifests must preserve old objects and old reads. Bootstrap copying is charged once, genuinely new written bytes are measured separately, and full changed-owner input/mapping/audit work remains explicit. Both implementation and independent review are assigned, and the pilot supplies owner-level storage accounting.

The read-only driver feasibility review finds that a complete ten-million-vector performance replay is not currently executable within the existing twenty-minute budget. The driver lacks checkpoint blocks and a verified Linux optimization binary. Retaining both archive and extracted objects would require about 89 GB against a dated 62.5 GB free observation; a streaming restore remains unreviewed. The prior source-transfer approval rejection is unresolved. No transfer, restore, cloud start or alternate route is attempted. Fresh GitHub and checkout identities match lyon-chroma. Compute and retained-storage observations remain dated. Evidence is dynamic-review/PERFORMANCE-PHASE-ASSESSMENT.md and pilot/results/performance-readiness/task-driver-10m-feasibility.json.


## Run 368: Shared-store tests pass after a retained controller import failure

The shared-store implementation passes three Rust layout tests. Its first Python test import fails on a missing closing parenthesis before any Python test or native experiment executes, so the build is skipped in that attempt. A separately frozen controller-only repair passes the two Python accounting tests, and the previously skipped native build passes in 6.758 seconds. The unchanged Rust tests are not repeated. Version one retains the failure and version two retains the repair, 895 source pins and the built writer. The sole paired native experiment remains subject to independent exact-binary review.

The retained ten-million-vector inventory measures complete owner checkpoints ranging from 105.79 MB to 893.40 MB, with a 329.49 MB median and 44.457 GB total. Review identifies an unequal-work sensitivity formula that omits the baseline's new writes. The pilot is reassigned to preserve that original and issue a corrected comparison: baseline bootstrap plus copied checkpoints plus new writes, versus shared-store bootstrap plus new writes. Neither new-write fractions nor predicted savings are reported as runtime measurements.

Implementation and review continue toward the complete copy-removal experiment. Fresh GitHub and checkout identities match lyon-chroma. No cloud operation occurs; compute and retained-storage observations retain their dates. Evidence is dynamic-review/shared-object-append-v1, dynamic-review/shared-object-append-v2 and pilot/results/performance-readiness/selected10m-owner-copy-accounting.json.


## Run 369: Shared-object append accepted; on-demand routing begins

Independent complete reconciliation accepts shared-store append. Repeat checkpoint copying falls from 89807 bytes across 41 files to zero; initial shared-store bootstrap still costs 89807 bytes. Each path retains eleven new UUID objects totaling 24310 bytes. Additional nonobject payloads are 11811 bytes for clone and 10086 for shared. Both complete fifty-row saved-vector and leaf-membership audits pass, as do old pinned, fresh old-generation and new-generation reader comparisons. Every old shared-store path and the old manifest head remain unchanged.

The native version-two run passes, but its new-object classifier omits the data directory prefix and incorrectly reports zero new UUID objects. That report is retained and explicitly invalid for this counter. Version three repairs only classification, passes two accounting tests and derives corrected totals from preserved inventories without repeating the native run or build. The final review binds 895 source pins and original evidence hashes. Evidence is dynamic-review/shared-object-append-v3/independent-complete-runtime-review.json. Logical retained bytes do not establish physical write traffic or a first-use advantage.

The next coherent milestone separates on-demand immutable routing from selected-owner mutation, addressing eager all-owner node/centroid loading. Implementation and independent review are assigned exact routing parity, separately charged bootstrap summaries and a representative local measurement. The pilot inventories retained real-data routing metadata without fetching block files. Fresh GitHub and checkout identities match lyon-chroma; no cloud operation occurs and dated compute/storage observations remain unchanged.


## Run 370: On-demand routing reaches whole-source review

The implementation is frozen with 897 source pins for independent review. It routes additions against an immutable parent before mutation and opens mutable writers only for selected owners. Tests and native execution remain pending; no byte, memory or latency improvement is claimed. Review requires exact global beam and tie behavior, unchanged routing topology across no-balancing additions, separately charged bootstrap verification and complete saved-state parity.

Retained ten-million-vector metadata records 12480 lower nodes, including 12207 leaves, across 128 owners. Owner node counts range from 29 to 335. Assuming one 768-dimensional float32 centroid per node gives 38338560 payload bytes, compared with 393216 bytes for the flat boundary roots. These are arithmetic payload estimates rather than measured allocations or process memory. Full level widths and provider residency are unavailable from metadata alone. The pilot prepares a consolidated capacity sensitivity table that preserves these limits and the existing accepted baseline.

The reviewer receives the ready whole-source assignment and the worker continues the coherent milestone. Fresh GitHub and checkout identities match lyon-chroma. No new runtime result, failed attempt or cloud operation occurs in this continuation; cloud compute and retained-storage observations retain their dates. Evidence is dynamic-review/shared-object-append-v3/independent-lazy-routing-design-review.json and pilot/results/performance-readiness/lazy-routing-metadata-and-scenarios.json.


## Run 371: Continuation handoff preserves active ownership

The new continuation chat reads the durable plan, accepted performance evidence and realistic replay prerequisites. The previous implementation and independent-review agents remain active on the single on-demand routing milestone. Their ownership is preserved: this chat starts no duplicate build, experiment, controller or implementation edit. The existing recurring follow-up remains paused until that milestone completes and ownership transfers.

The three routing contract tests pass across preserved attempts. Version two retains two passes and a test-runtime panic; version three repeats only the corrected differential test successfully. Its benchmark build fails in JSON result formatting before producing the new executable. The existing agents own that repair and the remaining native experiment. No routing byte, memory or latency improvement is accepted yet. Evidence is dynamic-review/lazy-routing-selected-mutation-v2 and dynamic-review/lazy-routing-selected-mutation-v3.

The state file now identifies the two active old-chat agent IDs and this continuation chat. Superseded pending tasks are preserved under a historical key so completed correctness work and excluded production gates are not accidentally restarted. Actual checkout Git author and committer match lyon-chroma. GitHub identity cannot be freshly verified through the restricted network; no commit, push or GitHub write is attempted. No cloud operation occurs, and dated resource observations remain dated.


## Run 372: Routing correctness passes with a negative byte result

Independent review accepts the completed on-demand routing mechanism. Three routing tests pass, the repaired benchmark builds, and the paired retained 96-to-98-row experiment preserves exact routes, mappings, both complete fifty-row saved audits and old/new reader results. The source retains 897 matching pins and 1072 unchanged fixture files. The test-runtime failure and JSON-formatting build failure remain preserved in earlier epochs.

Mutable writer opens fall from two to one, but both routing trees are fully visited: all 34 nodes are fetched. The eager parent observation returns 61610 encoded bytes across 29 fetches. Lazy routing returns 54388 bytes across 26 fetches, and opening the selected writer adds 31350 bytes across 15 fetches. The combined 85738-byte count is only a lower bound because the selected-writer observation omits later old-store reads during saving. It already exceeds the eager observation; no encoded-byte, total-memory or latency advantage is established. The owned routing payload/slot estimate peaks at 8624 bytes and excludes provider/decode/allocator and full selected-writer state.

The accepted receipt is dynamic-review/lazy-routing-selected-mutation-v4/independent-complete-runtime-review.json. Full parent verification and each mode's 89807-byte shared-store bootstrap remain separate costs. The next decision should address representative measurement prerequisites or a consequential remaining algorithmic barrier; it must not repeat this tiny fixture to manufacture a performance claim. No cloud action occurs.


Both prior agents subsequently confirm idle with the complete milestone durable. The existing automation is updated through the app to this continuation chat and resumed at its existing ten-minute interval. No duplicate automation is created. The next task is a local posting-stage timing/readiness audit; cloud restrictions and the unresolved transfer rejection remain unchanged.


## Run 373: Posting timing audit identifies a usable reporting path

A reproducible local audit verifies the archived fixed sixteen-query selection and its hashes. Those real-data records contain zero posting substage profiles. The retained candidate-selection correctness run contains sixteen native profiles across its two profiled policies; all six nonnegative substage intervals fit inside each recorded native operation envelope. This is timer-containment evidence on a tiny fixture, not a representative performance measurement. No search, build, cloud operation or source modification executes.

Source inspection distinguishes storage reads from posting preparation. The load interval also includes shared-lock waiting, version lookup/filtering, code-array copying and centroid loading. Scoring includes result insertion; allocation reservation and some preparation/destruction remain outside the substage intervals. Native operation elapsed time excludes final serialization and transport. Summing concurrent reader wall times does not produce query latency. Exact source hashes and interval definitions are saved in dynamic-review/posting-timing-audit-v1. This new analysis has no independent review and does not supersede accepted implementation receipts.

An executable check of the actual report projections finds that the held-out compact report preserves the profile, whereas the concurrent-load allowlist omits it. A future held-out replay can use the existing reporting path without a code change. No missing historical timings can be reconstructed. The next useful local readiness step selects a representative owner from retained real candidate counts and checkpoint inventories, with explicit component-only claims and prerequisites. The complete ten-million-vector replay remains constrained by missing local checkpoint blocks, Linux build/capacity and the unresolved source-transfer approval rejection. No alternate transfer route is attempted.


## Run 374: A representative component replay is selected without execution

The selection uses candidate counts rather than timing or observed speedup. Among fifty owners reached by all sixteen fixed queries, owner 27 has the closest median candidate count to the overall per-call median: 26167 versus 26136. Its counts span 5409 to 51986. It holds 64406 rows and a 274381668-byte complete checkpoint. The exact snapshot hash, sixteen query vectors and ordered reached-leaf lists are bound in dynamic-review/posting-component-plan-v1; leaf counts and unique IDs match retained per-owner records. These reconstructed request payloads are not historical wire captures.

The proposed component experiment compares full-sort and partition selection across three paired repetitions, up to 96 scoring calls and a five-minute measurement deadline after verified setup. It checks exact candidate/distance parity and saves substage timings alongside the complete native envelope. This selects a typical candidate-count workload, not a representative tail, full-query recall or horizontal throughput workload. Planning has no independent review and grants no runtime admission. No search, build, restore, transfer or cloud operation runs.

Owner 27 resides in the worker-one archive containing 22290892800 bytes. A smaller extracted checkpoint does not establish a smaller network transfer; no verified random-access index or selective restoration is established. Restore/build resources remain outside the measurement budget. A reviewed restore, verified Linux build, resolution of the prior normal source-transfer approval rejection, independent runtime review and fresh execution budget/guards remain prerequisites. The recurring follow-up is paused through the app because local measured milestones and readiness work are complete and the next material evidence requires those external decisions. This is not a new latency improvement or a claim that the 100B executable exists. All failed and accepted evidence remains preserved.


## Run 375: Upload, build and restore are authorized; fresh authentication fails

Lyon explicitly authorizes upload to the test machine, the Linux build and data restore. This supersedes the prior no-cloud-work restriction for the scoped experiment, while retaining sandbox-only operation, protected-resource exclusions, ordinary approval controls and fresh resource guards. No additional discretionary user approval is requested for those tasks.

The Cops configuration has no sandbox profile and its default has no login session. A private temporary task configuration combines the Cops settings with the default AWS login-session reference without exposing credentials. A normal network-enabled read-only identity request reaches AWS but returns an expired-session error. No current account identity is established, no instance starts, and no upload or restore executes. The user is asked to refresh the sandbox login or identify the named profile they refreshed.

The historical driver guard is inspected and cannot be reused: its deadline is expired and its configuration targets the old task profile. Fresh identity, capacity and independent absolute guards remain agent work after authentication. The exact authorization and failed check are saved in fleet/component-replay-resume-v1/authorization-and-auth-check.json.


## Run 376: Refreshed Cops session permits bounded Linux setup

The refreshed Cops SSO session verifies sandbox account 079819354890. Fresh EC2 reads find all four task instances stopped. Only the existing m6i.2xlarge driver starts, with a fixed one-hour local stop watchdog and a remote systemd shutdown at 2026-10-16 01:56:54 UTC. The other workers and preparation instance remain untouched. Fresh remote preflight records about 62.5 GB free disk and 33 GB RAM; these observations do not establish future headroom.

Normal SCP upload of the frozen source and manifest succeeds, resolving transfer for this exact authorized attempt. A subsequent bulk upload reports a nonregular __pycache__ directory; regular payload files arrive and source verification passes. No alternate transport is used. All 892 source pins from posting-candidate-selection-v2 verify in a private copy of retained driver source. Rust 1.92 installs successfully, and an isolated release build starts with four jobs, a ten-GiB free-space floor and per-command deadlines bounded by the machine stop. No native query executes yet.

The version-pinned 2.67 MB archive inventory downloads locally as metadata and matches its SHA. Owner 27 has 220 expected files totaling 274381668 bytes. The exact restore script, build controller and paired replay controller are prepared under fleet/component-replay-resume-v1/upload. The independent reviewer confirms the sixteen query/leaf payloads and owner-selection rule. Further restore/replay source review and actual Linux binary/restore reconciliation remain pending. Lyon authorizes communication with existing reviewers/chats; the original parent coordinates its existing reviewer because direct app input to that subagent is unsupported. No new agent is spawned.

The recurring follow-up resumes in this chat, with explicit instructions to monitor the existing controller and never extend the fixed deadline or duplicate work. Raw guard, source, review and capacity receipts remain in fleet/component-replay-resume-v1.


## Run 377: Linux build passes and the reviewed restore resumes

The frozen Linux release build completes in 370.029 seconds, and all four intended selection tests pass. The retained binary is 14321696 bytes with SHA a5d8ff088622c3604eb78bff00c429b662552d1720e7acb3141385da96912c2c. All 892 frozen source pins match. Build dependency records additionally identify eighteen retained SQLite migration inputs outside that manifest; their hashes are disclosed in build-provenance.json for independent review. This is build evidence, not a performance result.

The first independently admitted owner restore fails before download because the driver has no AWS executable. Its script and error remain preserved. An isolated AWS CLI installation succeeds, and independent delta review admits the single executable-path correction with every original restore guard retained. The exact-version archive download now runs under the unchanged 01:56:54 UTC machine deadline. A small receipt-fetch attempt using unsupported SCP brace expansion also fails without transferring data; explicit file paths succeed. No alternate transfer route is used.

Independent review admits the corrected replay controller conditionally. Actual binary and restored inventory admission remain required before any query. The current chat coordinates the existing reviewer through its parent; no new agent or duplicate controller is started.

Independent review accepts the observed build dependency closure after correction. The verified omission is relative compiler dependency paths; the earlier suspected whitespace-regex defect is superseded by direct review of the preserved collectors. The corrected scan records 281 source-local inputs: 263 match original frozen pins and eighteen embedded SQL migration files match the actual implementation checkout. Postbuild configuration and dependency observations are explicitly weaker than a complete prebuild frozen dependency snapshot. Both incomplete receipts remain preserved. No rebuild is required by the reviewer for this scoped component comparison.

The admitted restore completes successfully: the exact 22290892800-byte archive matches its full SHA and all 220 extracted owner files match expected sizes and hashes. The exact manifest, mapping and five sparse-index roots are available for independent reconciliation. At 01:18:49 UTC both fixed stop guards remain active, the exact admitted binary/controller hashes match, and no query has yet executed.


## Run 378: The real-owner component replay completes and the driver stops

Independent final admission permits one exact 96-call paired comparison. All calls pass in a 1.467-second controller interval, all six readers exit normally, every paired local-ID/f32-distance result matches, historical candidate counts match, and the 892 source pins and 220 owner files pass before/after checks. Raw responses and the exact Linux binary are retained locally; restored corpus data stays on the stopped driver.

Preliminary analysis separates the first request of each fresh application reader from its subsequent requests; OS caches remain uncontrolled. Subsequent requests have a median paired native speedup of 1.1416 and a selection-plus-sort speedup of 6.8564. Median native times are 3.4635 ms for full sort and 2.95704 ms for partition. First-request median paired native speedup is 0.9923, so that subset establishes no improvement. Load/preparation accounts for 88.90% and 90.24% of summed first-request native time. On subsequent partition calls, distance scoring accounts for 45.37%, load/preparation 18.28%, deduplication 16.87%, unassigned work 13.98%, quantization 2.88%, and selection plus sorting 2.62%. These are serial component stage shares, not full-query latency. Three repetitions of sixteen fixed queries on one owner do not establish tail behavior, recall or 100B performance. Independent whole-runtime reconciliation remains pending.

Fresh sandbox identity precedes early stop. EC2 confirms the driver and all three other task instances stopped, and the local watchdog is retired only after that verification. All archives and failed epochs remain retained. The recurring follow-up remains active for review and consequential local analysis; it cannot restart this completed controller or reuse its fixed resource window.

Independent whole-runtime review accepts the completed experiment without rerun. It independently recomputes all 48 paired comparisons, confirms the six responses with a 1001st cutoff-tied candidate, matches every evidence hash and verifies the reported timing arithmetic. The accepted subsequent native ratio is 1.1416×, with no first-request gain. The all-ties regression, postbuild dependency-provenance caveat, sampled-memory limit and single-owner claim boundary remain explicit. Review receipt: fleet/component-replay-resume-v1/independent-completed-runtime-review.json.


## Run 379: Repeated-use preparation is already amortized

An offline analysis of the accepted exact request order separates queries that encounter any new leaves from those whose reached leaves are all previously visited. Six queries encounter new leaves; ten do not. Across the thirty partition-policy observations of those ten queries, load/preparation occupies 0.691% of summed native time, distance scoring 54.592%, deduplication 18.832%, and unassigned work 19.101%. Per-query median load time spans about 0.006–0.027 ms. Leaf newness is reconstructed from the request sequence, not measured as a provider or OS-cache miss counter. No query runs.

Source inspection confirms that loaded leaves already skip version filtering and posting copies. An additional posting cache is therefore not justified by this repeat-load evidence. The measured scorer uses one-bit codes and integer population counts, not the four-bit path that allocates unpacked float vectors. A consequential next hypothesis is a posting-batch integer scoring interface that amortizes query and per-code work while preserving the exact floating-point correction order. Its benefit is unmeasured; duplicate live copies still require minimum-distance semantics and cannot be skipped without proof. The existing reviewer is asked to assess this opportunity before implementation. Evidence and source hashes are in dynamic-review/posting-next-opportunity-v1. No cloud, code change or new agent occurs.

Static inspection of the exact accepted Linux binary narrows the next hypothesis further. The scoring symbol contains software vector population-count sequences and no hardware population-count instruction. The complete symbol disassembly and binary hash are retained in posting-next-opportunity-v1. This is compiler evidence, not a hardware capability check or speed comparison. A supported-hardware integer scoring path with a portable fallback should be evaluated before changing stored layout or quantization; preserve floating-point correction order, complete duplicate minima and cutoff ties. Independent review is checking the exact disassembly.

Independent opportunity review accepts the sequence classification and independently reproduces the software population-count disassembly. It ranks runtime-feature-dispatched exact integer scoring first, with one dispatch per posting batch, a portable fallback and unchanged float correction. Hardware capability and speed remain unproved. The existing implementation worker is requested through its owning parent to prepare that coherent local source milestone and meaningful correctness tests. No new agent, cloud operation or measurement is authorized by this dispatch; a fresh resource window and complete independent source/runtime review remain required.


## Run 380: The next paired comparison is prepared alongside implementation

The existing implementation worker remains active on the hardware-dispatched exact scorer. The current chat prepares a separate local experiment proposal that fixes partition selection, requests and checkpoint while comparing portable and supported-hardware scoring in one executable. The controller must verify actual selected kernel and CPU support; a silent fallback cannot count as a hardware treatment. Three fresh-reader AB/BA/AB pairs retain the 96-call, five-minute measurement limit and exact ID/f32/tie checks.

The proposal reuses the retained 220-file owner checkpoint and requests no new corpus transfer. A new setup window is proposed at no more than 45 minutes and a one-dollar compute ceiling, contingent on a fresh applicable rate and exact identity/resource/capacity/guard checks. Existing retained storage remains separately charged. No instance starts, no resource window is armed and no new binary executes. The existing reviewer is asked to review fleet/hardware-scorer-comparison-v1/proposal.json while implementation continues. Source and CLI details remain pending, so this is not runtime admission.

Independent proposal review accepts the experiment design conditionally and keeps source, Linux hardware tests, current resource pricing and fresh guards as explicit gates. The worker supplies the exact portable/runtime CLI and requested-policy/actual-kernel response contract. A local controller adaptation now fixes partition selection, checks CPU POPCNT capability and every response's actual kernel, binds the fresh CPU receipt, preserves all prior deadlines/parity/output/cleanup checks, and references the unchanged retained owner. Python syntax validation passes; no controller execution occurs. The existing reviewer receives the controller, and the worker is asked to pre-pin the eighteen SQL migration dependencies in the new source epoch. Implementation and meaningful tests remain active.


## Run 381: The scorer source is frozen for independent review

The existing worker supplies a 916-file immutable source epoch and isolated change patch. The new freeze includes all eighteen SQL migration inputs before compilation. Five existing source files and one new scorer file change. Explicit portable and runtime policies share the batch interface and floating-point correction; runtime CPU detection protects the x86-specific integer loop. Proposed tests cover exact integer sums, padding and unaligned codes, malformed batches, correction bits, saved-posting minimum distances and cutoff ties. These tests are written but have not yet passed; the worker waits for independent source review before bounded local execution. ARM fallback tests cannot prove x86 execution.

Independent controller review conditionally accepts the exact prepared script hash edb92e749a8c4a629be68100ee52f0cb8901ef56f4555ea7a69d7fc5f35e83bd. It retains source/test, actual CPU and Linux-code-generation evidence, data revalidation and fresh resource/guard admission as separate gates. The existing reviewer receives the complete source epoch. No cloud machine starts, no controller runs, and no performance result changes.


## Run 382: Local scorer correctness passes and fresh Linux setup is prepared

Independent whole-source and completed-local review accept the frozen scorer. Four library tests, one saved-posting parity/minimum/tie test and the benchmark compile check pass with all 916 source pins unchanged. ARM macOS exercises portable fallback only; x86 code generation and execution remain unverified. No measured speedup is added.

Fresh Cops paths and AWS reads verify sandbox 079819354890, all four task instances stopped, and an applicable Linux shared m6i.2xlarge price of $0.384/hour. The proposed 2700-second window implies at most $0.288 compute at that rate, below the $1 ceiling; retained storage is separate. The first read-only identity attempt encounters an automatic permission-review timeout, not an unsafe-action finding. The expressly allowed single retry succeeds; no alternate route is used.

New epoch setup scripts are prepared for independent review. A fresh local watchdog and unique remote absolute stop timer bound the existing driver only. Preparation privately copies retained source and overlays the exact 916-file archive. Preflight hashes all 220 retained owner files, requires POPCNT in the intersection of logical-CPU flags, checks disk and records compiler/environment. Linux work runs the four hardware tests, saved-posting test and release benchmark build under disk and absolute time limits. No archive redownload, instance start or setup-controller execution occurs before review. Exact receipts and scripts are in fleet/hardware-scorer-comparison-v1.

Independent setup review admits the bounded preparation and build. Ordinary start approval succeeds. The existing driver starts with fixed deadline 2026-10-16 02:45:31 UTC, local watchdog PID93415 and a newly verified remote shutdown timer at that same absolute time. The current address is18.232.124.50. Ordinary SCP upload is in progress. This is a new epoch, not an extension or reuse of the completed window; no query is admitted yet.

Source preparation and remote preflight pass: all 916 source pins, all 220 retained owner files, POPCNT in all eight logical-CPU flag sets, and 35495673856 free disk bytes. Compiler-selector and target-flag overrides are empty; the build explicitly selects Rust1.92. The bounded Linux build/test controller runs under the verified fixed shutdown. Small receipts are retained locally and sent for independent setup reconciliation. Performance replay remains held. The existing automation now names this active epoch and controller so it cannot start a duplicate or follow the prior completed window.

Linux validation completes successfully: four library tests explicitly exercise x86_64-popcnt, the saved-posting/minimum/tie test passes, and the release benchmark builds. Stage times are376.031,58.005 and54.005seconds. The binary SHA is9674aabe3ee46e8e00c62e279bd3cbd489a828e433fb6058f968db964f985829 and size14573464bytes. Source-local dependency inspection finds284inputs, all pre-pinned, no mismatches; all916sourcepins match. Exact disassembly confirms four hardware POPCNT instructions in the dispatched integer loop and retains the separate portable implementation. Fresh guard evidence and full logs are supplied for independent final runtime admission; no performance call has executed.


## Run 383: Hardware scoring preserves results and improves the real-owner component

Independent final runtime admission permits one exact 96-call comparison. Every call passes requested-policy and actual-kernel checks, historical candidate counts and paired local-ID/f32-distance parity including cutoff ties. The 1.415-second controller interval ends with six normal child exits; all 916 source pins and 220 checkpoint files match before and after. No rerun occurs.

Preliminary paired analysis reports 1.928× faster distance intervals and 1.3213× faster native component intervals for subsequent partially warmed requests. Their separate native medians are 2.92609 ms for the explicit portable batch policy and 2.013769 ms for hardware dispatch. For the sequence-derived subset with no newly reached leaves, paired native ratio is 1.3447× and distance ratio is 1.9202×. First-request paired native ratio is 1.0487× across only three correlated pairs with uncontrolled OS caches; it is not a robust cold-latency claim. Both arms use the same binary and fixed partition selection. This does not compare with the omitted-flag legacy path, multiply with the earlier selection gain, or establish full-query latency, recall, throughput or 100B performance.

Fresh sandbox identity precedes early stop. Timestamped API evidence at02:15:16UTC confirms all four task instances stopped, and local watchdog93415 is retired afterwards. Small outputs, exactbinary, source, archive and owner data remain preserved; no retained-root cleanup occurs. The existing reviewer receives raw outputs, analysis and lifecycle evidence for whole-runtime reconciliation. The automation now reflects the completed epoch and forbids restarting its controller or reusing its window.

Independent whole-runtime review accepts the completed same-binary hardware-scoring milestone without rerun. It recomputes all48exactpairedcomparisons and timing ratios from rawresponses, verifies six1001candidate cutoff-tie responses, all actualkernel echoes and every evidence binding. Accepted subsequent native/pipe/distance ratios are1.3213×/1.2413×/1.9280×. First-request improvement is not generalized from three pairs. Timestamped stopped-state and guard-retirement order are accepted. Receipt: fleet/hardware-scorer-comparison-v1/independent-completed-runtime-review.json. No next implementation or cloud window is active.


## Run 384: First-use preparation needs attribution before optimization

The accepted hardware result shifts attention toward first-use preparation, which occupies92.685% of summed first-request native time, versus1.0785% on the sequence-derived no-new-leaf subset. These are three first requests on one owner with uncontrolled OS cache, not a general cold-start distribution. The aggregate load interval cannot distinguish lock waiting, storage/provider work, version filtering and copies.

Source inspection confirms that selected leaf preparation serializes scalar-version access, issues point reads in64-item batches with16futures and clears scalar block pins after eachbatch. It also identifies a concrete API trap: the Arrow range reader selects blocks by prefix range before applying key bounds inside blocks. Replacing selected-ID point reads with a broad prefix/range call would not establish bounded block work. No such change is implemented. A failed path guess while locating the reader is corrected through the source file inventory; no experiment fails or executes.

The existing implementation worker is requested to analyze bounded block-grouped owned-version retrieval and its pin lifetimes, and the existing reviewer to assess a minimal meaningful attribution milestone. They receive distinct read-only artifact ownership under dynamic-review/posting-preparation-opportunity-v1. No new agent, source edit, cloud action or runtime is started. All accepted scoring results and failures remain preserved.


## Run 385: Bounded preparation attribution is selected

The next milestone measures why first-use posting preparation dominates the observed first request. Independent opportunity review accepts bounded attribution before optimization, and the existing worker receives the implementation assignment through its original parent. The observer must separate version lookup, lock waiting, provider work and filtering/copying while retaining exact candidate and float-bit behavior. Its maps and events have explicit limits; overflow invalidates attribution. Observer overhead is not a speedup. Whole-change review and a representative experiment remain prerequisites for choosing an optimization. No new cloud window or controller is active.

The source-only worker analysis and independent review agree that broad prefix-range retrieval can overscan and that existing installed leaves already bypass preparation. The current lookup completion order also requires care before any future grouping change. The assignment preserves these boundaries and excludes a grouping implementation. Evidence remains in dynamic-review/posting-preparation-opportunity-v1; new implementation artifacts belong in dynamic-review/posting-preparation-measurement-v1. No new tests or performance calls execute in this continuation run.


## Run 386: Preparation observation protocol and minimal replay proposal

The existing worker is implementing a fresh request-owned observer, and the existing reviewer issues explicit design conditions for quiescence, overflow and meaningful error/cached-leaf tests. The draft protocol separates reader opening from request preparation and disables full per-ID capture. Source completion and test results remain pending.

A local proposal in fleet/preparation-attribution-v1/proposal.json specifies one observer-off/on pair over the same sixteen requests, for 32 calls total. Its purpose is mechanism attribution and parity, not a speedup estimate. Fixed arm order, uncontrolled OS cache and observer perturbation remain explicit limitations. The proposed new 45-minute setup window and $1 compute ceiling are unexecuted and require fresh rate, identity, capacity, guard and independent runtime admission. No cloud action, controller, new agent or repeated experiment occurs.


## Run 387: Local observer checks pass; exact replay awaits review

The preparation observer passes five local tests and a benchmark compile check with all 917 frozen source inputs unchanged. Independent review admits the source and the single-pair measurement design. Completed-local reconciliation and exact controller review remain pending. The new controller requires valid, quiescent, nonoverflowed profiles and exact paired candidate results before interpreting attribution. It is syntax-checked only; no Linux build, cloud action or performance call executes.

A source search includes an incorrect benchmark path; the exact admitted source list identifies the correct file and the follow-up read succeeds. No experiment fails. The controller and retained local results are sent through the original parent to the existing independent reviewer. The completed hardware window is expired and remains closed.


## Run 388: Local evidence accepted; controller accounting repaired

Independent review accepts all five local tests and the benchmark check with 917 source pins unchanged. Controller review identifies incomplete leaf/batch accounting and unchecked repeated cache/open metadata. The repaired controller checks exact requested leaf coverage, bounded batch histograms, scalar pin/provider/decode identities, explicit matching cache settings and constant one-time reader-open metadata. The previous controller is preserved. The repair is syntax-checked and sent for independent review; no replay or cloud action executes.

A guessed provider-source path fails; the file inventory locates the actual cache policy module. A draft message contains an incomplete hash fragment; a follow-up supplies the exact controller hash 48d56de34216a2d1811b541c4b9488b69a82e508686c69406377d6ed18a7d3b0. Neither issue executes an experiment or changes the frozen implementation.


## Run 389: Controller admitted and Linux setup prepared

Independent review accepts the repaired measurement controller in fleet/preparation-attribution-v1/independent-corrected-controller-review.json. The Linux setup package now binds the frozen917-file source, retained owner/request inventories and syntax-checked preparation, preflight, release-test/build and provenance scripts. Existing reviewer receives the package through its parent. Setup review, fresh cloud checks, new absolute guards and exact Linux binary admission remain required before the32-call measurement. No cloud action or performance call executes.

The package assembly initially looks for source-manifest.json in the source artifact directory. That path is absent; the actual manifest.json is copied unchanged to the upload name. The failed local copy is preserved here and does not execute setup.


## Run 390: Linux compiler binding made explicit

Independent setup review requires an explicit compiler binding: selecting the Cargo toolchain alone leaves an ambient compiler override possible. The build now selects the exact Rust1.92 Linux compiler, records its hash/version and effective compiler selectors with the working directory, and rejects ambient wrappers, flags and target selectors. Provenance includes that actual build-environment receipt. The original scripts and package are preserved. The corrected package is syntax-checked and sent for independent delta review. No cloud window, Linux build or measurement executes.


## Run 391: Setup assertion repaired and sandbox rechecked

The setup review catches a deterministic environment check that rejects its own build output directory. The correction validates that directory separately while continuing to reject target compiler overrides. An isolated check of the exact assertion code accepts the assigned directory and rejects target flags; no build executes. The correction is sent to the existing reviewer. Fresh Cops-backed AWS identity confirms sandbox079819354890 and a fresh API read confirms all four task instances stopped. No execution window is opened.


## Run 392: Setup accepted and fresh window budget verified

Independent review accepts the corrected Linux setup source. A fresh AWS price lookup confirms $0.384 per hour for the existing Linux driver, so the proposed45-minute window bounds compute at $0.288 below the $1 ceiling; retained storage remains separate. The new epoch has a copy of the reviewed lifecycle script, with no start or deadline created. The existing reviewer receives the window procedure for review. Actual new watchdog and remote absolute timer verification, Linux build evidence and runtime admission remain pending.


## Run 393: Latency diagnosis priority and interrupted launch reconciliation

Lyon prioritizes a matched explanation of query latency: clustering quality, navigation work, distribution and implementation overhead. The referenced unsharded report skips its GT cluster diagnostic. Source inspection identifies version-valid leaf coverage of true neighbors at90/95/100percent as a navigation-independent measure, with missing-coverage and lazy-materialization caveats. Existing worker/reviewer receive a read-only design and retained-baseline audit. Same vectors, metric and ground truth are required; compare vectors scanned as well as leaves and match recall/rerank/cache before latency comparisons. No cross-dataset quality conclusion follows. The preparation observer remains preserved and unmeasured; the interrupted launch produces no start/watchdog receipt and a fresh API check confirms the driver stopped. No automatic launch retry occurs while the next milestone is reassessed.

The first local process check fails because unmatched shell globs and sandbox process-list permissions prevent inspection. A direct directory listing finds no lifecycle receipts, and the subsequent read-only AWS query confirms stopped state.


## Run 394: Preparation experiment shelved; bounded quality diagnostic assigned

Lyon explicitly shelves the preparation timing experiment because its priority and setup overhead are disproportionate. Its source, reviews and failed setup attempts remain preserved; do not launch or continue setup. The recurring instructions now reflect this decision. Independent review accepts the index-quality prerequisite audit: no matched unsharded baseline exists in inspected evidence. The next implementation is a bounded saved-index ground-truth membership diagnostic, with complete unique coverage, root reachability, strict live versions and per-leaf vector costs. Existing worker receives implementation ownership and one meaningful saved-fixture oracle test; reviewer receives one consolidated whole-change package after completion. No new agent or cloud work is requested. This diagnostic first separates clustering requirements from excess navigation work on the same tree; comparing sharded versus global construction still requires a matched baseline.


## Run 395: Smaller matched cohort preference recorded

Lyon selects2M vectors as the starting scale for a new matched sharded/unsharded experiment, preferring smaller tests and proportionate time/token use. Reusing a retained10M tree remains distinct from building a new baseline, but no10M build is planned. Measure actual build time rather than assume a ten-minute bound. The current diagnostic remains cohort-independent and is still being implemented; the reviewer waits for one complete package. Lyon prefers fresh focused workers for distinct subsequent tasks, after this diagnostic finishes and within existing lifetime limits. No cloud action or new agent occurs.


## Run 396: Diagnostic compatibility repair and consolidated validation

The diagnostic source review finds the coverage and work-oracle semantics sound, but identifies a test-target import incompatibility before execution. The worker preserves V1 and repairs test placement. Necessary bounded local correctness tests and compile checks may follow the repair; independent review receives one completed package rather than another routine pre-test review round. No test result or real-data finding is claimed yet. The retained10M archive inventory proves metadata for128owners but contains no backing root/block payloads; this is not evidence that remote retained data is missing. The future matched experiment remains targeted at2M.


## Run 397: Diagnostic correctness accepted; matched2M design outlined

Independent whole-change review accepts the saved-index diagnostic and three focused correctness tests. Reader-target compatibility compilation and benchmark check pass;920source pins and1072fixture files remain unchanged. These are local correctness results, not real-data quality or latency evidence. A2M matched comparison outline separates initial grouping diagnostics from subsequent recall-versus-work tests. Exact ground truth must be recomputed for the2M corpus: filtering the10M top100 cannot supply a valid2M reference. Input availability, equivalent build entry points and proportionate execution budget remain to be resolved; no cloud action occurs.


## Run 398: Retained2M source candidate identified

The local archive inventory identifies version-pinned vector chunks sufficient for a candidate2M cohort, avoiding any need to rebuild10M or download a new corpus. Exact canonical chunk order and accepted attempts still need reconciliation against completion manifests, and live object availability remains unverified. The common native builder accepts the same raw vector format and construction parameters for one tree or multiple owner trees, providing a controlled baseline without equating it to the unrelated100M report. Candidate source arithmetic and exact object receipts are saved in dynamic-review/matched-2m-quality-plan-v1/retained-source-candidate.json. No transfer, build or cloud action occurs.


## Run 399: Canonical2M input and live archived versions verified

The2M input is now reconciled against the ordered ingest manifest and both accepted source-file receipts, not lexical filenames. It uses21 archived chunks, with9644rows from the final chunk:6.144GB of vector payload from6.421573632GB of whole objects. A fresh sandbox identity and exact-version S3 metadata check confirms all21objects exist with expected sizes and version IDs. No payload is downloaded or rehashed; payload verification remains required on the test machine. This removes the input-order and archive-availability uncertainty without starting compute. Equivalent partition construction, reference computation and a proportionate executable plan remain next.


## Run 400: Construction and reference controls specified

The matched2M proposal now specifies spatial partitioning into32owners rather than modulo shards, with the same frozen native builder and vector order for a global tree and the owner trees. The32owner choice targets roughly comparable owner sizes, not equivalence to the10M index. Existing blocked CPU float64 reference code can compute the16query top100 with deterministic ID ties and bounded working matrices. Source inspection exposes a construction confound: default checkpoint cadence counts rows per owner, so an unchanged10K flag does not give matching global insertion/balance schedules. The plan explicitly requires a common schedule or a declared single-final-balance comparison after memory sizing. No build or measurement executes.


## Run 401: Common insertion/balancing schedule selected

The balancing schedule is resolved as twenty shared100K-row global insertion rounds, projected into each owner in canonical order. Both layouts balance and commit at those same corpus boundaries. A single final balance is rejected because add registers vectors without balancing, which would accumulate a giant initial leaf and change the construction problem. Input/download/partition staging alone is18.71GB before indexes and build artifacts, so the previous driver free-space observation cannot establish sufficient capacity. Historical many-owner10M performance is not a monolithic2M build-time forecast. The next implementation package must support variable owner batch sizes and mapped resumes, with one consolidated review.


## Run 402: Matched round projection implemented and checked

Independent review accepts the matched design direction and identifies cumulative ID maps as a concrete runner requirement. A local projection module now splits common corpus rounds into owner batches, preserves relative order, retains unchanged cumulative map prefixes and skips empty owner rounds. Two standard-library checks pass, including delayed owner initialization and an independent row/map correspondence check. No native build or real-data execution occurs. New-agent capacity cannot be verified because the recorded lifetime limit remains reached; continuation owns runner implementation rather than spawning around the limit. The completed runner will receive one consolidated review.


## Run 403: Bounded build orchestration drafted

The draft matched runner now constructs exact mapped-resume commands from projected rounds and executes owner jobs sequentially under one external absolute deadline. It verifies binary/input hashes, checks disk and sampled native-process memory limits, retains per-job logs and checks saved generation/count after success. This is syntax-checked local code only, not reviewed or executed build evidence. Remaining integration includes frozen spatial labels, global identity-map batches, exact references, diagnostic specs, complete post-run validation and consolidated whole-runner review. No machine or cloud operation starts.


## Run 404: Source checkpoint commit blocked by hooks

A local commit checkpoint is attempted after verifying both Git identities and matching all261changed manifest-covered files against the accepted920-file source epoch. Nine files outside that manifest and the unfinished runner are excluded. Initial staging hits sandbox shared-Git-metadata permissions; ordinary approved escalation permits staging. Commit hooks then fail and modify two formatting-only files, so no commit is created. Hook outputs are preserved in dynamic-review/commit-checkpoint-attempt-v1 and both files restored from hash-verified staged originals. The frozen snapshot remains staged, with source bytes unchanged. Hook failures must be resolved explicitly before retry; no hooks are bypassed and nothing is pushed.


## Run 405: Paired preparation integrated in draft

The paired preparation draft now derives spatial assignments, global identity assignments, projected round inputs and a single exact2M reference from one hash-bound corpus/query pair. It uses existing frozen-model and blocked-reference routines and remains syntax-only, unexecuted code. The actual draft materializes both global and spatial round batches, adding6.144GB beyond the earlier staging estimate; cumulative maps add about336MB. Capacity planning must therefore allow roughly25.19GB before indexes, source/build targets and retained files. Commit-hook configuration applies Black, flake8 and strict mypy to Python research tools; the failed commit cannot be reduced to two whitespace fixes without inspecting remaining failures. Frozen source remains unchanged and staged; no hook bypass or commit retry occurs.


## Run 406: Dynamic global construction takes implementation priority

Lyon prioritizes implementation of one globally evolving tree with dynamic ownership, as specified in DYNAMIC-GLOBAL-TREE-DESIGN.md. Begin with ordinary one-machine construction and move existing branches at a configurable capacity threshold; preserve topology and global insertion/split/merge/neighbor-eligibility/reassignment rules across owners. Placement, routing-cache byte budget and reader publication are independent. The existing worker receives isolated dynamic implementation ownership; continuation retains the matched2M runner, controller, state and commit repair. No new agent bypasses the lifetime limit. Existing diagnostic and matched data remain the evaluation path, but static32owner construction is no longer the primary implementation objective. Quality-phase success requires either sharded approach to match unsharded clustering on matched data plus assessed navigation/recall-work, or an evidenced hard ceiling; infrastructure blockage and failed tuning are not ceilings. Once quality is controlled, optimize query latency/data loaded. Extra construction communication is acceptable. No cloud scope or budget is extended.


## Run 407: Placement boundary reviewed; evaluation supervisor drafted

Independent review accepts the first dynamic-tree slice as local placement-parity work only: the reference writer remains centralized while an independent ownership directory moves existing branches. The worker owns one new writer module, one new test target and the module export. This cannot establish remote state execution or distributed quality; global neighbor eligibility/reassignment dispatch remains required afterward. In parallel, continuation adds a draft Linux task supervisor for the evaluation runner with an absolute task deadline, sampled process-RSS/disk/log limits, owned-process termination and retained failure receipts. It is syntax-checked only and does not replace independent machine shutdown guards. No Linux/cloud task or real-data experiment executes.


## Run 408: Local placement accepted; actual owner scan integration assigned

Independent review accepts two local placement tests: branch ownership changes preserve tree bits/routes and old saved-reader rerank results while reference construction continues centrally. This is explicitly a local simulation, not distributed mutation, transfer savings or clustering parity. The next worker slice is actual owner-dispatched neighbor eligibility scanning with cross-owner candidate parity against the reference. The comparison runner now has a syntax-checked spec builder binding final saved snapshots and common query/reference artifacts to the accepted diagnostic; integrated execution/validation remains pending. No cloud or real-data experiment runs.


## Run 409: Owner scans accepted; live split integration assigned

Independent review accepts owner-thread eligibility scans on stable snapshots:14requests across5owner threads return the same ordered candidates as the single-owner control, including3eligible vectors on3owners. Default live split callbacks still execute centrally, and actor vectors are copied from the stable writer rather than fetched remotely. A corrected payload counter and its failed predecessor remain preserved. The next assignment integrates actual split callbacks with mutation visibility, global destinations and saved current-membership validation. In parallel, diagnostic spec preparation now checks each saved cumulative map against its projected hash and proves complete unique coverage of all corpus global IDs, not just queried GT. That runner change is syntax-checked only. No cloud or real-data results are added.


## Run 410: Saved-map rejection checks pass

The matched runner passes a focused synthetic saved-map check: complete disjoint owner mappings are accepted, while duplicated or incomplete global coverage rejects before a diagnostic specification is emitted. This is not native diagnostic or real-data validation. The dynamic worker reports a passing first actual live-split pair with a cross-owner reassignment and later version visibility; the completed frozen package and independent review are still pending, so no acceptance or quality claim is added. No cloud work occurs.


## Run 411: Live split dispatch accepted; authoritative mutation targeted

Independent review accepts actual live-split predicate dispatch in FP and quantized modes. Two owner threads execute eligibility work; immediate global reassignment advances a version observed by a later stale-copy scan. Exact instrumented events/tree and saved10-ID membership census match the controlled reference, and old5-ID publication remains intact. Authoritative mutable postings, versions and structural mutation are still central, so this is not distributed construction. V3 quantized positive-eligibility failure and V4 repair remain preserved; random cause is not reconstructed. Next worker assignment targets a coherent authoritative-owner mutation boundary and post-move mutation, avoiding another narrow predicate demonstration. No real-data quality or performance result follows.


## Run 412: Evaluation stage entry points integrated

The evaluation runner now exposes explicit Linux-only preparation, build and diagnostic stages from a frozen execution specification, with binary-policy checks and no automatic machine actions. All modules parse. It remains an unadmitted draft: preparation needs the external task supervisor, full source/package bindings and integrated validation are unfinished, and the current builder supports global/static comparison rather than the pending authoritative dynamic treatment. Dynamic worker continues storage/writer-boundary design. No repeated tests, cloud calls or new measured result occurs.


## Run 413: Supervised stage launcher prepared for consolidated review

A Linux-only stage launcher now binds the runner file set and execution specification, checks the unchanged deadline and existing timer, and supervises preparation as one child. Build/diagnostic stages replace the launcher so their native-process supervisors retain direct child ownership. Exact machine timer/deadline and independent local watchdog remain external admission requirements. The consolidated runner is sent for static review after the priority owner-authority review, with local projection/map tests separated from unexecuted Linux stages. Worker reports a passing96-to98row authoritative-owner operation sequence; completed independent acceptance is pending. No cloud action occurs.


## Run 414: Authoritative-owner repair ready for consolidated review

Authoritative-owner V4 passes two local tests and the benchmark check. Owner actors hold mutable postings and current versions, transfer branch and current-record payloads directly, and fetch vectors through durable references. Independent review identifies two admission corrections in the prior package: require full-precision navigation and consistently enforce the aggregate physical-row bound. V4 reports both repairs; consolidated acceptance is requested through the existing parent. Automatic split/merge and parent edits remain unsupported, so this is fixed-topology correctness evidence rather than a complete evolving distributed writer or a quality result. Earlier compile failures and the V3 positive epoch remain preserved. No cloud action occurs.


## Run 415: Owner authority accepted; consolidated runner repair underway

Independent review accepts fixed-topology authoritative owners, including repaired admission checks, saved-state parity and the unchanged staged index. The existing worker receives the next coherent integration assignment: combine owner authority with evolving split and reassignment semantics. The matched runner review finds missing derived-input bindings, inconsistent budgets, incomplete output limits and implicit construction settings. The pre-repair runner is preserved. Local repairs now freeze prepared files into a separately admitted manifest, verify it before and after downstream stages, and require matching admission budgets. Focused checks reject changed input bytes and mismatched thread budgets. Full implementation-source closure, native configuration validation, aggregate output limits and failure receipts remain unfinished; no dry integration or cloud execution is admitted. Disk staging requires about25.206GB before indexes/build/retained data, including both vector projections, cumulative maps and labels.


## Run 416: Build evidence and limits repaired locally

The parent-owned runner repair adds an aggregate build-log limit, saved construction/source/boundary checks, cumulative saved-map verification and retained root/block identities. Early build admission failures now leave a stage failure receipt; a local nonexecuting check verifies rejection of an unbound binary retains that receipt. All modules parse. This does not establish process-stop behavior, native integration, full implementation/compiler binding or real-data correctness. Those remain required before consolidated review and execution admission. Two exploratory source lookups used nonexistent filenames and returned no source; the actual native entry point was then located without changing checkout files. No cloud or native workload executes.


## Run 417: Implementation provenance required by runner

The runner now requires all imported preparation-package Python files plus a separately bound source manifest, compiler binary/version, native binary and build command. It verifies those files before and after a stage and rejects a different checkout or binary. The exact Linux provenance has not been instantiated, and this validates receipts rather than proving reproducible compilation. Three existing projection/map tests pass and all modules parse; the repaired package hashes include the launcher and new binding module. The worker reports a passing101-row owner-authoritative split sequence, but its frozen package and independent acceptance are still pending. No native experiment or cloud action occurs.


## Run 418: Repaired runner submitted as one package

The repaired runner package is submitted for one consolidated review after the priority evolving-owner review. Supervisor admission and launch failures now retain a receipt, and completed-child logs/deadlines are checked after exit as well as during polling. A local expired-deadline check rejects before launching any child and retains the failure. Actual termination and native integration remain untested. The worker reports a new discard test proving rejected private structural work restores the prior authoritative state; final independent review remains in progress. No cloud action occurs.


## Run 419: Bounded evolving authority accepted

Independent review accepts bounded owner-authoritative leaf splitting and immediate global reassignment on the101-row witness, including saved-state parity and preservation of old96-row readers. It does not accept general construction: internal splits, merges and multilevel parent edits remain unsupported; whole-owner backups and local actors remain scalability limitations. The existing worker is assigned coherent multilevel construction integration. Runner review leaves a concrete saved-build provenance gap; the repair now binds successful job receipts, final saved manifests/maps and root identities into an externally admitted build artifact manifest, checked before and after diagnosis against construction/boundary settings. A focused local check rejects a changed saved snapshot. No native/cloud experiment or2Mquality result occurs.


## Run 420: Commit hook runtime incompatibility identified

Read-only hook checks on isolated copies of196 staged Python files identify a concrete commit blocker: pinned Black23.3 runs under Python3.14.4 and fails on removed ast.Str for all196files. Flake8 separately reports genuine style violations, with the complete4.7MB log retained rather than flooding chat. The actual index and worker files remain untouched. A compatible hook runtime and a reviewed formatting/lint repair are required; hooks are not bypassed and no commit is made. Final runner build-output binding repair is sent to the existing reviewer, while the worker continues multilevel construction analysis. No cloud action occurs.


## Run 421: Compatible formatter verified without changing checkout

Pinned Black23.3 succeeds under the already installed Python3.12.13 on196 isolated staged-file copies. All196 parsed syntax trees remain identical after formatting. Remaining lint findings total138:60 unused imports,7 unused locals,46 combined imports,23 assigned lambdas and2 naming/spacing findings. No actual checkout/index edits, commit or hook bypass occurs. Independent review closes the runner saved-build binding gap conditionally on admitting the exact completed build artifact receipt; Linux provenance, native integration and dynamic CLI remain prerequisites. Worker reports implementation of internal splitting/root growth/merging with storage batches separated from balance boundaries; tests and acceptance remain pending.


## Run 422: Strict type-check scope identified

The isolated strict type-check exposes a second commit blocker beyond formatting: default module discovery assigns two names to the same research module and stops. An exploratory explicit-package-bases run preserves strict checking and reports6869 diagnostics in195files. These are unadjudicated diagnostics from an isolated staged copy, which excludes other repository dependencies; they are not6869 proven runtime defects. Logs and code counts are retained. The real hook configuration, staged index and working files remain unchanged. Commit repair is substantially broader than formatting and must not be represented as complete or bypassed. Dynamic multilevel work remains active; no cloud action occurs.


## Run 423: Multilevel package frozen; matched entry point scoped

The worker freezes a multilevel construction package covering225 final rows and preserving a192-row reader. Reported operations include45 internal splits,6 root growths,3 contractions,38 merges and7 placements; independent acceptance remains pending. The API separates storage chunks from balance cuts, removes per-add whole-state backups and retains reference100-round stops, which occurred twice in the fixture. Capacity values are arithmetic only; per-owner limits do not bound total RSS. Construction remains local owner threads with global saved roots, so the immediate matched executable can assess grouping quality but cannot establish cross-host latency. The parent requests coordination of the minimal dynamic native build entry point after acceptance, rather than another disconnected test milestone. No cloud action occurs.


## Run 424: Multilevel authority accepted; native paired build assigned

Independent review accepts local multilevel FP owner-authoritative construction, including splits, merges, root growth/contraction and saved parity. It preserves two100-round nonconvergence stops and distinguishes controlled test split decisions from independent quality builds. The existing worker receives isolated native paired-build CLI ownership; both reference and dynamic arms must explicitly use FP neighbor analysis because the profile CLI default differs. The parent records a dynamic treatment contract starting one owner with common balance cuts and no static partition. Direct canonical input slices may avoid unnecessary projected copies, but no storage saving is credited before implementation. No2Mquality, remote latency or cloud execution result follows.


## Run 425: Native paired construction accepted

Independent review accepts the construct-global entry point and strict saved-reader bridge. Both98-row local native builds use explicit FP settings and identical balance cuts; authoritative construction performs3 real local placement moves. Saved complete membership succeeds, and three full-ID exact-rerank queries match after reopening. Independent stochastic trees differ in physical census, so this is not ANN recall or equal topology. Parent preparation now has a syntax-checked global path that computes only the shared exact reference and binds original corpus/query files, without fitting static clusters or writing projected vector copies. It is not wired or executed yet. Linux/2M/768D/IP/resource evidence remains pending.


## Run 426: Paired native runner adapter validates retained outputs

The parent runner now has a draft sequential paired construct-global adapter using one common native specification and separate treatment outputs. It checks consumed-spec/input identity, common balance cuts, complete unique census, saved reader roots/configuration and freezes both output closures. Validation against both retained CLI outputs passes without rerunning native code. The first validation incorrectly compared the detailed root report with the compact reader root descriptor; it rejected and was corrected to compare exact id/kind identity. Launcher activation, quality diagnostic adaptation and consolidated integration remain unfinished. No cloud action occurs.


## Run 427: Native quality-diagnostic mapping gap identified

Global preparation and paired construction are wired into the Linux-only launcher with the same externally bound source and budget checks; no stage executes. Source inspection finds a real compatibility gap: the accepted GT diagnostic requires an explicit local/global map, while the native construct saved manifest uses implicit dense identity. The existing native worker is assigned a streamed saved identity-map bridge and an actual known-GT diagnostic integration test. This adds16MB per2M-row arm and must preserve the already reported identity digest. Parent will not rewrite an admitted saved manifest downstream. No cloud action occurs.


## Run 428: Saved diagnostic bridge accepted; global stages connected

Independent review accepts the original-manifest identity-map bridge and actual known-GT diagnostic on both fresh native fixtures. All21 expected distinct GT IDs are covered and independent exhaustive leaf/row oracle checks agree. Parent now wires paired diagnosis to admitted build outputs and the common reference, retaining source/input/config/root checks and explicit saved-map bytes. Both retained98-row snapshots generate valid-shaped diagnostic specifications without rerunning native tests. The complete global runner draft is packaged for consolidated review; controlled integration and Linux/resource admission remain pending. No2Mquality or speed claim follows.


## Run 429: Common cohort and construction provenance bound

Connected-runner review identifies two real gaps: equal build arms could disagree with the preparation shape/metric, and saved output closure omitted construction source/binary provenance. Repairs now require one prepared cohort descriptor across construction/diagnosis and retain the exact construction-stage specification plus source/compiler/binary descriptors. Diagnosis requires the same admitted provenance, and preparation forwards explicit rows/dimension for small integration. A local check accepts a matched cohort and rejects a changed dimension; all modules parse. Global input/map staging arithmetic is12,597,573,632bytes if source objects and canonical data are retained, before both built stores, targets and other retained files; this is not capacity admission. No child/native/cloud execution occurs.


## Run 430: Connected source accepted; fresh sandbox readiness read

Independent review closes both connected-runner source binding gaps. The next useful validation is a small connected Linux prepare/construct/diagnose test, not another source-only review. Fresh Cops-backed STS confirms sandbox079819354890 and fresh EC2 reads show all four experiment instances stopped. The existing m6i.2xlarge driver remains attached to vol-07951d3573187a716. These read-only observations start no resources and admit no window. Exact build/dependency provenance, execution package, current rate, disk capacity and new absolute guards remain required before running.


## Run 431: Small connected Linux fixture prepared

The small connected integration uses128 generated768-dimensional FP32 vectors and16 deterministic queries with exact top100. A file-only generator produces393216 vector bytes, explicit shared FP settings,13-row storage chunks and32-row balance cuts; it writes no corpus projection and launches nothing. A temporary local check verifies extent/query count. This fixture tests the actual dimensional/metric path and runner connections, not real-data quality. Exact Linux build provenance, stage admissions, resource checks and fresh absolute guards remain to be instantiated before running.


## Run 432: Bounded Linux build package drafted

A fresh AWS pricing read confirms the existing driver rate at$0.384/hour. The new integration proposal caps the entire machine window at15minutes/$0.096 compute, with a$0.10 compute budget and no extension; retained EBS cost remains separate. A Linux-only build draft verifies pinned source/compiler/cargo, rejects ambient compiler overrides, uses offline locked Cargo and captures the exact compiler-emitted executable plus provenance. It is syntax-checked only. Exact setup/controller review, current capacity, independently fixed guards and instantiated stage admissions remain incomplete; no machine starts.


## Run 433: Connected Linux integration package assembled

The new integration epoch contains a separate15-minute lifecycle controller, frozen Linux build script, deterministic fixture generator and connected stage driver. The controller retains sandbox/tag/volume checks and establishes a new local watchdog before any start; no controller executes. The driver binds freshly produced preparation/build descriptors before invoking downstream stages and retains process exits. Source parses, but Linux execution and terminal-child behavior are untested. The complete package is sent for independent review; concrete remote manifests, capacity and fixed remote guard still require admission.


## Run 434: Setup bindings and success conditions repaired

Setup review catches self-authorized current runner hashes and completion based only on stage exit codes. Repairs require externally pinned setup/runner manifests before imports and preserve expected module hashes in admissions. Final success now requires128unique currentIDs per arm, all16queries covering100GT IDs, matching32/64/96/128balance cuts, bound map/root identities and actual authoritative moves with multiple owners. Code is syntax-checked only. Cargo build flag/target overrides are also rejected. Remote manifest/config/dependency/capacity instantiation and guard verification remain required; no launch occurs.


## Run 435: Reviewed integration source frozen for upload

Independent review closes both setup source findings. A local upload bundle now binds all934 native source files, reviewed setup scripts and runner modules to absolute remote paths; every archived native file matches its accepted hash. The bundle is2,212,209bytes and remains local. Linux build, connected128-row integration and matched2Mquality remain unexecuted. Fresh cloud/resource checks, remote build configuration/dependency inventory, capacity and independent absolute guards remain required.


## Run 436: Current driver verified and remote inventory prepared

Fresh Cops-backed authentication confirms sandbox079819354890. The retained raw instance response confirms the exact m6i.2xlarge driver is stopped with its expected volume and task tag. A read-only Linux inventory script is prepared to collect disk, memory, compiler/Cargo identities, effective configuration locations, Python/NumPy availability, processes and timers before build admission. It is syntax-checked but unexecuted. No machine starts, upload occurs or deadline is consumed.


## Run 437: Linux archive completeness failure; driver stopped

The guarded Linux build fails after 5.51 seconds because the frozen archive omits rust/types/idl, a repository link to the protocol definitions. Local native builds used the full checkout, so matching source hashes did not prove archive completeness. The driver is API-confirmed stopped and its watchdog is retired afterward. No diagnostic runs. The existing worker is assigned extracted-archive build-input closure repair. Never reuse this completed launch epoch.


## Run 438: Corrected archive integrated into separate retry package

The worker supplies a corrected source archive containing the same 934 regular files plus the required relative protocol-directory link. Extracted-layout checks cover protocol paths, local dependencies and embedded SQL inputs; they do not replace the Linux build. The parent prepares fleet/global-runner-integration-v2 with unchanged reviewed setup/runner bytes, the corrected source and the known existing NumPy environment. The failed first epoch remains immutable. A fresh guarded window is still required, and the reviewer will assess the repair together with the next actual build result.


## Run 439: Linux build and connected synthetic integration complete

The corrected frozen archive builds successfully on Linux in 183.793 seconds. The connected 128-by-768 IP fixture completes shared exact-reference preparation, both native constructions and saved GT diagnosis. Both arms contain 128 unique current IDs and cover all 100 GT IDs for each of 16 queries at common 32/64/96/128 balance cuts; the dynamic arm records one move across two owners. The first integration attempt omits binary_bytes and stops before native construction; it is retained separately from the successful corrected specification. The exact binary is retained and hash-verified. The driver is API-confirmed stopped and its watchdog is retired afterward. Independent consolidated review is requested. Two top-level provenance manifests were not copied before shutdown; their absence is disclosed for review, and no restart is permitted solely to fetch them. These are pipeline correctness results, not representative real-data quality, ANN recall or latency results.


## Run 440: Independent integration acceptance and exact manifest reconstruction

Independent consolidated review accepts the corrected archive and completed Linux synthetic integration, with explicit limits on external build dependencies, configuration inventory and sampled memory. The parent reconstructs the missing 210734-byte implementation artifact manifest from retained bundle/provenance inputs; its SHA256 exactly matches the runtime-bound 6c9de211b74dde2d549b7689c30b253cd27ba32587fcab68b407bffbf4a1e5a7. Reconstruction is labeled separately from remote retrieval and does not supply missing compiler/dependency bytes. No cloud action occurs. The next representative experiment remains a matched 2M global-reference versus dynamically owned global-tree comparison, with no static initial partition and no latency claim from oracle membership alone.


## Run 441: Matched dynamic comparison and capacity question specified

The next comparison design now uses the accepted global reference and dynamically owned global tree on the same 2M-vector prefix, with common 100K balance cuts and 8192-row storage chunks. An eight-owner, 250K-row placement threshold is a candidate pending source-based capacity assessment by the existing worker. Sixteen held-out queries provide an exploratory paired clustering diagnosis; complete GT membership is a validity gate rather than evidence of quality parity. Source objects, canonical input and two maps total 12,597,573,632 bytes before both saved indexes, temporary state and reserves. The retained Linux binary can be reused without another build if source stays fixed. No cloud action occurs.


## Run 442: Placement headroom and concrete resource constraints

Source feasibility establishes a 12.288GB simultaneous raw-coordinate scenario for reference saving, before maps, postings, caches and transient allocations. Ten local owners at a 250K soft threshold replace the saturated eight-owner candidate, providing 20% aggregate unused capacity at 2M; this does not guarantee packing or divide process memory across machines. The source-plus-canonical/maps subtotal, two raw-vector saved payloads and a10GBreserve total34,885,573,632bytes before additional overhead, exceeding the recorded driver free space. The current driver is not admitted for the matched2Mrun. A64GiB-class candidate with adequate disk is a planning choice pending actual capacity/rate/budget, not a launch or measured resource bound. No source optimization or cloud action occurs.


## Run 443: Priced resource candidate and canonical input implementation assigned

Fresh AWS type and pricing reads identify r6i.2xlarge as a64GiB/8vCPU x86_64 candidate at$0.504/hour. Fifteen and20minute compute scenarios are$0.126 and$0.168; neither is a completion estimate or launch admission. Additional experiment storage and its retained-cost budget remain unresolved. The existing worker is assigned the missing Linux-only canonical input materializer, with full version-pinned object verification before slicing and small fake-source tests. No resources are started or modified.


## Run 444: Canonical materializer ready and storage cost explicit

The canonical materializer is complete with10fake-source tests and preserved failed attempts; real S3/Linux behavior remains unexecuted. Current gp3 storage pricing is$0.08/GB-month. A concrete proposal uses the existing stopped driver resized to64GiB RAM plus a separate64GiB experiment volume ($5.12/month retained), with a20minute/$0.168 compute scenario. This is review material, not provisioning approval or a completion forecast. Exact query/configuration binding and changed-resource lifecycle guards remain necessary before a run.


## Run 445: Restore accepted; exact query cohort and full configuration frozen

Independent review accepts the canonical restore and connected comparison source. The parent freezes the existing16test-labelled768Dquery artifact byte-for-byte, preserving embedding provenance and acknowledging that these queries have appeared in earlier research. Every common native configuration field is explicit, including beam1.5/10/16, RNG0/1,32reassignmentneighbors and FP/FP-NPA. Exact2MGT remains to be computed. The4096leaf diagnostic cap applies to each savedglobaltree, not to each placement actor; a cap failure must remain inconclusive. New-resource lifecycle admission remains pending; no cloud action occurs.


## Run 446: Restore-to-runner binding drafted

The real-data execution package now includes a draft input-binding step that verifies the complete restore receipt against runtime provenance, exact accepted plan/materializer/spec hashes, sandbox account, canonical payload hash/extent, frozen query bytes and unchanged deadline/reserve before emitting common native and preparation specifications. It writes files only and is syntax-checked, not independently accepted or executed. This closes a concrete wiring gap between restore and the already reviewed runner; lifecycle/provisioning and consolidated execution validation remain unfinished. No cloud action occurs.


## Run 447: Restore binding checks tested and tightened

Six focused restore-to-runner tests pass using small local metadata and a mocked6.144GBcanonical read. They cover changed payload bytes, mismatched deadlines, wrong producer identity, wrong canonical extent, receipt disagreement and successful specification emission. Preparation catches two additional gaps: canonical descriptor length must equal the accepted extent, and the deadline must remain live before and after verification. The previous draft is retained. These tests do not validate real-data transfer or replace the actual Linux runner evidence. New-resource lifecycle and one consolidated completed execution-package review remain pending; no cloud action occurs.


## Run 448: New-resource lifecycle isolated and validation assigned

A separate matched2M lifecycle draft now rejects wrong machine type and requires the exact original boot volume plus one64GiB encryptedgp3 experiment volume with matching task/epoch tags and attachment. Its new1200second deadline and local watchdog precede start; it does not create, resize or format resources. The existing worker is assigned mocked lifecycle ordering/rejection tests. The explicit setup sequence keeps new-volume identity/empty-device checks before formatting and retains all results before early stop. This package remains unexecuted and pending consolidated review/new-resource admission.


## Run 449: Startup-controller findings repaired; mocked tests pass

The mocked startup tests find four controller gaps: importing executed operations, the returned volume identity was not checked, duplicate mappings collapsed into a set, and the expected boot volume was not bound to the root device. The parent repairs all four, preserves the failing source/tests, and reruns the same assertions with separate result paths. All14mocked tests pass without AWS, processes or watchdog execution. New-resource lifecycle source still awaits consolidated independent review; no resource is provisioned or started.


## Run 450: Matched execution package completed for review

The matched-run driver now connects the reviewed stages to restored2Minputs and checks complete saved membership plus16x100GTcoverage and actual placement. Generated common/input specifications are frozen into an externally admitted manifest. A read-only mount checker requires the exact64GiBwhole-device EBSserial, ext4mount, empty filesystem and free-space allowance; it never formats. Six binding tests still pass and new scripts parse. The complete local package is submitted for consolidated independent review; realvolume/provisioning/format/start remain unexecuted.


## Run 451: Real query wiring repaired; bounded collection drafted

The independent execution review identifies an absent diagnostic query path and synthetic query IDs in the real-data runner. Both are repaired against the frozen ordered16query cohort; five focused tests pass. A metadata-only collector draft uses explicit admitted paths, an8MiBper-file/64MiBtotal bound and a deadline; three tests pass, including exclusion of stores, path escape, oversized files and expiry. Its path list and pre-stop integration still require review. No cloud action occurs, and resource approval remains outstanding.


## Run 452: Query repairs accepted; evidence closure expanded

Independent review closes both real-query integration findings. The collector now permits only the two exact original store manifests and the exact prepared ground-truth JSONL as data-directory exceptions. Its explicit list includes prepared/build closure, native specs and supervision results. It emits archive size/hash and collector/spec bindings; the64MiB bound is explicitly a payload limit. Five query tests and four collector tests pass with retained terminal logs. Restore/resource provenance paths and success/failure pre-stop invocation remain unfinished; no cloud action occurs.


## Run 453: Collection connected to matched-driver completion

A file-only execution wrapper now runs the matched driver and invokes bounded metadata collection after success, nonzero exit or a caught launch exception. It verifies admitted source/spec descriptors and requires120seconds between work and collection deadlines plus60seconds before the unchanged machine stop. A real small subprocess test covers success, exit7 and rejected reserve; terminal logs are retained. It does not yet cover restore failures before invocation or local archive retrieval, and it relies on existing stage supervisors plus independent machine shutdown for hung/detached work. No cloud action occurs.


## Run 454: Restore-failure collection exercised

The wrapper can run an admitted restore command with the same work deadline and collect metadata after a failed restore while skipping matched execution. Real small subprocess checks cover restore exit9, matched success/exit7 and reserve rejection; logs are retained. This closes failure-path control flow only. Successful restoration still needs generated binding/admission orchestration, exact metadata closure and archive retrieval before this becomes a complete experiment. No cloud action occurs.


## Run 455: Approved budget and experiment-first direction recorded

Lyon approves up to $1,000 cumulative SPANN research spending from the October16 side-chat approval onward, including the proposed machine and volume. Track compute and storage commitments; existing scope exclusions, fresh identity/resource checks and independent absolute shutdown guards remain mandatory. The old20minute proposal is not a runtime requirement. Freeze infrastructure scope, finish one consolidated review, fix only comparison-invalidating/execution-blocking/spending-limit defects, and run the matched2M comparison. Use existing runtime/transfer tools; defer recovery polish and further metadata-only review cycles. An interrupted run may be inconclusive. The completed worker package is submitted through the parent for one consolidated review. The recurring prompt is updated to prevent stale budget blockers. No cloud action occurs.


## Run 456: Consolidated acceptance and bounded execution handoff

The independent reviewer accepts the orchestration and retrieval package. Fresh STS verifies the sandbox account; the exact driver is stopped with its original type and boot volume. A two-hour window reserves $1.10 compute plus $5.12 for one month of new storage. Lifecycle and orchestration limits now agree at7200seconds. The old fixture failed its1200second expectation; the separate updated fixture also initially retained a mismatched watch argument, then all14checks pass after aligning it. These failures remain in tool history; rerunning the old test overwrote its attempt2 result file. Original accepted test evidence remains in the review/package. The parent is assigned sole cloud execution to prevent competing controllers. No cloud mutation occurs here.


## Run 457: Directly authorized upload succeeds

Normal automatic review accepts the exact package upload from the chat containing Lyon’s direct authorization. Remote size/hash match259466bytes and the admitted package. The same sandbox driver runs under fresh local and remote fixed16:56:18UTCshutdown guards; the existing experiment volume is remounted without formatting. The identified closed-attempt watchdog is retired after replacement readiness. Execution ownership returns to the parent/operator for extraction, bindings and the matched2M run. No scientific result exists yet.


## Human steering: Stop large benchmark and diagnose speed

OVERRIDING HUMAN STEERING: Stop the active benchmark through the sole operator, retain small logs/results, stop sandbox compute and verify stopped. No large relaunch or deadline extension. Diagnose BOTH the14.9minute reference build and60+minute dynamic build with VERY SMALL bounded profiled tests. Lyon reports his unsharded2M baseline takes under2minutes; comparison requires matching configuration and workload before attribution. No cause is yet measured conclusively. Stop request delivered and recurring instructions updated.


## Run 458: Tiny disabled-trace attribution reconciled

The tiny disabled-trace comparison completes and the driver is API-confirmed stopped. Baseline balancing takes 4.615442603 seconds with 56,244 evaluation events; lazy balancing takes 2.384279837 seconds with 40,616 events. These independently randomized workloads do not establish a causal 1.94× speedup. A separate fixed-decision test proves exactly 706 fewer owner commands with unchanged saved data and enabled traces. Lazy profiling records 88,619 provider hits and zero storage-origin block decodes; its cumulative vector-get span is 0.231522 seconds. The next source-selected target is the per-physical-row current-version lookup used to prepare a discarded before-scrub trace. Only a scoped count and timing span is proposed; no implementation, run, or large relaunch follows from this entry.

The first five-cell diagnostic retains its output-limit failure despite five passing native membership censuses. The later two-arm harness passes with metadata and saved-store bytes charged separately. Covered compute upper bounds total $1.035335109413146966 across the matched attempt and two diagnostics; the new volume retains a separate $5.12 monthly liability. These figures cover the listed fleet receipts, not the whole AWS account. Detailed evidence and the next-target hypothesis are in `dynamic-review/disabled-trace-attribution-v1/ACTUAL-RESULT-AND-NEXT-TARGET.json`.


## Run 459: Before-scrub trace cost measured

The single-arm before-scrub diagnostic passes on 8,192 retained vectors, and the driver is API-confirmed stopped. Trace preparation takes 0.402423264 seconds and semantic scrub takes 0.003076128 seconds inside 2.527743228 seconds of balancing. These disjoint elapsed spans include actor waits; they are not pure CPU fractions. Preparing the trace sends 23,244 owner presence queries across 18 posting-list reads. Saved membership remains complete and unique. A minimal local fix batches consecutive IDs at the same owner while preserving validation order and enabled trace output. For this fixed workload, the source predicts 23,226 fewer commands; no speedup is measured. No new cloud run follows automatically.

The latest compute upper bound adds $0.05291875427722931 once. Covered matched and three diagnostic epochs total $1.088253863690376276; retained new storage remains a separate $5.12/month liability. This is not whole-account billing. Source, local correctness, and the next package remain in `dynamic-review/lazy-before-scrub-v1`.


## Run460: Lazy before-scrub validation passes

The lazy before-scrub run passes with 8,192 unique live IDs and no duplicate or unreachable entries, and the driver is API-confirmed stopped at 18:41:32 UTC. Ordered validation checks 21,277 physical rows in 14 batches with zero individual Reference requests. Trace preparation is 2.858995 ms and semantic scrub is 2.542725 ms; total balance is 2.029748385 s. The independent randomized tree differs from the earlier run, so these timings do not establish a causal ratio. The fixed-decision local test remains the 392-command elimination proof. The next task targets historical vector-reader churn at explicit construction phases, rather than another trace optimization.

The compute upper bound adds $0.06183592409133911 once. Covered compute totals $1.150089787781715386; retained new storage remains $5.12/month, separate from whole-account billing.


## Run 461: Current construction view removes historical reader churn

The current-view fixed probe passes on 16,384 identical accesses over 32 appended roots plus genesis; both saved censuses contain 8,192 unique live IDs with no duplicate or unreachable entries. Historical/current reader opens are 66/1 and cache clears are 4/0. Successful encoded returns are 35,004,698/3,249,140 bytes, with 17/2 storage-origin decodes. Historical probe time is 405.541059 ms; current probe plus publication is 382.974138 ms, including 0.094848 ms publication. This is one warm single-leaf pair, not whole-build speedup or physical I/O. Independently stochastic balancing remains descriptive. The driver is API-confirmed stopped at 19:10:40 UTC. The next local milestone introduces explicit capacity reservation, add, balance and reshard boundaries; it does not automatically authorize another run.

The compute upper bound adds $0.05050483047008514 once. Covered compute totals $1.200594618251800526; retained new storage remains a separate $5.12/month liability, not whole-account billing.


## Local construction phases and split preparation

Explicit reservation, add, publication, balance and reshard boundaries pass local correctness review. Actual owner registration overlap increases from one to two spans in a multi-owner test; these spans include waits. Capacity placement occurs only at completed phase boundaries. Structural balance remains serial. The next accepted local change overlaps immutable input loading inside one current split, preserving the fixed reference decisions and saved state across 620 cross-owner reassignments. Two tests, a native build and three synthetic entry checks pass; actual fetch spans increase from one to two. The independent small warm constructions attribute roughly 10–15.5% of balance elapsed to preparation, without a speedup or CPU claim. The worker-job counter is exact for the intended one/two-worker comparison but can overcount actual chunks above two.

The next measurement partitions serial balance into nonoverlapping categories before selecting another optimization. Both local milestones remain unmeasured on Linux; no cloud launch or additional spend occurs. Covered compute remains $1.200594618251800526, with retained storage separately $5.12/month. Accepted packages and reviews are retained under `dynamic-review/phased-construction-v1` and `dynamic-review/parallel-split-preparation-v1`.


## Balance elapsed attribution and reference parallel construction

The single 8,192-row run passes full saved membership census and stops at 20:20:29 UTC. Balance wall is 2.869721823 seconds; the disjoint buckets sum to 2.869722338 seconds, exactly wall plus 515 nanoseconds of observation-boundary overhead. Eligibility, scrub and trace context accounts for 75.95% of elapsed time, rerouting and registration 10.42%, clustering 7.63%, structural installation 3.05%, and split input 2.93%. These categories include actor waits and measurement overhead; they are not CPU fractions. The depth-one tree has seven leaves, six splits, 48,445 evaluations and 4,613 destination decisions. The saved census contains 8,192 current and 429 stale physical rows, with zero duplicate or unreachable entries.

The result does not support treating input preparation as the main balance opportunity. The next implementation adapts the existing reference parallel insertion routing and subtree balancing scheduler, including its missing-node, orphan and concurrent-change handling, to owner authority. Independent parallel schedules need completed invariant and reader checks, not exact equality with one serial tree. No additional profiler cycle or cloud run is authorized by this conclusion.

This epoch adds $0.052251215634346 once. Covered compute becomes $1.252845833886146526; retained storage remains separately $5.12/month. This is covered-epoch accounting, not whole-account billing. Exact raw bindings and counter scopes are retained in `dynamic-review/balance-cost-breakdown-v1/ACTUAL-RESULT-AND-DECISION.json`.


## Reference parallel owner pair: transfer blocked before execution

The accepted paired reference/owner attempt is upload-review-blocked and API-confirmed stopped at 21:06:25 UTC. Automatic review rejected the research package transfer before execution because payload/destination authorization was not established. Uploaded bytes, builds and native arms are all zero. The source package and its local acceptance remain exact; no Linux performance or shape result exists. No transfer retry or alternate is attempted. The exact compute upper bound is $0.012958042573928833, counted once; covered compute becomes $1.265803876460075359. Retained storage remains separately $5.12/month, and this is covered-epoch accounting rather than an account invoice. Exact final receipt: `fleet/reference-parallel-owner-runtime-v1/final-receipt.json`; rejection: `fleet/reference-parallel-owner-runtime-v1/upload-review-rejection.json`.


## Reference parallel owner paired result

Epoch two passes both full 8,192-ID saved censuses and exact full-ID distance-bit equality, then stops at 21:29:46 UTC. Reference add/balance sums are 0.032557193/0.417727490 seconds; owner sums are 0.378285056/16.136954997 seconds, with 0.508668492 seconds placement. Independent stochastic work prevents a causal ratio. The owner tree has depth five, 102 nodes, 58 leaves, 65 splits and eight merges, 18 subtree jobs, four overlapping subtree contexts, three owner callbacks, 68 moves and four populated owners. Its critical hold sum is 1.390784870 seconds and wait sum 0.854585794 seconds; NPA context sum 30.998979351 seconds overlaps threads and nested work. All pending balance and mutable central mirrors are zero. The exact epoch cost $0.057208317575454706 is added once; covered compute is $1.323012194035530065, with retained storage separately $5.12/month. The blocked first attempt remains separate. Next work uses fixed-decision local attribution to reduce avoidable per-row owner round trips without changing current-version checks or immediate reassignment. Exactreceipt: `fleet/reference-parallel-owner-runtime-v1/epoch2/final-receipt.json`.


## One-owner versus four-owner attribution

The three-arm 8,192-vector diagnostic passes all saved censuses and exact full-ID distance checks, then stops at 22:11:21 UTC. Reference/one-owner/four-owner child wall times are 0.664814/20.891878/13.832646 seconds; user plus system CPU is 0.82954/18.803441/14.213740 seconds. One owner performs zero placement moves; four owners perform 51 moves. Both have multiple scheduler contexts. Balance current-version lookups total 1,385,752 in the reference, 1,516,613 with one owner and 1,019,439 with four owners. Final current-message service sums are only 0.400757305/0.271428529 seconds, versus caller sums 22.600063898/15.895037287 seconds. Those sums overlap across callers and include different phase scopes; they are not an additive wall partition. System CPU is 0.161740/12.722657/9.490795 seconds, and voluntary switches are 4,987/2,954,528/2,176,934. Splits and merges differ (69/18, 70/20, 59/10), so no causal ratio or exact total-gap attribution follows. The measured next target is per-row current-version local messaging, preserving immediate version visibility and single owner authority. No network cost is measured. Exact epoch cost $0.05581402708530426 is counted once; covered compute becomes $1.378826221120834325 with retained storage separately $5.12/month. Exactreceipt: `fleet/owner-count-attribution-v1/final-receipt.json`.


## Same-process current-read ablation

The one-owner same-process ablation passes both full 8,192-ID saved censuses and exact full-ID distance-bit parity, with zero placement moves, then stops at 22:41:15 UTC. Message/direct balance sums are 11.961642108/1.453448882 seconds. Child wall is 12.433061826/1.917347431 seconds, system CPU 7.752493/1.089927 seconds, and voluntary switches 1,782,259/223,382. Current callbacks fall from 937,972 to zero; the direct path performs 1,166,786 synchronized reads. It performs more balance version work (1,166,786 versus 937,972) and predicates (96,267 versus 86,966), while splits differ (53 versus 54). This materially supports local dispatch overhead, but independent schedules prevent a fixed-work causal ratio or exact total-gap attribution. The pointer is a same-process ablation, not a physical distributed solution. The next local target batches scrub validation against actual owner authority while preserving transfer visibility and exact swap/code order. Exact cost $0.055097255339622495 is added once; covered compute becomes $1.433923476460456820, and retained storage remains separately $5.12/month. Receipt: `fleet/owner-current-runtime-v1/final-receipt.json`.


## Grouped scrub validation result

The four-owner grouped-scrub pair passes both full 8,192-ID saved censuses and exact full-ID distance parity, then stops at 23:11:13 UTC. Legacy/batched balance is 17.307908218/3.058215242 seconds; child wall is 18.443179293/4.171147780 seconds. System CPU is 12.515775/3.165621 seconds and voluntary switches are 2,805,092/668,000. The batched arm validates 958,772 requested physical rows using 17,952 owner callbacks, avoiding 940,820 individual dispatches counterfactually within that same arm. Its balance current callbacks are 182,835: 17,952 grouped calls plus 164,883 remaining live individual lookups. Final current callbacks also include placement (205,196 versus legacy 1,332,858). Logical requested ID payload is 3,835,088 bytes and returned version-value payload has a 958,772-byte upper bound; neither includes framing or measures network traffic. Splits/merges differ (63/14 versus 65/18), and predicates remain about 105,000, so no causal timing ratio follows. Both arms have actual migrations and multiple scheduler contexts. Next work consolidates accepted scrub and immutable-predicate batching into one supported tiny construction/quality path while keeping NPA versions live. Exact cost $0.0546585156583786 is added once; covered compute becomes $1.488581992118835420, with retained storage separately $5.12/month. Receipt: `fleet/owner-scrub-runtime-v1/final-receipt.json`.


## Consolidated owner saved-locality result

The ordinary parallel reference and combined owner construction pass both full 8,192-ID censuses and exact full-ID FP distance-bit checks, then stop at 23:51:49 UTC. Exact top 100 is recomputed for the frozen 16 queries over only the 8,192-vector prefix; GT union contains 1,341 IDs and both original saved manifests/maps remain exact through diagnosis. Mean 95-hit coverage is 21.3125/21.125 leaves and 3,231.125/3,245.375 live rows. Owner uses fewer live rows on nine queries and more on seven, with deltas −819..+576; at 100 hits the row means are 4,153.8125/4,193.25 and the split is 8/8. These mixed paired differences establish neither locality equivalence nor a quality gain. Reference/owner trees have 104/95 nodes, 55/53 leaves and depth five; owner has 60 placement moves and four scheduler contexts. Construction child wall is 0.664788873/4.219639078seconds and balance 0.422138991/3.093062854seconds; independent schedules/work prevent causal ratios. Setup is 0.263586578seconds, including 0.118286323 for the prefix copy and 0.068686641 for FP64 GT; the scientific pipeline is 5.348865389seconds. The 25,165,824-byte evaluation copy is separately charged. Each arm writes 65,536 map bytes; nativeverify/controllerprepost/diagnostic read 65,536/131,072/65,536 bytes separately. Reused test queries have unproven corpus disjointness. One global saved snapshot is not physical actor fanout; leaf/weighted minima are locality oracles, not ANN recall or executed navigation work. Next decision is a tiny existing-reader recall/work diagnostic on these same saved snapshots, subject to exposing untruncated scoring count; no automatic run or scale increase. Exact compute cost $0.05999297303199768 is added once; covered compute becomes $1.548574965150833100, retained storage separately $5.12/month. Receipt: `fleet/consolidated-owner-quality-runtime-v1/final-receipt.json`.


## Saved-query recall and raw work result

The four saved-reader query cells pass on the same retained 8,192-vector snapshots and sixteen reused queries, then stop at 00:12:00 UTC. Reference/owner mean recall@100 is 0.4425/0.495625 with beam 8 and 0.82625/0.846875 with beam 32. Mean scored current posting rows are 1,273.8125/1,333.4375 and 4,918/5,149.5625 respectively; distinct-ID counts match those row counts. Every query reranks 200 exact candidates and returns 100. Owner recall is higher on ten queries at each setting, but lower on five/four and tied on one/two. Warm native medians are 0.8623525/0.8396765 ms and 1.293091/1.3548785 ms; one retained pass is descriptive, not a latency study. All four retained cells observe 256 vector-provider obtains/hits, zero vector storage callbacks/encoded returns/decodes; this is warm vector-cache evidence, not whole-query or physical I/O. The saved GT oracle says the best possible 32 leaves could cover 99.8125%/99.5625% of GT on average, but actual selected-leaf IDs and pre-rerank candidate IDs are absent, so the remaining recall gap cannot be assigned to navigation or candidate trimming. The next material decision is a bounded GT stage-coverage trace at these same settings, rather than more beam tuning or a larger run. Original manifests/maps/GT remain unchanged; the global saved reader does not measure actor fanout. Reused-cohort/disjointness and FP32-versus-FP64 tie limits remain. Exact compute cost $0.0514655516242981 is added once; covered compute becomes $1.600040516775131200, with retained storage separately $5.12/month. Receipt: `fleet/saved-query-quality-v1/final-receipt.json`.


## Saved-query stage coverage result

The bounded stage trace passes and the driver stops at 00:30:45 UTC. All 64 retained queries reproduce the prior ordered result IDs, FP32 distance bits and work counts exactly. Mean losses per 100 GT neighbors are navigation/trim/exact-return 55.6875/0.0625/0 for reference beam 8, 50.25/0.1875/0 for owner beam 8, 13.5/3.875/0 for reference beam 32 and 11/4.3125/0 for owner beam 32. Selected-leaf GT equals pretrim GT for every query, and all subset/completeness checks pass. Most observed missing neighbors are absent from the selected leaves; the trace does not yet distinguish centroid quantization from beam/tree effects. Source inspection establishes one-bit centroid scoring (Code<1>) despite the navigate_4bit name/comment. The next material decision is an opt-in FP-centroid navigation comparison at the same fixed leaf budgets, keeping downstream scoring/reranking unchanged and separately charging FP-centroid reads/cache/evaluations; no implementation or run follows automatically. Trace copies average about 0.43–1.24 microseconds and external GT joins about 0.31–1.00 milliseconds per retained query, but those spans omit other trace/serialization costs and are not a prior-latency comparison. GT join setup is 0.009835905 seconds; controller query stages take 0.919821388 seconds. FP32/FP64 tie limits, reused-cohort/disjointness uncertainty, tiny shape and global-root-not-actor-fanout limits remain. Exact compute cost $0.050343494033813475 is added once; covered compute becomes $1.650384010808944675, retained storage separately $5.12/month. Receipt: `fleet/saved-query-stage-coverage-v1/final-receipt.json`.


## FP navigation diagnostic assigned — 2026-10-17T00:54:47.825673+00:00

The accepted FP-centroid package is assigned to the sole operator for a fresh bounded comparison on the same saved trees, sixteen queries and exact prefix GT. Four reader children have ten seconds each and forty seconds total. Source and package remain frozen. Actual cloud state awaits receipts under `fleet/fp-navigation-quality-v1`; assignment does not establish a running instance. The comparison preserves the same per-level beam policy and reports actual leaves, centroid evaluations and posting work. The FP preload and cache costs are charged separately. The one-bit control will be checked against prior result IDs, distance bits and unchanged work fields. Exact compute cost is added once only after the stopped receipt; no additional operator, source change or experiment starts here.


## FP navigation result and stopped cost — 2026-10-17T01:01:54.575469+00:00

The four-cell FP navigation comparison passes on the same saved trees and sixteen reused queries, and the driver stops at 2026-10-17T01:01:09.595492+00:00. Mean recall@100 rises from 0.82625 to 0.879375 on the reference tree and from 0.846875 to 0.881875 on the owner tree. FP improves fourteen queries on each tree, worsens zero/one and ties two/one. Mean selected-leaf GT rises from 86.5 to 93.25 and from 89 to 93.4375; trim losses rise from 3.875 to 5.3125 and from 4.3125 to 5.25 per100 GT, with zero observed final-return loss. All policies actually select32 leaves and evaluate103/94 centroids here; mean posting rows fall slightly from4918 to4889.0625 and5149.5625 to5032.9375. The same per-level beam policy does not guarantee equal total leaves on other trees. All32 retained one-bit controls reproduce prior ordered IDs, FP32 bits and unchanged work fields; all64 raw warm-up/retained final results and selected-leaf lists match. Internal candidate sets match while unordered iteration can change their order. FP preload owns319488/291840 centroid bytes and takes1.878455/1.814396ms; each reader observes one1627642-byte encoded return and one decode. Root opening, provider/cache memory and physical I/O remain separate. This supports one-bit centroid scoring as a contributor on this cohort, not a general quality or latency claim. The next consequential design decision is a higher-fidelity bounded centroid scorer against this FP coverage target, with explicit bytes/evaluations; no larger run, sweep or implementation follows automatically. Scientific result review accepts the recomputed summaries. Exact compute cost $0.053140024027824403 is added once; covered compute becomes $1.703524034836769078, with retained storage separately $5.12/month.


## Four-bit navigation diagnostic assigned — 2026-10-17T01:14:39.560789+00:00

The accepted Code4 package is assigned to the sole operator for a fresh bounded comparison on the same saved8192 rows, sixteen queries and exact prefix GT. Six reader children have ten seconds each and sixty seconds shared, with one-bit and FP controls alongside the four-bit candidate. The same per-level beam policy remains; actual selected leaves and evaluation work are reported. Source and package remain frozen. Actual cloud state awaits receipts under `fleet/four-bit-navigation-quality-v1`. Preparation from persisted FP, retained code bytes, temporary payload and per-distance unpacking remain separate. Numeric control outputs will be checked against prior results; elapsed timing differs because validation overhead changes. Exact compute cost is added once only after the terminal stopped receipt. No additional operator, source change, sweep or larger experiment starts here.


## Four-bit centroid result and stopped cost — 2026-10-17T01:23:15.832876+00:00

The six-cell Code4 comparison passes on the same saved trees and sixteen reused queries, and the driver stops at 2026-10-17T01:22:27.386373+00:00. Mean recall@100 is0.87125/0.879375 for reference/owner Code4, versus0.82625/0.846875 one-bit and0.879375/0.881875 FP. Code4 improves fourteen queries and worsens one against one-bit on each tree. Against FP it improves/worsens/ties3/8/5 reference queries and4/5/7 owner queries, so it is not equivalent. Mean selected-leaf GT is92.25/93.375; trim losses are5.125/5.4375 per100 GT and final-return losses are zero here. All policies actually select32 leaves and expand every49/42 internal node, evaluating103/94 nonroot centroids; deeper pruning remains untested. Code4 mean posting rows are4867.4375/5062.3125. Both controls reproduce all64 retained IDs, bits and work counts, and all128 raw final results and selected-leaf lists; internal candidate sets match with unordered iteration differences. Additional retained centroid payload is41184/37620 code bytes versus319488/291840 FP bytes, a7.76-fold reduction in that payload only. Complete FP preparation remains charged:4.518937/4.283756ms, accounted temporary payload364140/332928 bytes excluding quantizer scratch/RSS, and6144 logical unpack bytes per distance evaluation. Each FP source load returns1627642 encoded bytes and decodes once; original FP objects and provider buffers remain. Warm-up is not cache matching: Code4 reference retains46 vector callbacks/74874476 encoded bytes/46 decodes, while one-bit owner retains27/43948062/27 and the other four cells have zero. No latency or query-I/O saving is claimed. The result supports the Code4 payload/leaf-ranking tradeoff within this reused cohort, not generalization, actor fanout or scale. The next consequential quality question is remaining one-bit posting-score candidate trimming at the fixed200 cutoff; an exact-scoring control on fixed selected leaves is a proposal only, not an automatic run or sweep. Scientific review accepts the recomputed results. Exact compute cost $0.06278358142852783 is added once; covered compute becomes $1.766307616265296908, retained storage separately $5.12/month.


## Cache diagnosis and attribution preparation — 2026-10-17T01:38:56.187418+00:00

The accepted cache diagnosis now has a completed local runtime-glue package. It reuses the exact Code4 Linux binary64eb40542e20100663c4625ff2cbdcf2eb3487d19e87ff8de5b1e0382a468a0e without rebuilding. Four ten-second cells compare64 versus one shard at identical capacity targets for reference four-bit and owner one-bit, with forty seconds shared. Query IDs, FP32 bits, numerical work, selected leaves and candidate sets are pinned to prior outputs. Root partitioning changes with block partitioning and is disclosed. Three local fake-process test methods pass; a nonreproducing baseline completes as inconclusive with no retries. The local collision test establishes a capacity-fragmentation mechanism, while historical hash buckets and eviction reasons remain unknown. No default behavior changes, cloud action or new cost occur. Focused glue review is pending; exact-posting quality control remains deferred.


## Cache-policy replay assigned — 2026-10-17T01:44:39.776481+00:00

The accepted same-capacity cache-policy package is assigned to the sole operator for one fresh window of at most900seconds/$0.15. It reuses the existing exact Linux binary without rebuilding. Four reader children have ten seconds each and forty seconds shared, comparing64 versus one shard on reference four-bit and owner one-bit. All numerical decisions and work remain pinned. Source and glue stay frozen. A baseline with zero retained callbacks is inconclusive and is not retried; no sweep follows. Actual cloud state awaits receipts under `fleet/vector-cache-shards-runtime-v1`. Callback, encoded-byte and decode changes will be reported without historical-cause, latency or RSS claims. Exact compute cost is added once only after the stopped receipt.


## Cache-policy replay inconclusive and stopped cost — 2026-10-17T01:52:17.707994+00:00

The cache-policy controller completes and the driver stops at 2026-10-17T01:51:37.864689+00:00, but both attributions are inconclusive under the prespecified rule. Reference four-bit and owner one-bit each have zero retained storage callbacks in their fresh64-shard baseline and one-shard cell. Every cell observes256 provider obtains/hits and zero misses, encoded returns and decodes across sixteen retained queries. All128 raw decision digests and policy echoes pass; ordered result IDs, FP32 bits, numerical work and selected-leaf decisions remain fixed. The existing binary is reused without a build, and the controller takes1.061130684 seconds. The local collision mechanism remains demonstrated, but this run does not reproduce or explain the historical46/27 fetches, establish a runtime fix, or support a latency/RSS/physical-I/O claim. Independent result review accepts the inconclusive classification. The cache branch stops with no retry, seed search or default change; future comparisons should keep cache policy and traffic explicit. A meaningful future compressed-navigation gate would exercise actual internal frontier pruning rather than full internal expansion, not another cache/bit/beam sweep or an exact-posting run merely restating known trim loss. That is a parent decision, not a new admission. Exact compute cost $0.0560436847114563 is added once; covered compute becomes $1.822351300976753208, with retained storage separately $5.12/month.


## Relayed criteria and next structural proposal — 2026-10-17T02:01:30.870781+00:00

The next proposal is16,384 retained rows, not an authorized launch. It uses the same vectors/order,768D inner product, four add/balance threads, branching4/split256/merge64, explicit FP construction, common4096-row balance cuts independent of8192-row storage chunks, and reference/dynamic1/dynamic4 local arms. It is the first and only larger prefix candidate. Current occupancy suggests roughly106/110 leaves, but no retained matched intermediate tree guarantees pruning. Every arm must actually discard internal candidates at per-level beam32 in at least80/100 fixed requests and visit fewer than all internals; otherwise stop as structurally insufficient without autosizing or retry. The proposed100 retained queries are larger but reused, and disjointness is not proven without a common-ID exclusion check. Samebeam does not establish matched recall; unequal-recall results cannot rank latency/bytes efficiency. The cache-capacity question stays separate, with one explicit capacity policy and adverse traffic retained. Current8192 source/trace caps need later bounded local changes; no implementation is performed to make this proposal appear launch-ready. Full decision criteria and unknowns are in `dynamic-review/next-matched-pruning-proposal-v1/proposal.json`.

- Latest quality treatment: four local owners on one machine, initially one owner then redistribution; no physical distributed claim.
- Tiny reused cohort, independent trees, unequal scans, all internal nodes expanded and about60% corpus scanned do not establish equivalence.
- Build ratio narrowing with scale is a hypothesis; pair phase times with work counts.
- Investigate cache variability and generation visibility; distinguish correctness from inefficiency; preserve adverse evidence.
- Next larger experiment is a proposal only: smallest fixture exercising internal branch pruning, cache-capacity crossing considered separately.
- Match vectors/order/metric/threads/thresholds/branching/construction search/cadence/precision/hardware; record seeds and scheduler variability.
- Consider reference, dynamic1 and dynamic4 arms; report all build phases and semantic work, memory and concurrency.
- Use fixed disjoint queries if available; compare work/bytes/latency at matched recall. No automatic size/tuning sweep.
- Advance due to structural limits, not presumed parity or amortization. Existing upload, sandbox, spending, identity, shutdown and scale restrictions remain.

No source/input/test/cloud action occurs. Fixed-cost amortization remains a hypothesis against the measured per-vector coordination risks; current cache and generation evidence stays separate from correctness claims.


The next structural proposal is independently reviewed as a proposal only. Its unchanged query policy uses a per-level minimum and maximum beam of32 with tau2.0, as the accepted native path specifies; there is no final leaf cap. The immutable proposal and POLICY-CLARIFICATION.json must be read together. The sole16,384-row candidate still requires actual internal pruning in at least80 of100 requests in every arm, with no automatic resizing or retry. No implementation, scale or launch authorization follows from the review.


The authorized local measurement slice adds bounded per-level internal-versus-leaf cutoff counters through the existing stage trace. Two deterministic tests pass and the native entry passes a compile check; focused review is pending. The fixture proves internal pruning under test beam one, leaf-only rejection and no-pruning controls, with exact off/on ID and distance-bit parity. All8192-row and cache caps remain unchanged. The accepted policy addendum uses tau2.0 with per-level beam32. The16K proposal still requires human scale authorization; no larger input, remote package, experiment or cloud action is created.


The internal-pruning counter milestone is locally accepted by the focused reviewer. Two tests and the native compile check pass; the941-file archive and staged index remain verified. The trace distinguishes actual internal-branch rejection from leaf rejection and marks overflow incomplete. Future runtime glue must validate scalar counters separately from the new optional navigation_level_trace object; frozen runtime harnesses remain unchanged. The16K proposal remains pending human authorization. This local work adds no cloud cost or scale admission.


The current reusable query-work validator closes the local consumer gap without editing frozen harnesses. Five tests pass, including32 retained responses and strict malformed/incomplete counter cases. Future callers bind the saved-generation internal count, validate scalar counters separately from optional level records, and require complete trace evidence before counting actual internal pruning. The941-source epoch and staged index remain unchanged. Focused review is pending;16K scale approval remains pending, with no runtime package or cloud action.


The reusable navigation-work validator is independently accepted as a completed local prerequisite. Five tests pass, including32 retained responses. Future harness integration still binds the exact saved generation and applies the prespecified cohort threshold; frozen harnesses remain untouched. No source, index, runtime, package, input-size or spending change follows from this acceptance. The16K proposal remains pending human approval.


Lyon directly authorizes the16K comparison and tests of any size: "i give you authorization to run your 16k vectors test and any other test of any size. do you my permission for anything else" This supersedes numerical size ceilings and repeated scale-approval requirements. It does not establish completed execution or remove sandbox, protected-resource, cumulative$1000budget, shutdown, upload-review or no-corpus-to-Mac restrictions. The parent persists this authorization in the existing automation; no duplicate automation is created. Current work prepares one bounded16,384-row three-arm comparison using the accepted tau2.0 policy clarification and pruning validator. Cloud execution remains with the sole operator after the completed package review.


The authorized 16K package is ready for focused review. It contains the reference, one-owner and four-owner arms with matching configuration and four construction threads. One Rust capacity test, five Python methods, three generated 64-row native arms and six traced Code4 queries pass. The exact retained 100-query cohort is reused; known ID and vector overlaps are rejected, while disjointness remains unknown. The actual internal-pruning gate applies to every arm, with no automatic resizing or retry. Source epochs, the staged index and both preparation-tool failures are preserved. The package is not executed; the sole operator still supplies the Linux binary, external deadline, sandbox guard and resource evidence.


The authorized 16,384-row package is independently accepted for local source and runtime glue. One Rust capacity test, five Python methods, three generated 64-row native arms and six traced Code4 responses pass. The final deployment archive is bound by SHA-256 dbeb669ddb9ed37d7af0ccc24a2f70457f249447867d5221c618b42fef656d4f. The reviewer verifies all 941 source files, archive members and the unchanged staged index. No retained 16K or cloud execution occurs in this preparation. The sole operator still supplies the actual Linux binary, compiler configuration, deadline, resource guard and shutdown evidence. High-level read_exact chunk counts are not OS or physical I/O counts. Source, inputs and the reviewed package are now frozen for execution.


The parent assigns the accepted 16K package to the sole Linux pilot for one fresh 1,800-second window with a $0.30 compute ceiling. Receipts belong under fleet/matched-pruning-16k-v1. The worker has no start, build or scientific result receipt yet and makes no cloud-state assertion. The accepted source and deployment archive remain frozen. Spending stays unchanged until the terminal stopped receipt; the worker remains available for concrete fixes or result reconciliation without proactive source changes.


Lyon authorizes refining the research goal as subgoals change, finish or emerge. RESEARCH-GOAL.md now defines the five required deliverables and distinguishes experiment termination, research completion and genuine blockage. Current plan and state reference it. The active controller remains active with its existing objective because the exposed goal interface supports status changes but not objective edits; no completion or replacement is claimed. This documentation change adds no cloud cost or experiment.


The reviewed 16K comparison passes and the driver is stopped at 05:18:08 UTC. All three saved censuses contain 16,384 unique live vectors, exact distance bits agree, and all 300 retained query traces satisfy actual internal pruning. Recall is 0.8072/0.8040/0.8146 for reference/one-owner/four-owner; mean scored rows are 5299.09/5431.86/5266.20. Independent trees have unequal work, so these are neither equivalence nor causal timing results. Whole-child times are 1.266/5.472/7.476 seconds; balance alone is 0.715/4.558/5.344 seconds. One-owner balance has 392,835 current callbacks, of which 343,947 are kernel entries and 48,888 grouped scrub calls. Four-owner balance has 405,529, of which 358,736 are kernel entries and 46,793 grouped scrub calls; the larger 457,463 final count also includes placement. Inclusive caller sums exceed service time but overlap across workers. The next local change targets live-prefix version batching without stale caching or a shared-pointer distributed claim. The exact $0.0665382691335678compute bound is counted once; covered compute is $1.888889570110321008 and retained storage remains $5.12/month. Earlier 100B conditional-model acceptance does not complete this global-tree goal. The separate next-quality decision bounds fixed-leaf posting improvements and does not select a calibration sweep.


The next local construction change passes four focused tests and the native compile check. It batches at most 64 authoritative current-version reads, consumes rows in the existing order to the first reassignment, then releases locks and discards suffix observations before mutation. The fixed decision tape preserves evaluated IDs, ordered actions and owner code bytes while reducing current-read actor calls from 131 to 5. Transfer visibility and opposite-order lock progress pass. The generated 256-row integration reaches splits, merges, four-owner work and placement, then checks the full saved census, vector bits and pinned old reader. Requested rows exceed consumed rows, and this extra suffix work is retained. The policy is off by default; reassignment checks remain unchanged. One completed-change review is pending. This is local actor evidence, not a distributed performance claim or authorization to launch another run.


The accepted live-prefix algorithm now has minimal native and runtime wiring. One explicit boolean policy selects baseline or live-prefix validation while preserving the same 16K input, four-owner construction, configuration, cadence and actor reads. Three wiring methods and a generated 64-row two-arm native check pass, including full saved censuses, exact-distance agreement and rejection of an invalid policy before input access or output creation. The accepted four-test algorithm suite is unchanged and is not repeated. The frozen package retains suffix and predicate work, message counts, histograms, phase time, CPU, concurrency and integrity data. It proposes 30 seconds per arm, 60 shared, plus 10 seconds for prefix hashing; build is separately capped at 600 seconds. The proposed fresh 1,200-second, $0.20 resource window remains a parent decision. One narrow readiness review is pending; no cloud action occurs.


The existing reviewer accepts the live-prefix native policy and runtime glue without further fixes or tests. The final deployment archive is bound by SHA-256 1e156bbcf41f5df16b5318766db45ceee2f50b680f40842a4d9683c4fe57aa5e. The source archive contains 941 verified files and the index remains unchanged. This acceptance does not claim an executed runtime pair. The parent supplies actual resources, the fresh absolute deadline and shutdown guard; the sole operator builds and runs the accepted package. No additional spending is recorded before terminal evidence.


## Live-prefix runtime assignment — 2026-10-17T05:39:08.018802+00:00

The parent assigns the accepted live-prefix package to the sole Linux pilot for one fresh guarded 1,200-second window with a $0.20 compute ceiling. The pair uses the same 16,384 rows and four owners, with 30 seconds per arm and 60 shared; prefix hashing and build retain their separate bounds. Receipts belong under fleet/live-prefix-runtime-v1. Actual start, deadline, build and scientific results remain pending pilot evidence. Source and deployment stay frozen. Spend remains unchanged until a terminal stopped receipt.


## Reviewed live-prefix result — 2026-10-17T05:47:44.885933+00:00

The live-prefix pair passes full 16K census and exact-bit checks and is independently reviewed and stopped at 05:44:38 UTC. Current callbacks fall from 439,699 to 221,278; balance callbacks fall from 387,740 to 169,111. The policy requests 989,455 rows, consumes 224,191 and discards 765,264 suffix observations (77.34%). Predicate work rises from 244,327 to 989,455 rows. Independent schedules prevent a causal whole-build ratio. The opt-in remains available, and the next local target is bounded authoritative filtering of split-input rows under the existing structural lock. The exact $0.05342920998573303 compute bound is counted once; covered compute is $1.942318780096054038, with retained storage separately at $5.12/month. No new cloud run is assigned.


## Split-input owner filter ready for review — 2026-10-17T05:52:09.608807+00:00

The bounded split-input filter passes three focused tests, three wiring methods and a generated 64-row native pair. The fixed 135-row decision tape preserves ordered flags and owner code bytes across a later-ID mutation while reducing current actor callbacks from 133 to 4. A real structural guard, paused cross-owner transfer and opposite-order multi-ID requests complete safely. The generated 256-row construction exercises splits, merges, placement, full saved census, vector bits and an old pinned reader. Only split-input current work is tagged; the prior epoch cannot attribute all remaining calls to that site. Missing-to-zero and deleted-byte comparison remain unchanged. The default is off. Both proposed runtime arms retain the accepted live-prefix policy and charge its discarded suffix/predicate work. The frozen package awaits one completed readiness review; no cloud run or new spend occurs.


## Split-input package accepted — 2026-10-17T05:52:57.594127+00:00

The existing reviewer accepts the complete split-input owner filter, native flag and minimal runtime glue without fixes or retests. Review SHA-256 is fb48e27532ae91159e92911a1eeb854545af2cd8701f50be2098346b1c9edf8b. The deployment archive remains 73d421893699585a3e2987f69214bba76a95899f845f367ea0b705fede67e29e, with 941 source pins and the staged index preserved. Version-match rows precede embedding availability, and logical scalar calls can exceed actor calls for missing directory entries. The accepted package retains prefix overwork and independent-schedule limitations. Actual Linux binary, resources, absolute deadline and shutdown guard remain external prerequisites. No additional spend or runtime is claimed.


## Split-input build and conditional quality plan — 2026-10-17T05:58:16.262173+00:00

The sole pilot reports normal upload, exact source verification and active offline build for the accepted split-input pair. The fixed stop is 06:13:26 UTC; receipts belong under fleet/split-input-owner-filter-v1. No scientific or terminal result is received, so spending is unchanged. The frozen source/package is preserved. The next quality protocol remains planning only: retained three16K snapshots, a separately reserved32-calibration/256-evaluation document cohort, target95%, one-point matching margin and at most two FP navigation policies. Width36 remains below retained pure-internal frontiers39/49/49; actual pruning still gates every proposed arm. The protocol does not claim representative quality, sufficient power, matched recall or a launch.


## Split-input result reviewed; quality decision next — 2026-10-17T06:01:04.332762+00:00

The split-input pair passes full 16K membership and exact-bit checks and is independently reviewed and stopped at 05:59:46 UTC. The enabled arm performs 54,129 tagged version checks through 954 grouped calls, with zero tagged scalar reads; the independently scheduled baseline performs 46,035 scalar checks. Whole-child time is 5.223/5.373 seconds, with 118/145 splits and 19/41 merges, so no wall-time improvement or causal ratio is claimed. Total callbacks are 316,717/309,003; current callbacks are 188,323/164,911. Prefix predicate work rises from 819,184 to 970,086 and remains charged. The opt-in is retained, and construction micro-optimization stops here. The next planning-only quality protocol uses immutable snapshots and reserved calibration/evaluation queries; root chooses whether to implement it. The exact $0.05322768260955811 compute bound is counted once. Covered compute is $1.995546462705612148, with retained storage separately at $5.12/month. No active controller or new run is claimed.


## Reserved-cohort quality package ready for review — 2026-10-17T06:15:10.838983+00:00

The reserved-cohort quality package is implemented without native edits or a build. Four local methods pass, including real fake-reader subprocesses, calibration failure with no evaluation, reader-error preservation, policy-lock tampering, a tiny scalar ground-truth oracle and deterministic inference. The fixed protocol uses 32 calibration and 256 evaluation document queries, a 95% recall target, a one-point paired margin and actual pruning gates of 26 and 205 queries. Five 99% bootstrap intervals form an approximate simultaneous 95% conditional family; no population or between-build claim follows. A hash-bound policy choice precedes evaluation labels and reader launches. New ground truth joins complete pretrim/retained/returned IDs; old 100-query membership reports are unused. Original 16K snapshots predate live-prefix and split-filter construction. The exact existing binary and frozen package await one completed review; no runtime, new spend or automatic rebuild occurs.


## Reserved-cohort quality package accepted and assigned — 2026-10-17T06:20:59.782191+00:00

The existing reviewer accepts the repaired runtime-only quality package without further fixes or retests. Review SHA-256 is b75fd3c773815a79c2377dfb43eb3ccaf4bb61796bf986bc20395c9dbf07f071, and the accepted V2 deploy is c06f7af86a78a198f500611b1d879fa7778fdc290871234c4551407d35d4138e. V1 and its output-cap finding remain preserved. The sole pilot is assigned one fresh 900-second/$0.15 window, with no build, 240 scientific seconds and 180 seconds reserved for preservation. Actual start, deadline and scientific results remain pending pilot receipts. Exit code 2 means calibration misses the prespecified gate; it is collected without evaluation or retry. Extracted document vectors and all query/request payload files remain remote and are excluded from Mac retrieval. The old saved construction epoch, conditional inference and disjointness limits remain explicit. Source and package stay frozen; spending is unchanged until the terminal stopped receipt.


## Reserved-cohort negative and separate amendment — 2026-10-17T06:34:19.310741+00:00

The reserved-cohort run ends with a reviewed calibration failure and API-confirmed stop. All six cells prune on all 32 calibration queries. Beam 36 recall is 95.34375% for reference, 94.65625% for one owner and 95.03125% for four owners. The one-owner result is 11 returned hits short across 3,200 true-neighbor hits, with 10 lost in trimming; this does not establish a general ceiling or parity. Evaluation and bootstrap inference do not run, and document-vector payload stays remote. The exact $0.03912928483009338 compute bound is counted once; covered compute is $2.034675747535705528, with storage separately at $5.12/month. Root authorizes one separately declared beam-38 calibration for the one-owner tree, reusing passing beam-36 controls. The original negative remains immutable. No evaluation, target reduction, sweep or automatic run is inferred.

## Beam-38 amendment accepted locally — 2026-10-17T06:40:57.084774+00:00

The one-cell calibration amendment is locally tested and independently accepted. It reuses the verified reference and four-owner beam-36 results and runs only the one-owner beam-38 calibration cell. A new locked selection precedes any evaluation access; a calibration miss ends without evaluation or retry. One focused test method covers five cases, and unchanged scientific helpers are reused without repeating their suites. The deploy is 38,418 bytes with SHA-256 9f898e803895e6e04e551105174771284c0daae391625a911709201e0a9e56f0; review SHA-256 is ad9dc10ae2d6856cf36d715477557a935b644fd2e3db38a48b391697974cbb19. The proposed fresh window is at most 900 seconds and $0.15, with 150 scientific seconds and 180 seconds reserved for preservation. Actual runtime admission remains external. No cloud execution or new spend occurs; the original negative and reserved evaluation pool remain preserved.

## Beam-38 amendment assigned — 2026-10-17T06:42:44.868522+00:00

The sole pilot is assigned the accepted beam-38 calibration amendment in a fresh window of at most 900 seconds and $0.15, with 150 scientific seconds and 180 seconds reserved for preservation. Actual start, results and lifecycle evidence await pilot receipts. The package remains frozen, and spending remains unchanged until the stopped receipt. Lyon’s direct any-size authorization in SCALE-AUTHORIZATION.json supersedes numerical row ceilings, including the earlier 250M progression restriction. The $1,000 cumulative budget, sandbox, protected resources, excluded experiments, upload review and shutdown requirements remain in force.

## Scaling arithmetic and query-cost boundary — 2026-10-17T06:44:14.651175+00:00

The corrected 100B arithmetic is partial scaling evidence, not a throughput forecast or completion of the research goal. Node counts use the minimum internal-node count at maximum branching four, and depth 15 is a minimum possible maximum leaf depth rather than a measured skew bound. Direct implementation remains blocked by u32 IDs, centrally resident vectors and routing, global publication and local coordination. The query-cost proposal separately requires the terminal quality gate before describing costs as matched recall; physical I/O and network cost remain unmeasured.

## Beam-38 calibration negative reviewed and stopped — 2026-10-17T06:46:13.997842+00:00

The separately declared beam-38 calibration amendment fails the 95% target at 94.9375%, with 32/32 queries showing internal pruning. It reaches 3,048 true neighbors but trims ten and returns 3,038 of 3,200; the target requires 3,040. Evaluation and inference remain untouched. The independent review accepts science and stopped lifecycle. The exact $0.0156261203289032 compute bound is counted once; covered compute is $2.050301867864608728, with retained storage separately at $5.12/month. Retained traces omit approximate candidate scores, so the next read-only diagnosis distinguishes expected approximation from unproven cutoff ties or scoring defects; no beam sweep, new run or quality equivalence follows.

## Candidate-score diagnostic assigned — 2026-10-17T06:53:57.802949+00:00

The sole pilot is assigned the accepted calibration-only candidate score and cutoff diagnostic in a fresh window of at most 1,200 seconds and $0.20, including a 600-second build, 120 scientific seconds and 180 seconds reserved for preservation. Actual start and outcome await pilot receipts. Score copying and cutoff scans fall inside the selection timer, so elapsed samples support no latency claim. Evaluation, policy and frozen snapshots remain unchanged. Spending remains unchanged until the stopped receipt. The reviewed partition contract remains planning-only and identifies connected POC requirements rather than final scaling evidence.

## Candidate-score fidelity reviewed; posting policy decision next — 2026-10-17T06:58:43.799554+00:00

The reviewed candidate-score diagnostic passes and the driver is API-confirmed stopped. All 32 calibration decisions match the prior result. Ten excluded true neighbors across eight queries have zero approximate cutoff ties and exact ranks at most 100 within the kept-plus-excluded subset. This identifies approximation-driven exclusion, not a tie bug; it does not establish full-candidate exact recall. The next proposal reuses existing four-bit residual scoring at fixed navigation and 400 exact-rerank candidates, with preparation and larger code/unpack costs charged. It requires one consistent scoring policy across all three arms before any evaluation. No implementation or new run follows automatically. The exact $0.044338439869880675 compute bound is counted once; covered compute is $2.094640307734489403, with retained storage separately at $5.12/month.

## Four-bit posting package ready for review — 2026-10-17T07:11:42.425608+00:00

The common four-bit residual posting scorer is locally implemented and tested in a frozen package awaiting one focused review. All three old saved trees use fixed FP navigation beams 36/38/36, the existing exact trim to 400 and full-precision rerank to 100. Bounded leafwise sidecars bind original generation and live posting/centroid bits without changing saved files. Two native tests, an entry-point check and two Python methods pass; intermediate failures remain preserved. Only three passing calibration cells and a synced configuration/overlay lock permit reserved evaluation. No cloud execution or spending occurs. The independent old 100-query leaf oracle reports physical-row lower-bound means 4,449.31/4,487.18/4,372.06 for reference/one owner/four owners; this is a separate locality bound, not current calibration, equivalent quality or physical I/O.

## Four-bit posting package accepted and assigned — 2026-10-17T07:12:53.664425+00:00

The common four-bit residual posting package is independently accepted and assigned to the sole pilot in a fresh window of at most 1,200 seconds and $0.20, including a 600-second offline build, 180 scientific seconds and 180 seconds reserved for preservation. Each query exactly reranks 400 candidates and returns 100 results; earlier abbreviated 100-rerank wording means 100 returns. All three new calibration cells must pass before the complete configuration and immutable overlay hashes are synced and locked ahead of reserved evaluation. FP query residuals differ from the one-bit quantized query, and preparation warms caches; there is no bit-width-only or latency claim. The leaf-copy payload counter is not a full transient-memory peak. Source, defaults, original saved roots and the deploy remain frozen. Actual runtime and terminal receipts are pending, and spending is unchanged.

## Code4 controller failure reviewed; no-build repair pending — 2026-10-17T07:22:02.267841+00:00

The Code4 posting build succeeds, but the controller fails before calibration because it calls a helper name that the imported module does not export. The reviewed failure and API-confirmed stop remain preserved; no calibration or evaluation runs. The minimal controller repair calls the real snapshot-admission helper and exercises all three admissions through actual modules with metadata-only local fixtures, including a malformed-map rejection and adjacent export audit. Native source and the successful binary remain unchanged. The exact $0.060906431889534 compute bound is counted once; covered compute is $2.155546739624023403, with retained storage separately at $5.12/month. The repair awaits one review and external admission for any fresh no-build window.

## Code4 controller repair accepted locally — 2026-10-17T07:23:38.860069+00:00

The controller-only repair is independently accepted without further fixes, tests or a rebuild. It calls the actual snapshot-admission helper and audits all eight adjacent module references; the focused test executes three real-module admissions and malformed-map rejection. The exact completed binary is pinned for reuse. The proposed fresh no-build window is at most 900 seconds and $0.15, with unchanged 180 scientific seconds and 180 seconds reserved for preservation. Native source, policies, original failed package and reserved evaluation remain unchanged. Actual runtime admission is external, and spending is unchanged.

## Code4 repair assigned — 2026-10-17T07:23:46.193193+00:00

The sole pilot is assigned the accepted controller repair for a fresh no-build window of at most 900 seconds and $0.15, with 180 scientific seconds and 180 seconds reserved for preservation. Receipts belong to fleet/code4-posting-quality-repair-v2. Actual start and scientific outcome remain pending. Source, package, exact binary and old roots stay frozen. The original failed window and its counted cost remain separate; spending is unchanged until the new stopped receipt. No next policy follows before this scientific result.

## Code4 fixed inference recovery — 2026-10-17T07:38:14.466556+00:00

The Code4 reserved evaluation is consumed and the unchanged local inference is accepted. Mean recall is 94.30859375% for the reference tree, 94.7421875% for the tree constructed with one local owner, and 94.44140625% for the tree constructed with four local owners. Both paired 99% conditional intervals lie within ±1 percentage point, but every absolute mean misses 95%; the outcome remains target-not-met. All remaining losses are navigation coverage, with no trimming or exact-return losses. The original output-writing failure remains intact. The API-confirmed stopped window adds exactly $0.028037746958732606 once; covered compute is $2.183584486582756009 and retained storage remains separately $5.12/month. The next local package describes costs at these fixed observed policies; it does not revise quality criteria or provide a new evaluation. These readers query global saved trees, and their labels describe construction provenance.

## Fixed-policy cost package frozen — 2026-10-17T07:42:30.843257+00:00

The no-build fixed-policy query-cost package is frozen for a narrow repair review. Its admission and local fixture bind the exact 256 canonical document IDs retained by the accepted evaluation; numeric-only IDs are rejected. The persistent native readers receive the same explicit four-thread Tokio/Rayon and single-thread BLAS/OpenMP/MKL settings used by the accepted quality protocol. Two focused local methods cover real admission, a persistent fake child, inherited environment, and summary CLI serialization. Native source, existing binary, saved trees, overlays and failed V1 package remain unchanged. Runtime admission is pending, and no new spending is counted. The original 95% target remains unmet; this package describes cost only at the frozen observed quality.

## Fixed-policy cost package accepted — 2026-10-17T07:45:19.215771+00:00

The fixed-policy query-cost package is independently accepted with no further tests or fixes. The accepted repair binds the exact retained canonical document IDs, rejects numeric-only identities, and freezes the accepted thread environment. Deployment is pinned by SHA-256 ab70a4827a2557e64f7cd19e4050032df1ad5a05d0ea8051e56e733d3e3a142c. It reuses the existing binary without a rebuild. Runtime remains conditional on root and sole-operator admission for a fresh bounded window. The original failed quality target and consumed evaluation remain explicit; this cost-only protocol supplies no new quality evidence or distributed-execution claim. Spending remains unchanged.

## Cost window assigned — 2026-10-17T07:45:43.087440+00:00

The accepted fixed-policy cost package is assigned to the sole pilot for one fresh window of at most 900 seconds and $0.15, with no build, 120 scientific seconds, and 180 seconds reserved for preservation. Actual runtime receipts are pending and spending is unchanged. The local implementation task now connects bounded immutable vector storage to the existing parallel reference scheduler; it does not alter the frozen cost binary, saved inputs, or quality criteria.

## Fixed-policy cost complete — 2026-10-17T07:53:22.055104+00:00

The fixed-policy query-cost run completes and is independently accepted with API-confirmed stop. Mean native query elapsed is 4.45910 ms for the reference saved tree, 4.72750 ms for the tree constructed with one local owner, and 4.42549 ms for the tree constructed with four local owners; client means are 5.46796, 5.74069, and 5.44655 ms. This describes old saved global trees at frozen observed recall, with fixed arm order and warmed provider/OS caches, rather than active owner-distributed query execution or a causal speed claim. The original 95% target remains unmet. The exact $0.02137409257411957 compute bound is counted once, bringing covered compute to $2.204958579156875579; retained storage remains separately $5.12/month. The connected bounded vector implementation now proceeds locally without a new query run.

## Bounded owner-vector repair frozen — 2026-10-17T08:05:54.408987+00:00

The connected owner-vector repair is frozen for independent acceptance. Four focused local tests and the ordinary native compilation check pass. The missing-first-target retry now advances its local pending version without publishing current authority or an owner handle; the eventual posting and version match the ordinary reference-kernel path. Full saved current/code/vector checks and old-reader preservation pass for the bounded 1,024-row fixture. The access result remains adverse: the final no-provider-cache fixture performs 7,243 block decodes and returns 99,641,662 encoded bytes for 131,072 staged logical bytes, while retaining at most 8,192 cache-owned vector bytes across two owners. Provider metadata, caller arrays, mutation buffers and RSS are separate. Repeated decode is the next access-design question, not a performance gain. The existing 32-phase limit remains a prototype capacity barrier. No cloud action or new ledger charge follows.

## Connected bounded vector seam accepted — 2026-10-17T08:10:39.086052+00:00

The connected bounded owner-vector seam is locally accepted after the first-registration retry repair. Current authority remains absent until a successful first posting, while failed target retries preserve the reference version. The accepted source and adverse decode evidence remain frozen. The next local change tests same-root reader reuse within the existing 16-handle fetch against a fixed request tape, with explicit admitted decoded-pin byte accounting and an alternating-root control. It does not compare stochastic build speed, add a cloud run, or change spending.

## Bounded batch reuse accepted; access tuning complete — 2026-10-17T08:16:30.783941+00:00

The bounded fetch-batch reuse change is independently accepted for local correctness and fixed-request evidence. An identical contiguous 80-request tape reduces reader opens and decodes from 80 to 5 and encoded returns from 964,128 to 60,258 bytes. An alternating-root control retains 16 opens and decodes in both modes. Five focused tests and the ordinary compilation check pass. The comparison isolates reuse within an authored 16 KiB block layout; it does not measure a full-build speedup.

The reader-pin admission cap is 256 KiB per fetch context, checked after decoding. A 1 KiB negative observes 9,664 bytes before rejection and marks traffic telemetry incomplete, so this is not a maximum allocation or RSS bound. Root-index, provider-cache and caller allocations remain separate. Current-version authority, immutable-root resolution, saved census and old-reader proofs remain intact.

This completes the access-tuning cycle. The original 16K quality trees remain unchanged and the absolute 95% target remains unmet; the new construction epoch supplies no new quality or query-cost result. Root now owns conditional architecture/resource synthesis. No cloud action or ledger charge follows. Covered compute remains $2.204958579156875579, with retained storage separately $5.12 per month. The acceptance receipt is `dynamic-review/bounded-vector-batch-reuse-v1/independent-focused-review.json` (SHA-256 1637fad5a5a81e76091a1b30585b54f25a793952f1a8d1afa326bbdc8260a55f).



## Final independent acceptance

The scoped research assessment is independently accepted with an adverse conclusion. The local POC and measured cost mechanisms are established. The absolute 95% quality protocol fails; query costs are qualified by the prespecified ±1 percentage-point conditional recall tolerance. Current direct 100B execution is contradicted by implementation barriers, and future partitioned practical feasibility remains unestablished. See dynamic-review/FINAL-RESEARCH-ASSESSMENT.json and final-research-completion-acceptance.json. No further experiment is scheduled. Covered compute is $2.204958579156875579; retained new storage continues at $5.12/month. Compute is stopped according to retained API closure receipts; disks and evidence remain preserved.


## 100B design search resumed — 2026-10-17T17:14:25.738693+00:00

Lyon supplies the side-chat handoff and directs continued work toward a feasible design. The goal now requires a connected POC and supporting resource envelope; an adverse implementation assessment is a checkpoint. Informative million-vector tests are encouraged within existing authorization. Existing worker and sole operator own separate readiness assessments; no new agent, cloud window, benchmark or charge occurs. Earlier evidence and failed quality outcomes remain preserved.


## Retained 2M diagnostic admission corrected — 2026-10-17T17:16:00.802121+00:00

Source and manifest review rules out the proposed no-build FP diagnostic: snapshot row limits, centroid-cache bounds, candidate trace bounds and manifest schema are incompatible. The tree has 28 internal nodes, fewer than beam32; its population does not make it an internal-pruning experiment. Pilot prepares bounded reader/controller repair for leaf-selection economics. Worker implements chunked ingestion within logical cuts, with oversized splits explicitly unresolved. No remote run or charge occurs. Prior reviewer resume hits the agent thread limit; root will independently review worker/operator changes.


## Query observer implementation

Root implements a bounded ground-truth stage observer that retains at most100 supplied exact-neighbor IDs, with unchanged candidate selection and separate navigation counters. FP centroid preparation now accepts an explicit node and payload admission budget while preserving the existing defaults. Tests cover candidate cutoff ties above the old trace size, integrated search identity and rejected budgets; execution awaits the worker compilation slot. The28 internal-node count alone cannot prove absence of pruning because mixed leaf/internal frontiers compete; the diagnostic must inspect actual topology and counters. No remote execution occurs.


## Connected construction and query integration review

Root reviews logical-phase reservation, chunked registration, failed-writer publication exclusion and pre-detach split reservations. No blocker is found in these inspected paths; native smoke evidence remains pending. The isolated larger-query driver is registered. Controller review identifies effective-default, mixed-frontier leaf-count, exact-GT binding, process-deadline and physical-versus-distinct-row accounting corrections. Pilot owns repairs and integration tests. A new301-centroid regression must verify incomplete observation is explicit while full centroid validity remains checked. No cloud window or charge occurs.


## Native construction wiring verified

The259-row synthetic reference and bounded-owner native runs succeed with matching complete saved memberships and exact distance bits. Logical193/66-row cuts span64-row storage chunks; an incorrect prefix hash rejects before output creation. Both native targets pass compilation. The301-centroid reader regression passes, verifying full centroid validity with explicitly incomplete bounded access maps. These tests establish integration and failure behavior, not million-vector speed or100B capacity. Query package freeze and independent admission remain next; cloud spending is unchanged.


## Chunked construction independently accepted

Root verifies74 evidence bindings and all4 owned source files, then accepts the connected construction milestone. Native259-row parity and invalid-prefix rejection establish wiring and correctness; they do not establish large-input performance. Worker now owns configurable admitted staging grain and a fixed-input comparison of storage publication cost. Query package freezes the accepted source plus final reader/driver changes independently of subsequent construction edits. No new cloud charge occurs.


## Retained 2M query package accepted for guarded execution

Root verifies21 packaged files and accepts the exact deploye090262b9582887b6d583cd7ad2e813fc87416b390fa35c222d4f0fa4bcd3748 for one1500-second/$0.25 window. The operator must establish fresh independent local/remote shutdown guards and sandbox identity before execution. Build600seconds, science240seconds and preservation180seconds remain subordinate to the absolute deadline. Start and spending are not yet confirmed. FixedFPbeam32/rerank400 on16consumed text queries diagnoses ordinary-tree leaf/candidate economics; no quality parity or100Bclaim follows.


## Query execution authentication blocker; local construction continues

Fresh Cops identity verification fails because the SSO token expired and refresh failed. The operator receipt confirms no machine start, watchdog, upload or runtime execution. Spending remains unchanged. One `cops aws login` is required; no credential workaround is attempted. Local staging-grain implementation remains active. Its fixed-input tests expose snapshot failures in a single-leaf fixture and an oversized-posting fixture; their causes are not established. The worker preserves both failures and uses a seeded two-leaf, 513-row topology with logical cuts of 257 and 256 rows to isolate staging publication behavior without random split decisions. The focused test is live in session 53307. The current plan and state now reflect the actual pre-start blocker.


## Next construction dependency identified

Root source review identifies full publication assembly as the next connected construction change after staging-grain freeze. The owner adapter save path copies every leaf and every owner current-version map into a fresh writer and commits without a previous generation. The existing persistence layer already supports immutable-root forks and changed-key writes. The proposed seam must stream owner-local changes without whole-owner clones, preserve deletion and topology semantics, and retain dirty authority until every flush succeeds. Existing persistence clears dirty sets before flushing, so direct reuse without a publication transaction would lose retry information. The latest staged vector root must also advance independently of inherited posting roots. This is an implementation direction, not a completed mechanism or measured scale claim.


## Staging grain accepted; incremental publication implementation starts

Root accepts configured staging after source review, fixed-input evidence, native compilation and native smoke. The fixed513-row comparison performs9 versus3 durable vector publications; the259-row native wiring comparison performs6 versus2 and preserves complete census and exact vector distances. Invalid budget rejects before output. No performance ratio or total-memory bound follows. The worker now implements incremental publication after the pilot independent design review. The review requires dirty authority to survive flush failure, staged vector lineage to preserve published centroids, actor-authoritative version deltas and explicit empty-posting deletion rather than unloaded-shell skipping. No cloud execution or spending change occurs.


## Connected identity migration boundary identified

A read-only review identifies stable 64-bit vector identity plus partitioned owner/version authority as the next architectural boundary after publication. Widening an external ID map alone leaves central per-vector location and lock maps intact. The connected migration must cross posting serialization, vector handles, authority, movement messages, generation publication and query results. Existing blockstore keys lack unsigned64 support; immutable home-partition prefixes with checked local32 keys offer a reuse path, while postings require an explicitly versioned wide-ID format. A proposed collision test uses three IDs sharing the same low32 bits through movement and old/new query generations. This is planning evidence only; no wide-ID mechanism, physical execution or new spending is claimed.


## Incremental owner publication locally accepted

Root accepts the changed-data publication path after source review, nine existing integration tests, the focused multigeneration/failure test and native compilation. The final test verifies five generations, real structural deletion, intentional empty-posting removal, six failure boundaries and old-reader/object preservation. A centroid-only change copies no posting or version rows. Append and merge each copy all512posting rows in this fixture, so the result establishes mechanism and correctness rather than representative write reduction. Changed-key metadata and provider buffers remain separate unbounded costs. AWS authentication remains expired after one fresh read-only recheck; no resource action or spending change occurs.


## Identity implementation boundary selected

The worker proceeds in two connected stages. First, construction and publication call partition authority instead of coordinator per-vector owner and lock maps. Second, the same path carries wide identities through a distinct posting format and query results. Authority accepts wide IDs from the outset, while legacy callers temporarily occupy home partition zero. Immutable home identity is separate from physical placement; ownership epochs, membership versions and pinned query generations remain distinct. Partition-local state must have a paged or explicitly bounded working-set path. Stage one alone is not high-ID support or completion of the architecture requirement.


## Partition authority local integration and progress checks pass

Root verifies ten connected methods and four progress methods. The latter cover transfer visibility during immutable vector fetches, split-input and live-prefix reads, opposite lock orders and stripe collisions. Pending registration admission, ownership epochs and cancellation are exercised by the full-width authority module. The connected kernel still uses legacy32-bit identities, and local capped actor tables do not establish a paged or physically distributed authority service. The worker preserves this stage before continuing the required storage/query width migration. No cloud execution or spending change occurs.


## Connected wide-identity witness passes; final acceptance remains pending

The connected 96-row witness passes after the generic kernel migration. Three immutable home partitions contain distinct IDs with identical low 32 bits. Two local owners execute a 12-vector move followed by further splitting; two saved generations preserve complete vector, code and membership census and old-reader results. The selective query uses global navigation and selects one of eight leaves, scans 12 of 96 physical rows and reranks eight candidates. This establishes connected identity behavior, not representative throughput or physical distribution. Five posting-format tests also pass.

Root review requests poisoning of a partially failed placement phase before further mutation or publication, because movement removes source payload before fallible authority operations. Final regression and publication-failure checks remain pending. Reader candidate memory grows with selected live identities; the node admission check occurs after range materialization and does not bound provider allocation. These costs remain open for the scalable design. No cloud action or spending change occurs. Evidence is retained under dynamic-review/partition-authority-v1/test-wide2 and test-codec2.


## Next central dependency reviewed

The sole operator performs a read-only architecture review while authentication remains blocked. The remaining globally resident topology map serves both construction and query navigation; the wide reader loads every node and centroid before applying its admission cap. The next candidate replaces this dependency with owner-local node records and a bounded coordinator cache, while retaining the same global mixed-frontier ranking and neighbor eligibility. Live construction epochs and immutable query generations require distinct cache visibility rules. A decisive connected test must exceed cache capacity, force eviction through structural changes and ownership movement, and preserve old-generation results against the resident reference. Selected-candidate memory and provider allocation remain separate costs. This is a design recommendation, not implemented paging or remote evidence.


## Connected wide identity locally accepted

Root verifies all11 changed frozen/current source bindings and accepts the connected local mechanism. Thirty-two changed-path tests, two legacy replay tests, five codec tests and native compilation pass. The partial-move failure now poisons the writer before mutation and prevents subsequent save/add. Failed publication preserves dirty authority and prior generation; saved readers operate after live authority is destroyed. The missing-output-environment replay failure remains preserved. The accepted scope is full-width identity through construction, movement and selective saved queries; capped local authority, central routing, trusted snapshot descriptors and selected-candidate memory remain limitations. No physical distribution, representative performance or quality result is claimed. No cloud action or spending change occurs.


## Saved routing point reads pass forced-eviction checks

The shared ordered-wave traversal and saved-reader point-read path pass three focused tests, including mixed internal/leaf frontier ties, root-leaf distance, full-width identity and old-generation behavior. A two-entry512-byte cache records245evictions and288bytes peak accounted cache payload with zero fetch errors. These are logical clean-record payload counters, not RSS or physical I/O; caller wave records, score/frontier vectors and provider allocations are separate. Parent-plus-child wave retention now has a conservative accounted upper-bound counter. The initial helper-visibility compile failure and parity-test compile failure remain preserved under bounded-topology-access-v1. Final connected live implementation and consolidated review remain pending.

The next live cut moves full-precision centroids into durable owner node records behind bounded caches. Central mutation/scheduler metadata still contains node IDs, parent/child links and leaf lengths and must remain an explicit unfinished dependency. Publication locking must preserve the existing structural lock order, avoid read-to-write upgrades and pre-admit recursive mutation working sets before detach. No cloud action or spending change occurs.


## Live topology review identifies write and reservation costs before integration

The live node store is under implementation. Root review identifies a durable fork/flush for each record update; wiring ordinary leaf-length changes through this path would introduce per-vector publication overhead. The worker is instructed to preserve immediate visibility with bounded mutation state and batch structural publication, or explicitly isolate and count an initial structural-only baseline. Root also requests write-scope enforcement for epoch updates, a poison-state recheck after acquiring the write gate and accounting for simultaneously retained record/encoded copies. These findings are pending repairs, not accepted live paging evidence.

A single fresh Cops-managed STS check at18:23UTC still fails with an expired SSO token. The operator preserves fleet/reference-2m-selectivity-v1/authentication-recheck-20261017T182300Z.json and performs no resource action, retry or credential workaround. Local implementation continues; covered compute and recurring storage commitments are unchanged.


## Centroid-consumer audit extends connected live test coverage

A read-only audit identifies consumers beyond navigation that must load the current authoritative centroid when resident nodes contain metadata only. Publication and the full-tree comparison oracle directly copy centroid fields; split/merge extraction and both neighbor-eligibility modes also require rehydration. Registration must quantize against the same live topology binding as its destination leaf. The worker receives concrete code anchors and adds saved-centroid and posting-code consistency to the connected eviction/mutation witness. These are implementation requirements, not accepted live-storage results. No cloud action or spending change occurs.


## First connected live centroid-storage witness passes

The96-row live witness passes with empty FP centroid fields in kernel and owner leaf shells. It exercises seven splits, a12-vector move, later structural mutation, two incremental saved generations and old-reader consistency. A two-entry512-byte clean cache records416evictions and288bytes peak accounted payload;542durable point reads and11owner-root commits across nine structural scopes are logical mechanism counts. Registration alone commits zero topology roots. Modeled mutation reservations peak at336384bytes under1MiB and return to zero at quiescence. These are not total-memory, physical-I/O, throughput or physical-distribution results.

Structural writes now batch by owner at the outer mutation boundary. Nested reservations remain charged through outer flush, and pending-record admission occurs before insertion. Explicit live-versus-resident navigation parity, failure/progress checks and consolidated review remain pending. Evidence is bounded-topology-access-v1/test-live1.stdout; initial compile failures remain preserved. No cloud action or spending change occurs.


## Expanded live structural witness stops at reassignment assertion

The expanded live test fails before merge coverage because the chosen explicit reassignment does not advance the vector membership version. Calling normal reassignment on an already-preferred leaf does not guarantee movement; that fixture explanation remains a hypothesis until the worker checks eligibility and destination. The failed test-live-merge logs are preserved. Acceptance requires an exercised reassignment and nonzero real merges, not removal of the assertion or extrapolation from the split-only pass. No cloud action or spending change occurs.


## Live merge and root contraction coverage passes

The corrected expanded fixture passes with seven actual merges, depth-zero root contraction and a third saved generation containing all96current IDs with matching vectors and codes. Old generations remain unchanged. Live, resident-reference and saved navigation preserve ordered results on the same tree. The final live counters report930durable point reads,22owner-root commits,780evictions and288bytes peak clean-cache payload; modeled mutation reservations peak at657408bytes under1MiB and return to zero. Counts include the authored coverage sequence and do not imply representative construction economics.

The initial reassignment failure is confirmed as a selected-source no-op. The corrected fixture temporarily moves the source routing centroid and narrows the beam, verifies an alternate selected destination, invokes unchanged reassignment and restores the centroid/policy before saving. This is deliberate mechanism coverage, separate from natural construction evidence. Both that failure and the subsequent helper-visibility compile failure are preserved. Failure-before-detach and concurrency/progress tests plus final consolidated review remain pending. Evidence: bounded-topology-access-v1/test-live-merge3.stdout.


## Connected centroid-storage mechanism locally accepted

Root verifies ten changed source bindings plus the frozen manifest and patch, reviews the repaired mutation/publication boundaries, and accepts the local mechanism. Final35connected tests, two legacy replay tests, three live-focused tests and native compilation pass. Failure-before-detach preserves source payload and old snapshots; concurrent readers/exclusive writers and recursive reservation lifetime make progress. Central mutation metadata, resident partition authority and serialized structural publication remain open costs. The next step is native diagnostic integration and bounded cost attribution before another major refactor. No cloud action or spending change occurs.


## Native centroid-cost diagnostic wiring passes

The existing native entry point now supports resident and bounded live centroid storage. Both64-row wiring runs verify complete current census, exact vector bits and returned query distances; native compilation passes. Root verifies the three changed current source hashes against the frozen handoff. Live-store fetch, structural-flush and gate-wait timers report cumulative overlapping elapsed spans, not exclusive CPU.

The fixed16K proposal targets cache eviction under ordinary100/2048/512 tree settings. Technical review identifies that split admission must cover existing leaf occupancy plus the next4096-row insertion phase; the worker is correcting that bound and preparing a supervised invocation. No16K execution or cloud action occurs. Covered compute and recurring storage commitments remain unchanged.


## 16K development build reaches its deadline

The first16K attempt fails because the sandbox denies the RSS probe; its child is killed and reaped with no science output. A fresh normally escalated attempt runs the identical resident dev arm and reaches its60-second deadline. The supervisor kills and reaps the child at60.053142seconds; peak RSS is148668416bytes, user CPU52.958469seconds and system CPU19.949158seconds. The live arm does not start. Partial stores and both failures are retained. No phase-level or structural result supports a cost comparison. The next action is an optimized binary plus immediate phase-progress records before another scientific run; no automatic retry or deadline extension follows. Cloud actions and covered spending remain unchanged.


## Optimized diagnostic build starts

The worker starts one offline two-job opt-level3 build with a600-second process-group deadline. No optimized current entry point is available in the inspected caches, so dependency rebuild time is expected and uncertain. The prior development binary is retained by hash. The only source change adds immediate stderr records for major phase entry and completion; root inspects this change. The research work policy now requires optimization-profile verification and phase progress before cost benchmarks. No scientific run or cloud action is active.


## Remaining topology dependence reviewed during optimized build

The read-only architecture review identifies two connected dependencies: parent/child mutations still use resident node shells, and balancing repeatedly traverses the whole graph to find work. Replacing point reads alone would risk full remote scans on every balance round. The next candidate therefore combines owner-local full topology records and checked mutations with dirty-work queues and subtree summaries. Global frontier ranking and cross-boundary eligibility remain unchanged. A discriminating test must include many idle branches beyond cache capacity and verify that reads follow affected work while preserving reference semantics and old query generations. This is a design candidate pending the current cost diagnostic, not an implemented scalability result.


## Optimized build reaches its fixed deadline

The optimized build is killed and reaped at600.0340415seconds with exit-9 and deadline_hit=true. Completed dependency artifacts and compiler logs remain retained; no successful optimized native executable is returned. The worker performs no automatic retry, wiring run or science execution. The next decision examines completed cache evidence to determine whether a fresh bounded build can finish the remaining project crates. This is a build limitation, not a scientific performance result. Spending and cloud state are unchanged.


## Optimized16K cost diagnostic completes

Both optimized16K synthetic runs complete with intact current census/vector bits/returned distances and sufficient cache-crossing structure. Resident and live trees have12and14nodes at depth1, so this tests cache eviction, not internal-branch pruning. Whole child elapsed is31.529and40.756seconds; balancing is28.209and37.850seconds, insertion1.597and1.486seconds. Peak RSS is193921024and188579840bytes. Different stochastic work (11/13splits,8759/10113reassignments) prevents a causal speed ratio. Live centroid fetch is0.429729seconds cumulative, structural flush0.016119seconds and read-gate wait2.251491seconds; overlapping spans are not exclusive wall time. Resident records95316vector decodes and3969balance calls; live records5581balance calls. Next diagnose repeated balancing/scrubbing and vector access before a larger topology refactor. The raw summary incorrectly labels the binary unoptimized; an additive correction binds the actual opt-level3 profile without altering raw evidence. No cloud action or spending change occurs.


## Split gathering bypasses bounded reader reuse

The source diagnosis identifies scalar split-input fetching after64-ID membership filtering. Each accepted vector calls embedding_owned separately, so the bounded reader reuse inside one fetch call cannot span the window. The existing fetch_vectors path already batches immutable handles and restores request order. The worker now replaces only this gather path, with fixed-request ordered-bit/counter evidence and connected split validation. Recursive balancing/scrubbing remains unchanged because its exclusive cost is unmeasured and skipping it may change semantics. Split workspace admission precedes detach; vector fetch currently follows detach, so failure claims must preserve poisoned-writer and old-query behavior rather than invent pre-detach atomicity.


## AWS login renewal authorized

Lyon reports renewed AWS authentication and explicitly authorizes running `cops aws login` for future renewal. The sole cloud operator verifies fresh sandbox identity and the exact task driver state before any execution decision. Browser confirmation may still require Lyon; this permission does not expand resources, budget or execution-window rules.


## Refreshed authentication enables retained query diagnostic

The sole operator verifies sandbox079819354890/us-east-1 and exact driveri-00268d1e1dfea0357 stopped with both retained disks at19:08:05UTC. Root assigns the unchanged independently reviewed2M query package for one fresh1500-second/$0.25 window, with600-second build,240-second science and180-second preservation bounds. Startup remains conditional on fresh full resource/rate/hash checks and independent shutdown guards. No actual compute charge is recorded before lifecycle evidence. Local split-input batching proceeds separately without changing the frozen query source.


## Split-input fixed-request batching passes

The fixed-request test preserves97ordered ID/version/vector tuples across three immutable roots and two owners with a disabled provider cache. Reader opens and decodes fall from97to8; encoded returns fall from1087866to84816bytes. Copied FP payload remains12416bytes and peak single-reader pin remains14464bytes. Missing/stale/duplicate/empty cases preserve the documented behavior. The connected after-detach failure test poisons the writer while preserving old saved queries, and the existing live split/merge/census test plus compilation pass. Root inspects the changed gather and tests; final frozen optimized build remains pending. This is mechanism evidence, not an end-to-end speed claim.


## Cloud query window stops at capacity admission

Fresh cloud startup reaches remote preflight, which reports13336399872bytes free on the root filesystem, below the reviewed20GiB build requirement. The sole operator requests immediate stop before upload, build or science. Retained disks and failed evidence remain intact; no garbage collection, fallback build or deadline extension follows. API stop confirmation and the exact covered compute bound remain pending. The next capacity decision uses known storage evidence rather than repeating the failed window.


## Capacity-failed cloud window closed

API confirms the driver stopped at19:11:22UTC. The retained receipt bounds compute at94.71057891845703seconds and$0.013259481048583984, counted exactly once. Covered compute is now$2.218218060205459563; retained new storage remains separately$5.12/month. No upload, build or science occurred. The next proposal considers adding16GiB to the existing root volume without garbage collection or moving scientific data; no storage modification is performed yet.


## Split batch16K run passes; decode cost remains

Both fresh16K cells pass integrity and structural gates. Resident/live elapsed is26.801/23.873seconds and balance23.538/20.934seconds. Reader opens are24620/20581, but decodes remain95904/82782. Split/reassignment work differs from the baseline, preventing a causal speed ratio; resident decoding is near the prior95316. Peak RSS increases to229425152/221233152bytes and remains recorded as adverse evidence. Next diagnose remaining decode sites without a cache-size sweep. Separately, automatic approval review rejects the proposed100→116GiBroot expansion because it requires direct authorization for persistent storage cost. No disk change or start occurs; root requests exact user approval while local work continues.


## Remaining decode attribution requires a profile

Resident split batching reduces root-reader opens from51072to24620, while logical vector requests remain117926versus117668 and decodes95904versus95316. The aggregate counters cannot distinguish distinct blocks within a window from rereads across windows or attribute all decodes to neighbor reassignment. Root selects a short sampled balance profile using the existing optimized binary before another instrumentation/build cycle or cache change. The proposed profile is not yet executed. Persistent disk expansion remains pending explicit user approval after automatic review rejection; no cloud mutation follows the goal continuation.


## Balance profile identifies scrub directory roundtrips

One unchanged optimized16K resident run completes in32.8965seconds with integrity/structural gates passing. The two-second10ms sample begins at the first balance entry and completes before its exit. The sampled active balance worker repeatedly waits in recursive scrub validation through per-ID home-partition location calls and actor replies. Vector access appears in smaller sampled branches. This prioritizes bounded batched directory lookup investigation; it does not measure exclusive CPU or a whole-run time fraction. Sampling overhead and stochastic work invalidate a causal timing comparison. Small profile/result evidence is retained, and no cloud action occurs.


## Bounded scrub authority batching enters implementation

The previous authentication reply verifies already-recorded permission and adds no research evidence. Root revalidates the completed worker analysis and assigns the existing implementation worker the profiled scrub boundary: at most 64 ordered identities grouped by immutable home partition, under unchanged identity guards through physical-owner validation. Fixed-request equivalence, actual message counts, transfer visibility and connected topology checks determine acceptance. No cloud action occurs; root disk expansion still awaits direct approval after automatic review rejection.


## Scrub batching mechanism passes; connected contraction fails

The fixed-request authority test passes with 64 to 1 messages for one home and 6 to 3 for three homes, plus 40 transfer-progress operations. Five scrub tests pass. The connected split/move/merge/old-reader test fails its forced root-contraction check: 100 balance rounds leave one undersized leaf and depth 1 instead of 0. The worker must distinguish a semantic regression from fixture eligibility before further admission; no assertion weakening or retry-to-green is authorized. A same-input bounded performance proposal and fresh output specifications are prepared but remain unadmitted. No cloud action or spending change occurs.


## Parallel contraction remains a correctness question

The worker confirms no test process is live and identifies concurrent forced merges as a hypothesis, not a diagnosis. The failed receipt lacks final occupancy and visibility details. Root rejects replacing the failed parallel contraction with a passing serial-only witness. The next diagnostic retains parallel contraction and strict census/root assertions, captures pre/post topology and pending work, and compares scalar versus batched home lookup with unchanged scrub locks and a deterministic authored starting state where feasible. Performance admission remains held.


## Active plan shortened without losing evidence

Root preserves the complete prior CURRENT-PLAN.md byte-for-byte in dynamic-review/current-plan-history-20261017.md (SHA-256 ec118d132b3db7f688a3a7aa6997b46d06b97de344c1acc81248f9a3e679986a) and retains the current active queue with a history pointer. This removes repeated obsolete instructions from routine reads while retaining every historical checkpoint. The worker identifies a concrete candidate cause for the contraction failure: root promotion removes only one unary level. An identical authored depth-two tree will test scalar and batched authority paths before any repair.


## Shared unary-root contraction failure repaired

The identical authored depth-two tree fails in both scalar and batched location modes: each retains one internal root with one 96-row leaf and one pending balance item after 100 rounds. Both modes preserve current memberships, old-reader census and live centroid visibility. Repeated root contraction, with cycle/depth checks and working-memory admission before mutation, changes the same strict test to depth zero and no pending work. The original four-thread connected witness, five scrub tests, fixed authority parity/progress and native compilation all pass. The optimized build runs under a 300-second limit; final frozen review and measured construction remain pending. No cloud action or spending change occurs.


## Scrub authority batching completes connected cost measurement

Independent focused review accepts the frozen batching and root-contraction repair; the optimized build completes in 51.204 seconds. The one-attempt retained-input 16K pair completes with all existing integrity/structural gates passing. Resident/live elapsed is 9.258/9.130 seconds; balancing is 6.043/6.493 seconds, versus prior 23.538/20.934 seconds. Different stochastic split/reassignment work and the combined repair epoch prevent causal speed ratios. Batched location rows are 4,186,585/4,214,925 across 67,018/67,493 batch calls, with one home per batch in this fixture. Peak RSS is 238,714,880/233,930,752 bytes. Resident navigation reports 67 missing-node observations versus live zero; a read-only source/evidence interpretation is assigned, without dismissing the count from final census alone. Small results remain under scrub-authority-batch-v1/runtime-results. No cloud action or spending change occurs.


## Navigation counter mismatch exposes visibility question

Source inspection shows the shared live traversal returns before the resident missing-node counter is incremented, so resident 67 versus live zero is not a matched comparison. Earlier resident runs contain 65 and 192 observations. A copied child can disappear during concurrent split because resident navigation does not take the structural tree lock; this is consistent with the observations but event identities and epochs are absent. Partial beams omit missing children, so final census cannot establish preserved routing quality. The next source proposal must address construction visibility and deterministic detach/read behavior, with comparable missing-record reporting, rather than dismissing misses or tuning further microcosts. No new execution or cloud action occurs.


## Experimental construction visibility boundary selected

The source assessment distinguishes reference semantics from the promised construction view: ordinary resident traversal intentionally skips concurrent missing nodes, and the live zero counter bypasses its instrumentation. Root authorizes a coherent experimental-owner implementation using one shared existing visibility scope from root read through final navigation classification, with separate live membership revalidation. Structural edits remain exclusive, readers remain concurrent, and read guards must end before registration or structural lock acquisition. A deterministic paused-detach test and a dangling-edge negative, followed by recursive/reader progress and connected old-reader checks, govern acceptance. Ordinary reference defaults must not silently change. Implementation and independent review are assigned to existing agents; no performance or cloud execution follows.


## Visibility tests expose partial traversal

The paused-detach test returns leaf 2 at distance 2592 while the completed tree returns leaf 1 at distance 32. After repairing a test setup omission of an immutable vector root, the dangling-child negative also fails because resident owner navigation accepts a partial result. These are deterministic counterexamples to the proposed stronger structural-view contract, not new evidence of missing final memberships. Independent design review identifies and avoids a registration lock inversion: acquire the read gate explicitly around discovery, never implicitly inside every live node fetch. The worker proceeds with one shared experimental-owner visibility scope; ordinary reference defaults remain weak and explicit. Failed test evidence is preserved under topology-visibility-v1.


## Structural visibility counterexamples pass after implementation

The two deterministic tests pass for both resident and live owner construction after extracting one shared visibility gate and adding strict missing-record classification. The ordinary reference remains ungated. The tests cover navigation during paused detach and rejection of a dangling child; expected negative panics are caught. Reader-overlap and recursive-progress checks, connected regression and final independent review remain pending. This is correctness evidence, not measured performance or proof of physical distribution.


## Connected merge exposes writer-owned detach semantics

The focused visibility tests pass, but the connected live test rejects missing child 8 and poisons construction. Source diagnosis identifies merge destination navigation inside the structural write scope after its own source is detached but before parent replacement. The next repair must permit only explicitly tracked transaction-owned source exclusions, with bounded tracking cleared at outer completion/error, separate expected-detach counts, and a deterministic unknown-missing negative. External readers continue waiting for a complete structural interval. This is a narrow correction to the proposed strict contract; no blanket writer exemption or performance admission follows.


## Strict visibility exposes stale queued merge eligibility

The call-scoped, transaction-tracked source exclusion passes visibility tests and the original connected witness. The authored contraction fixture then rejects missing root 2. The scheduler reads parent status before taking the structural lock; another worker can promote the candidate to root before the queued merge removes it. Root authorizes revalidation of non-root eligibility under the acquired structural/write scope before detach, with deterministic queued-promotion coverage. Missing roots remain errors; no broader exclusion is allowed. Failed test-unary2 evidence is retained. No performance or cloud execution occurs.


## Complete structural-view implementation frozen

Five focused visibility tests pass, including a reader-attempt handshake, simultaneous readers, recursive writer navigation, explicit merge-source exclusion, missing-record poisoning and a deterministic queued merge after root promotion. The authored contraction pair, original connected witness, authority progress and pre-detach failure checks pass; native compilation completes in 5.19 seconds. The 952-file source manifest is 6bc15205d7192905252a89259410b38b0a32a95097ecba9f9cd8ccec72a6faee. Ordinary reference defaults remain unchanged. Final independent review is pending, and one warm optimized build is assigned with a 300-second limit to prepare measurement of the new visibility policy. No performance or cloud run occurs yet.


## Complete-view construction measurement passes

Independent source review accepts the coherent visibility change. The optimized build completes in 55.183 seconds. The one-attempt 16K resident/live pair passes structural and complete current-membership gates, with zero unknown root/parent/child records and no poison in both arms. Elapsed time is 9.688/8.385 seconds and balancing 6.442/5.908 seconds. Both observe four concurrent navigation readers; cumulative read-gate acquisition spans are 2.233/1.305 seconds across threads, not exclusive wall or CPU. Splits are 13/11 and reassignments 9,012/8,931; independent work prevents a causal ratio. Neither arm merges, so merge-source exclusion remains covered by deterministic regressions rather than these cost runs. Peak RSS is 238,927,872/239,665,152 bytes. Source and small results are retained under topology-visibility-v1. No cloud action or spending change occurs.


## Bounded mutation storage and scheduling become the active design question

Existing agents are assigned a connected replacement for resident mutation metadata and whole-subtree scheduling, preserving global frontier and cross-owner semantics. Paging alone is insufficient if scheduling still scans all nodes. A new arithmetic sensitivity artifact uses branch cap 100 and assumed mean occupancies 512/1024/2048: minimum node counts are 197,285,354/98,642,677/49,321,339. These are assumption-bound lower counts, not measured quality, throughput or memory totals. The discriminating fixture must contain many idle branches beyond cache capacity and demonstrate bounded work for a small affected branch. No new benchmark, cloud operation or spending occurs.


## Independent design review confirms coupled storage and work-discovery replacement

The review identifies hidden global state beyond the node cache: leaf-owner lookup, internal-node placement at owner zero, whole-tree scheduler and placement enumeration, and dirty publication sets. The accepted direction combines owner-local durable records, bounded transaction pins/write sets, partitioned point location and incremental eligibility/ancestor work. Physical occupancy and parent/root changes must update work, and tasks created while draining must not be lost. A 128-idle-branch/cache-eight authored fixture separates scheduler reads from legitimate global neighbor discovery and checks unchanged saved generations. All three arithmetic scenarios are independently verified; no implementation or runtime claim follows. The worker is finalizing the concrete implementation sequence.


## Incremental owner scheduler enters implementation

Root accepts the concrete dependency sequence and assigns source work: bounded durable dirty-leaf scheduling first, then full-record mutation leases and partitioned location replacing the resident graph. The first slice removes descendant enumeration from owner balance work discovery while preserving global neighbor selection. Pages are at most 64 and in-flight work is bounded; dirty epochs created during processing must survive, and physical occupancy, scrub state and root eligibility are revalidated. No hidden all-node resident queue map may be described as bounded memory. The existing worker implements and the existing reviewer checks the coherent result; no performance or cloud run is assigned.


## Durable work-page representation selected

The worker selects fixed 64-node durable flag pages with a bounded cache, separate dirty epochs and Check/NeedsScrub flags, and a durable segmented queue of page IDs. Fixed per-home roots and cursors replace an all-node queued map; one claimed page bounds in-flight jobs. Completion must retain newer flags/epochs created during processing. Root accepts this representation for the first scheduler slice and requires actual parallel job concurrency reporting rather than inferring it from page size. Source implementation is underway; no runnable test or measured result exists yet.


## Durable scheduler core obtains executable feedback

After excessive design coordination, root narrows the next action to an executable queue core before integration. The first test exposes blockstore prefix write ordering; failed evidence is preserved and ordering repaired. The focused core then passes in 0.02 seconds with one cached flag page, durable eviction/reload, repeated-event coalescing, work added during a claimed page, newer epoch retention after acknowledgement and capacity rejection. Observed counters include three claimed pages, five claimed rows and one retained newer row. The worker proceeds to owner scheduler/event hooks with held structural admission credits. Connected no-scan behavior and construction semantics remain unverified; no performance or cloud run occurs.


## Owner scheduler integration reaches compilation

The mutable source now routes owner balance entry points and pending-work checks through the dirty-work scheduler, with pre-registration and reassignment-source work events. The integrated test target compiles in 13.18 seconds. This source/compile progress does not yet prove complete mutation event coverage, absence of idle-branch scheduling reads or preserved connected construction. The implementation worker continues toward that behavioral test; no new acceptance or benchmark claim is made.


## Connected incremental scheduling avoids idle-branch scan in authored fixture

The connected test passes with 128 idle branches and 393 authored nodes. The measured small edit uses 18 scheduler node point reads, performs cross-owner work from owner 1 to owner 0, retains the old 1,024-row snapshot and yields 1,030 current rows with no pending candidates. Setup is outside the measured delta; global navigation remains separately counted. The topology cache cap is eight records, but 394 mutation nodes remain resident. Only one worker context is observed, so this is not parallel-speed evidence. The first connected attempt fails compilation and is retained; the later passing run supplies the behavioral evidence. Coherent regression and independent review remain pending.


## Incremental scheduler slice frozen for review

The final two queue tests, two connected tests, five visibility regressions, original connected witness and native compilation pass. Event admission now explicitly precedes destination membership mutation and source reassignment. The frozen 953-file manifest is 845508c02aa0e960bdc9aed78bce8edeedd294b855577e439f7dcc54e3eb0893. The small-edit delta is 18 scheduler node reads, four claimed pages, six candidate rows, one scheduled leaf and 18 queue commits. Pending work is conservative until drained, and fixed constructor thresholds reject silent reclassification. The resident graph, global locator and placement scans remain unresolved; no speed or whole-architecture bound is claimed. Independent review precedes the full-record/locator migration.

## 2026-10-17T20:05:35.285925+00:00 — Scheduler acceptance and authoritative record migration

The dirty-work scheduler is independently accepted with no blocking findings. Its 18 node reads cover scheduling and revalidation only; balancing, global discovery and publication remain separate. The fixture retains 394 resident nodes and observes one worker context. The active implementation now makes full node records durable and authoritative, uses fixed-home lookup for internal and leaf nodes, and replaces both resident node and leaf-owner maps with bounded leases. Acceptance requires the connected 393-node fixture to keep both maps empty through append, split, cross-owner reassignment, scheduler drain and saved-generation validation. Cache, active pins and dirty transaction budgets are separate. No further depth-one cost run is scheduled for this slice. The existing implementation worker owns source and Cargo; the existing reviewer remains independent. Local work adds no cloud spending.

## Full-record lease integration hazards

The independent read-only access audit identifies two concrete requirements for the active migration. Root creation must release the current node snapshot before recursive access; centroid recomputation and live-centroid publication must use the held record rather than reacquire its stripe. Any multi-node acquisition must sort and deduplicate stripe identities, including collisions between different node IDs. The reviewer sent source anchors directly to the implementation worker. These findings guide implementation and are not passing runtime evidence. No cloud action or spending change occurs.

## Authoritative node-record core passes

The first two full-record storage tests pass in 0.01 seconds after a 13.86-second test build. They exercise internal and leaf records at fixed homes, eviction, a write lease, posting-owner epoch changes, immutable old roots and malformed record rejection. The fixture reports six evictions, seven point reads, three home commits, 104 peak cached bytes under a 256-byte cap and 256 peak pinned bytes under a 512-byte cap. These are tiny storage-mechanism checks; construction still requires kernel and locator integration. The worker continues that integration with explicit stripe-collision and pre-detach admission checks. Evidence: dynamic-review/authoritative-node-records-v1/test-core.stdout. No cloud spending changes.

## Mutation access-layer compilation

The first access-layer compile rejects four helpers tied to resident-map mutable guards. The repaired lease-compatible helper compiles and both core storage tests pass again, as recorded in authoritative-node-records-v1/test-access-compile2.stdout; the failed compile remains preserved. Construction integration remains incomplete. The active implementation also restricts unaccounted record handles, moves committed drafts instead of cloning them, and reserves structural dirty capacity before detach. Those admission changes require connected tests before acceptance.

## Full-record capacity reservation passes

All three core/admission tests pass. The competing-thread witness rejects an unrelated update while the reserving transaction retains and consumes two dirty-record slots. Raw record handles are private and the locator interface copies metadata; connected activation remains in progress. Neither central map is removed yet. Evidence: dynamic-review/authoritative-node-records-v1/test-admission.stdout. No performance, whole-construction memory or distribution claim follows.

## Connected authoritative-record test exposes integration waits

The connected fixture activates fixed-home records and clears both central maps, but it does not complete. The first stall reveals a temporary state-mutex guard surviving into reservation acquisition; separating the snapshot fixes the focused clean-record write regression. The interrupted process exits 130. A subsequent 90-second bounded connected run still stalls. The worker is adding stage markers and may use normally approved inspection of the exact test process to locate the remaining wait. Assertions remain unchanged, failed attempts are preserved, and no connected correctness or memory-removal acceptance follows.

## Connected construction passes with central maps empty

The remaining timeout is a retry loop caused by five multiline resident-map reads, including reassignment target validation. The process sample shows active navigation and decoding; stage markers locate reassignment. Converting those reads resolves the failure without changing fixture assertions. Connected4 passes four tests in 0.31 seconds. The 393-node fixture preserves same-prestate navigation, both empty resident maps, cross-owner movement and reassignment, six appends, a split, and current/old saved censuses of 1,030/1,024 rows. Scheduling reads increase by 18 amid 128 idle branches. The two-entry cache observes 1,839 evictions and a 560-byte peak; accounted pins peak at 24,576 of 131,072 bytes. These small-fixture counters do not bound provider memory or establish throughput. Negative admission and reachable-call audit precede one independent review. Evidence: dynamic-review/authoritative-node-records-v1/test-connected4.stdout.

## Full-record implementation frozen for independent review

Five focused methods, 16 existing wide/visibility regressions, native CHECK and explicit saved centroid/posting-code proof pass. The frozen handoff is dynamic-review/authoritative-node-records-v1/HANDOFF.json, source manifest 40a2af2598151b54f7ce44aec68e4dd3255a6899d8617e9c67ce9e80ff164e7c. The existing reviewer is active. Explicit joined leaf movement works, but bulk redistribution rejects full-record mode pending an admitted placement catalog; the complete add/balance/redistribute cycle remains unfinished. Provider memory, posting payload maps, publication keys and query frontiers remain separate limits. The staged index is preserved and no cloud costs change.

Full-record authority is independently accepted with no blocking findings. Both central maps remain empty through the connected mutation and publication fixture. Bulk redistribution remains incomplete. The active slice adds durable per-owner placement candidates, exact maintained load summaries and bounded page/move budgets, using the accepted joined leaf transfer. It must handle stale split/deleted/moved entries, improve feasible skew, stop explicitly when no feasible move or budget remains, and preserve a subsequent insertion/balance cycle. No global-sort equivalence or performance claim is assumed.

## Bounded placement connects the next construction cycle

The catalog core and three connected methods pass. The 393-node fixture redistributes 504 vectors in 126 whole-leaf moves, changing owner loads from [1021, 9] to [517, 513]. It reads nine admitted catalog pages and checks 1,030 physical posting rows. Both central maps remain empty. A subsequent insertion/balance/save cycle produces 1,034 current rows while preserving 1,030-row and 1,024-row snapshots. Separate tests stop with unfinished skew when whole-leaf movement cannot help and reject a full destination catalog before moving membership or payload. Owner-load summaries use maintained table lengths, but candidate current-row counting is charged separately. Final regressions and independent review remain pending. Evidence: dynamic-review/bounded-placement-catalog-v1. No cloud spending changes.

## Placement frozen for independent review

Final three placement methods, 18 regressions and native CHECK pass. The final stochastic fixture moves 505 vectors in 126 leaf moves, reads nine pages and inspects 1,247 posting rows, producing loads [516,514]. Earlier [517,513] evidence remains separate. The source manifest is 258d291cd5f453fc5725ff3e5877a31ae441e177c9fcf035ac86482a2872e114; independent review is running. The worker is checking representative benchmark admission requirements read-only while source stays frozen.

Independent review accepts the bounded placement catalog and closes the local functional insertion/balance/redistribution/save gap with both central maps empty. Per-owner FIFO fairness is demonstrated; global move-cap exhaustion does not guarantee fairness across owners. The next active work prepares a representative connected-mode construction benchmark, checking harness activation and retained capacity before execution. The disk expansion remains unapproved, and no cloud action or spending change occurs.

## Full-record benchmark wiring passes

The warm native development build completes in 14.87 seconds. The real entrypoint verifies 64 vectors and exact query distances across four cuts in resident-default and full-record modes; incompatible modes reject before output creation. Full mode reports zero resident mutation nodes and active durable scheduling/placement plus point-miss/flush timers. This is wiring evidence, not optimized performance. A single dynamic 1M attribution proposal is being frozen for review; integrity queries do not establish held-out quality. Retained capacity review confirms the root build floor remains unmet and data-disk capacity is stale/unmounted in the latest receipt. No cloud action or spend changes.

## Benchmark activation independently accepted

Independent review accepts the three-file activation/reporting change and native wiring, with no source blockers. The 1M protocol remains planning-only until root build capacity and concrete Linux/source/prefix/supervisor bindings are established. The exact development build receipt is 14.987506209 seconds; earlier handoff wording of 14.87 seconds is superseded for accounting and is not a performance measurement. Source manifest: fafdc07d2ad9c2782d92f02191818ee74d52cd5168cd861afeefc4a2a7b52c7e. No cloud action or new spending occurs.

## Direct storage growth authorization received

Lyon authorizes storage growth as needed within the total $1,000 limit. The selected action remains the reviewed 100→116 GiB existing root expansion (+$1.28/month), rather than a larger speculative allocation. Authorization is persisted in STORAGE-AUTHORIZATION.json and the goal/state. The sole operator must submit through normal review and verify fresh sandbox/resource identity, shutdown guards and filesystem capacity before execution. No mutation has yet been confirmed.

## Authorized volume modification submitted

AWS accepts the exact root expansion from 100 to 116 GiB at 20:47:55 UTC, preserving gp3 IOPS/throughput. Filesystem proof and stopped lifecycle remain pending; the sole operator owns closure and ledger reconciliation. Independent per-vector accounting verifies 121r+101 selected logical bytes per current vector on a 64-bit target, where r counts physical copies. Two dirty-ID sets can add 16 bytes per new vector before save. These aggregate field counts exclude padding and allocation overhead; membership maps are BTreeMaps with constant-work maintained lengths.

Both authorized capacity repairs are complete and the driver is API-confirmed stopped. Root free space is 29,978,263,552 bytes and data free space is 31,110,856,704 bytes, passing their 20/24 GiB admission floors. The two repair windows total $0.02753689750 compute upper bound and add $1.92/month storage, counted once in the ledger. The concrete million-vector runtime package has four passing local supervisor tests and is under independent technical review. No scientific run has started; fresh execution bindings and guards remain required.

## Runtime guard repair before admission

Independent review withholds runtime admission because TERM can bypass child cleanup, leader exit can leave descendants, output paths lack containment checks, and limits/thread settings are not locked. The corrected supervisor handles signals through process-group kill/reap, uses Linux subreaper mode, rejects unsafe paths and enforces the accepted configuration. Six local methods pass in 3.538 seconds; original package and failed symlink fixture remain retained. Corrected supervisor SHA is 7e28f557f2cd57a0b9bb00327e93c0132aadb001a721d4bbe85632b215934756. Focused re-review is active; native source is unchanged and no cloud run starts.

## One guarded million-vector attribution window dispatched

Root accepts the corrected supervisor review and dispatches the sole operator for one fresh window capped at 3,600 seconds and $0.55 compute. Operational admission requires six tests on the exact Linux interpreter, fresh sandbox/resource/capacity identity, reviewed source closure, retained input/prefix hashes, exact optimized compiler/binary binding and independent local/remote absolute shutdown. Build is capped at 600 seconds, science at 2,400 seconds, with shutdown/collection reserves preserved. Any gate failure stops the attempt without automatic retry or extension. No scientific start or result is asserted by dispatch.

## Upload review rejected; closed window preserved

Automatic approval review rejects the 2,401,926-byte reviewed package before transmission, stating that the standing authorization file is untrusted and specific trusted human authorization is absent. The operator submitted a paraphrase rather than the direct quote. The attempt closes with API-confirmed stop, zero uploaded bytes/tests/build/science and $0.017407026588 compute upper bound, counted once. Root dispatches one fresh normal-review attempt with the same transfer mechanism/package and the verbatim direct human blanket-upload instruction, including its explicit changing-payload/IP scope. This is an authorization-evidence resubmission, not an alternate route. Another rejection must stop without further retries.

## Specific upload approved and transferred

After a second normal-review rejection, Lyon directly authorizes the exact archive and destination. Epoch2 closes API-stopped at $0.018953086012 compute upper bound. In fresh epoch3 the sole operator suspends actions and hands root one verified transfer. Normal approval accepts the direct human reply; SCP exits zero for the unchanged 2,401,926-byte archive. Root relinquishes control to the sole operator for remote hash, Linux tests, input/binary and reserve checks. Deadline remains 22:05:44 UTC; no science is yet asserted. All rejections remain preserved.

Lyon clarifies that 95% recall is aspirational, not mandatory. Current evaluation and goal completion must assess measured recall–cost tradeoffs and reference comparisons without a mandatory absolute 95% gate. Historical protocol failures remain recorded under their original rules; they do not impose a current acceptance threshold. The active 1M construction-attribution experiment remains integrity-only and unchanged.

## Linux admission passes; optimized build active

Epoch3 verifies the uploaded archive and 955-file source closure, then passes all six supervisor tests on assert-enabled Python 3.12.3 in 3.138 seconds. The retained 6.144GB input hash matches; the exact million-row prefix binds to e80e393e4036076150d1b10ea32ad587788ca2d67d2384df3197e487a15fc62e. Input binding takes 45.865 seconds. The sole offline optimized build is active under operator handle 65016, remote PID1390; compiler identity is verified and the absolute 22:05:44 UTC deadline is unchanged. Science has not started.

Lyon directs routine baselines to reproduce the fast optimized parallel reference rather than accommodate experimental limitations with a slow reference. Serial tests require a named concurrency hypothesis. Match data, hardware, thread counts, cadence and navigation settings, then verify effective parallelism. The historical serial FineWeb 2M result remains harness evidence only; correctness did not prove representativeness. The active four-thread dynamic attribution run is not a matched comparison. The relayed Wikipedia timings/configuration require source verification and reproduction before quantitative comparison.

## Native failure and primary reference evidence

The epoch3 native construction exits101 during balance at8192 rows with `predicate owner missing` in reference_owner_adapter.rs:708. The first4096-row cycle completes. The operator reports11.6586s native elapsed and is closing the guarded window; shutdown and final spending require the closure receipt. The implementation worker owns a focused correctness repair and regression. This failure is not a throughput or recall result.

The primary raw CLI source confirms optimized parallel reference checkpoint totals40.80s and28.14s, totaling68.94s for2M Wikipedia vectors. It reports32 insertion/balance workers, Euclidean1024D, branch100/split2048/merge512, one replica, epsilon0, RNGfactor0, FP write navigation/NPA, tau0.5 with schedule[*,*,0.30], and1GB block cache. Snapshot time is explicitly excluded; the first checkpoint also reports two undersized leaves remaining after a bounded balance loop. Historical hardware CPU, compiler/binary and exact data revision remain unverified. The source excerpt is dynamic-review/fast-reference-primary-excerpt.txt; this supports the baseline configuration, not a matched performance ratio to the experimental path.

## Optimized reference reproduction recovered

The primary parent report supplies the full reference command, r6id.16xlarge hardware (64 vCPUs,512 GiB,local NVMe), production and harness commits, and published binary hash. The saved excerpt is dynamic-review/fast-reference-reproduction-primary.txt. This closes the missing historical hardware/command questions without executing another benchmark. The historical command uses a separate balance-thread argument, while the current source inventory exposes a parallel-balancing flag; source and command generation must be matched before reproduction. Exact dataset revision and compiler remain unknown, and published hashes have not been revalidated against original artifacts.

## Epoch3 closure verified

The API readback confirms the exact sandbox driver is stopped. The watchdog retires only after that proof. The operator records $0.08165959326 compute upper bound once, with no additional storage change. The failed output remains on retained disks; 23 small evidence files total1,093,874 bytes locally, with no vector payload. The active work is the focused predicate lifetime regression and repair, followed by independent review.

## Reference implementation mismatch identified

The historical reference source is available in local Git at bec6e7213b37ca97066545535bc82d4252f9c0dc. Its harness calls add_batch_buffered, which collects destination-leaf changes in parallel and flushes them before balancing. The current checkout instead invokes add for each vector and lacks that batch implementation. This is an algorithmic execution difference, not just a renamed thread flag. The next reference admission must bind the exact optimized source; experimental batching is a separate candidate after the ownership failure is repaired. The sole operator is conducting a read-only source admission review while the implementation worker owns the predicate repair. No new cloud window or source port occurs.

## Ownership failure reproduced deterministically

A focused test reproduces the exact predicate-owner panic by retiring the source locator between the first64 rows and row65 of an immutable predicate snapshot. The repair binds the execution owner while capturing that snapshot; current membership and reassignment checks remain live. The repaired test is still running, and independent acceptance is pending. The original failing test and an intermediate compilation failure are preserved.

## Predicate repair frozen for independent review

Four predicate tests pass, including retirement during a65-row snapshot and a real32-row split/move/save with old-generation vector checks. The393-node connected mutation test, placement with later insertion, and native compilation also pass. The three-file repair captures computation actors with immutable predicate inputs; live membership/version/reassignment checks remain unchanged. The reviewer owns acceptance and, if accepted, local preparation of one fresh bounded1M attribution package. Cloud start is pending package bindings. The worker separately assesses bounded parallel batched insertion read-only.

## Predicate repair accepted

Independent review accepts the frozen three-file repair with no blocking findings. Execution actors are captured before topology retirement, while live membership/version checks remain unchanged. The exact unlogged runtime call chain remains unknown; a fresh bounded attempt must establish progress beyond8192 rows. The operator prepares the coherent source package locally; the driver remains stopped pending new execution bindings and guards.

## Reviewed replay dispatched

Root verifies every955 source member and exactly three accepted repair changes in the sealed v2 archive; supervisor, guard tests and scientific settings are unchanged. The sole operator is dispatched for one fresh3600-second/$0.55 window with the existing build/science/collection limits. Root owns only the normal-reviewed upload during an explicit operator pause. Fresh identity, capacity, absolute shutdown guards and executable/input bindings remain mandatory. The implementation worker can advance bounded batching independently because the replay package uses immutable source.

## Epoch4 reviewed upload succeeds

Fresh local and remote absolute guards bind22:23:16UTC on the verified sandbox driver. Normal approval accepts the standing upload authorization for the explicitly new reviewed v2 archive; root SCP exits0 and sole cloud control returns to the operator. Runtime hash, Linux guard tests, optimized binary/input binding and remaining reserves determine science admission. No scientific start is yet asserted.

## Epoch4 orchestration failure before build

The v2 archive,955 source hashes and six Linux guard tests pass. The next generated Python body fails parsing at `assert __debug__and not`, before executing any input binding, compilation or science. The operator stops the window without retry under its stop rule. This is an orchestration error, not algorithm evidence. Before another admission, the complete generated build and science launch bodies must be saved and parse-checked locally; the failed body remains retained. Shutdown and cost await the closure receipt.

## Epoch4 accounting closed; batching controls advance

The epoch4 closure confirms stopped compute, retired local watchdog and $0.03908923718 compute upper bound counted once, with no storage change or scientific execution. The new batching tests preserve exact129-row posting data and161current/129old vectors after later mutation, while reducing owner callbacks129→3 and reserve/activate home dispatches129→4 each in the fixed fixture. Capacity/partial-flush failure and paused-collection visibility tests pass. A distinct-leaf control initially violates scheduler activation preconditions in its fixture; that failure is preserved and the corrected fixture is rerunning. No performance ratio is inferred.

## Bounded insertion frozen; launcher accepted with Linux gate

The five-file opt-in insertion path passes fixed-work, distinct-leaf, collection-visibility, failure, saved-generation and native wiring checks. The hot-leaf129-row case reduces callbacks/node leases129→3; provider point reads remain1 in both paths, so no decode reduction is demonstrated. The64-distinct-leaf case retains64 callbacks/leases. Independent review is active. Root accepts the repaired launcher with four Linux lifecycle tests and six supervisor tests mandatory before build/science. The next package should consolidate accepted batching and predicate repairs, retaining prior source epochs and unchanged runtime limits; cloud remains stopped.

## Combined batching runtime accepted for fresh attempt

Independent review accepts bounded insertion. Root verifies all955 source members and exactly five accepted source changes in v3, unchanged supervisor and the sole scientific delta stable_insert rows64/worker_bytes1048576. The exact extraction helper succeeds against a fresh local temporary directory, verifying source/Cargo.toml placement. Epoch5 guard preparation is dispatched for one driver-only bounded1M attribution run; four Linux lifecycle and six supervisor tests must pass before compilation/science. Fresh identity/capacity/input/binary/shutdown checks remain required. No science has started.

## Epoch5 combined package transferred

Fresh sandbox identity, exact disks/capacity and independent absolute guards bind22:34:46UTC. Normal review accepts the standing upload authorization for the2,414,526-byte v3 archive; root transfer exits0 and sole control returns to the operator. Four Linux lifecycle tests and six supervisor tests precede input binding/build/science. No scientific start is yet asserted.

## Linux runtime gates pass; paged metadata implementation begins

Epoch5 verifies the package and955 source files, then passes all six supervisor and four lifecycle tests on Linux, including separate-session cleanup. The same saved launcher handle6163 is binding retained input and compiling; no science result is asserted. The next implementation replaces duplicate current-owner/version/content-handle tables with one durable home record and bounded live state, preserving immutable query-root tuples and live reassignment checks. Acceptance requires the connected cache-crossing cycle with old tables empty; field-count arithmetic is not an RSS estimate.

## Fresh optimized3M reference evidence

The side reference run completes with measured checkpoint totals42.20/26.43/26.07s (94.70s sum),106.771s full process,21.87GiB peakRSS and11.34 averageCPU cores. It uses the historical optimized source and32/32workers on pinned Wikipedia1024D/L2, with rootEBS rather than historicalNVMe. The depth3 tree reports3M unique valid IDs and zero orphans; a separate persisted-ID audit is absent. The same100 corpus queries produce93.96% recall at11.1ms warm and5.12% scan, or97.99% at18.7ms warm and11.62% scan with8xreranking. These are exploratory corpus-query results with retained OS cache, not held-out quality or physicalnetwork measurements. Groundtruth is saved and hashes pinned in ../reference-3m-20261017/summary.json. The worker is API-confirmed stopped; its unique$1.5290144056549073 compute receipt awaits soleoperator ledger reconciliation once. This reference removes the need to treat sub-minute million-vector throughput as merely anecdotal, but is not a matched dynamic ratio.


## 2026-10-17: Epoch5 build timeout and verified shutdown

The optimized build reached its 600-second limit before the scientific benchmark started. The saved launcher reports TimeoutError from its owned-process timeout. All six supervisor and four lifecycle Linux checks passed beforehand. The API stop-readback confirms driver i-00268d1e1dfea0357 stopped; watchdog retirement follows that observation. This is a build-path failure and supplies no construction performance evidence. The operator owns final failure collection and ledger reconciliation. Diagnose build profile/artifact invalidation before another execution window; continue the connected paged-metadata implementation locally. Evidence: fleet/full-record-million-runtime-v1/epoch5/bind-build.txt, stop-readback.json and watchdog-retired.json.


## Metadata writable-admission review

Source review identified a reachable admission failure when a home flush clears a dirty record after an update has acquired its writable pin. The worker confirms identity locks do not exclude that flush. The correction retains dirty status while writable pins exist and releases the writer count before unpinning. A deterministic regression exercises flushes between repeated updates through the same admitted pin. Execution and the connected save/move/query tests remain pending; this is not final acceptance or evidence of corruption. The intermediate review is dynamic-review/unified-paged-vector-metadata-v1/root-intermediate-credit-review.json.


## Connected paged metadata publication result

The corrected connected test passes insertion, balance, an actual cross-owner leaf move, later insertion/balance from256 to288 rows, old/current saved census and globally routed full-ID queries. Legacy resident authority/current/handle/dirty maps remain empty. The first publication attempt failed generation/metric validation after out-of-order scalar writes; the correction uses the existing unordered scalar writer and retains its buffering as an unresolved publication-memory dependency. With eight clean records per home, the passing fixture performs6,242 point reads,6,186 decodes and3,100/3,622 home evictions. These are adverse mechanism counts, not representative throughput. Writer-drop validation, consolidated negative tests and independent acceptance remain. Evidence: dynamic-review/unified-paged-vector-metadata-v1/connected-snapshot2.stdout.


## Local metadata acceptance and epoch6 startup failure

Root accepts the frozen paged-metadata local correctness slice after source review, seven-file hash verification and inspection of the five passing tests plus final-slot contention regression. Diagnostic wiring is next. Provider I/O fault injection, publication buffering, posting/provider/candidate memory and global structural serialization remain explicit limitations. Review: dynamic-review/unified-paged-vector-metadata-v1/independent-focused-review.json.

Epoch6 builds successfully in155.47seconds with opt3/debug0, then fails before insertion because the fresh epoch output parent is absent and native directory creation is nonrecursive. Input verification succeeds; there is no construction measurement. The operator confirms API shutdown and records$0.06266137556 once. A local parent-creation regression must pass before another fresh window, using the verified retained binary where possible. Evidence: fleet/full-record-million-runtime-v1/epoch6/final-receipt.json and evidence/.


## Optional paged-metadata diagnostic accepted

Root reviewed the two frozen source files, checked admission arithmetic and the seven native-case assertions, and inspected successful receipts. The65-row path exercises catalog placement and saved census/query integrity. It fixes final placement load reconciliation to use maintained home summaries instead of empty legacy tables. Five invalid configurations reject before output creation. This accepts wiring, not throughput, memory RSS or physical distribution. The failed first placement smoke is retained. Review: dynamic-review/paged-metadata-diagnostic-v1/independent-focused-review.json.


## Epoch7 reaches construction

The corrected launcher reuses the verified optimized binary and passes provider/setup. Retained native progress reaches40,960 inserted vectors at33.605seconds before entering balance; the sole operator confirms the same handle5745 remains live under the fixed23:00:01UTC deadline. This passes the earlier8,192-row failure boundary but is not a completed build or final membership validation. Evidence: fleet/full-record-million-runtime-v1/epoch7/progress-first.txt.


## Side comparison cache correction

The side operator reports stopping the partial3M one-owner run with exit143 after identifying256 provider cache units versus1024 in the reference. Partial timings remain adverse evidence but cannot support a matched-cache comparison. A revised isolated build is reported under the unchanged side deadline. Equal configured units still do not imply equal useful capacity: the reported weight function rounds each16KiB vector block to one1MiB unit, while reference blocks are roughly3MiB. Root has not independently verified the revised binary or results. Keep effective payload, block reads, decodes and evictions explicit before attributing performance. No additional root cloud action or spending is introduced.


## Provider cache capacity mechanism verified

Source assessment confirms owner staging explicitly selects16KiB blocks while the diagnostic provider defaults to3MiB blocks. Legacy cache weight rounds every block to at least1MiB, making equal configured units unequal payload targets. The worker is implementing an opt-in benchmark-only byte-charge adapter with fixed-request tests; block sizes, reader limits and active epoch7 remain unchanged. This is a verified capacity mechanism, not measured attribution of balance time. The assessment also identifies ordinary resident embeddings as a separate memory difference. Current-state budget mirrors the receipt-backed ledger: $4.102656330460366863 covered compute and $7.04/month new storage; active windows are excluded until closure, with no new ledger charge added here.


## Byte-charge cache retains adverse fixed-request outcomes

Root inspected fixed4 and fixed5 logs: both single-touch and double-touch request sequences decode all12 blocks again on the second pass, despite101,376 decoded bytes fitting the262,144-byte target. Double-touch records24 cache hits but still12 second-pass decodes. The presumed zero-miss assertion is not a correctness requirement. The worker must verify replacement semantics before further workload changes; retain both failures and separate byte-accounting correctness from empirical performance benefit. These one-shard tiny fixtures do not establish construction throughput.


## Cache retention investigation identifies stale usage after clear

The worker traced the cleared-provider discrepancy to foyer-memory0.17.3: clearing drains entries without resetting the shard usage counter, while insertion evicts against that counter. Root independently inspected the dependency source. This explains why a fresh direct cache behaves differently from the provider fixture after staging and clearing; a deterministic correction test remains required. Preserve all adverse logs and use fresh providers for the unchanged request tape. The selected local correction replaces only the opt-in adapter cache on clear with the same configuration, preserving returned block clones. This does not attribute the active construction slowdown to clear; its actual clear behavior must be checked separately.


## Byte-charge local mechanism accepted

Root verifies both frozen source hashes and accepts the focused byte-accounting/content slice. Four tests pass and native compilation succeeds. Fresh-provider unchanged request tapes decode12 blocks on the first pass and zero on the second with byte charging, versus12 and12 under legacy weighting. Repeated block-cache replacement clears retain expected contents; old returned clones and retired-generation isolation pass. Root-cache clear remains unchanged, soft capacity is not RSS, and no construction speedup is established. The existing65-row native fixture will validate optional diagnostic wiring before any remote admission.


## Native byte-charge integration accepted; side timeout relayed

Root verifies unchanged source hashes and inspects six native integration receipts. Four65-row owner-mode arms with legacy/paged metadata and absent/enabled byte charging preserve census and exact bits; two invalid policies reject before output. These are not ordinary-reference comparisons.

The side operator reports its revised1024-unit one-owner attempt exits124 at1200.447seconds, with800K inserted and balancing at the last progress point. A sequential four-owner attempt is reported active under its unchanged deadline. Root has not independently inspected those runtime receipts. There is no completed3M comparison or recall result; effective cache payload remains unmatched.


## Epoch7 closure and monitor correction

The API confirms the driver stopped after the deadline ends construction at 917,504 inserted vectors. Publication and final census are absent. The operator records $0.36600635358 once, with no storage delta, and reconciles the separate side comparison once. The synchronous output scanner consumes 1,504.56 seconds of concurrent elapsed and blocks deadline checks; its pre-scan scheduling timestamp causes immediate repeat eligibility after long scans. This is a consequential measurement defect, not a subtractable native slowdown estimate. The existing worker owns a new bounded scanner revision and focused tests; frozen source and failed evidence remain preserved. No new cloud window starts.


## Monitor review requests guaranteed native cleanup

Root source review finds that an exception while closing the scanner can skip native-process termination because both cleanup operations share one try block. The worker owns a new revision with nested cleanup and an injected failure regression. The current monitor is not admitted remotely. This is an actionable correction, not a human blocker; no new benchmark starts.


## User selects100K construction/cache diagnostic

Lyon recommends100K vectors instead of1M to reduce iteration time. The operator is adapting local preparation before cloud execution. Preserve algorithm settings and the250K redistribution threshold, which means this run does not exercise capacity redistribution. The307.2MB raw FP32 input exceeds the256MiB cache target, but only measured fetched working set and decode/eviction behavior can establish cache pressure. Select a shorter runtime bound from retained prefix evidence and retain actual tree shape and work counts.


## Single-owner performance first

Lyon directs single-owner diagnosis and improvement before multi-owner work to shorten iteration and separate the costs. The pending100K package now requires exactly one owner and four worker threads, with no redistribution. The operator is updating the unlaunched package and reconciling copied preparation metadata. The physical transport proposal remains retained but deferred. No cloud execution occurs.


## User selects32physicalcore single-owner baseline

Lyon recommends32workers on32physicalcores. The unlaunched four-worker package admission is superseded. The sole operator will verify an eligible task machine, physical core topology, rate and capacity, then adjust worker-dependent memory reservations and execution bindings. The100K single-owner/no-redistribution scope remains. No cloud execution starts from the superseded package.


##32worker engine admission dependency

Root finds an explicit workers<=4 admission condition in the bounded vector store, so changing only thread settings would fail before construction. The implementation worker owns the32worker admission and memory-accounting revision with focused correctness evidence. The operator separately resolves fresh hardware identity through normal review after an inventory rejection concerning a temporary side-run exclusion. No cloud execution occurs and no rejection is bypassed.


##32worker revision accepted locally

Root verifies the four frozen source hashes and accepts the32worker wide-owner admission revision after source and test review. The core test holds32reader slots and160modeled reservation guards; connected65-row native execution preserves saved membership/vector/query bits and rejects invalid admission before output. Use8GiB modeled split capacity and256MiB parallel-window budget for the proposed100K run. These tests do not establish32-core throughput; native peak fetch concurrency is1. Hardware authorization and exact input presence remain unresolved, while local package preparation can proceed.


##32worker local package review complete

Root verifies the frozen2,436,724-byte package, exact member contents,100K single-owner32worker policy and host/environment tests. The package is accepted locally, not admitted for cloud execution. Hardware identity, input path, mount and deadline remain deliberately unfilled. The pending direct authorization is required by normal approval review before candidate-instance inspection or reuse; no cloud retry occurs.


## 32-worker guarded preparation and exact input

Lyon directly authorizes reuse of the idle side-run machine. Fresh inventory and remote identity confirm sandbox instance i-011ff180f03584f40, 32 physical cores and 512GiB memory. The sole operator opens a guarded preparation window ending at 2026-10-17T23:48:41.387120Z at $4.8384/hour. Root selects one verified blank EC2 instance-store disk for scratch through normal review because root build headroom is thin; all EBS and retained build files remain preserved. The first scratch preflight exits before mutation on a padded serial comparison, and read-only reconciliation confirms both disks remain blank. The operator corrects comparison normalization.

The accepted canonical input descriptor identifies an exact 100K-row first chunk: 307,200,000 bytes, SHA256 5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c. Its pinned object version permits a small direct restore without moving the full 6.144GB input or downloading vectors to the Mac. Root accepts this input binding; scientific admission remains conditional on package integrity, Linux checks and remaining-time gates. See dynamic-review/100k-exact-prefix-binding.json and fleet/100k-owner32-runtime-v1/epoch1/.


## Preparation completes; build waits for a fresh window

Root accepts the narrow runtime binding overlay (SHA256 a3664c67f4993267faa7062ada5438e828548f70aa721e0c2c143a37677e34b3) after comparing input settings, interpreter/target paths and every archive member. The exact scratch device is mounted with 1,861,676,580,864 free bytes and the retained target copy completes. The pinned input fetch exits successfully; final receipt verification and durable prefix preservation remain operator tasks.

At 23:35:26 UTC only 795 seconds remain in the fixed window, below the existing 600-second build plus 300-second preservation reserve. Root declines to reduce that allowance merely to fit elapsed preparation. The operator preserves the verified prefix on EBS, closes preparation, verifies shutdown and accounts for this window once before a fresh execution window. No construction measurement is produced. Preparation and coordination, rather than scientific computation, consume this window; reuse the now-resolved hardware, input and package bindings to reduce the next setup cost.


## Build-space gate clarified from implementation

The durable 307,200,000-byte input passes destination SHA and size verification on EBS. The preservation script then reports 20,816,793,600 root filesystem bytes free, below its 20GiB check; preserve this failed check and the unexpected free-space decline. Root inspects the actual supervisor: the existing build-space gate checks the filesystem containing source_root, not the operating-system root. With source and target on scratch, the unchanged 20GiB gate applies to scratch. No disk growth, garbage collection or reduced threshold is needed for this layout. The operator must bind source and target to scratch and retain the EBS observation separately.


## Epoch2 stops on source archive structure mismatch

The uploaded base and overlay pass checksum verification, but mandatory extraction validation fails before tests or build: rust/types/idl is a directory in source.tar.gz while the accepted source manifest declares the symbolic link ../../idl. Root independently confirms the tar member type is directory. Earlier archive-byte checks prove transfer integrity but miss this source-structure inconsistency. Preserve that review gap and the remote failure rather than treating the package as runnable.

The operator closes epoch2 without scientific execution. The existing implementation worker owns a new immutable packaging revision, preserving engine bytes and prior artifacts. Before another remote admission, extract the corrected archive locally with the same extractor and validate every manifest file and link, plus archive membership closure. No source-validation bypass or cloud retry is authorized until that evidence is reviewed.


## Archive repair accepted after independent extraction

The corrected consolidated runtime archive is 2,438,195 bytes with SHA256 0268503aa26d73dc554a4247c1a14263fcca5c1c9019a54d92a5a36aac3734ef. Root independently extracts the full package and its source using the accepted extractor, verifies all 957 regular file sizes/hashes and the exact IDL symbolic link. The source manifest and engine file bytes remain unchanged; source archive SHA and its references are updated. Root accepts archive-repair-v4 and dispatches the existing sole operator for a new guarded 1,800-second window, keeping per-stage limits unchanged. No overlay is needed.


## Compilation concurrency limits development speed

While epoch3 build handle9744 remains live, root inspects build_command and finds explicit -j2. The observed two rustc processes match that setting on a machine with 32 physical cores and 512GiB memory. This is build-system concurrency, separate from the configured 32 construction workers. The active build, profile and deadline remain unchanged. The implementation worker prepares a local-only future revision using 32 compiler jobs with unchanged optimized profile and source bytes. Actual build outcome determines whether a further execution is needed.


## Epoch3 build reaches its cap; development path revised

The optimized two-job build terminates at its 600-second cap (600.116945 seconds, TERM -15, process group absent). It is still compiling dependencies; no construction run occurs. The scratch target contains 10,038,935,552 bytes, versus 7,330,922,496 bytes in the retained original target. Root free space is 20,910,882,816 bytes, insufficient to preserve a separate target while respecting the existing build-space floor. The operator closes the window and retains small build evidence.

Root selects persistent build storage for subsequent work: grow the exact existing EBS volume vol-08bda85902f848a75 from 100 to 160GiB through normal review under Lyon’s standing storage authorization, preserving volume properties and all files. The quoted incremental commitment is $4.80/month, subject to fresh operator verification and ledger accounting. The worker prepares a narrow durable-target path revision with the already-reviewed 32 compiler jobs. The 20GiB source-filesystem floor and science settings remain unchanged. This addresses build and cache retention costs; no construction-performance claim follows.


## First successful 100K single-owner 32-worker attribution

Epoch4 builds successfully in 139.324 seconds with 32 compiler jobs, then completes native execution in 102.505 seconds. Membership and exact vector/query-bit checks pass for all 100K current rows. Insertion takes 12.540 seconds and balancing 85.015 seconds; sampled peak RSS is 1.682GB and output is 576.562MB. The tree has 69 leaves, one internal root, 68 splits and 54,388 reassignments. This is a depth-one local-owner fixture, not internal-branch pruning or distributed evidence.

Native CPU time totals 104.812 seconds, roughly 1.02 cores on average despite 32 configured workers. Vector-provider decoding totals 0.381 seconds across 72,597 decodes; full-node point misses total 4.949 seconds. Version/current calls total 1,612,410 and location-home messages 2,821,898. These overlapping spans and counters nominate repeated ownership/version handoffs for source investigation; they do not explain all balancing wall time or establish a causal speedup. The existing worker traces this path before implementation. Raw results and root assessment are retained in epoch4/evidence and dynamic-review/100k-owner32-epoch4-assessment.json.


## Immutable handle batching passes local review

Root accepts the sole adapter change after lock-order, bounds and visibility review. Identical 16-request single-owner tapes reduce home and owner dispatch from 16 to 1 each; mixed 17-request tapes reduce each from 17 to 3. Exact handles/vector bits, duplicate order, transfer contention and opposite lock order pass. Connected tree/current-old reader, transfer and unchanged paged-path regressions pass. The operator prepares a coherent package for the same100K experiment; no remote admission occurs before whole-package extraction validation. Larger lock scope may change schedules, so the next build must compare work as well as time.


## Comparison start rejected by AWS capacity

AWS rejects the exact authorized instance start with InsufficientInstanceCapacity before upload, build or science. The sole operator verifies terminal instance state and retains the failed start receipt before retiring its guard. No alternate instance or immediate operator retry occurs. Root requests a read-only compatibility and price assessment for the same stopped instance using r6i.16xlarge and retained EBS, without mutation. A different instance type or storage path would confound elapsed comparison with epoch4 and must be disclosed; fixed-request dispatch correctness remains separately established.


## Epoch6 rejects an unchanged executable for changed source

The same-hardware paired attempt passes 25 Linux checks, but its nominal batched build completes in 7.43 seconds and emits the exact retained baseline executable SHA256 afb126d7e5933d86e33b29ee3f50e3c687cea0dcd1b50c5e1d74f035ab1e4ea6. The operator rejects this as evidence of the changed implementation, terminates the exact pair controller and collects evidence before shutdown. No paired performance conclusion is valid.

A changed source manifest alone does not prove the compiler rebuilt the changed source. Preserved archive timestamps and retained Cargo fingerprints are the leading hypothesis. The worker owns a local build-validity correction with a real incremental-build fixture, preserving source bytes and retained artifacts. Require explicit invalidation and compiler freshness evidence before another remote attempt; do not delete caches or bypass provenance checks.


## Build reuse reproduced and repair checked

A real dependency-free Cargo fixture reproduces the failure across separate source directories sharing one target: changed bytes with preserved timestamps produce the old behavior and fresh=true. Refreshing the validated changed benchmark and consuming entrypoint produces fresh=false and the new behavior. Root inspects the repair and independently passes seven focused tests. The source archive bytes remain unchanged and retained targets are preserved. The sole operator prepares the next paired admission locally; remote execution awaits coherent gate review. Epoch6 is API-confirmed stopped and its compute bound is recorded once by the operator.


## Queue cache boundary becomes a concrete next hypothesis

Read-only source and baseline inspection finds 211,079 work marks, 29,632 flag-page reads and 29,874 durable queue commits for 746 scheduled leaf jobs. The admitted queue cache holds one 64-slot flag page, while the implementation defaults to four. The 70-node tree necessarily spans at least two pages; sparse or retired IDs can increase the working set. Cache misses synchronously flush under home and admission locks. Existing configuration can test four bounded pages without source changes, but queue-specific elapsed attribution is absent and no large-tree inference follows. Select a treatment only after the active handle-batching pair is assessed. See dynamic-review/immutable-handle-batching-v1/next-decision.md.


## Epoch7 batching comparison preserves adverse evidence

Both epoch7 arms pass 100K current membership and exact vector/query-bit checks. Baseline takes 118.966 seconds (102.314 balancing); batched takes 159.525 seconds (142.592 balancing). Handle-related Other callbacks fall from 960,873 to 111,436, while Current callbacks remain near 1.59 million and their cumulative caller span rises from 39.248 to 65.396 seconds. CPU totals fall from 105.988 to 100.949 seconds; average CPU use is below one core in both arms. Queue commits remain high at 25,602 and 26,995. Trees differ (67 versus 70 leaves; 67 versus 69 splits), so elapsed differences are not a clean causal estimate. This is adverse end-to-end evidence, not an accepted batching speedup. Independent interpretation precedes the next bounded single-owner experiment. See dynamic-review/100k-owner32-epoch7-assessment.json.


## Queue churn removed without material balance-time gain

The baseline binary with a four-page queue cache passes all 100K membership and exact-bit checks. Only three pages are used. Queue commits fall from 25,602 to 236 and flag reads from 25,372 to 3 versus epoch7 baseline. Balancing remains 102.218 seconds versus 102.314 seconds; total process is 115.215 seconds and CPU105.527 seconds. Different stochastic trees/windows limit timing attribution, but this result weakens queue publication as the dominant balance cost. Retain the bounded four-page policy for this fixture and stop cache-cap sweeps. The next diagnosis targets remaining synchronous eligibility/version handoffs with fixed decisions or within-execution attribution.


## Fixed-decision eligibility mechanism selected

After queue churn is removed without balance-time improvement, root selects the independent proposal for an opt-in bounded owner-local eligibility prefix and local fixed-decision oracle. It preserves immediate global reassignment and reacquires suffix versions after each action. The tests must expose discarded predicate work and contention, including action-every-row behavior, rather than infer speed from message counts. No new cloud run is admitted. The earlier adverse handle-batching result remains separate and must not be silently included in a claimed improved treatment.


## Eligibility-prefix mechanism accepted locally

Root reviews the two-file implementation, fixed oracle and connected evidence. No-action64-row Current/home dispatch falls64 to4 without extra predicate work; action-heavy and alternating-owner negative controls preserve their expected limits. The action-every-row case requests904 rows but consumes and evaluates64, so lookahead overhead remains visible. All identity guards end before reassignment, and suffix versions are checked again after mutation. Native integration must expose eligibility and handle resolution separately and exercise the enabled full-record path before remote admission. This is local mechanism evidence, not measured construction speedup.


## Native eligibility integration accepted

Root accepts separate handle and eligibility policy wiring after source review, enabled full-record ownership-move/split/saved-generation checks and native admission evidence. The sole operator prepares an immutable four-file source update and one optimized binary for a100K off/on eligibility comparison. Both arms explicitly select scalar handles and the four-page queue cache. Requested/consumed prefix checks and predicate work remain distinct from legacy scalar-helper counters. No remote execution is admitted until package/controller review.


## Epoch9 eligibility comparison closes

Epoch9 completes both100K arms using the same verified newly compiled binary. Eligibility off takes97.375 seconds (104.310 CPU seconds); on takes111.590 seconds (95.354 CPU seconds). Balancing takes83.977 versus98.687 seconds. Both pass complete membership and exact vector/query-bit checks with zero missing-node counters. The on arm consumes929,771 prefix checks, requests1,512,815 rows and discards583,044 suffix rows; scalar version-request counters exclude these prefix checks. Trees differ: off has70 splits,2 merges and57,287 reassignments; on has68 splits,1 merge and53,751 reassignments. This is adverse wall-time evidence, not an accepted speedup. Keep prefix disabled for the next baseline and investigate repeated leaf cleanup and authority synchronization using retained evidence and fixed-work local tests. AWS confirms shutdown; compute upper bound is $0.476364420896, recorded once by the operator, with no new storage commitment. See dynamic-review/100k-owner32-epoch9-assessment.json.


## Investigation workflow and reference-first design

Lyon requests a continuously reprioritized experiment record and closer alignment with the fast unsharded implementation. INVESTIGATION-QUEUE.md now records ranked questions, completed experiments, interpretations and decisions. RESEARCH-GOAL.md makes this policy persistent beyond single-owner diagnosis. Meaningful deviations require evidence, an explicit testable theory or a necessary sharding constraint. No cloud action or spending is added.


## Cleanup reference comparison

The frozen optimized reference performs cleanup under a target-leaf lock and checks versions locally. The active dynamic path copies memberships and holds identity stripes across home and owner validation callbacks before a second compaction callback. The direct single-owner treatment targets those unnecessary routing steps; it does not remove necessary concurrency protection or generalize across ownership transfers. Existing batching/transfer regressions will be reused. Source findings and required invariants are recorded in dynamic-review/cleanup-reference-comparison.json; no performance result is claimed.


## Epoch10 cleanup result

Epoch10 completes the100K cleanup comparison with one verified optimized binary. Off/on elapsed time is119.121/48.887 seconds and CPU time109.491/76.023 seconds. Balancing takes106.248/35.735 seconds; insertion remains8.470/8.788 seconds. Both arms pass100K membership and exact vector/query-bit checks. Cleanup snapshot/validation traffic disappears in the direct path, but work differs:39.344M versus32.142M scanned rows and69 versus66 splits. Aggregate cleanup identity waits fall from199.529 seconds to zero; concurrent waits overlap and are not exclusive wall time. The on arm records one add_missing_nodes event, so independent source/retry diagnosis remains open. The observed improvement is promising single-owner evidence, not a general2.4x scaling claim. No additional run is admitted. See dynamic-review/100k-owner32-epoch10-assessment.json.


## Cleanup accepted; bounded cache implementation selected

The cleanup assessment is accepted for the bounded nonpaged single-owner baseline: enable direct cleanup in subsequent diagnostic experiments, while preserving the source default and multi-owner fallback. The focused regression proves a failed registration leaves the current version intact and a subsequent real reassignment navigates to a replacement root with one current copy. It does not recreate the exact full-record race or force Retry within that same reassignment invocation; source control flow supplies the retry-loop ordering evidence. The runtime event remains preserved with this limit. The next active change is an explicit384 MiB per-owner decoded-vector payload policy, preserving default32-row behavior and all checksum/observation code. No cloud run is active.


## Bounded decoded-cache implementation accepted

Root independently accepts the explicit payload policy after reviewing overflow-safe per-owner/aggregate admission, unchanged FIFO and full-handle semantics, fixed-request/eviction/error/concurrent-root tests, connected saved generations and native configuration cases. Four frozen file hashes match the checkout; two runtime changes plus two test-only changes form the compiled source closure. The default remains32 vectors and the admitted implementation establishes no construction speedup. Immutable package preparation is active; final package review precedes any cloud run.

## Decoded-cache pair admitted

Root independently verified the final package hashes and reran full archive validation: 957 files, the IDL link, and exactly four accepted replacements relative to the cleanup experiment. The sole cloud operator is dispatched for one fresh bounded 100K-vector, 32-worker, single-owner comparison of the default 32-vector decoded cache against a 384 MiB payload allowance. Both arms retain local cleanup, scalar handles, disabled eligibility batching, and queue cache capacity four. The window is capped at 1,800 seconds and $2.016, with fresh identity and independent absolute shutdown guards required. Results and spending remain pending; dispatch is not evidence that compute has started.

## Epoch11 cache result and decision

Epoch11 completes both 100K-vector arms with full membership and exact vector/query-bit checks. Default versus 384 MiB cache elapsed time is 54.012 versus 40.709 seconds; balancing is 40.465 versus 26.980 seconds and CPU is 80.071 versus 62.152 seconds. Reader opens fall from 377,590 to 6,475 and copied vector bytes from 3,047,743,488 to 279,220,224. Logical gets differ (995,416 versus 955,088), as do splits (68 versus 65), reassignments (54,836 versus 53,071) and leaves (69 versus 66). Both trees have depth one and zero missing-node counters. The result supports retaining an explicit bounded cache for this diagnostic, with timing qualified by unequal work and fixed run order. It establishes neither cache-capacity-crossing behavior nor large-scale economics. Structural synchronization becomes the highest-priority implementation question; further cache sweeps are deferred. See dynamic-review/100k-owner32-epoch11-assessment.json.

Epoch11 lifecycle closure is retained in its final receipt. Compute is $0.32676874944, entered once by the sole operator, with no new storage. The next local implementation removes the global mutex from a bounded single-owner reassignment transaction only after destination capacity, dirty credit and version/append atomicity are secured. The multi-owner path remains unchanged pending its own protocol.

## Reassignment comparison prepared while implementation continues

The implementation worker is confirmed running. The next experiment proposal keeps the accepted 384 MiB cache and direct cleanup fixed while comparing only destination-local reassignment. The retained 100K fixture already supplies more than 53,000 reassignments across dozens of leaves; it answers the local synchronization question without requiring a larger tree. The proposal requires capacity, dirty-credit, retirement and saved-generation tests before review and remote admission. It does not authorize execution. See dynamic-review/local-reassignment-experiment-proposal.json.

## Local reassignment test setup failures retained

The implementation compiles. Initial focused tests expose two fixture problems: a private helper call and full-record initialization before its required dirty scheduler. The implementation worker is correcting fixture setup using the accepted initialization order; neither failure is performance evidence or a demonstrated transaction failure. The raw attempts remain under dynamic-review/local-reassignment-transaction-v1. The comparison controller passes independent review and four focused checks, but implementation acceptance and cloud admission remain pending.

## Reassignment concurrency fixtures pass

The fifth retained focused run passes seven tests. Coverage includes held dirty credit across concurrent flush, exhausted admission before membership mutation, destination-local progress while another thread holds the structural mutex, retry and stale-version handling, competition for the final owner slot, colliding identity requests, post-append unwind poisoning and connected split/saved-generation checks. Expected injected panics are caught by their tests; they are not uncaught run failures. Native admission and the existing connected regression remain in progress, so source acceptance and remote admission are still pending. Earlier failed fixtures remain preserved.

## Destination-local reassignment accepted for packaging

Root reviewed the frozen five-file change, atomic membership operation, lock ordering, held dirty admission, publication-failure guard and focused test bodies. Eight focused tests, the existing connected regression and six native cases pass. Native smoke establishes wiring and integrity; the deterministic connected fixture supplies successful-registration coverage. The default and multi-owner paths retain structural serialization. The sole operator is preparing the exact immutable package over epoch11; final package review remains before any cloud admission. No performance improvement is claimed.

## Epoch12 reassignment pair admitted

Root reran complete archive validation and verified the five frozen replacements and package/controller bindings. The sole operator is dispatched for one fresh bounded off/on pair under the reviewed controls. The relevant 100K fixture supplies tens of thousands of reassignments across dozens of leaf destinations. This experiment asks whether removing unnecessary whole-tree serialization reduces construction work/time; it does not establish a distributed transaction protocol. Fresh guards and identity remain mandatory, with no retries or deadline extension.

## Epoch12 result and priority decision

Epoch12 passes complete100K membership and vector/query-bit checks in both arms. Local reassignment performs26,615 successful transactions with no reported errors, retries or missing-node events. Structural acquisitions fall25,331→168, but elapsed time is39.208seconds off versus40.963on; balancing is25.426versus27.347seconds. CPU also increases61.686→63.230seconds. Trees have equal final68-leaf shape but different split/merge/scrub work. Retain the mechanism as an opt-in and keep it disabled in the performance baseline because no build-time improvement is established. Prioritize a reference-like larger balancing interval before a broader split-lock redesign. See dynamic-review/100k-owner32-epoch12-assessment.json. The worker is validating staging and split admission locally; no subsequent cloud experiment is admitted.

## Cadence comparison controls selected

The next proposal changes only the balancing interval: 4,096 versus 100,000 insertions. Both arms use the same 100K split-row and 64 GiB modeled reservation ceilings, while bounded staging, cache, cleanup and disabled local reassignment remain fixed. This prevents changing admission limits from becoming an additional arm difference. The reservation ceiling is separate from actual memory and the existing RSS guard. Local correctness and controller review remain pending; exact epoch12 binary reuse is preferred if runtime source is unchanged. No cloud execution is admitted.

## Cadence fixture exposes block-storage failure

The first connected cadence fixture terminates with exit101. Its row-cap and byte-budget rejection cases preserve the attached8,194-row source. The admitted case hits a debug subtraction-underflow panic at single_column_storage.rs:228. The worker is adding test-only phase markers and collecting one backtrace to distinguish balancing from publication. This is a correctness diagnosis, not evidence of cadence performance. Failed artifacts remain under dynamic-review/reference-cadence-validation-v1; remote admission is withheld.

## Posting-block failure scope identified

Column markers show that scalar versions finish saving. The subsequent encoded posting write, containing8,817u32 words after a9,667-word posting, triggers the ordered block split underflow. The save path awaits writes sequentially and the ordered writer retains its own mutex, so this observed path does not support a concurrent-mutation explanation. The worker owns a minimal storage reproducer and exact confirmed accounting correction, preserving the original cadence failure. Epoch12 binary reuse is not admitted if compiled blockstore code changes; the existing no-build controller remains a planning artifact with readiness false.

## Correction: split item count causes the underflow

Exact numbered source identifies line228 as total item count minus scanned item count, not value bytes. When the scan excludes a final oversized value from the left half, it subtracts prefix/key/value bytes but leaves the item count incremented. The resulting left/right counts disagree with stored items and a later split underflows. The worker is constructing a sequential oversized Vec<u32> reproducer before correcting the excluded-item count. This supersedes the earlier provisional value-size diagnosis; those observations remain preserved.

## Count fix exposes saved-posting lookup failure

The minimal ordered and unordered oversized-Vec<u32> split regression fails before the correction and passes after decrementing the excluded final item count. The connected cadence fixture now completes balance and save, then fails census with missing new64 posting. The correction is therefore insufficient for end-to-end acceptance. The worker is extending the minimal reproducer through commit and reopen to isolate the saved block lookup path. All failed receipts remain, including test-green, whose name does not indicate an end-to-end pass. No cloud admission follows.

## Minimal read-back reproducer confirms oversized-block loss

A fresh ordered blockfile containing the two traced posting sizes reproduces the missing second key after reopen. The worker identifies a second split of an indivisible single-value block: an empty original and replacement share a lookup boundary, and cleanup can remove the replacement entry. The narrow proposed correction keeps a one-value block in place instead of producing extra blocks. Ownership includes ordered_block_delta.rs for that runtime fix and ordered_blockfile_writer.rs for fresh/forked read-back tests; sparse-index semantics remain unchanged. The original cadence census remains the end-to-end acceptance check.

## Oversized posting read-back regression passes

The fresh/forked minimal roundtrip passes after retaining zero/one-item deltas under their existing block identity. Both exact posting payloads are readable and the old root remains unchanged. This complements the excluded-item count correction. The worker is checking ordinary splitting and large-value regressions before rerunning the original connected cadence fixture. Sparse-index code is unchanged. The runtime wrapper is being narrowed to the exact three changed blockstore paths, including one test-only file; source binding and cloud admission remain pending.


## Cadence storage validation and next decision

The cadence correctness fixture now passes all 8,194 current vector checks and preserves the old snapshot. Two narrow storage fixes correct split item accounting and preserve the identity of an indivisible oversized posting block. Independent source review accepts the frozen four-file change for packaging; a fresh optimized storage dependency and benchmark build is required. The next experiment compares balancing every 4,096 versus 100,000 insertions on the same 100K vectors with 32 workers. Both arms use identical split admission and cache settings, with local reassignment disabled. The sole operator is preparing the package; final package review and fresh shutdown guards precede cloud execution. Epoch12 remains closed and API-confirmed stopped.

The local fixture proves admission rejection preserves attached state, successful balancing and save preserve current vector and posting-code bits, and the prior saved generation remains readable. Five focused storage regressions and the native compile pass. Both failed attempts remain retained. This 32-dimensional fixture does not establish 100K throughput or memory feasibility. The next measurement remains cadence; broader lock redesign and cache sweeps remain lower priority. No additional cloud spending occurs during this local validation. See dynamic-review/reference-cadence-validation-v1/HANDOFF.json and root-review.json.


## Cadence package admission

Root independently verifies the 957-file source closure, preserved IDL symlink and exact four reviewed changes over epoch12. The final package is 50e2da7709b0033ea77329d19c960b7a20c8cc4e40a10848d406e916f70eb5be. Twelve packaged operator tests pass. The sole operator is dispatched for one fresh 1,800-second/$2.016 maximum window with new shutdown guards, fresh optimized dependency and benchmark compilation, and no retry or extension. Lifecycle and measurement evidence remain pending. In parallel, the existing implementation worker performs a read-only check of the optimized reference entrypoint and settings needed for a matched comparison; no additional benchmark is admitted.


## Epoch13 cadence result and decision

Both 100K-vector arms pass complete current membership and exact vector/query-bit checks. Balancing every 4,096 insertions takes 46.531 seconds versus 59.202 seconds for one 100K insertion interval. Insertion takes 8.623 versus 4.273 seconds; balancing takes 33.523 versus 50.635 seconds. CPU falls from 65.485 to 58.151 seconds. Larger cadence alone is not an accepted elapsed-time improvement.

The long interval has 75 splits and 43,378 scrubs versus 69 and 28,625 for short intervals. Structural lock holding rises from 7.839 to 35.809 seconds; peak distinct subtree workers falls from 22 to 15. These spans overlap and trees differ, so they motivate inspecting split preparation rather than proving its causal cost. The short arm reports one missing-node add event; final integrity passes, but the exact retry sequence is not witnessed. Preserve this adverse counter.

Keep 4,096 as the measured dynamic performance baseline for now. First prepare the genuinely optimized buffered reference with matching input and construction settings, then use that comparison to guide split preparation changes. The implementation worker owns the isolated reference adapter. API shutdown and spending closure remain pending operator confirmation. See dynamic-review/100k-owner32-epoch13-assessment.json.


Epoch13 closure: the final lifecycle receipt confirms API stopped and watchdog retirement after proof. Compute upper bound is $0.41803263264, already recorded once by the operator; new storage commitment is zero. All retained roots remain intact.


## Optimized reference validation gate

The isolated retained-input adapter passes three focused tests covering exact input bits/order, input rejection, buffered insertion, a real split, packed navigation and old/current saved-vector readback after writer drop. Native harness integration remains under validation. Root review identifies that saved-vector hashes prove storage contents but not unique tree membership. Existing root-reachable validation compares ID sets and therefore hides duplicate current postings. The worker adds duplicate detection only to benchmark validation; the indexing kernel remains unchanged. The reference controller must enable existing pre/post posting validation and require the additional uniqueness check before reporting a comparable build. One 100K reference run is proposed against retained epoch13 long-cadence evidence; no cloud window is admitted.


## Optimized reference package admitted

Root verifies all 814 packaged source files and the IDL symlink, the nine accepted changes, preserved optimized writer and exact controller bytes. Six controller tests pass independently. Package c368a1e489e6540418353985309cef73d2732aced79f7b3d35b8e3bebb1216f0 is admitted for one fresh guarded 100K reference run, capped at 1,800 seconds/$2.016. Source and saved-membership gates require fresh optimized compilation, 100K unique current postings with zero duplicates before commit and after reopen, and matching saved vector bits. No retry, extension or additional science is admitted. The operator owns lifecycle and spending; results remain pending.


## Reference epoch1 upload rejected and instance stopped

Automatic approval review rejects the frozen reference package upload before execution. The operator quotes the human blanket authorization but the reviewer treats it as untrusted agent context; the justification does not name UPLOAD-AUTHORIZATION.json. No bytes, build or science execute. No alternate route or retry is attempted. API shutdown is confirmed and watchdog retired after proof. Compute upper bound is $0.085129282672, recorded once with zero new storage commitment. Root requests a direct reply for a subsequent normal-review attempt in a new guarded window. Retain exact rejection and attempted justification in fleet/matched-reference-runtime-v1/epoch1/upload-rejection.json.


## Local work while reference upload approval is pending

The reference machine remains API-confirmed stopped and its upload question remains pending. The existing implementation worker is assigned a default-off prepared-split prototype and fixed-work correctness fixtures. This uses the source-confirmed difference from the optimized reference: vector fetch, clustering and quantization currently hold the dynamic structural mutex. The prototype must retain source visibility, bound scratch memory and revalidate before detach; it does not establish a speedup or admit another cloud experiment. The frozen reference package remains unchanged.


## Prepared split preliminary review finding

Root inspection finds that rejecting a changed prepared snapshot can return after consuming its scheduling item. A version-only change need not mark the source dirty, so preservation of source bytes alone is insufficient. The worker adds explicit dirty-work re-queuing before return and extends the stale-version fixture to require queued work and eventual balancing with no pending leaves. The validation comparison also temporarily clones source rows and handles; the two admitted scratch reservations require explicit simultaneous-allocation accounting before source acceptance. Local tests are in progress; the feature remains default off and no remote experiment is admitted.


## Prepared split first correctness result

Four focused tests pass: paused preparation preserves source visibility while disjoint structural work progresses; fixed prepared groups preserve current and old saved generations; current-version-only and centroid changes reject before detach and re-queue to eventual balance completion; failure of the second scratch reservation releases the first charge before copying or detach. The receipt is dynamic-review/prepared-leaf-split-v1/tests-second.stdout. This establishes the exercised mechanisms, not construction speedup, all possible mutations or physical distribution. Explicit allocation accounting and frozen independent source review remain pending; the policy remains default off.

## Prepared split prototype: local review accepted

Root verifies all six source hashes and all handoff evidence. Five focused tests and one default-path regression pass, and the native source check passes. Acceptance is local and default-off: source visibility, stale revalidation with explicit requeue, modeled reservation admission and saved-generation checks are covered. Performance, physical distribution and actual flush-I/O failure remain unmeasured. The existing worker prepares native wiring and a 100K/32-worker pair at cadence 4,096. The reference upload remains blocked pending its direct reply; the closed window stays closed. This local work adds no cloud compute or storage commitment.

## Prepared split native wiring accepted

Root verifies the frozen two-file native integration and every evidence hash. Three valid native configurations preserve all 65 vector bits and exact query-distance bits; five incompatible configurations reject before output. Parent replacement and physical posting reorder reject stale preparations and preserve 64 current and old saved vectors. The paired specifications differ only in the feature flag. The proposal keeps 100K vectors, 32 workers, cadence 4,096, 64 GiB split scratch and 4 GiB output per arm. Local runtime packaging is next; no cloud execution or speedup is established. The reference measurement retains priority and its upload rejection remains unresolved. No new cloud compute or storage commitment is incurred. See dynamic-review/prepared-leaf-split-native-v1/root-review.json.

## Routing centroid investigation

The source comparison identifies routing work that the document-vector cache cannot remove: full-record navigation clones centroid and child arrays on cache hits, decodes complete routing records on misses, and fetches selected candidates again. Posting rows remain separate from these routing records. The optimized reference scores borrowed slices from shared packed child centroids. Retained node-miss timers combine multiple consumers and overlap, so they do not establish the cost attributable to navigation or a speedup. A 100-child, 768-dimensional packed centroid array occupies 307,200 bytes before metadata and exceeds the current node-record/cache limits. Any prototype therefore needs separately bounded versioned chunks and explicit centroid-update visibility. Keep reference and split measurements ahead of this candidate; defer implementation until their evidence is available. See dynamic-review/packed-centroid-readiness-v1.md. No source changes, tests or cloud spending result from this investigation.

## Prepared split runtime review

The frozen local runtime package passes root review: all 957 source files, the sole IDL link and packaged operator hashes match, and an independent rerun passes all 15 controller tests. The pair uses one newly compiled binary and changes only the prepared-split flag. Shared absolute deadlines and existing resource limits remain enforced. The controller requires 1,080 seconds remaining within a 1,200-second window; slow setup rejects admission rather than extending it. The package remains local and cannot replace or bypass the rejected reference upload. Reference measurement is still first priority; no new compute or storage commitment occurs. Evidence: dynamic-review/prepared-leaf-split-runtime-v1/root-review.json.

## Local work selected during upload block

The next available question is whether reference-like packed scoring preserves exact traversal results while avoiding per-child node materialization. The existing worker owns a test-only fixed-tree comparison using the actual current traversal as baseline. This advances the queued mechanism investigation without changing frozen benchmark packages or adopting an unmeasured production design. The fixture must separate logical fetch/copy counts from real provider I/O and account for bounded scoring chunks. Connected update/publication visibility remains a prerequisite for adoption. Cloud spending remains unchanged.

## Direct reference upload approval received

Lyon replies, “i give you upload approval,” to the pending reference-upload blocker. UPLOAD-AUTHORIZATION.json records this direct reply with the reviewed package and sandbox destination. The sole operator may submit it through normal review and establish a new guarded window for the already reviewed reference experiment. The rejected window remains closed; approval does not imply an upload or benchmark has executed.

## Reference upload rejected after direct general approval

Normal review rejects the epoch2 upload before execution. The reviewer recognizes Lyon’s direct “i give you upload approval” reply but requires trusted authorization to specify both the source package and destination. The submitted justification includes the exact package, instance, account, path and saved authorization file. Root asks for the exact complete payload-and-destination confirmation; the sole operator stops the instance without retry. No upload, build or science occurs. API closure and spending reconciliation remain pending the operator receipt.

## Rejected reference window closed; exact approval available

The epoch2 receipt confirms API-stopped state, retirement of the local watchdog after proof, zero uploaded bytes and no build or science. Its compute upper bound is $0.08673355488, recorded once by the operator, with no new storage. Lyon supplies the exact payload-and-destination authorization. The sole operator prepares a wholly new epoch3 window under unchanged reviewed protocol; no runtime success is yet established.

## Reference upload succeeds under exact authorization

The sole operator reports epoch3 normal upload approval and successful transfer of the unchanged reviewed reference package to the explicitly authorized path. The new absolute deadline is 1792296155.6913762, with local watchdog 32600 and the independent remote stop timer active. Local preflight receipts record the retained input and available memory/disk. Package verification and optimized build follow; upload success is not a benchmark result. No duplicate operator or split run is dispatched.

## Packed centroid fixture result and decision

The fixed 132-node, 768-dimensional fixture passes exact ordered ID and distance-bit parity for 32 queries across cold, repeat and eviction passes. Each scorer performs 4,192 distance evaluations per pass. Packed access reduces full-record requests from 4,672 to 480, but adds 544 chunk-file reads, records zero cache hits and slightly increases returned payload. All three internal nodes are expanded; this is a mechanism test, not selective-pruning or quality evidence. Root source review and evidence hashes pass. Keep production integration deferred: actual provider cost and connected centroid publication remain unproven, and the pending reference/split measurements have priority. No cloud spending is attributable to this local fixture. See dynamic-review/packed-centroid-fixture-v1/root-review.json.

## Matched reference completes with a construction gap

The optimized reference completes 100K vectors with 32 workers and one 100K checkpoint. Both precommit and reopened membership checks report 100,000 unique current rows and zero duplicates; the saved vector-bit digest matches dynamic epoch13. Displayed insertion is 0.049 seconds and balancing 13.85 seconds, versus dynamic long-cadence 4.273 and 50.635 seconds. Both finish with 76 leaves and 75 splits, but reassignment work differs. Reference navigation reports about 11.8K missing-node observations under its concurrent semantics; preserve this distinction from the dynamic complete-view contract. These are construction measurements, not query-quality evidence. The gap supports measuring the already prepared split change next, after full result/lifecycle review and fresh admission. Shutdown and final cost receipts remain pending. See dynamic-review/matched-reference-epoch3-assessment.json.

## Prepared split upload rejected before execution

Normal approval review rejects the prepared-split package despite the quoted standing and recent general upload authorizations. It requires direct authorization naming this package and destination. Zero bytes transfer; no build or science runs. The exact confirmation question is pending and the sole operator is closing the instance/window. The next useful local task reconciles reference and dynamic insertion timer boundaries and buffering, using the completed matched reference evidence. No source change or competing cloud operation is authorized by that investigation. See fleet/prepared-leaf-split-runtime-v1/epoch1/upload-rejection.json.

## Insertion boundaries and batching decision

The insertion comparison includes different work. Dynamic insertion includes 0.496 seconds of nested input rereading/decoding/hashing and 13 synchronous durable staging calls totaling 2.297 seconds. Reference insertion starts after loading resident vectors and defers durable publication to commit. Dynamic insertion also sends 1,568 small batches to the owner, whereas the reference joins and regroups worker results before appending to the initial root. Worker flush durations overlap and cannot be treated as exclusive elapsed time. Both initial insertions target a single root; packed child-centroid access cannot explain this initial gap. Promote bounded stage-level regrouping above additional vector lookup/cache work as the next construction candidate after the pending split comparison. Preserve durable handles, atomic membership and explicit additional buffer admission. Source investigation and proposal: dynamic-review/matched-insertion-gap-v1.md. No performance gain is yet measured.

## Bounded insertion regroup prototype selected locally

The completed timer analysis supports an isolated local implementation question: can workers join and regroup one already admitted staging chunk before an atomic destination append, eliminating repeated small destination transactions? The existing worker owns a default-off single-owner prototype and exact-order, membership, capacity, failure and saved-generation tests. Additional delta and overlap memory require explicit checked admission; durable handles and public phase visibility remain unchanged. This local work does not alter the frozen prepared-split runtime or bypass its pending upload approval. No new cloud spending is authorized or incurred by the local task.

## Regroup failure-path investigation

The first fixed-order regroup fixture passes and demonstrates fewer destination appends, but two failure-path fixtures fail. Worker diagnosis identifies fixture assumptions: pin exhaustion precedes the intended dirty-credit check, and a postappend assertion reads a deliberately poisoned live view. Separately, source review identifies a real correctness requirement: once any destination append succeeds, a later destination failure must poison the entire stage. The worker extends the guard across the destination loop and adds a two-destination regression. These corrections are not yet independently reviewed; no prototype acceptance or performance claim follows. Original failures remain retained under dynamic-review/stage-regroup-v1.

## Regroup failure-path tests advance

The implementation worker reports six focused tests passing, covering failure after an earlier destination append, pending reservation release, preappend rollback, concurrent flush and preserved old readers. The same failure guard must also span multiple durable staging chunks within one public insertion phase; that extension and the final default regression remain in progress. This is an intermediate test result, not independent acceptance or a measured performance gain. The active plan now explicitly records that the rejected split-upload window is closed and no cloud execution is active. Spending is unchanged.

## Insertion regroup handoff reaches review

The frozen prototype passes seven focused tests, the default connected regression and native compilation checks. Root verifies all frozen and working-source hashes and evidence hashes, then inspects deterministic regrouping, the single owner callback, identity-lock ordering, reservation cancellation and phase-wide poisoning. Acceptance remains pending a concrete derivation of the temporary-allocation allowance and confirmation of external mutation-hook admissibility. No native wiring, remote execution or performance gain is established. The split runtime stays frozen and cloud compute remains stopped.

## Regroup review finds a hook compatibility defect

The new insertion path omits the callbacks used by an external owner-view hook. Allowing that hook would leave its view stale for later balancing, even though the hook cannot mutate through callbacks that never execute. The worker will reject this combination both when enabling the policy and before insertion, with a focused regression. Independent acceptance remains pending this correction and the explicit temporary-memory derivation. The ranked investigation queue is reconciled with the completed reference and packed-centroid results; split measurement remains first remote priority and regrouping remains the available local priority. No new cloud spending occurs.

## Revised regroup prototype accepted locally

Root verifies the revised source and evidence hashes, reviews hook rejection and the explicit allocation derivation, and accepts the default-off single-owner prototype for native diagnostic wiring. Nine focused tests pass; the unchanged default connected regression and native compilation checks pass. The modeled stage allowance charges 96,010,240 bytes at 8,192 rows, 768 dimensions and 112 code bytes, separately from worker scratch and owner residency. This is a pinned-toolchain allocation model, not an RSS bound. No runtime speedup or distributed behavior is established. The next step is one bounded off/on proposal and native policy validation; the pending split upload remains the first remote priority. Spending is unchanged.

## Regroup native integration accepted

Root verifies all source/evidence hashes and the two-file diagnostic delta. Three valid native configurations preserve saved vector and query-distance bits; nine incompatible or under-budget configurations reject before output. The paired specifications differ only in output paths and the regroup policy. The reviewed proposal uses 100K vectors, 32 workers and cadence 4,096, with a 1,200-second window and $2 compute cap. Runtime source/binary bindings and shutdown guards remain prerequisites. The sole operator may prepare an immutable package locally; no cloud execution or speedup is established.

## User requests stopping after current experiment

Lyon requests stopping once the current experiment is complete. Root stops the newly assigned publication investigation and limits the sole operator to its already active local regroup package preparation. No new cloud window or subsequent experiment is dispatched. The upload blocker remains unchanged; after the current handoff and review, pause at that boundary if execution cannot proceed.


## 2026-10-19: Design, plan and findings consolidated

Lyon authorizes document consolidation while implementation and benchmarks remain paused for architecture discussion. DESIGN.md now defines the intended shared upper tree and machine-owned subtrees, including postings. CURRENT-PLAN.md owns the ranked future-work queue and milestone exit criteria. findings/README.md indexes topic records covering measured gains, adverse results, implementation gaps, prepared experiments and source-specific evidence. OPERATIONS.md separates authorization, spending and lifecycle navigation from research priorities.

The old overlapping design and queue entry points redirect to those resources. The source documents are copied byte-for-byte into archive/2026-10-19-document-consolidation with a hash manifest. Existing reports keep their content with a current-plan notice. Raw benchmark data, runtime/source packages and receipts remain in place. The research goal and continuation state point to the consolidated resources and record the architecture-discussion pause. The intended design must be connected before single-machine performance optimization resumes; older local-owner gains remain scoped evidence.

This task changes documents only. No cloud execution, implementation change, new spending commitment or fresh AWS inventory occurs. The spending ledger is unchanged. Runtime stopped states remain observations at their receipt times.

Documentation validation checks 159 local links with no missing targets, verifies all 11 archived document hashes, confirms that all four retained report bodies are unchanged after their notices, and parses the reconciled continuation state successfully.


## 2026-10-19: Lyon specifies the next development sequence

The agreed sequence is to answer questions about the implemented design, develop a new design together (potentially using /grill-me), implement it, establish benchmark results, and enter a repeated benchmark–identify improvement–implement–benchmark–keep or reject cycle. README.md records the approach. The plan, design context, goal navigation and continuation state reflect that understanding comes first and design choices remain open for the new discussion. Each experimental result and keep/reject decision belongs in findings, followed by queue reprioritization. Implementation and benchmarks remain paused; this update changes documents only.

## October 19, 2026 — Historical implementation snapshot pushed

Lyon requested a historical branch before redesign. Commit `e55794b8ec2fbff8cb3cd74952d292fed24bfaad` is pushed to `chroma-core/chroma` on `codex/sharded-spann-historical-2026-10-19` using verified lyon-chroma author, committer and GitHub authentication. A separate checkout at `/private/tmp/chroma-sharded-historical-20261019` restores 1,979 missing tracked files from the preserved staged index and overlays all 251 surviving source files, including 39 new source files. Exact source hashes, original-index preservation, file inventory and clean checkout are verified. The snapshot excludes generated bytecode and the duplicate retained reference source tree. Implementation and benchmarks remain paused; no build, benchmark or cloud spending occurs. Receipts: `dynamic-review/historical-snapshot-20261019/preparation.json` and `result.json`.

## 2026-10-20: Single-shard implementation authorized

Lyon authorizes implementing the agreed design, admitting newly produced balancing jobs immediately, benchmarking one shard, and then stopping. Undecided multi-machine operations stay explicit stubs. An isolated checkout under the approved temporary source root uses optimized version-free reference commit 14dc9698 as its base. Resident-node locking and persisted four-bit reader centroids are implemented but await consolidated compilation; typed continuous scheduling and protected structural publication are under validation. No performance claim is established. Fresh read-only sandbox checks find both approved instances stopped. No instance starts, uploads or new compute spending occur during preparation. The active plan records the matched FineWeb100K comparison and the requirement to review correctness before remote admission.

## 2026-10-20: Recover after application crash

The application crash clears the live subagent registry. A fresh process check finds no surviving build, test or lifecycle controller. Fresh sandbox API checks confirm both approved instances stopped. The retained source and artifacts are intact; scoped review and the sole cloud-operator role are re-established without duplicate jobs.

The first recovery test attempt times out after 180 seconds in detached-source rejection. The replacement-source attachment loop retries forever when the source cannot reach a publication region. Attachment and removal now bound that retry and fail closed; production mutation guards prevent saving a poisoned writer. The new forced-interleaving tests also existed without a module declaration, so they were not being run. They are connected to the test runner, and a hierarchical reservation test verifies ancestor/descendant exclusion in both directions while an unrelated branch progresses. The consolidated run passes 58, 67, 66 and 58 tests across four wrappers (249 executions, including repeated shared cases). Receipts: `single-shard-redesign-v1/local-tests/recovery-combined-01.*` preserve the failure; `recovery-combined-02.*` record the passing run. Benchmark compilation and final independent admission remain pending. No cloud compute spending occurs.

## 2026-10-20: First guarded window closes before upload

The approved worker starts with a 45-minute local absolute shutdown guard, but SSH cannot reach it because the shared security group permits an old operator address. Attachment checks show that the group also serves the excluded preparation instance, so its rules remain unchanged. The operator stops only the approved worker and verifies the stopped state through the API. No upload, build or benchmark occurs. The closed-window compute upper bound is $0.19733598545074463 and is counted once in the spending ledger. The old watchdog is retired after verified shutdown so it cannot interfere with a later window. A dedicated worker-only SSH group restricted to the current operator address is being prepared while compute is stopped. Any later execution requires fresh local and remote absolute guards and a new window.

## 2026-10-20: Worker access repaired; fresh comparison admitted

A dedicated security group permits SSH from the current operator address only and is attached only to the approved worker. The shared group remains unchanged. Lyon directly authorizes updating the IP. Source archives, independent review and native correctness/build/smoke receipts pass the frozen admission checks. A new epoch2 manifest uses a new remote output path and authorizes one fresh 45-minute guarded comparison; epoch1 records remain immutable. The sole operator must collect small evidence, stop the worker, restore its original security-group attachment and remove only the temporary group. The fixed reference–candidate–candidate–reference workload and benchmark-then-stop boundary remain unchanged.

## 2026-10-20: One-owner comparison complete; stop boundary reached

The new one-owner implementation and optimized reference both build and pass all four FineWeb100K/32-worker construction/save/reopen checks in order R,C,C,R. Build-and-save seconds are 6.795, 10.607, 7.459 and 7.831; the candidate is slower in the first pair and slightly faster in the second, with a descriptive mean ratio of 1.235. Candidate navigation distance counts average about 43% higher and deferred transfer-group retries increase; the measured structural reservation wait is negligible. Candidate insertion CPU and memory high-water marks through balancing also increase. Final trees contain one internal root and 67–72 leaves, with no occupancy violations, so this does not exercise internal balancing, deep subtrees, cache capacity or held-out quality. Full interpretation and limits are in single-shard-redesign-v1/REPORT.md.

Both approved instances are API-confirmed stopped. Original worker networking is restored, the temporary group deleted, and the watchdog retired. Epoch2 costs at most $0.45316268798828124; together with the failed connectivity window this adds $0.6504986734390259, counted once in SPEND-LEDGER.json. No new storage commitment is made. The future queue prioritizes balancing work/critical-path attribution, insertion CPU/transient memory, and a separately justified established-tree append. No further implementation or benchmark starts. The bounded user request is complete; the broader distributed/100B research goal remains incomplete.

## 2026-10-20: Three-million-vector attribution authorized

Lyon requests a matched 3M run and attribution of latency differences to implementation differences. Root prepares a shared 12-cycle harness using 100K batches through 1M then 1M batches to 3M, per-phase work/CPU/memory counters and CPU-profile alignment. No engine algorithm is changed. The sole operator verifies 32 exact retained sandbox S3 object versions covering the canonical 3M prefix; the first 100K matches the prior fixture. The corpus is assembled only on the approved worker. Persistent storage is expanded under standing permission so old and new artifacts remain intact. New review, source/config freeze and independent absolute guards are required before execution. The research heartbeat stays paused.

## 2026-10-20: 3M preparation failure preserved

The first guarded 3M window stops before input materialization or compilation. Its filesystem check incorrectly requires more than 250 GiB usable space on a 256 GiB volume, ignoring filesystem metadata; the exact volume and expanded partition are correct. The profiler is installed and accepts the required options, but kernel.perf_event_paranoid=4 denies user-process sampling. Root authorizes correcting the capacity check and temporarily setting this dedicated worker to user-process-only profiling level2, with original4 recorded and restored, under normal approval review. Both changes require a focused delta review and a fresh absolute window. No algorithm or input changes occur. The API-stopped window costs at most $0.20073165729522705, counted once.


## 2026-10-21 UTC — Matched 3M attribution complete; stopped

The fixed reference/candidate/candidate/reference experiment completes on identical FineWeb3M×768 input, 32 workers and twelve 100K/1M-cadence cycles. All four runs pass exact-once membership, full-vector bit checks, one-bit posting checks, four-bit saved centroid checks and reopen after writer retirement. Final trees have three levels; candidate internal jobs are exercised. Construction plus saving averages 61.8676s reference and 63.8080s candidate (+3.1364%). Add differs by +0.3256s, balancing +1.5254s and save +0.0889s. The initial 100K balance contributes 1.4360s (74%) of the whole gap with similar CPU work and only about two effective cores. Its causal dependency chain remains unresolved.

Phase profiles identify candidate resident-node handle cloning at 2.63–2.83% of insertion CPU samples, but this is not net wall-time overhead. Candidate balancing performs 12.72% more navigation distances and uses 8.74% more CPU; later balances have nearly equal elapsed time. Transfer retries and conflicts decrease at 3M, transferred vectors are essentially equal, and saving/peak memory are similar. Source/assembly and independent interpretation review support these qualified conclusions. See single-shard-3m-attribution-v1/REPORT.md, analysis.json, profile-analysis.json, IMPLEMENTATION-DIFFERENCES.md and INTERPRETATION-REVIEW.md. No engine algorithm is tuned in this campaign.

The fresh epoch2 uses new independent absolute guards; epoch1 remains a closed failed preparation attempt. Final lifecycle receipt verifies worker and driver API-stopped, runtime profiling permission restored to4 before stop, watchdog retired, original worker security group restored and temporary group deleted. Input/index/raw profiles/source epochs remain on disk; only7.77MB of small evidence is retained locally. An unsuccessful annotation-symbol selection is preserved; resolving the compiled symbol succeeds without another benchmark.

Epoch2 compute upper bound$1.399443608627 plus already recorded epoch1$0.200731657295 totals$1.600175265923 for this campaign. The96GiB root growth adds$7.68/month, counted once. Covered research compute is$17.608938069481; all identified recurring storage totals$19.52/month; compute plus one reserved month is$37.128938069481, not a whole-account bill. Current plan is reprioritized toward initial-balance dependency tracing, resident-node acquisition and fixed-decision navigation work. The requested benchmark/report is complete; no further experiment or implementation starts and the broad heartbeat remains paused.

## October 21, 2026 — Distributed implementation and comparisons authorized

Lyon requests implementation of the agreed design, actual two-machine correctness, then matched 100K and 3M performance comparisons against the unsharded reference. Owner vector capacity must produce substantial occupancy on both machines, approximately 30% or more each. The standard 100K cadence only permits redistribution after construction; Lyon additionally authorizes a separate matched diagnostic with two 50K batches so construction continues after handoff. Correctness gates precede performance. The broad automation remains paused. Local work begins; no cloud execution or new spending has occurred in this campaign.

## October 21, 2026 — Integrated distributed tests expose two blockers

The first integrated distributed suite passes 42 of 43 tests, including actual loopback remote navigation/transfers, reference-oracle beam selection, phase protocol checks and fresh-process saved-reader scoring. The failed duplicate-save test returns Unresolved after an already sealed phase; a correction makes sealing idempotent for the same run/epoch and awaits verification. A separate 100+25-vector construction fixture forces internal branching, ownership refinement and post-handoff construction with split threshold8, branching4 and four workers. Its first balance fails on a stale parent reference before reaching handoff. Source-parent revalidation is being corrected. These are local correctness failures, not performance evidence; the failed receipts remain in two-machine-v1/local-tests/.

The implementation now includes bounded handoff and immutable-object messages, actual owner-local construction, and a fresh-reader executable. Full construction, concurrency review and real two-machine evidence remain incomplete. The standard100K baseline and the separately approved two-50K diagnostic are retained. No cloud window is open and spending is unchanged.

## October 21, 2026 — Post-handoff construction fixture passes

The integrated loopback fixture builds 100 vectors, refines ownership, moves a subtree, adds 25 further vectors and balances again. Exact membership and posting-code checks pass in engine-focused-06.log. The failed attempts remain preserved. The fixes address retirement during navigation-to-transfer preparation, region coverage across root replacement, and shared parents incorrectly treated as owner-local ancestors. The passing fixture uses four workers and two local TCP owner services; it does not establish physical distribution or performance. Shared upper-tree maintenance and 32-worker tests remain under development and independent review. No cloud window is open and new compute spending remains zero.

## October 21, 2026 — Integrated distributed suite passes 138 tests

The complete native wrapper passes 138 tests, including ten construction repetitions with 32 workers per owner, exact membership and code checks after handoff, shared-tree splits and root collapse, simultaneous opposite-direction transfers, recovery from a lost activation reply, replay/body fencing, active-operation phase closure, and fresh-reader access to a previous saved generation after an owner save failure. The accepted-socket correction restores blocking I/O on macOS; a delayed-frame regression preserves this finding. Receipt: two-machine-v1/local-tests/engine-focused-16.log. Failed intermediate runs remain preserved.

A bounded posting-transfer change remains necessary before source freeze because serialized JSON code payloads can exceed the frame limit for large reassignment groups. Executable smoke tests, final review and the physical fixture remain pending. Fresh read-only AWS checks confirm the exact worker and driver stopped in account079819354890/us-east-1. The verified r6i.16xlarge rate is$4.032/hour per machine. No new cloud compute or storage has been started.

## October 21, 2026 — Physical correctness window starts

All140native tests pass, including bounded posting-transfer chunks and active-job phase closure. The actual correctness CLI passes as two native processes, with independent fresh-reader validation. Its first wrapper attempt passes tree validation but reports a cleanup PermissionError while signalling an exiting group; subsequent process inspection finds no surviving task process. The corrected wrapper passes in cli-smoke-02. Independent admission review and the frozen source hashes match.

Both authorized sandbox instances are configured as r6i.16xlarge with retained volumes preserved. A temporary group sg-02164ca5d2b61aeed allows SSH from the current operator /32 and ports9100–9101 from the exact two private addresses. The shared security group is unchanged. A fresh45minute window installs independent local API and remote OS shutdown guards with absolute deadline2026-10-21T20:21:59Z and maximumcompute$6.05. The reviewed source/runtime package uploads to the worker and passes remote SHA256 verification. Linux compilation is running under the exclusive task lock. No physical correctness or performance result is claimed yet; finalAPIstop and spending reconciliation remain required.


## 2026-10-21 — Physical two-machine correctness and durable restore passed

The 125-vector fixture passed remote insertion/navigation, shared root replacement, whole-branch handoff, cross-owner posting transfer and merge, and capacity rejection. Final memberships are 61 and 64 vectors on distinct sandbox machines. This tests mechanisms, not throughput. The first S3 export attempt received AccessDenied because it used a campaign-named prefix; the task role permits only content-addressed keys. Reviewed format-2 transfer scripts use that existing namespace. Conditional creation rejected conflicting bytes with HTTP 412. Thirty logical objects totaling 35,068 bytes were published with exact version IDs; a fresh reader on the other machine verified all 125 memberships, posting and centroid codes, and full-vector checksum after both writers stopped. Small receipts are in `two-machine-v1/operations/window-epoch1/`. Both instances are API-confirmed stopped. This closed window adds $2.5933829372406003 compute upper bound, recorded once. Existing disks remain retained; no new disk commitment. Performance comparisons remain pending.


## 2026-10-21 — 100K reference passed; distributed input-path failure preserved

The standard 100K reference completed with 0.094643 seconds insertion, 4.962093 seconds balancing and 5.665887 seconds construction plus saving. Saved membership and codes passed. The first distributed arm exited before input readiness or construction: the operator generated its descriptor on macOS using Path.resolve, which rewrote the remote Linux path to /System/Volumes/Data/home. Both supervised processes confirm cleanup. Exact corpus and binary checksums passed separately; no algorithm timing is accepted from this failed attempt. The invalid metadata and receipts remain retained. The correction uses literal Linux paths, verifies their remote sizes, and will run an explicitly named replacement attempt with fresh job/output/run IDs. The unchanged benchmark implementation and settings do not require a new build.


## 2026-10-21 — 100K construction failure; both machines stopped

The corrected-path attempt reaches real construction, but concurrent split/reassignment work exhausts its revalidation progress budget. The first recorded cause is `transfer routing dependency changed`; the owner enters unresolved state while other jobs continue to report failures. Attempt IDs and newly allocated node IDs grow far beyond the expected scale. The root operator stops the exact job after 64.24 supervised seconds, and both owner process groups confirm cleanup. There is no completed construction, placement or membership result. The small physical fixture does not establish correctness at this workload condition. The existing engine worker owns a bounded red-capable reproducer and repair; the protocol worker inspects dependency validation independently. Both instances are API-confirmed stopped. Window-epoch2 adds $1.6125195140075683 compute upper bound once; no disk growth. Reference100K remains valid, and other performance arms have not run.

## 2026-10-21: transfer dependency repair and fresh epoch3 window

The transfer validates exact centroid bits, placement, node type and source/destination membership under endpoint protection. Unrelated posting edits no longer invalidate correctly prepared transfer codes. Broad revisions remain required for structural snapshots. The deterministic red and dense construction failure are retained; the repaired suite passes 144/144 tests, including adverse endpoint changes and prompt queued-work cancellation after failure. Independent review finds no blocker. Three native CLIs compile. Epoch3 starts with a fresh 90-minute local and remote shutdown deadline and a $12.10 compute cap. Physical correctness must pass again before the remaining fixed comparisons. No benchmark outcome is claimed from native tests.

## 2026-10-21: repaired physical correctness passes

Fresh Linux compilation produces new hashes for all three executables. The exact binaries are verified on both machines. The physical 125-vector fixture passes all required mechanisms and fresh-reader validation in epoch3, ending with 60 and 65 memberships. The standard 100K distributed replacement is admitted. No performance conclusion follows from the small fixture.

## 2026-10-21: merge evacuation failure, epoch3 closed

The repaired standard 100K run completes insertion in 0.171024602 seconds, then fails during balancing: `merge evacuation left current postings`. The supervisor exits with code 1 after 18.9647 seconds, and both owner process groups confirm cleanup. Both exact EC2 instances are API-confirmed stopped. The closed epoch3 window adds a $0.9478509422 compute upper bound with no additional storage commitment. This is an invariant failure, not a completed build or comparative latency result. The implementation worker owns a deterministic local merge concurrency regression and repair; the independent reviewer checks the same invariant. Remaining benchmark arms wait for another coherent correctness gate.

The epoch3 peer stop command encounters a transient `/proc/.../environ` permission error after signaling. The peer supervisor separately records `operator_stop`, exit -15 and `cleanup_confirmed: true` in `serve-receipts.json`; both EC2 stopped states are verified through the API. The command error remains retained and does not weaken the shutdown requirement or justify broader process signaling.

## 2026-10-21: local concurrency gate catches centroid publication mismatch

The expanded merge suite passes 148 tests and fails the 32-worker-per-owner repeated scheduling test. After balancing, a parent scoring centroid differs from the current child centroid. The failure remains in `two-machine-v1/local-tests/merge-revalidation-final-suite.log`. A successful rerun alone does not close this invariant; the implementation author and reviewer investigate the publication dependency. Cloud execution remains stopped, with no new spending.

## 2026-10-21: coherent repair accepted for epoch4

The combined repair passes 150/150 tests and all three native CLI builds. Independent review finds no blocker to a bounded physical correctness attempt. Merge arrivals are preserved, known pre-mutation rejections revalidate, unknown or poisoned outcomes remain fatal, and shared centroid no-op publication uses the same protected turn and fresh dependencies as a changed publication. Earlier failed suites and the exact causal limit of the stress failure remain recorded. The final archive is `source-epoch4-reviewed.tar.gz`; the preliminary epoch4 archive is not admitted. Epoch4 has a fresh 90-minute deadline and $12.10 compute cap, and requires Linux recompilation plus physical correctness before performance.

## 2026-10-21: construction completes; benchmark report serialization fails

Epoch4 physical correctness passes. The standard 100K run records insertion 0.174346379 seconds and balancing 16.478724946 seconds, ending that phase with 100,000 memberships on the initial owner, 69 leaves and 68 splits. After redistribution the benchmark panics while converting a placement plan containing 128-bit node IDs into a JSON Value: `number out of range`. These are completed phase observations, not an admitted end-to-end result because saving and fresh-reader validation have not run. Preserve the failed attempt. The engine worker owns a report-only decimal-string ID representation and regression, without changing numeric IDs or RPC serialization. Both machines are API-confirmed stopped; epoch4 adds a $0.7770198397 compute upper bound and no storage commitment.


## 2026-10-21: report regression exposes another visibility failure

The full suite passes 151 tests and fails one after the placement-report repair. The failed test detects a parent/child centroid mismatch, but its diagnostic JSON conversion also rejects a 128-bit node ID and masks the useful context. Preserve the failed log, repair diagnostic serialization, and capture the actual authority and centroid dependencies. A successful rerun alone does not close the correctness gate. Cloud remains stopped and spending is unchanged. Epoch5 operator scripts are prepared by namespace-only substitution; no admission or execution window is created.


## 2026-10-21: shared-parent races reproduced; separate local-writer failure classified

Controlled tests reproduce two centroid failures: shared reparenting copies an obsolete scoring row despite obtaining a fresh owned image, and refresh returns before the owner finishes a published reparent. The repaired focused tests pass. The full wrapper passes 153 of 154 tests; its remaining failure belongs to the separate local-writer parent/leaf split test. Independent review and a standalone reproduction establish that this test constructs no distributed runtime, and its writer source matches frozen epoch4 exactly. It is not the retained reference benchmark implementation. Preserve the failure and gate the current distributed work on the applicable suite, CLIs, review and physical fixture. Cloud remains stopped and spending unchanged.


## 2026-10-21: epoch5 guarded build admitted

The coherent report and shared-parent repair passes 153 applicable tests and all three native CLI builds. Independent review records the separately reproduced unchanged local-writer failure rather than claiming the full wrapper passes. The frozen source archive SHA256 is e8ca17a59cd817e90323421f783faa9719bfb8a760da392ce8c5d3b2bcdfd857. A fresh 90-minute epoch5 window verifies both exact sandbox resources and $4.032/hour per-host rates, installs independent local and remote absolute guards, and starts Linux compilation. Automatic approval review rejects a combined preparation command because it copies a benchmark token; the accepted safer alternative uploads only source/runtime and supervisor and reuses the token already present on each host. No token is copied or exported. The path-only amendment is retained in the window receipts.


## 2026-10-21: standard distributed 100K validated

Epoch5 physical correctness passes all required mechanisms, ending with 56/69 memberships. The standard distributed 100K arm then passes exact saved membership, codes and fresh-reader validation. Insertion is 0.168353722 seconds, balancing 14.820681021 seconds, placement 0.895684027 seconds, saving 1.182100986 seconds, and construction plus saving 17.083504830 seconds. Final owners hold 54,642 and 45,358 vectors, passing the 30% occupancy gate. Construction occurs before redistribution in this baseline. The retained reference totals 5.665886778 seconds; independent trees and unequal useful work prevent assigning this ratio solely to sharding. The separately authorized two-50K reference passes with 4.040947274 seconds construction plus saving; its distributed counterpart is next.


## 2026-10-21: 3M reference validates; distributed transfer-region failure at 200K

The 3M reference validates all 3,000,000 memberships, vectors and posting/centroid codes. Insertion takes 14.698520770 seconds, balancing 33.618728459 seconds and saving 17.181223258 seconds; total construction plus save is 65.520032462 seconds. The distributed run completes its first 100K cycle, then fails balancing after the second 100K insertion because posting-transfer preparation repeatedly reports that its region is not granted. It exits after 43.01 supervised seconds, before placement or a complete index. This is a correctness/progress failure, not a 3M latency result. The engine author and independent reviewer investigate the grant dependency at the internal-branch boundary. Both exact machines are API-confirmed stopped. Epoch5 adds $1.923287929840088 compute upper bound once, with no new storage commitment. The passing100K results and3Mreference remain retained.


## October 21: guarded epoch6 reservation repair execution

The reviewed repair retains relevant ancestry and placement validation for queued regions while allowing unrelated structural edits. A controlled endpoint test fails before the repair and passes afterward. The final applicable suite passes 156 tests; the separately recorded local-writer split failure remains excluded explicitly. Both sandbox task machines have fresh identity/resource readback and independent local and remote absolute shutdown guards with deadline 2026-10-21T23:06:26Z. The reviewed source/runtime archive is uploaded without credentials; existing on-host credentials and retained corpus paths are reused. Build and physical correctness precede the three explicit distributed arms. Epoch6 compute is open and not yet added to the ledger.


## October 21: epoch6 physical fixture and both100K arms validate

Fresh Linux artifacts pass all required physical mechanisms in0.8635seconds with62/63 finalmemberships. Standard100K construction plus save is17.6257seconds, with54.584%/45.416% placement. The two50K diagnostic validates in11.4546seconds with55.328%/44.672% placement. Both stopcommands hit the retained exiting-process /proc permission race, but independent supervisor receipts confirm completed cleanup for both processgroups; cleanup-reconciled.json records that evidence without rerunning the workloads. The bounded3M arm is now running under the same immutable source and deadline. Reference results remain unchanged.


## October 21: epoch6 repeats the200K progress failure; bothmachines stopped

The3M attempt passes its first100K balance in18.4356seconds, adds the next100K in0.5371seconds, then fails its secondbalance after12.0669seconds with transfer region not granted. The retained failure contains200K memberships,136leaves,139nodes and32peak activejobs on owner1; owner2 has no currentpostings. This disproves sufficiency of the queued-disjoint repair for the larger failure. The remaining initial-admission and terminal-status paths require controlled diagnosis. Source, logs and priorresults remain retained.

Both taskinstances are API-confirmed stopped. Epoch6 adds exactly one compute upperbound of$1.8540565028381348. Covered compute is$27.317055735268056183; identified recurring disks remain$19.52/month, with no new capacity. Compute plus one reserved month of those commitments is$46.837055735268056183. This is task-specific accounting, not the whole-account bill.


## October 21: two remaining transfer progress defects reproduced locally

A controlled endpoint test reproduces initial admission returning Replan after an unrelated split changes the global topology sequence between capture and submission. Atomic current-path admission for posting transfers passes that regression while structural requests retain their snapshot fence. A separate controlled test completes one slow transfer, then fails on one legitimate retry because the total elapsed time already exceeds five seconds. Its repair retains the five-second interval and 1,024-round cap but measures time without a fully acknowledged transfer; successful nonempty transfers reset the stall deadline. These controlled defects do not establish the exact ordering in the retained epoch6 cloud failure. Broader tests, precise status/progress diagnostics and independent review remain admission gates. Both task machines remain stopped.


## October 21: epoch7 admits atomic transfer admission and consecutive no-progress accounting

The frozen repair passes 161 applicable tests and all three native CLI builds. Independent review accepts the four-file change and records both controlled failures, adverse checks and attribution limits. The next physical window has fresh sandbox identity, exact resource and rate checks, a local absolute watchdog and two remote shutdown timers ending at 2026-10-21T23:32:08Z. The reviewed runtime archive has SHA256 f65a6e4e6c07ff06b6e2d90676923d5ef7781c660344ad48a559ed0055adae31. Physical correctness precedes the 3M arm; the two 100K arms are refreshed only after 3M succeeds. Existing on-host credentials and corpus are reused without copying. Epoch7 is open and not yet added to the ledger.


## October 21: epoch7 reaches two million and stops at its fixed deadline

All ten 100K construction cycles through one million complete. Their balance phases sum to 249.17804469 seconds. The next million additions take 40.931231861 seconds, and balancing at two million is still running when the absolute job deadline fires. Both process supervisors record deadline shutdown and confirmed cleanup; the coordinator has no construction error on stderr. No completed 3M result or quality claim follows. This is a runtime-bound failure, distinct from the prior 200K progress errors.

Both task instances are API-confirmed stopped. Epoch7 adds exactly one compute upper bound of $2.020666703872681; covered compute is $29.337722439140737183, and identified recurring disks remain $19.52/month. Preparation now targets a separately reviewed 1,800-second bound only for the unchanged 3M performance workload, with the same source, executables, input and hardware. It requires a fresh guarded window and does not extend this closed one.


## October 21: epoch8 starts the reviewed longer 3M workload

The fresh physical fixture passes all required mechanisms with 52/73 final memberships. Both hosts verify all three retained epoch7 executable hashes. The new supervisor admits the 1,790-second absolute job bound only for the fixed 3M arm and exact retained inputs. Independent local and remote shutdown guards end at 2026-10-21T23:55:00.334085+00:00. No credentials or corpus are copied. The prior closed window remains unchanged. Epoch8 is open and is not yet charged to the ledger; its compute ceiling is $12.10. After validated 3M, refresh the two candidate 100K arms on the same source, then report and stop.


## October 21: larger tests stopped; owner-local diagnosis takes priority

Lyon directs stopping the active 3M run and deferring larger comparisons because the initial-owner 100K balance is roughly three times the reference elapsed and CPU cost despite negligible peer traffic. The two exact process groups stop through their verified supervisors; both report cleanup confirmed. Partial evidence ends during 2M balancing and contains no completed 3M result. AWS confirms both instances stopped. Epoch8 adds $1.6277948393249513 once; covered compute is $30.965517278465688483, with unchanged $19.52/month identified storage commitments.

The next experiment is a minimal native diagnostic of retained reservation-history scans on the actual manager. Repeated posting compaction and navigation allocations remain alternative hypotheses. No implementation speedup is claimed before measurement, and no larger test will run merely to finish the superseded sequence.


## October 21: minimal reservation-history diagnostic confirms extra work

The real reservation manager receives the same uncontended request on one unchanged leaf with no competing reservations. It visits one record with zero history and 65 records after 64 completed/released attempts; both cases perform zero active overlap checks. Duplicate replay still succeeds. The deterministic regression fails in under 0.01 seconds. First-use timing noise prevents a latency claim. The next correction separates active-conflict discovery from retained replay outcomes, preserving conflict and replay semantics. End-to-end contribution remains unmeasured.


## October 21: active-conflict index passes review; fixed 100K validation admitted

The correction preserves replay records and separately indexes Granted attempts. The same minimal diagnostic visits zero conflict records with either zero or 64 completed attempts. Twenty-six protocol tests, 164 applicable distributed tests and three native CLI builds pass. Independent review confirms state-transition, cancellation, replay, FIFO and root-replacement consistency. Source comparison with epoch7 changes only reservation.rs.

Epoch9 has fresh independent guards ending at 2026-10-21T23:17:14.878982+00:00, with a $4.04 compute ceiling. It admits one explicit standard100K before arm and one after arm, using identical retained inputs/settings and 32 workers. Both are unprofiled; phase CPU and work counts accompany timings. Source/runtime upload and fresh Linux build proceed under standing authorization. No larger test is admitted.


## October 21: fixed 100K pair validates; remaining overhead needs CPU attribution

The reservation-only correction validates complete saved membership. Balance time is 13.292576451 seconds versus 16.949308926 before, and process CPU is 51.84 versus 56.38 seconds. The corrected run also has fewer counted distances and jobs, preventing isolated attribution of the descriptive 21.6% elapsed reduction. The deterministic history test remains the causal work-removal evidence. Keep the correction and prioritize one bounded CPU sample of the same corrected 100K initial-owner balance. Larger tests remain deferred.

Both task instances are API-stopped. Epoch9 adds $0.9592716291809082 once; covered compute totals $31.924788907646596683, and identified storage remains $19.52/month. Retained stop-helper permission errors are reconciled with independent successful cleanup receipts and API shutdown. See two-machine-v1/local-tests/RESERVATION-HISTORY-RESULT.md.


## October 21: profiler permission preflight stops before workload

The fresh guarded epoch10 window finds perf 7.0.14 installed, but perf_event_paranoid=4 blocks the reviewed unprivileged sampler. No payload upload, benchmark, recording or permission change occurs. Both task instances are API-confirmed stopped. This failed preparation adds $0.27099863830566406 once; covered compute totals $32.195787545952260743, with unchanged $19.52/month identified storage commitments. The next preparation uses a separately reviewed, bounded privileged observer tied to the exact benchmark process, leaving kernel settings unchanged. It requires a new execution window.


## October 21: early balance profile moves clustering to the top of the queue

The reviewed privileged observer samples only the corrected benchmark process with unchanged kernel permissions. The fixed 100K build, save and fresh-reader validation pass. The recording reports approximately 2,000 samples with zero losses; 49.60% land in Euclidean distance and 40.16% in the split routine itself. These are exclusive instruction-location shares from an early balance slice, not whole-phase time attribution. Balancing takes 15.2577 seconds; final current memberships are 54,804/45,196. Compare exact reference/distributed clustering paths and work before selecting a correction. Larger tests remain deferred.

Both jobs and profiler confirm cleanup, the runner exits successfully and both instances are API-stopped. Epoch11 adds $0.35508287628173824 once; covered compute is $32.550870422233998983, with unchanged $19.52/month identified storage commitments. See two-machine-v1/local-tests/OWNER-LOCAL-PROFILE-RESULT.md for evidence and limitations.


## October 21: exact clustering-path audit narrows the next experiment

The measured reference and profiled distributed executable use byte-identical clustering source, the same four candidate center pairs and maximum 128 main iterations, and serial work within each split. No missing alternative reference kernel is found. The distributed path prepares outside the source lock and can discard a prepared split after dependencies change; the reference retains a source claim. The next proposed diagnostic counts clustering attempts, input rows, iterations and rejected preparation on two neighboring oversized leaves, with controlled interleaving and initialization. The mechanism has not yet been measured, and no further benchmark starts.


## October 21: full-balance matched100K observability authorized

Lyon authorizes comparable missing instrumentation, complete-balance profiling of both implementations on100K vectors, latency attribution and findings/plan updates. The shared kernel records per-call work and thread CPU; construction counters distinguish preparation, transfer scans/copies, navigation and synchronization. A profiler acknowledgment gates entry to the timed balance, and completion gates profiler shutdown outside that timer. Source algorithms, settings and uncontrolled initialization stay unchanged. Consolidated native tests and independent review precede a fresh execution window. Both machines remain stopped and spending is unchanged during preparation.


## October 21: reviewed full-balance pair enters a fresh guarded window

The final applicable native suite passes168tests, both reference counter tests pass, and all four native executable builds pass. Independent review accepts the frozen instrumentation and bounded profiler handshake, with perturbation and inclusive-timer limits explicit. Epoch12 has fresh local and remote absolute guards ending at 2026-10-22T03:28:06.480359+00:00. Its maximum compute commitment is$6.05; actual closure accounting remains pending. The reviewed46,110,891-byte runtime archive has SHA2568504bfe477af86cf1360e2d9704e27917d6dc8b04b75eb77cba596722cf2609b. Both exact sandbox task hosts are active for fresh Linux builds, then one reference and one distributed100K profile.


## October 21: complete 100K balance pair identifies repeated split preparation

Both instrumented 100K arms pass saved validation and full timed-phase profile coverage with zero lost samples. Distributed balancing consumes 51.85 CPU-seconds versus 26.07 in the reference, while elapsed time is 14.0546 versus 12.9650 seconds. The reference's first serial round lasts 7.847 seconds, so the earlier threefold elapsed gap does not reproduce. Distributed preparation makes 324 attempts, commits 72 and discards 252, wasting preparation over 1,721,645 input rows. The common kernel accounts for 15.6979 of 25.78 additional CPU-seconds. Protecting source preparation more like the reference is the next priority; repeated posting compaction and navigation copies remain secondary measurable costs. No additional benchmark or implementation starts.

The first upload preflight stops before transfer because compiler disk headroom was applied to the peer; actual runtime headroom is sufficient without resizing. The first reference attempt stops before balancing because installed perf emits a trailing NUL in its acknowledgement. A bounded compatibility check and focused parser test permit one explicitly selected corrected pair in fresh paths with unchanged binaries and deadline. The peer stop helper later fails while reading an unrelated process; the successful coordinator and profile evidence remain valid, and the root verifies both exact task instances API-stopped. All failed artifacts remain retained.

Epoch12 adds $2.2087163510131838 compute once. Covered task compute is $34.759586773247182783 and identified recurring storage remains $19.52/month. The findings, assessment and current plan record the result and prioritize a shorter development loop: existing harness, focused counters/tests and enough evidence to choose the next change. See two-machine-v1/local-tests/FULL-BALANCE-OBSERVABILITY-RESULT.md.


## October 21: source-leaf claim experiment authorized

Lyon requests source-leaf claims spanning split preparation and installation, readable navigation, independent-leaf concurrency and safe lock ordering. The implementation and focused interleaving tests precede one same-workload 100K/32-worker candidate measurement. Existing epoch12 instrumentation, profiler controls, reference measurements and retained input provide the comparison; no new measurement framework or size sweep is needed. Both machines remain stopped during preparation, with no new compute commitment.


## October 21: source-leaf claims pass review and enter guarded measurement

The candidate claims source membership through preparation and installation while routing remains readable. Deterministic tests verify independent leaf and ancestor splits, transfer/merge/append conflicts, migration rejection cleanup and claim release on failure. A controlled red test exposes 1,024 retries against a live claim; retaining those IDs as deferred jobs fixes progress without increasing budgets. Four focused tests, 172 applicable tests and three native executables pass. The same unrelated writer test remains explicitly excluded. Independent source and execution review finds no remaining blocker.

Epoch13 has independently enforced local and remote absolute shutdown guards ending at 2026-10-22T04:30:31.929700+00:00. The $6.05 maximum compute commitment is not yet actual spending. Only the fixed 100K/32-worker candidate is admitted, using retained matched baseline data. Frozen source SHA256 is f8abad2404a85a9561a82ecb4b2da3c4ae103492255dd85b5e32c98ac722f6c0; no additional storage or larger experiment is admitted.


## October 21: source-leaf claims remove preparation waste; elapsed improvement remains unproven

The single fixed 100K/32-worker candidate passes full timed-balance profiling and fresh-reader validation of all 100,000 memberships, full vectors, posting codes and centroid codes. All 70 prepared splits commit and none are discarded, versus 324 preparations/72 commits/252 discards before. Balance CPU falls 40.8%, from 51.85 to 30.70 seconds; clustering CPU falls from 37.626 to 18.880. Elapsed rises from 14.055 to 15.449 seconds. The longest observed job grows from 2.496 to 7.988 seconds, and reservation wait counters rise. Keep the claim correction for removed waste and validated behavior while retaining adverse elapsed evidence. Transfer coordination, then direct local navigation, lead the revised plan. No additional implementation or benchmark starts.

Construction occurs on the initial owner; redistribution afterward leaves 54,990/45,010 memberships on the two physical machines. Four focused tests, 172 applicable tests and native/Linux builds pass; one known unrelated local-writer test remains explicitly excluded. A retained red test proves that synchronous claim retries exhausted 1,024 rounds; deferred jobs preserve the IDs, allow other work and participate in drain checks. The migration rejection path also releases its unmutated protection. Source and operations receive one consolidated independent review.

The build takes 71.04 seconds. Workload, profiler and peer cleanup succeed. Both exact task instances are API-stopped. Epoch13 adds $0.8633927565002442 once; covered compute is $35.622979529747426983 and identified storage remains $19.52/month. Findings and plan are reconciled at two-machine-v1/local-tests/SOURCE-LEAF-CLAIM-RESULT.md and operations/window-epoch13/summary.json. The requested experiment is complete; the broader research remains incomplete and its automation remains paused.


## October 22: 500K follow-up with retained binaries authorized

Lyon requests the same binaries on 500K vectors and a second result document. One matched reference/candidate pair uses five100K intervals and32workers, without an external profiler on either arm. A500K owner capacity keeps construction on the initial owner through the last balance and triggers final redistribution; this isolates owner-local construction. A fresh30-minute window has a maximum$4.032 compute commitment. Preparation changes configuration and reporting only. The earlier4.962-second reference100K balance remains visible alongside the instrumented12.965-second result; the discrepancy is unexplained and this size change does not isolate its cause.


## October 22: 500K retained-binary comparison validates and exposes owner-local overhead

Both unchanged binaries validate 500,000 memberships, vectors and posting/centroid codes after five 100K construction intervals with 32 workers and no external profiler. All candidate construction remains on the initial owner; only final redistribution moves branches, leaving 266,275/233,725 vectors. The reference has levels [1,5,354], and the candidate [1,6,347]. Identical input SHA is 165f6baa847dacb65c1e9c718c1f6edd976f52ec3907136c447de9ebf5972f48. Retained source verification takes about81seconds perhost before timing.

Balancing takes 54.346 seconds sharded versus 9.054 reference, a6.00× gap, with117.80 versus52.70CPU-seconds. Candidate clustering uses30.918CPU-seconds versus35.296 reference, while CPU outside clustering is86.882 versus17.404. All349counted preparations commit and none are discarded. Subsequent four intervals take39.487 versus2.314seconds; the gap does not disappear after initial construction. Candidate insertion totals5.487 versus0.699seconds and grows by interval. Transfer coordination remains first, then direct local navigation and insertion attribution. Save/build totals and all limitations are in the separate two-machine-v1/local-tests/SOURCE-LEAF-CLAIM-500K-RESULT.md; the100Kresult remains preserved.

The reference's first100K balance takes6.740seconds, with fewer clustering distances and a shorter first serial round than the separate12.965-second run. The earlier4.962-second reference remains relevant. This pair does not isolate profiling overhead, but establishes the current construction gap without another size increase.

Both workload supervisors exit successfully. A peer-stop wrapper permission error is preserved and reconciled with the independent peer operator-stop cleanup receipt. Both instances are API-stopped. A normal-review rejection over missing window-label metadata is resolved without changing the shutdown deadline. Epoch14 adds$1.2061185787963866compute once; covered compute totals$36.829098108543813583, recurring storage remains$19.52/month, and the covered first-month remainder is$943.650901891456186417. No further implementation or benchmark starts.

## 2026-10-22T04:47:32.147607+00:00 — Three reference-style changes authorized

Lyon requests same-owner bulk transfers, locally accumulated comparison counters and contiguous parent child-centroid storage, followed by one 500K benchmark against the retained reference. Existing workers own transfer and navigation changes; the existing reviewer covers the coherent source and narrow execution-control delta. The sole operator has prepared candidate-only adapters without starting cloud compute. Preserve the epoch14 reference and candidate as baselines. The experiment tests the combined implementation and cannot isolate each edit’s share of elapsed improvement. The active plan records correctness checks, workload, $4.032/30-minute window cap and stop conditions.

### Epoch15 native validation, first pass

The six packed-navigation tests and six local-transfer tests pass. The first applicable suite reports 154 passed and 28 failed: 27 tests cannot bind local TCP listeners under the filesystem/network sandbox, and the existing no-progress test expects a failure from the old protocol capture hook that the new local path no longer calls. Preserve `two-machine-v1/local-tests/epoch15-suite-01.log`. The implementation worker is updating meaningful progress coverage for the new dispatch; the root will rerun localhost tests through normal tool review. No cloud window has opened.

### Epoch15 implementation validated and admitted

The corrected applicable suite passes 185 tests with the same one known unrelated writer exclusion as the baseline. All three native executables build. Independent review resolves a routing publication race by retaining ordered leaf guards through publication; the controlled regression passes. The source archive is frozen at SHA256 `2b36ff49ce3ca38aff2e02fd596cada211d0eaf96036221f7cbd3ebf1fa5a0af` (23,031,985 bytes), with 18 distributed source/test files changed from epoch13 and no removal. Both Git indexes retain their recorded hashes. The sole operator has admission for one fresh guarded 30-minute/$4.032 window, incremental build and one500K candidate run against retained epoch14 evidence. Runtime results remain pending.

## 2026-10-22T05:12:01.289958+00:00 — Epoch15 three improvements and500K comparison complete

All three changes are implemented, independently reviewed and validated. The native applicable suite passes185tests with one known unrelated exclusion. Linux build and the physical500K candidate run succeed. Against the previous candidate, insertion falls5.487→1.340s, balance54.346→19.379s, balanceCPU117.80→66.54s and construction/save68.566→30.002s. Retained reference is0.699s insertion,9.054s balance,52.70CPU-seconds and12.601s total. Keep the changes; parity remains unproven.

Local transfers perform zero staged protocol calls during balancing,3943source compactions versus29038, and350reservation requests versus60617. Transferred rows remain comparable162587versus161484. Last-interval scoring performs28.5million distances in both candidate runs; scoringCPU falls55.691→4.789s. Combined edits and independently built trees prevent assigning separate causal percentages. Full fresh-reader membership/vector/code checks pass; final owners265479/234521. Construction remains on the initial owner before final redistribution.

The source publication race found in review is corrected and covered. The peer-stop helper still encounters a live unreadable-process permission error; preserve that adverse result, independent supervisor cleanup and API-confirmed stopped states in the cleanup reconciliation. No retry or extra experiment occurs. The closed401.109-second window adds$0.8984837236022949compute once and no storage. Covered compute is$37.727581832146108483; identified recurring storage remains$19.52/month.

The new result is `two-machine-v1/local-tests/LOCAL-TRANSFER-PACKED-NAV-500K-RESULT.md`; machine-readable evidence is `two-machine-v1/operations/window-epoch15/comparison.json`. Reprioritize direct local navigation, saving work and balance scheduling/serial dependencies. The requested task is complete; broad research automation remains paused and no subsequent run starts automatically.

## 2026-10-22T05:16:29.887601+00:00 — Autonomous construction improvement resumed

Lyon authorizes investigating the remaining costs, implementing fixes and benchmarking while he sleeps. This supersedes the previous stop after epoch15. The existing heartbeat is resumed with the current design, no-membership-version decision and current authorizations; obsolete instructions are removed. The first selected implementation is direct local navigation, using epoch15’s measured wrapper copying and CPU as evidence. Existing workers separately own navigation, save investigation, read-only scheduler review and cloud preparation; the root remains the only local Cargo/integration operator. No cloud window is open. Each experiment will update the ranked plan and findings before the next decision.

## Direct local navigation passes native validation — 2026-10-22T05:22:18.300755+00:00

The direct local navigation implementation passes three focused equivalence/work-count tests and the applicable native suite (188 passed, zero failed, one retained unrelated exclusion). All three native benchmark/reader executables build. The fixed-tree regression compares actual local navigation with the envelope path and the reference, including mixed-depth trees, ties, beam policies, exclusions, mixed local/remote rows and stale-image handling. Local query-copy and reply-validation counters are zero. Only the three navigation source/test files differ from epoch15; independent review is in progress and cloud compute remains stopped. Save and scheduler source audits identify separate future experiments, not additional changes in this benchmark.

## Scheduler claim readiness reproduced — 2026-10-22T05:27:43.065215+00:00

A deterministic actual-transfer/reassignment fixture fails the expected assertion: the queue dispatches a retry while the child leaf it needs is still claimed, because the active producer is keyed by another node. Independent work remains dispatchable and post-transfer membership/code checks pass. Preserve `two-machine-v1/local-tests/scheduler-readiness-red-01.log`; this demonstrates unnecessary retry eligibility, not a measured latency contribution. A narrow correction is in design while epoch16 runs from its frozen navigation-only source. Save observability is prepared separately; no new benchmark window is opened.

## Direct local navigation kept; epoch16 closed — 2026-10-22T05:31:27.843716+00:00

The500K candidate validates and reduces insertion/balance/build-and-save from1.340/19.379/30.002 to1.014/16.167/26.640seconds. BalanceCPU falls66.54→54.30; inclusive navigationCPU19.939→10.770 with zero localquerycopy/replyvalidation, while candidate rows remain66.55M. Later balancing wall remains8.626versus8.540seconds despite reducedCPU. Keepnavigation; prioritize reproducedclaimreadiness and unresolvedwaiting. Freshreader verifies all500Kmemberships/vectors/codes; finalowners218052/281948; bothAPIstopped. StophelperPermissionError retained and independentlyreconciled. Count$0.7909735502624512 once,no newstorage; coveredcompute now38.518555382408559683. Full interpretation: two-machine-v1/local-tests/DIRECT-LOCAL-NAV-500K-RESULT.md. Autonomouswork continues locally; no openwindow.

## Scheduler producer context passes its regression; fixture correction pending

The exact producer-attempt implementation passes the actual split/neighbor-transfer readiness regression. The broader suite passes 191 tests and fails one new diagnostic, with the same unrelated baseline test excluded. The failing diagnostic requests a full snapshot while its intentional unscheduled claim remains held; the existing drain check correctly rejects it. Correct the fixture to inspect resident membership while held and perform snapshot validation after release. Preserve `epoch17-suite-01.log`; do not weaken the production drain check. No cloud window is open.

## 2026-10-22T05:49:54.953963+00:00 — Epoch17 passes native admission checks

The scheduling correction and save/wait measurements pass 192 applicable native tests and all three benchmark builds. One known unrelated reference-writer test remains excluded. The independent review passes and binds all ten changed source files; the source archive is frozen. Both fixture failures and the old scheduler regression failure remain retained. No new cloud window is open; fresh identity, resources, rates, guards and runtime bindings remain required before the single 500K run. Spending is unchanged.

## 2026-10-22T05:58:53.356938+00:00 — Epoch17 validates but does not improve elapsed balancing

Exact producer readiness is retained for its verified behavior, without a performance claim. Balancing21.067s/57.13CPU versus16.167s/54.30CPU; kernel work increases. Tail9.067s/34.66CPU versus8.626s/34.77CPU. Admission/terminalization account for most transfer waiting. Save7.413s includes1.582s copying already durable local objects. Investigate the operation/queue mutex convoy and implement local immutable-object reuse with preserved durability. Full interpretation: `two-machine-v1/local-tests/READINESS-SAVE-500K-RESULT.md`. BothAPIstopped; known helperfailure preserved and independentcleanup verified. Added$0.8265974235534668compute once; no newstorage.

## 2026-10-22 — Queue convoy mechanism reproduced

A controlled test holds the scheduler queue and enters each of the three enqueue handlers and phase close. Every handler holds the operation mutex while waiting; unrelated admission and completion cannot acquire it. The test fails as expected in 0.00 seconds, with all four cases recorded in `two-machine-v1/local-tests/queue-convoy-red-01.log`. The narrow correction takes the queue before the operation mutex at those four atomic decision points. Native correctness and performance evidence remain pending. Local saved-object reuse is also implemented and awaiting the coherent suite. No cloud window is open and spending is unchanged.

## 2026-10-22 — Epoch18 admitted with fresh shutdown guards

The queue-before-operation correction and verified local saved-object reuse pass197 applicable tests and three CLI builds. Independent review finds no blockers and binds the seven changed source files. The expected red convoy regression remains retained. Source SHA256 `b4ce293a8729fcd8334a0729037717fa61864cf6e3f38b96281699d639a32409` is admitted for one fixed500K candidate run. The sole operator verifies local and both remote guards with a fresh absolute deadline06:37:37UTC and$4.032 compute cap. No reference rerun, external profiler or automatic retry is included.

## 2026-10-22T06:17:07.690498+00:00 — Epoch18 removes transfer waiting and local duplicate writes

Keep both fixes. Construction plus saving falls from30.986 to22.388seconds; later balance falls from9.067 to2.067seconds versus2.314reference. Admission/terminal worker-wall sums nearly vanish. Saving falls from7.413 to6.470seconds, with zero local copied bytes. All memberships/codes validate. CPU and reassignment retries increase, so investigate reference-style combination of compatible pending jobs next. Full report: `two-machine-v1/local-tests/QUEUE-SAVE-500K-RESULT.md`. Both instances are API-stopped. Worker process cleanup is confirmed; peer exit receipt is missing and process cleanup remains unconfirmed. Preserve the launcher failure and late-read timeout. Added$0.7944031401062012compute once; no newstorage.

## 2026-10-22T06:33:07.421311+00:00 — Epoch19 is reviewed and admitted

Compatible ready reassignment requests can share one bounded navigation pass while preserving original request identities, producer dependencies and current membership checks. The fixed-tree fixture reduces actual query rows from five to three and rounds from three to one with identical membership and codes. The applicable suite passes 203 tests with the same known exclusion; all three CLI builds pass. Independent review binds five changed source files. Source archive SHA256 is `6e27f2d8e490aed29f9e3c479769be441ab07318d5a3a3fb525b480dce7dbc6e`. One fresh guarded 500K/32-worker run is admitted, capped at 30 minutes and $4.032 compute; no reference rerun or new storage. End-to-end benefit remains unmeasured.

## 2026-10-22T06:38:04.129225+00:00 — Prepare the independent saved-object representation change

While epoch19 runs from its frozen archive, prepare a narrow saving candidate: encode only saved-object chunk bytes as bounded base64 inside the existing JSON protocol. Epoch18 receives 103.80MB of framed save responses for 30.29MB raw copied objects in 3.060 seconds. This supports testing representation overhead but does not quantify a speedup. Preserve object identity, offsets, completion, phase checks, checksums, durability and reader format. Mixed old/new writer binaries fail closed; deploy matching binaries together. Tests must include exact TCP multi-chunk parity, framed byte counts, invalid encodings and both encoded and decoded size bounds. No extra cloud window is admitted yet.

## 2026-10-22T06:42:48.456568+00:00 — Epoch19 validates bounded pending-work combination

Keep the fixed-work reduction and validated request semantics. Construction plus saving is18.317seconds versus22.388previous and12.601reference; balance is9.174seconds versus13.422previous. Most elapsed improvement occurs in the variable first interval; later balance is2.121seconds versus2.067previous and2.314reference. Reassignment rounds fall8.6percent and copied query bytes2.5percent, while nonkernelCPU and rejectedgroups rise. Full result: `two-machine-v1/local-tests/COALESCING-500K-RESULT.md`. Both instances are API-stopped and both matching supervisor cleanup receipts pass; retain the launcher permission error. Add$1.0451570373535155compute once, no storage. Next prepare bounded saved-object base64 encoding and measure exact bytes before another500Krun.

## 2026-10-22T06:47:14.203352+00:00 — Epoch20 compact saved-object transport admitted

Four focused tests pass, including exact TCP multi-chunk bytes, more than twofold framed-byte reduction on the fixed fixture, malformed and oversized input rejection, and whole-object checksum failure. All207 applicable tests and three CLI builds pass, with the same known exclusion. Independent review finds no blocker and binds the two changed files. Source archive SHA256 is `5a229d7a9b78b76e10fe6212e7ef03719d901b98416441640e33f35a25153ad4`. One fresh500K/32worker window is admitted at30minutes/$4.032compute, with matchingnew binaries onbothhosts, unchangeddurability andretainedreference. End-to-end savebenefit remainsunmeasured.

## 2026-10-22T06:55:46.385129+00:00 — Epoch20 reduces saved-object traffic and copying

Keep bounded base64 encoding. Save-only framed traffic falls from 104.798MB to 41.316MB; remote copying falls from 2.936 to 1.876 seconds. Saving takes 4.683 seconds versus 6.477; faster local storage also contributes, so the full reduction is not isolated to encoding. Construction plus saving is 16.962 seconds. Every saved membership and code validates. Full result: `two-machine-v1/local-tests/SAVED-CHUNK-500K-RESULT.md`. Both instances are API-stopped and both matching supervisor cleanup receipts pass; this launcher exits cleanly without implying its historical race is fixed. Added $0.9477872760772705 compute once, no storage. Next assess the meaningful 3M phase with construction on both owners using retained binaries and input.

## 2026-10-22T06:58:41.872841+00:00 — Select the meaningful two-owner construction test

The readiness assessment recommends one fixed 3M comparison using current retained binaries and input. With 2.16M capacity per owner and the standard cadence, redistribution occurs after the 2M balance, making the final 1M add/balance the first meaningful construction phase on both machines. Epoch6 failed before placement; epoch7 timed out during 2M balancing; epoch8 was explicitly stopped there by Lyon. These do not establish a distributed correctness ceiling or a completed 3M result. The current reference must run once because its instrumentation differs from the older 65.520-second historical reference. A separate offline analyzer sums each owner process and kernel once, retains work and traffic by phase, and passes a cross-check against the 500K owner-local result. Local runner preparation is underway; no new cloud window is admitted yet.

## 2026-10-22T07:05:38.372353+00:00 — Paired 3M comparison admitted

Independent review passes the retained-binary runner, exact input and workload settings, distinct-owner accounting and shutdown bounds. The root admission binds 26 artifacts and permits one current reference run followed by one candidate run in a fresh 30-minute/$4.032 window. Each arm has a 590-second workload bound; the candidate starts only after reference validation and a fresh remaining-time check. There is no rebuild, automatic retry, sweep, profiler or new storage. Exact input and runtime checks on both physical machines precede execution. Final acceptance requires real insertion and balancing work on both owners during the final 1M interval, complete durable validation, and meaningful final placement.

## 2026-10-22T07:18:36.484336+00:00 — Epoch21 exposes post-handoff navigation failure

The reference validates all3M vectors/codes in63.295seconds including saving. The candidate completes2M construction, redistributes to1,491,238/508,762, then adds746,974/253,026 vectors in31.920seconds. That insertion uses945CPU-seconds and58.846GB applicationtraffic. Balancefailsafter0.510seconds with whole-attempt navigation retry exhaustion; no complete3M candidate result. Preserve all partial evidence in `two-machine-v1/local-tests/TWO-OWNER-3M-EPOCH21-RESULT.md`. The engine worker diagnoses a controlled reproduction; the storage worker independently attributes remote insertion payloads. Both machines are API-stopped; peer process cleanup receipt remains missing. Added$1.361019497680664compute once, no storage. The failed window is closed with no retry.

## 2026-10-22 — Controlled navigation failure and insertion attribution

A small actual-Cluster fixture reproduces the exact eight-attempt navigation failure when centroid-only shared publications occur between navigation steps, despite unchanged topology, placement and live owned records. The repair will distinguish compatible centroid refreshes from structural changes and retain finite retries. This is a demonstrated mechanism compatible with the physical failure, not exclusive physical-trace attribution. Independent insertion analysis finds 4.5% fewer scored children than reference despite 4.328 times elapsed time and 4.583 times CPU. Most network bytes are JSON scored-child replies, not centroid vectors. Candidate assembly adds ordering and duplicate-ordinal work. Record these as the next fixed-tree mechanism questions after correctness.

## 2026-10-22T07:35:40.833149+00:00 — Navigation repair admitted for one 3M candidate

The exact centroid-refresh retry mechanism passes seven focused tests, including real remote-owner selection, retired/relocated routes and finite structural retries. The applicable native suite passes213 tests with one unchanged local-writer exclusion; all three CLI builds pass. Independent review binds four changed source files and complete source22b SHA25645d56253789916dbacc04a656aee18f42f3d654a0780454759925b5cde8e9af0. The first local archive22 omits four benchmark/test entry points because an older packager is used; it is rejected before admission and retained. The corrected packager includes the known files and matches the prior closure with no omissions. One same3M candidate is admitted in a fresh guarded30minute/$4.032window, retaining valid reference21. No reference rerun, profiler, retry or sweep. A separate local-only candidate assembly experiment begins after the frozen source boundary; it is not included in the admitted run.

## 2026-10-22T07:47:50.082431+00:00 — Epoch22 fails on a distinct local-route retry condition

The repaired candidate completes1M and adds the next1M in5.884seconds, then fails10.702seconds into2Mbalance beforehandoff. Eight attempts over702queryrows take0.495514seconds, sharedsequence2 remainsunchanged, and lastrejection is stale local image. No completecandidate result exists. Keepcontrolled shared-centroid fix; investigate obsolete local references versus legitimate churn and safe deferred discovery. Canonicalassembly remainslocal-only/unbenchmarked. Bothmatchingcleanupreceipts pass and bothAPIstopped; add$1.3260795054626464compute once, no storage. Fullreport `two-machine-v1/local-tests/NAVIGATION-3M-EPOCH22-RESULT.md` preservespartialwork and limits.


## 2026-10-22T08:15:14.962348+00:00 — Epoch23 continuation and assembly admitted

The controlled local-route test reproduces valid concurrent retirement during post-install reassignment; conditional scheduler continuation preserves pending IDs and only waits behind other active structural attempts. Review finds and resolves deferred merge retirement: queued evacuation now retires the source after actual drain. Stable broken routes remain fatal, replay is bound, and required work blocks phase completion. Separate neighbor selection and insertion retain their existing finite failure paths. Canonical navigation assembly preserves exact fixed-tree reference ordering and removes measured extra ordering work.

The combined source passes 220 applicable tests with the same one known unrelated exclusion and builds all three native CLIs. The immutable review is `two-machine-v1/local-tests/EPOCH23-LOCAL-ROUTE-ASSEMBLY-REVIEW.md`; validation is `EPOCH23-LOCAL-ROUTE-VALIDATION.json` in the same folder. Complete archive SHA256 `5344d8ad2480d8fe9e9b92f0b7a01b4c49ff17b743e8f8e3d956cb68addc32c1` contains exactly six changed files versus source22b, with no omissions. Both staged indexes remain unchanged.

`two-machine-v1/operations/admission-epoch23.json` binds 27 artifacts for one candidate-only 3M run with 32 workers per owner, unchanged inputs/settings/capacity and retained valid reference21. The sole operator receives the execution, fresh independent absolute guards and shutdown obligation. Maximum window is 30 minutes/$4.032, with no new storage, automatic retry, sweep or reference rerun. No cost or physical result is claimed at admission.

## 2026-10-22T08:28:25.020414+00:00 — Epoch23 completes distributed insertion and fails final balance

The final million additions take25.719seconds/741.78CPU versus31.920/945.00 in epoch21, with0.87% morecandidate rows. AssemblyCPU312.972→82.279; traffic59.967GB remains. Finalbalance fails1.298seconds in with unexpectedEOF; peerstderr shows oversizedreplyframes beforeconstructionfailure. Preservepartialmeasurements, do notclaimcomplete3M or finalownershipvalidation. BothAPIstopped; workercleanupconfirmed,peerexitmissing/helpererror retained. Add$1.2023860912323compute once,no storage. Fullreport `two-machine-v1/local-tests/CONTINUATION-ASSEMBLY-3M-EPOCH23-RESULT.md`. Next: controlledframefailure diagnosis/correction plus separatelyimplementedlosslesscodec, thenconsolidatedreview.

## 2026-10-22T08:57:15.464240+00:00 — Epoch24 bounded expansion and lossless encoding admitted

The corrected candidate preserves complete scored-child replies and exact reference selection. Versioned little-endian numeric fields reduce the fixed reply fixture from 267,587 to 94,516 framed bytes. Read-only expansions use at most 64 queries per request and subdivide only on explicit size errors, sharing the original absolute deadline. A single parent that cannot fit remains a clear error; arbitrary serialization errors and mutations are not retried. The controlled oversized legacy response reproduces the observed connection failure.

The applicable suite passes 236 tests with one unchanged local-writer exclusion; all three native CLIs build. Independent review resolves exact-full-buffer error classification. Admission binds 28 artifacts and only source24b SHA256 `9e5f4d1ed698acaf22fddb29dc4ae50e336e48e9280f896bfc7eba48391869eb`. The initial archive24 remains unadmitted. The sole operator confirms fresh guards through 09:23:31 UTC and verified uploads. One unchanged 3M candidate comparison against valid reference21 is allowed within 30 minutes/$4.032, without automatic retry, profiling, new storage or a reference rerun. Physical results and cost remain pending.

## 2026-10-22T09:07:37.718933+00:00 — Epoch24 completes physical 3M construction and validation

The candidate validates all memberships, vectors, posting/centroid codes and fresh-reader independence. Both physical owners add and balance the final million; final ownership is 1,490,472/1,509,528. Construction plus saving takes 117.411 seconds versus 63.295 reference. Final insertion improves from 25.719 to 16.292 seconds, CPU741.78 to439.86 and traffic59.967 to36.834GB; changed work and placement limit causal attribution. Final balancing completes in26.204seconds with934.76CPU, of which35.222 is clustering. Save33.305seconds contains19.433seconds of sequential independent owner materialization. Keep the bounded compact expansion candidate; investigate parallel materialization and audit remaining transfer overhead. Full report `two-machine-v1/local-tests/COMPACT-EXPANSION-3M-EPOCH24-RESULT.md`. Both instances API-stopped; peer cleanup receipt remains missing. Add$1.4137360649871826compute once, no storage; first-month covered bound$66.955721418861806683. No next cloud window admitted.

## 2026-10-22T09:18:31.994562+00:00 — Epoch25 bounded concurrent owner saving admitted

Nine saving tests and the existing failed-owner fresh-process regression pass; all three native CLIs build. Deterministic actual local/TCP gates prove two in-flight saves, the no-early-copy/publication barrier, joining the remaining writer after a peer error, unchanged previous generation and exact replay/readback. Independent review accepts bounded batches of two after all owners seal. Successful owner acknowledgments are checked before serial copying; payload validation retains its existing serial order before publication. This does not claim every payload is prevalidated before copying.

Only source25b SHA256 `fe3fd6d0b0ebe475f50a7e4fac1c0263521dddf1cbba002997bb520b3d2a639c` is admitted. The initial25 archive predates a measurement-label clarification and remains unadmitted. The final closure has three changed/new files and no omissions; staged indexes are unchanged. Admission binds24artifacts for one same3M32-worker-per-owner candidate against complete24 and reference21, within a fresh1800second/$4.032window. No automatic retry, profiler, reference rerun or new storage. Source25b remains immutable while later local work may proceed separately.

## 2026-10-22T09:32:34.386506+00:00 — Epoch25 confirms concurrent owner saving benefit

The3M candidate validates allmemberships/vectorbits/codes and independentreader. Save33.305→24.258seconds; summedownercalls19.737 overlapinto10.021secondsjoinedwall, while remotecopy12.882seconds remainsserial. Keep boundedownerconcurrency. Fullbuild/save104.710 versus117.411previous and63.295reference; construction variation is notattributedtosaving. Bothownersconstruct andfinalshare49.6118%. Bothmatchingcleanupreceipts andAPIstoppedconfirmed; launcherhelperpermissionfailurepreserved. Add$1.5120071969604492computeonce,no storage; firstmonthcoveredbound$68.467728615822255883. Report `two-machine-v1/local-tests/PARALLEL-OWNER-SAVE-3M-EPOCH25-RESULT.md`. Nextcandidate26removesrepeatedreplyvalidationscans/clones; localtests/reviewpass, physicaleffectunmeasured.

## 2026-10-22T09:34:49.894659+00:00 — Epoch26 reply validation admitted

Fixed actualTCP/reference-oracle work preserves exactselectedorder and rejection semantics while coverage visits1,750,016→103,424 and logicalreplyclonebytes8,421,376→0. Visits exclude duplicate equality; clones exclude unrelated allocation. Initialfixturetiming0.469091→0.488146seconds remains adverse and establishes no speedup. Focused23routing/codec+8bounded-expansion tests and3nativeCLIs pass; independentreviewclear. Source26SHA256`e80f8b914a4d35ca02217d12e661169d8d1cc2c4f3d79b9c0b24e82c720d34c1` hasexactly2changedfiles/noomissions; bothindexesunchanged. One same3M candidate isadmittedagainstcomplete25/reference21 inafresh1800second/$4.032window, noautomaticretry/ref/profile/storage. Rootownsresultinterpretationandledger; soleoperatorownsclosure.

## 2026-10-22T09:51:56.803213+00:00 — Epoch26 validates but does not improve total elapsed time

The physical 3M candidate takes 109.605 seconds including saving, versus 104.710 previous and 63.295 reference. Validation clones are removed; final-balance validation CPU per row falls approximately23%, while insertion validation CPU per row rises approximately6%. Preserve both findings and the overall slowdown. Final-balance transfer calls rise196,168→245,267, so changed work limits causal attribution. All memberships, vector bits and codes validate; final ownership is1,492,163/1,507,837. Both instances are API-stopped; worker cleanup passes, peer receipt is missing and the helper permission failure remains. Add$1.3843924262237548 compute once, no storage; covered first-month bound$69.852121042046010683. See `two-machine-v1/local-tests/REPLY-VALIDATION-3M-EPOCH26-RESULT.md`.

The separate fixed-transfer diagnostic passes exact membership/code comparison for24 moves into8 destinations. It measures88 protocol calls,56 total remote calls and8 source compactions versus1 local bulk compaction. Native debug81.462ms versus1.770ms is a loopback mechanism result, not physical CPU attribution. Next implement response-local metadata sharing; prepare the smallest safe transfer-batching design separately. No new cloud window is admitted.

## 2026-10-22T10:08:18.510671+00:00 — Epoch27 exact metadata sharing admitted

The response-local dictionary preserves complete decoded replies, full-width identities, exact score bits and order. The fixed64-query payload falls from413,199 to62,266 framed bytes. This is measured fixture traffic, not a whole-run speedup. Twenty-seven routing/codec tests (including the four dictionary tests), eight bounded-expansion tests and three native CLI builds pass. Independent review finds no remaining blocker. The archive contains2,307 verified files with five changed and two new files versus26, no omissions; both staged indexes remain unchanged.

Admission binds25 artifacts and source27 SHA256`10d628b01cf040fe2be1782f79860f5272deed435272584be258d0f5e2a42059`. One unchanged physical3M candidate is admitted against complete26/reference21 in a fresh1800-second/$4.032 window with independent absolute guards. The sole operator owns fresh checks, execution and API shutdown. No reference rerun, profile, sweep, automatic retry or new storage. Root will reconcile measurements and spending after closure. Later local transfer work stays outside frozen27.

## 2026-10-22T10:21:22.473942+00:00 — Epoch27 reduces navigation traffic and insertion cost

Keep exact response-local metadata sharing. Physical3M construction plus saving takes97.369 seconds versus109.605 previous and63.295 reference. Final-million insertion takes12.183 seconds/342.12CPU/7.206GB versus16.638/448.44/34.624GB, with0.6% fewer candidates and7.5% more validated remote rows. Final balance improves but also performs fewer splits, candidate rows and transfer calls; preserve that limitation. All memberships/vectorbits/codes and independent reader validate; final ownership is1,511,280/1,488,720. Both instances are API-stopped, worker cleanup passes, peer receipt is missing and helper failure remains. Add$1.388759104537964 compute once, no storage; first-month covered bound$71.240880146583974683. Report `two-machine-v1/local-tests/METADATA-DICTIONARY-3M-EPOCH27-RESULT.md`. Owner-batched transfer implementation and failure review continue locally, outside frozen27; no next cloud admission.

## 2026-10-22T10:29:18.637331+00:00 — Save-path assessment changes the follow-up priority

The source audit identifies central remote-object copying as a filesystem fixture/backend-reader limitation, not a requirement of the target sharded design. Prefer owner-direct durable publication and independently resolved immutable references after the transfer experiment, rather than optimizing a central relay solely for speed. The current save also rewrites unchanged postings under each new attempt, so reference-style changed-block reuse remains a separate gap. Coordinator synchronization costs are nested within save; the12.667-second copy phase is not a network-throughput measurement. No source, cloud or storage change occurs in this assessment. See `two-machine-v1/local-tests/REMOTE-SAVE-COPY-ASSESSMENT.md`.

## 2026-10-22T10:34:11.036927+00:00 — Transfer-batching review finds two correctness gaps before admission

Independent review identifies a phase-publication race: opening a new epoch before clearing the old batch registry can erase a freshly admitted endpoint record. It also identifies a partial-chunk scheduling gap: an earlier chunk can commit, but a later rejection prevents the construction caller from scheduling affected leaves for balancing. The second issue is not a demonstrated loss of membership. Both require controlled regressions and correction before final review or any physical benchmark. Candidate27 remains the accepted frozen baseline; both machines remain stopped.

## Owner-direct storage feasibility

The read-only feasibility check identifies existing durable-storage and blockfile interfaces for the next save-path improvement. The first step adds backend access to the generation store and saved reader so owners publish immutable objects directly. Ordinary current-key reads must not be described as version-pinned reads; use conditional write-once keys plus integrity verification or an explicit version-aware resolver. Incremental changed-block reuse remains a separate step requiring full-width sharded records and dirty/tombstone tracking. No source, cloud execution or spending change occurs. See `two-machine-v1/local-tests/OWNER-DIRECT-STORAGE-FEASIBILITY.md`.

## Candidate28 local validation and admission

The bounded one-source, one-peer transfer candidate passes257 applicable tests with zero failures and one documented unrelated exclusion. Three native CLIs build. Independent review resolves three epoch-opening/chunk-scheduling findings with controlled regressions; earlier failures remain retained. The fixed24-move oracle reduces protocol calls from88 to11, total remote calls from56 to14 and source compactions from8 to1 while preserving exact membership/codes. These are mechanism counts, not physical speedups. Frozen source and review are bound in `two-machine-v1/operations/admission-epoch28.json` for one fresh guarded3M candidate comparison against27/reference21. No cloud action or new cost occurs in this admission step.

## Candidate28 initial approval rejection

Automatic approval review rejected the combined cloud preflight/start before execution, citing the obsolete tiny-diagnostics restriction. The later direct human authorization permits tests of any size, explicitly requests physical two-machine3M comparisons, and authorizes autonomous fixes and validation overnight. The sole operator is supplying those later instructions through normal review. No28 instance start or compute charge is established at this point; local owner-direct storage implementation continues.

## Candidate28 approval resolved and window opened

Normal approval review accepted the unchanged lifecycle start after fresh read-only preflight and the later direct human authorization were supplied. No bypass or new user permission occurred. The sole operator records a new absolute deadline of1792668240.031864 (11:24:00UTC),1800seconds and$4.032 maximum compute. Both remote guards must precede uploads/build/work. The rejection/evidence remain in `two-machine-v1/operations/epoch28-approval-review.json`; actual cost is reconciled only after closure.

## 2026-10-22T14:03:27.896128+00:00 — New-chat handoff and interrupted window reconciliation

Both agents report usage-limit failures and are interrupted. Deadline closure and fresh AWS API verification confirm both task instances stopped. No final28 Linux-build or benchmark receipt is collected; no machine is restarted. Partial29 contains six changed files and two new files, with successful test-target compilation but no executed-test or review receipt. Both Git indexes remain unchanged. Add $4.080092862548828 compute upper bound once, including shutdown observation latency; covered compute $55.800973009132802683, storage $19.52/month. The user reports the follow-up paused. See NEW-CHAT-HANDOFF.md and two-machine-v1/local-tests/EPOCH28-INTERRUPTION-RECONCILIATION.md.

## Experiment28 build recovery — 2026-10-22T14:15:29.369984+00:00

Manual continuation completes the next unfinished recovery action. Both frozen28 Linux build receipts succeed and bind the admitted manifest. Current executable hashes, peer installation and the physical benchmark remain pending. Both exact task instances are API-stopped; the recovery adds $0.4207406246948242 compute once and no storage. The recurring follow-up remains paused. See [the recovery report](two-machine-v1/local-tests/EPOCH28-BUILD-RECOVERY.md).

## Experiment28 physical failure — 2026-10-22T14:33:11.623212+00:00

Frozen28 fails final3M balance with centroid-refresh retry exhaustion. Both process cleanup receipts and API shutdown checks pass. No saved-reader result or speedup claim. The window adds $0.6338985388946533 compute once. A local finite-revision-churn regression reproduces the exact error; correction validation is underway. Candidate29 storage work runs in a separate user-requested subagent. See [the failure report](two-machine-v1/local-tests/EPOCH28-3M-FAILED-BALANCE.md).

## Corrected28 validated;29admitted

Corrected28 completes3Mconstruction/save and independent reader validation in98.593seconds. Final balance protocol calls fall201637→38638andCPU688.61→563.75seconds; end-to-end time does not improve. Worker cleanup passes; peer cleanup is unconfirmed; both instances API-stopped. Add$1.0134531854248046compute once. Candidate29source de891e2e22813bddf12e52b3843b3ec199c076cdbe61fa6098fc0015ccb67969passes266tests and independent review; separate guarded Linux/S3/3Mwindow now admitted. Follow-up remains paused.


## Epoch31 construction and saving — 2026-10-22

The accepted combined profile reduces median final-million construction from 37.512 to 29.234 seconds, with higher CPU demand. First saves fall from 131.141 seconds sequentially to 7.996–11.072 seconds with bounded parallel posting writes. Unchanged saves reuse every posting block. All timing arms validate exact 3M membership and posting codes; six fresh readers validate durable generations. The separate bounded reader reduces validation turnaround to approximately 52–56 seconds while preserving all checks. The [report](two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md) and [completion receipt](two-machine-v1/local-tests/EPOCH31-COMPLETION.json) retain controls, hashes, limits and outcomes. Machines remain running under renewed guards; the open reservation is updated without closing or double-counting compute. The recurring follow-up stays paused.

## October 22: coordination investigation closed by user

Coordination is accepted and this investigation is closed at Lyon’s request. Six end-to-end pairs and four exact post-insertion pairs consistently show faster balancing with higher CPU consumption. The CPU increase is established for this 3M workload; its complete cause remains unresolved. Keep per-boundary coordination, the notification-based listener and the accepted saving improvements. Preparation under the global publication turn remains a separate opt-in experiment, disabled in the accepted configuration; its full-workload validation is deferred.

Revisit coordination as part of the future larger-index comparison already specified in [the current plan](CURRENT-PLAN.md#future-scaling-hypothesis-coordination-during-balancing). Separate index size, worker count and batch size. Measure conflicts and publication work per million inserted vectors as well as elapsed time and CPU; a smaller coordination fraction alone does not demonstrate less coordination work.

The next recommendation is navigation candidate processing, validation and ordering. The epoch32 report records detailed evidence and attribution limits. Experiment queues are stopped; the last in-flight profile is retained.

## Epoch33 transport and navigation complete — 2026-10-22

Connection reuse and bounded server pools pass correctness, exact navigation replay, 3M construction and persistence validation. Three fresh pairs accept cached parent lookups for 6.6% faster insertion, 7.6% lower insertion CPU and 6.3% lower balancing CPU. Balancing and complete construction elapsed-time differences remain inconclusive. The incomplete v1 cohort retains an unconfirmed frame-limit failure; v3 includes a separately reproduced bounded complete-view encoding fix. Native validation passes 315 tests, with documented ignores/exclusion. First and unchanged saves pass fresh-reader checks. See the [report](two-machine-v1/local-tests/EPOCH33-TRANSPORT-AND-NAVIGATION.md) and [completion receipt](two-machine-v1/local-tests/EPOCH33-COMPLETION.json). Both machines remain running under guards by user request; the open reservation is updated without double-counting closed compute. Recurring follow-up stays paused.
