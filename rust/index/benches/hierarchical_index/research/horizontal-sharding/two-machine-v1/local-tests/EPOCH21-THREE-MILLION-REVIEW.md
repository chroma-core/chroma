# Epoch21 paired three-million-vector admission preparation review

The fixed paired 3M preparation has no blocker within this review scope. It reuses the reviewed epoch20 candidate binaries and retained instrumented reference without compiling new source. Its purpose is to measure the final million additions and balancing after placement starts using both physical owners. A result that merely ends with two occupied machines does not answer that question.

## Workload and retained identities

The reference template preserves 768 dimensions, 32 workers, 100K add/balance intervals through the first 1M and 1M intervals afterward. The candidate preserves the same cadence and worker count, split 2,048, merge 512, branching 100, full-precision L2 construction navigation, its existing beam and reassignment settings, one-bit postings and four-bit saved centroids. Both owner capacities become 2,160,000, with no first-interval override and no next-interval reserve. Neither the placement trigger nor its receiver target changes.

The retained corrected descriptor identifies exactly `/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb3m.f32`: 3,000,000 rows, 768 dimensions, stride 3,072 and 9,216,000,000 bytes, SHA256 `18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba`. The candidate points to the neighboring retained `3m.json`; the reference points to the same vector file and hash. This is a local descriptor check, not a fresh remote-file verification. The operator must verify the descriptor and actual bytes and hash on both hosts after the independent guards are active.

The generator requires the exact instrumented reference `8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6` at the epoch12b reference path. It requires the epoch20 writer `5efa72832ae7647de5361dd45ed4a4a95f8905a8e47ee77ade79a8da5b539167` and reader `5d98397f6a4b159cc6e818d667ddfb1888c4c75a6e78e4cb47d33e7b3c3917f4`. It rejects absent or changed runtime-registry contents. That registry is absent at review time and must be created only after fresh remote checks. Both candidate hosts must use the matching binaries; the historical reference beginning `6ff2767b` is separate evidence.

## Structural question and acceptance

The 80% trigger is 1,728,000 vectors. Consequently this cadence reaches placement after the 2M balance, and the final 1M interval can exercise construction on both owners. A 2M result ending at placement would miss that behavior. The 70% receiver ceiling prevents a single 2M-root move and requires refining existing branches. These are policy arithmetic and an intended condition, not a claim that placement or subsequent work succeeds.

Offline acceptance must check nonzero membership on both owners after 2M placement, positive new-membership increments on both during the final add, and actual completed owner jobs with useful split or transfer work during the final balance. It must also require exact 3M membership, vector identity, posting and centroid code checks through the fresh reader, and the existing observed minimum 30% final owner share. Missing participation or a failed operation produces an incomplete or negative result, not a successful two-owner performance conclusion. No new live orchestrator is required for these checks.

The vector-count capacity is not a hard RAM bound or reserved headroom for the next interval. Both processes retain the complete immutable corpus cache; this workload remains below its configured 12GiB cache capacity and does not test cache-capacity crossing. Record actual per-owner loads and memory rather than extrapolating the 500K proportions.

## Execution controls

The operator invokes exactly one reference arm first and then separately invokes exactly one candidate arm. Each invocation is explicit and has its own fresh output namespace. The runner does not automatically advance, retry or sweep. It checks the original window remains open and has at least 660 seconds remaining, then sets one shared absolute job deadline no later than 590 seconds from that invocation. The supervisor retains its 600-second cap, and the fresh instance window remains at most 1,800 seconds. Peer input verification and warming consume that same candidate deadline; they do not extend it.

The candidate waits for the peer listener before running its coordinator. The retained supervisor checks exact binary and reader hashes and configured resource headroom before launch. Reference free-disk headroom is 12GiB and candidate headroom is 4GiB; both require 32GiB available memory and 32 physical cores. The independent local and remote shutdown guards, exact sandbox resource identity, rate and budget checks remain operator admission obligations. No rebuild, profiler, new storage, corpus download or guard change is prepared.

The known stop-helper failure remains preserved if it occurs. Cleanup reports must match the current job identity, and the operator must verify both task instances stopped through the API. An API-stopped state and a missing process cleanup receipt are distinct observations. The runner’s two arms can be invoked independently, so the sole operator enforces the documented reference-first sequence; there is no automatic paired controller.

## Offline accounting

The two-owner analyzer subtracts each owner’s own before/after CPU, kernel, work and activity snapshots, then sums the two distinct processes once. It retains jobs, splits, transfers, distances, expansions, vector fetches, scheduler readiness and coalescing observations. It does not treat the coordinator-only kernel snapshot as a cluster total or add the peer’s duplicate transport endpoints to already combined RPC accounting.

A cross-check on the retained epoch20 result gives zero peer balance jobs and kernel CPU in every interval, summed kernel CPU of 35.384215492 seconds and summed snapshot CPU of 66.06 seconds. The exact coordinator balance timer reports 65.44 seconds; the analyzer labels the wider sequential snapshot boundaries. Inclusive scopes remain overlapping, and cumulative peak memory does not prove simultaneous cluster peak memory. A reference run uses one 32-core machine while the candidate uses two, so report CPU and compute cost alongside wall time. The retained 16-query smoke check is not evidence of recall equivalence.

## Validation and final bindings

All eight file hashes in `operations/preparation-epoch21.json` match at review time. The five local preparation tests pass in `epoch21-preparation.log`, covering all three process roles, retained hashes and settings, missing/changed registry rejection, prohibited reference serving and the 590-second bound. The source-unchanged epoch20 native receipt remains 207 applicable tests passing with one explicitly excluded unrelated reference-writer test. No new compiled source or native test result is claimed for this operational preparation.

The root operator binds final configuration status and runtime hashes, fresh identity and preflight receipts, immutable templates, the unchanged source20 closure, input readbacks and both absolute guards before starting. Configuration status and source/runtime metadata may change for admission while the reviewed workload and bounds remain fixed. This reviewer runs no Cargo, cloud operation or Git write.

## Reviewed hashes

Paths are relative to `horizontal-sharding/two-machine-v1`.

| File | SHA256 |
| --- | --- |
| `operations/make-3m-spec-epoch21.py` | `5cc8da20eee977a12a9573af26866950450fa7e360a248871df7861f99388ae9` |
| `operations/run-one-arm-epoch21.py` | `fa5dc982fc7b1288e550741808316740e51a056abbd12d41836cef0b94830538` |
| `operations/remote-job-epoch15.py` | `3cc6798a1ae5ef4b8e2787dc36f0233f38ee9ef3030150e6ddfe4fabce2e0f0b` |
| `operations/owner_local_epoch21_test.py` | `b36d260f1755350cc63a90f156e61e70e5d4f79e722d98216ea1d8e0a46ea13b` |
| `operations/owner-local-epoch21-preparation.md` | `5870a3d770d5e0b7abac8d2d92508f3361d719fe48458a0ddd79edab9a0da67d` |
| `operations/config-epoch21.json` | `1e00e1b5af0f8a4769d8f6bb2205af20377a5b2015de89aaf3d9276dc5f68d76` |
| `operations/window-epoch14/500k-reference/run.json` | `a0d6070bc6b3a5c62427db8433fc7cae1604a8000081bd36bbd0c3c423e36827` |
| `operations/window-epoch20/500k-distributed/run.json` | `93fac2e5925e18c1b06e92b33790ef8028f8c9fcfc43651799a7a8eb20ec1f18` |
| `local-tests/summarize-distributed-construction.py` | `91115956f80d0547bd20820528809cb7febf665a62054385e3092cbf917e675e` |
