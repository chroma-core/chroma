# Navigation investigation after PR #7842

Child-node lookup is the largest measured elapsed-time component inside full-precision navigation during balancing. Two sampled MS MARCO replays attribute about 61–63% of sampled navigation time to child and parent map lookup, including hashing and read-guard acquisition. Distance evaluation accounts for about 26–27%. Copying child IDs accounts for less than 0.3%.

## Sampled navigation breakdown

One complete full-precision navigation call in 64 is timed on each balance worker. Nested timers separate parent/child map access, child-list copying, centroid-distance evaluation, and sorting. These percentages describe only the sampled navigation calls. They are not percentages of whole-build time or all balance-worker time. Timer overhead is included, and periodic sampling can be biased by workload patterns.

| Component | Replay 1 | Replay 2 |
| --- | ---: | ---: |
| Node-map lookup and guard acquisition | 61.4% | 62.6% |
| Centroid-distance evaluation | 26.8% | 26.2% |
| Child-list copying | 0.27% | 0.28% |
| Sorting | 2.24% | 2.17% |
| Remaining sampled navigation work | 9.29% | 8.81% |

The lookup measurement combines hashing, shard synchronization, and time waiting for a guard. It does not isolate lock waiting from the other lookup costs. The remaining category includes routing, some uninstrumented root/retained-candidate lookups, allocation, guard release, and writer counter updates. The distance stage includes reading centroid memory and arithmetic. Splitting those costs requires a further experiment.

## CPU samples and instructions

A separate replay records approximately 30,000 cycle samples with no lost samples. The distance routine accounts for 54.21% of sampled cycles across the entire replay. Navigation dispatch accounts for another 15.22% directly, with sorting and node-map functions also visible. Because the replay includes input loading, balancing, commit, and recall, these are whole-process CPU sample proportions. Some recorded call chains contain invalid or unresolved frames, so the caller report cannot reliably assign every distance sample to reassignment.

The distance routine's instruction annotation confirms that the L2 loop uses 128-bit SSE loads and arithmetic. The EC2 CPU supports AVX2 and AVX-512. The shared distance implementation compiles its wider branches only when the corresponding target features are enabled; this binary's L2 loop remains SSE. Benchmarking a wider distance implementation is a separate candidate. Different accumulation order and fused arithmetic require recall and borderline-decision checks.

The process-wide hardware counters report about 7.03% cache misses among cache references and 1.29 instructions per cycle. These counters include input decoding and blockfile operations. They cannot establish the navigation loop's cache-miss rate or prove that centroid arithmetic is the dominant memory bottleneck. The sampled elapsed timers provide the clearer ranking within navigation.

The first CPU capture finishes its benchmark but gets stuck during profiler symbol processing. The successful capture uses `perf record --no-buildid` and reports symbols without inline resolution. Conclusions use the successful capture only. Its raw `perf.data` remains on EC2; text reports and the instruction annotation are preserved locally.

## Controlled short-guard experiment

The experiment takes a shared reference to a child's immutable navigation payload while holding the map guard, then releases the guard before calculating its distance. It keeps live-tree lookups, beam policy, distance implementation, and writer configuration unchanged. It adds shared-reference increments and decrements, so a benefit from shorter lock holds can be offset by that extra work.

Four runs execute sequentially in this order: baseline, candidate, candidate, baseline. Every run independently copies the same immutable 800k-vector checkpoint, then adds two 100k checkpoints. The baseline binary is the exact saved binary from the preceding reassignment profile. The candidate differs only in shortening the child guard around distance evaluation.

| Run | 900k balance | 1M balance | Sum of balance time | Sum of checkpoint wall time | Cold R@100 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline 1 | 4.14 s | 4.59 s | 8.73 s | 39.95 s | 80.73% |
| Candidate 1 | 4.31 s | 4.45 s | 8.76 s | 39.52 s | 80.78% |
| Candidate 2 | 4.26 s | 4.46 s | 8.72 s | 39.88 s | 80.70% |
| Baseline 2 | 4.16 s | 4.09 s | 8.25 s | 39.48 s | 80.83% |

Mean summed balance time is 8.49 seconds for the baseline and 8.74 seconds for the candidate, about 2.9% slower for the candidate. The measurements do not support promoting this change as a performance improvement. Two repeats and varying split decisions do not establish a statistically significant regression either. Whole-checkpoint means are effectively equal because source loading dominates these replays.

All four processes exit successfully and report 1M unique vectors with zero orphaned vectors. The source checkpoint metadata checksum matches before and after. The two relevant test harnesses pass all 35 test executions, including shared tests executed in both harnesses. The sampled-timer refinement passes the 14-test policy/navigation harness. Formatting and commit hooks pass.

## Next implementation to evaluate

The strongest next experiment should remove most per-child map lookups during balancing. A parent should publish its child IDs and their centroids together as a consistent immutable payload. Navigation should briefly look up the parent, take a shared reference, release its guard, and score all children directly from that payload. The current packed representation serves additions but is disabled while balancing changes the tree.

Every structural mutation must update the payload: replacing split children, removing a merged child, adopting orphaned nodes, and creating a new parent. Build replacement data before taking the parent mutation guard where possible; avoid holding one node-map guard while acquiring unrelated guards. Publish child IDs and centroid rows together so replacing the same number of children cannot accidentally retain centroids belonging to the old IDs. Preserve handling of destinations that disappear before registration.

Validation should compare scores and candidates with live-tree navigation on a fixed tree, verify that payloads match the reachable tree after mutations finish, exercise concurrent split/merge/reassignment, and compare recall. The experiment must measure balancing as well as snapshot-update cost. Reusing a stale additions snapshot throughout balancing would not meet those requirements.

A wider SIMD distance path is the second navigation-specific experiment. It targets a substantial remaining cost, but the measured lookup component explains why changing distance arithmetic alone cannot address most sampled navigation time.

## Notion follow-up

Destination posting-list loading is recorded as suggestion **12. Defer destination posting-list loading during reassignment** in [the experiments page](https://app.notion.com/p/3e658a6d81918194a2a0d59a238d1618). The note includes the observed 14–15% of exclusive worker time in full builds and 20–21% in controlled replays, separates this from the small remaining registration cost, and requires the replacement posting to exist before its new version becomes visible. The page is fetched again to verify the new section.

## Reproduction and source

The node is Lyon's `r6id.16xlarge`: 64 vCPUs, 512 GiB RAM, Intel Xeon Platinum 8375C. All investigation runs use the established MS MARCO configuration: 1024-dimensional vectors, L2, 64 add and balance workers, full-precision navigation and neighbor reassignment, write tau 1.5, RNG factor 4, a 1 GiB cache, 100 precomputed recall queries, recall tau 1.5, rerank factor 1, and no ground-truth cluster diagnostic. They resume at 800k and end at 1M with 100k checkpoints.

The baseline is commit `85d41e1de71d1166214a6ed8b46d9126d81f8772`. Sampled instrumentation is saved in commit `9f9cfac3e` on `codex/reassignment-profile-7842`, local checkout `/private/tmp/chroma-reassignment-profile-7842`. The short-guard experiment is commit `526369d2c` on `codex/balance-navigation-short-guards`, local checkout `/private/tmp/chroma-navigation-short-guards`. Both branches remain local investigation branches.

The local `cpu`, `sampled`, and `short-guards` directories contain logs, exact commands, and the available profiler and resource reports. Remote artifacts are under `/home/ubuntu/benchmark-data/navigation-cpu-profile-7842-retry`, `/home/ubuntu/benchmark-data/navigation-sampled-7842`, and `/home/ubuntu/benchmark-data/navigation-short-guards-7842`. The remote binaries and full patches are saved with their run artifacts. All benchmark jobs have completed; the EC2 instance remains running.
