# Shared child navigation during balancing

## Result

Scoring children from a shared parent payload reduces balancing time by about 40% in repeated MS MARCO replays from the same validated tree. Fresh 1M builds show 32% lower balancing time for Wikipedia and 38% lower balancing time for MS MARCO. The observed gain is larger than the variation between the repeated runs. This implementation is a useful next improvement.

The checkpoint total includes source-vector loading, addition, balancing, commit, and reopen. It excludes final recall, final diagnostics, and separate correctness checks. Source loading limits the improvement in the full checkpoint total.

| Comparison | Control balancing | Candidate balancing | Reduction | Control checkpoint total | Candidate checkpoint total | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MS MARCO 800k→1M replay mean, two runs each | 9.01 s | 5.42 s | 39.8% | 39.95 s | 36.13 s | 9.6% |
| Wikipedia fresh 1M build | 53.07 s | 36.23 s | 31.7% | 79.53 s | 62.17 s | 21.8% |
| MS MARCO fresh 1M build | 56.49 s | 35.30 s | 37.5% | 149.65 s | 127.98 s | 14.5% |

Fresh builds use one timing run per version and dataset. The repeated replays provide the stronger controlled comparison. Parallel balancing can produce different trees, so these percentages are observations for this workload rather than guarantees for every index.

## How it works

A parent shares its child IDs and their full-precision centroids as one immutable navigation payload. A search obtains a reference to that payload, releases the parent's map guard, and scans the contiguous centroid array. It checks the selected nodes before continuing through the tree. Scoring a cached child no longer requires fetching its node and taking its map guard.

The writer publishes a fresh payload when a structural operation changes the parent's children. It invalidates the centroid array before changing IDs, then publishes a matched array while holding the existing tree-structure lock. If an internal node's centroid changes, its parent receives that new centroid too. New internal nodes and roots receive payloads before they become reachable. Searches holding an old reference retain matching old IDs and centroids because mutations copy the shared navigation data before changing it.

A parent with an invalidated or uninitialized payload falls back to live child lookups. Temporary missing children receive the same absent-child marker used by the stable add layout. Restoration paths refresh their parent, and the joined tree is refreshed after balancing. The initial add on a reopened writer also initializes the layout. This is a per-parent snapshot; a search does not take a single snapshot of the entire tree.

The add phase already uses packed child centroids. This change enables that layout during balancing and keeps affected parents current during structural mutations. Distance arithmetic, beam policy, destination posting-list loading, vector-version handling, and dataset settings match the control.

The implementation uses the existing shared navigation object rather than adding a separate child-ID cache. Its code is in `rust/index/benches/hierarchical_index/writer/writer.rs`, with payload documentation in `common.rs`. The node lookup test counter is compiled only in tests and adds no benchmark counter overhead.

## Repeated checkpoint results

Each replay independently copies a fresh 800k MS MARCO checkpoint, adds two 100k batches, and commits and reopens at each boundary. The sequential run order is control, candidate, candidate, control.

| Run | 900k balance | 1M balance | Balance sum | Checkpoint total | Peak RSS | Cold recall at 100 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control 1 | 4.18 s | 4.91 s | 9.09 s | 40.19 s | 6.91 GiB | 80.95% |
| Candidate 1 | 2.46 s | 2.77 s | 5.23 s | 35.97 s | 6.92 GiB | 81.14% |
| Candidate 2 | 2.82 s | 2.79 s | 5.61 s | 36.28 s | 6.97 GiB | 81.20% |
| Control 2 | 4.31 s | 4.62 s | 8.93 s | 39.71 s | 6.79 GiB | 81.25% |

Mean source loading is 28.14 seconds for the control and 27.97 seconds for the candidate. Mean add time is 0.969 versus 0.974 seconds. The timing gain comes from balancing.

Combined worker time in reassignment navigation falls from approximately 240 seconds to 69.86 seconds on average, a 71% reduction. These worker elapsed times overlap and are rounded in the source table; they are not build wall time. Other work, including destination posting-list loading, accounts for a larger share after navigation becomes faster. The navigation result supports the intended mechanism without requiring a wider SIMD distance implementation.

## Recall and memory

Recall is similar in these measurements. The replay mean is 81.10% for the control and 81.17% for the candidate. Fresh Wikipedia builds measure 77.38% versus 77.66%, and fresh MS MARCO builds measure 81.09% versus 81.33%. Each recall measurement uses 100 precomputed queries. These results show no observed reduction; they do not establish statistical equivalence or a recall improvement.

| Fresh build | Control peak RSS | Candidate peak RSS |
| --- | ---: | ---: |
| Wikipedia | 7.33 GiB | 7.40 GiB |
| MS MARCO | 7.07 GiB | 7.44 GiB |

The implementation does not establish a memory reduction. The candidate's peak process memory is modestly higher in these runs. Shared payloads can retain previous arrays briefly while searches finish, and different parallel trees and loaded blocks also affect peak RSS; these measurements do not isolate their contributions.

## Correctness and checkpoint provenance

The control includes the persisted-version registration fix at `39626fb14`. The candidate also includes that fix and retains eager destination posting-list loading, so the performance comparison isolates navigation rather than combining it with the preceding deferred-load experiment.

The fresh source checkpoint is generated with the corrected control. Exact set validation succeeds before every commit and after every reopen through 800k vectors. Its final tree has all 800,000 expected vector IDs reachable with valid postings. It therefore avoids the inherited 2,076-ID deficit in the source snapshot used for the preceding experiment. A missing ground-truth file at the 800k boundary causes the source builder to skip recall normally; its checkpoint and posting validation complete successfully.

The candidate passes 43 local test executions across the policy/navigation and posting-delta harnesses. One regression fails on the control with two child point lookups and passes on the candidate with zero. It checks matched scores after same-size child replacement, an unchanged retained old payload, and root collapse. A second regression checks zero child scoring lookups across cascading internal splits before the joined refresh. A parallel-split regression builds a multi-level tree, preserves the exact set of vector IDs, compares every packed centroid with its live child centroid, and compares cached and live navigation scores. Formatting, diff checks, and commit hooks pass.

Separate fresh EC2 candidate builds validate exact root-reachable vector-ID sets before every commit and after every reopen. These validation runs are excluded from the timing comparison. Their final outcomes are saved in `validation-summary.json` and the validation logs.

The implementation is committed at `cc6a16140` on `codex/balance-child-navigation-payload`. EC2 measures that commit. The branch also contains a test-only follow-up at `121e57b44`; production code matches the measured commit. The control is `39626fb14`. Neither branch is pushed and no PR is opened for this experiment.

## Reproduction

The existing lyon EC2 instance is an r6id.16xlarge with 64 virtual CPUs and 512 GiB of memory. Data and checkpoints use local NVMe. Both datasets have 1,024 dimensions. Builds use ten 100k checkpoints, full-precision navigation and neighbor reassignment, parallel balancing, a 1 GiB block cache, L2 distance, 100 precomputed queries, recall tau 1.5, and rerank multiplier 1.

Wikipedia uses 32 add and balance threads, beam tau 0.5, level taus `_,_,0.30`, and RNG factor 0. MS MARCO uses 64 add and balance threads, beam tau 1.5, and RNG factor 4. The percentage-floor flag is absent. All runs are sequential; exact commands and resource measurements are saved beside this report.

The replay source is `/mnt/data/child-navigation-source-800k`. A 3.4 GiB copy is preserved on the existing persistent EBS volume at `/mnt/benchmark-source/checkpoints/child-navigation-800k`, with matching checkpoint metadata. The volume is restored to its read-only mount after copying. Each mutable replay uses an independent copy. The source checkpoint metadata has matching SHA-256 checksums before and after the timing replays. Remote results are under `/home/ubuntu/benchmark-data/child-navigation-7842`; local logs, commands, scripts, revisions, and parsed results are saved beside this report. The EC2 instance remains running.
