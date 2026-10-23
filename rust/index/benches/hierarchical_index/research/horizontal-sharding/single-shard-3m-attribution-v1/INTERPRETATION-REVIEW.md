# Independent interpretation review

The four raw results support a small measured construction-time difference on this workload, with substantially different CPU work during balancing. They do not establish a stable performance distribution, equal clustering quality or a complete causal decomposition. This review reads the result files, phase profiles, analysis scripts and implementation; it performs no new benchmark, test or cloud operation.

## Verified accounting

| Measurement | Reference mean | Candidate mean | Interpretation |
|---|---:|---:|---|
| Build and save | 61.867582 s | 63.808011 s | The observed difference is 1.940429 s, or 3.1364%. Both candidate runs exceed both reference runs, but there are only two random constructions per arm. |
| All insertion phases | 13.933062 s | 14.258640 s | The observed difference is 0.325579 s. |
| All balance phases | 30.680554 s | 32.205979 s | The observed difference is 1.525425 s. |
| Commit and flush | 17.232361 s | 17.321303 s | The observed difference is 0.088942 s. |
| First 100K balance | 10.064136 s | 11.500098 s | Its 1.435963 s difference equals 74.002% of the whole build/save difference as arithmetic accounting. This does not assign a causal mechanism to that difference. |
| Remaining eleven balance phases | 20.616419 s | 20.705881 s | Similar elapsed time accompanies 471.415 versus 514.005 process CPU seconds. The candidate executes approximately 9.03% more CPU work in 0.43% more elapsed time. |

The residual outside initialization, insertion, balancing, commit and flush is approximately 21–22 ms per run. It includes diagnostic snapshots, shape gathering and event serialization. The build timer retains it; the report should not force every residual millisecond into an algorithm category. Input loading, writer retirement, reader opening, exhaustive validation and query smoke are separate measured costs outside this build/save interval.

## What explains the insertion difference

The insertion breakdown prevents assigning the complete wall-time difference to resident-node handles. Preflight means are 1.641402 s for reference and 1.828364 s for candidate, a difference of 0.186963 s. Collection means are 11.758901 and 11.907400 s, a difference of 0.148499 s. Posting append means are 0.527273 and 0.517375 s, with candidate lower by 0.009899 s. The preflight implementation is identical between arms. These observations demonstrate that the small insertion difference is not confined to the changed node-access path.

Candidate profiles directly observe resident-handle clone instructions in 2.634% and 2.831% of insertion leaf samples. This is evidence of a local implementation cost with a concrete source mechanism. It is not a measurement of the net wall-time penalty: insertion CPU means differ by only 2.545 s over 355.385 s, or 0.7161%, and other sampled work can compensate or vary. Inclusive node lookup and navigation counts overlap the clone samples. Top-80 symbol truncation, inlining and differing trees limit comparisons with symbols absent from the reference summary.

## What explains the balancing difference

The first balance has limited effective CPU use in both arms: approximately 2.24 reference cores and 2.01 candidate cores. Candidate first-balance CPU increases only 2.56%, from 22.5 to 23.075 process seconds, while elapsed time increases approximately 14.27%. The clustering routine appears in 83.57% and 83.66% of reference first-balance inclusive samples and 81.00% and 78.95% of candidate samples. These observations support an initial split-dependency and clustering critical-path explanation as a leading interpretation. They do not measure individual job critical paths or prove why the candidate’s first balance takes longer. Random convergence, work ordering and unequal available parallelism remain unresolved contributors.

Across all balances, candidate navigation distances increase 12.7216% and process CPU increases 8.7394%. Neighbor-vector evaluations increase 2.5202%, while split-self evaluations increase 0.3309%. Accepted transferred rows remain essentially unchanged: 1,131,603.5 reference versus 1,131,414.5 candidate, a difference of −0.0167%. Deferred transfer groups decrease 13.7291% and claim conflicts decrease 20.0260%. Therefore a claim that this 3M candidate performs more retries or fewer transfers would contradict the evidence, even though that description applies to the earlier 100K runs.

Later balance phases execute more CPU work in nearly the same elapsed time. Higher effective process concurrency is directly measured; attributing all of that improvement to the immediate scheduler would require controlling the changing work and tree trajectory. Navigation restart counts of 524 and 457 identify a real changed mechanism, but they do not by themselves explain the complete additional distance count. Different scoring sets, retirement visibility and resulting tree shape can also alter work.

Candidate structural reservations accumulate approximately 0.124–0.162 s of waiting and 0.597–0.605 s of holding per complete run. These are overlapping worker durations rather than exclusive wall time. They show that structural coordination executes and has a cost; they cannot be subtracted from the 1.940 s build difference or used to compare against an unmeasured reference structural-lock wait. Scheduler waiting is also an overlapping worker sum, not added wall latency.

## Scope and adverse evidence

The final trees have three levels, with 33–35 internal nodes below the root and 2,093–2,113 leaves. Candidate internal-job counts of 32 and 35 confirm that this campaign exercises deferred internal work absent from the shallow 100K fixture. Every saved tree passes complete exact-once membership, reachability, parent links, vector-bit, one-bit posting-code and four-bit centroid-code validation. These checks establish structural correctness for the measured outcomes, not equal clustering quality or recall.

Whole-process peak RSS is approximately 19.84–19.90 GiB across all arms. High-water RSS through the last balance is approximately 11.15 GiB. High-water values are cumulative and do not reset at phase boundaries. All construction vectors and postings remain resident, so this experiment does not measure cache-capacity crossing or lazy-storage costs. Saving and validation still perform separately timed storage work.

Profiles report zero lost samples and zero unparsed nonempty lines. Unknown leaf samples remain present, and phase boundaries include nearby diagnostic work. CPU samples measure execution share rather than wall-clock waiting; inclusive stacks overlap. Clustering iteration counts and per-job critical paths remain unmeasured. Neither this report nor the earlier 100K comparison proves that construction ratios improve as a general function of scale: batch schedule, tree shape, input working set and profiling conditions matter.

The defensible conclusion is that this one-owner implementation completes the authorized 3M workload within roughly 3% of reference build/save time in these four runs. It spends more balancing CPU and navigation work, with the initial 100K balance contributing most of the observed elapsed-time difference. Resident-handle cloning is an observed implementation cost; the remaining net gap is only partially attributed. No measured cost here is a network or physical-distribution cost.
