## Full 21M→22M comparison

The larger test confirms the same bottleneck in a complete committed increment. Each implementation and control runs twice, in the order control, block fill, bulk scan, bulk scan, block fill, control. Every run starts from an independent copy of the same preserved 21M tree. The fixture extends the validated 10M output to 21M with the block-fill implementation; it does not reuse or overwrite the original 40M tree. Each increment commits, reopens, and evaluates 100 exact queries against saved 22M ground truth.

| Measurement | Control | Metadata-block fill | Bounded bulk scan |
|---|---:|---:|---:|
| Balance samples | ~240 / ~216 s | 28.22 / 29.22 s | 27.50 / 22.20 s |
| Mean balance | ~228 s | 28.72 s | 24.85 s |
| Balance reduction | — | **~87%** | **~89%** |
| Initial-open samples | 146 / 140 ms | 145 / 145 ms | 1.42 / 1.38 s |
| Checkpoint samples as printed | 4.7 / 4.2 min | 1.1 / 1.1 min | 1.1 / 1.0 min |
| Mean initial open + checkpoint | ~267 s | ~66 s | ~64 s |
| Reduction including open | — | **~75%** | **~76%** |
| Full-process samples | 586.98 / 423.62 s | 110.19 / 109.67 s | 111.10 / 106.12 s |
| Mean full process | 505.30 s | 109.93 s | 108.61 s |
| Peak RSS range | 125.98–126.41 GiB | 121.55–121.84 GiB | 121.64–121.88 GiB |
| Recall range, tau 1.5 without full-precision reranking | 72.99–73.30% | 73.15–73.22% | 73.12–73.23% |
| Valid unique vectors / orphans, every output | 22M / 0 | 22M / 0 | 22M / 0 |
| Balance rounds | 3 / 2 | 8 / 8 | 6 / 3 |

The CLI rounds durations longer than 60 seconds to 0.1 minutes. Control balancing and all checkpoint totals therefore have roughly ±3 seconds of display rounding; their averages and percentage reductions are approximate. Full-process times come from the precise external process timer. The full-process comparison includes expensive post-reopen diagnostics and recall, so it does not replace the separately reported write-checkpoint comparison.

The amount of reassignment work remains similar while destination loading gets much faster. In the first sample, the control performs 454.7k reassignments and 772 splits; block fill performs 451.2k reassignments and 767 splits. Average destination registration within reassignment falls from 10.5ms to 31.5µs, and the average entire reassignment falls from 33.9ms to 2.7ms. These timers include waiting and nested work and cannot be added to obtain wall time.

The two fixes have similar overall performance in this sample count. Bulk scan has a lower mean balancing time, while block fill preserves faster opening. Parallel work ordering changes the number of balance rounds and tree shape; two samples do not establish a small performance difference between the fixes or recall equivalence. Candidate recall ranges fall within the control's observed range. The peak-memory difference also includes changes in parallel work and reader pinning, so it is not attributed solely to cache filling.

The shared 21M source remains at `/mnt/data/bulk-version-cache-fixes-source-21m` for future comparisons. All 57,510 files match its before/after manifest, totaling 93,212,784,474 bytes. Its checkpoint SHA256 is `bb1b21158c5f070c3a108fe7f2b9233b5e9477f827b423a016ee44e3e201095d`. The original 40M manifest and checkpoint hash also match. Existing global ground truth is restored after the comparison, the lock is released, and no benchmark is running.

