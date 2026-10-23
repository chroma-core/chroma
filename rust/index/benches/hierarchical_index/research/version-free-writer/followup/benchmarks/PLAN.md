# Wikipedia follow-up experiments

The experiment measures production insertion checks and the effect of compacting each source posting once across its destinations. The primary comparison holds the instrumentation and validation order constant between the instrumented original transfer algorithm and the optimized algorithm.

The instrumented reference is commit `8581e80920036247715da0b40a4d0a4702118122`. Its validation runs coordinate checks and ID checks in separate passes, whereas commit `9b9c7ebde93144055b1fc86ad29e7ea3b64c140b` interleaves them. The reference comparison therefore measures the compaction change against the same instrumented preflight structure.

Timed builds use Wikipedia only, 32 insertion and balance threads, full-precision navigation, write tau 0.5, level taus `_,_,0.30`, branching 100, split 2048, merge 512, and a 1 GiB cache. The 1M experiment commits every 100k vectors. The 3M experiment commits every 1M vectors. Production preflight remains inside the measured insertion time. The benchmark-only posting validation is disabled during timed builds; saved-index posting, coverage, embedding, and code audits run separately.

Resource samples record process CPU counters, IO counters, faults, RSS, and thread context switches once per second. Output lines receive receipt timestamps in a separate file. These records can distinguish a CPU-heavy interval from an IO or scheduling stall without per-vector profiling.

The verified 1M ground truth is reused. The existing 3M ground truth has the same 100 query vectors and passes a scoped exhaustive float64 L2 scan: all 100 neighbor sets match. The first 30 source shards match the documented source byte for byte. Preparation is excluded from benchmark times and shared caches remain unchanged.

The earlier 9M checkpoint has no retained exact 8M predecessor. An unrelated older 8M index cannot reproduce that tree trajectory. The 1M and 3M traces will first identify round costs. If they do not explain the prior outlier, the next proposed experiment is a fresh instrumented build through 9M, retaining an isolated 8M snapshot for repeated 8M-to-9M replay. That larger experiment requires the parent's decision before execution.

The host uses the existing shared benchmark lock. Outputs, archives, temporary files, and cargo artifacts stay on NVMe. The scripts execute fixed build and benchmark commands. Automatic approval rejected an earlier proposal to run arbitrary future queue scripts; that supervisor was not created or executed.

Transfer counters need careful interpretation across the compaction change. The reference counts a failed source-target pair as one conflict and queues one target group. The optimized algorithm counts a failed source claim as one conflict but queues all target groups; each group increments the retry counter. Pending IDs at drain entry therefore provide more useful volume evidence than raw conflict counts, although they count queue visits rather than unique IDs. Claim-held duration measures one claim object without weighting the number of reserved leaves. A smaller sum does not establish lower total leaf reservation time. Successful transfer batches count source compactions in the optimized algorithm and pair applications in the reference.
