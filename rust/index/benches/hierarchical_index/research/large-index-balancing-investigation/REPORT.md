# Large-index balancing investigation

The investigation confirms a substantial bottleneck in loading vector versions: the writer repeatedly rereads whole metadata blocks while it fills its compact version cache. A narrow replay of the real loading path is more than twelve times faster when those scalar blocks remain available. The full contribution to the 40M balancing time still needs a complete large-index checkpoint comparison.

## What the writer needs during balancing

Balancing moves vectors between leaves, which are the groups of vectors stored at the ends of the tree. Before moving a vector, the writer checks its current version to distinguish its live posting from an older copy. It also loads the destination leaf's posting list when that leaf has not yet been materialized in memory.

The compact version cache retains individual versions for the lifetime of an opened writer. That cache fills on demand. Fetching a missing version reads a block containing many nearby vector IDs from persisted scalar metadata. Leaf posting lists contain IDs distributed across that metadata, so even one leaf touches many scalar blocks.

The writer clears the scalar reader's retained blocks after every 64 requested versions. Subsequent misses depend on a separate shared block cache. That cache also serves posting lists and full-precision embeddings, and the benchmark limits it to 1 GiB. At large sizes, needed scalar blocks no longer remain available throughout this fill process. The writer rereads, copies, hashes and decodes them for other IDs in the same blocks.

One mutex serializes all cold version prefetches in an opened writer. Repeated block fetches therefore also make other balancing workers wait. Destination registration includes these loads, so its existing average timer includes much more than the final append. Recursive balancing timers include nested work and waiting; they are not independent phase costs that can be added to obtain wall time.

## A short reproduction using saved data

The investigation preserves the final 40M index and creates independent copies of the components needed for posting/version loading. The 40M component copy uses about 7.1 GiB and contains its scalar metadata, posting lists, tree metadata and centroid blocks. It excludes the corpus embeddings. The corresponding 9M copy initially uses about 1.6 GiB. Every copied file has an independent inode; neither writable hardlinks nor references back to the source data are used.

The replay opens the copied metadata and loads 200 fixed leaves through the actual writer loading method. It performs no balancing, embedding reads, commit or checkpoint overwrite. The leaf order is deterministic. Comparing cache settings on the same 40M copy isolates cache effects from tree shape and historical benchmark differences.

| Saved index | Shared block cache | Scalar-reader behavior | Time to load 200 leaves |
|---|---:|---|---:|
| 9M | 1 GiB | Clears blocks after 64 versions. | 0.920 s |
| 9M | 8 GiB | Clears blocks after 64 versions. | 0.912 s |
| 40M | 1 GiB | Clears blocks after 64 versions. | 20.331 s |
| 40M | 8 GiB | Clears blocks after 64 versions. | 1.629 s |

A second instrumented pass measures the byte amplification. Retaining scalar-reader blocks is a controlled diagnostic change, not a proposed final implementation.

| 40M configuration | Total leaf-loading time | Time inside version prefetch | Bytes copied from files during the replay |
|---|---:|---:|---:|
| 1 GiB cache; clear scalar blocks. | 21.093 s | 20.806 s | 34.624 GiB |
| 1 GiB cache; retain scalar blocks. | 1.598 s | 1.309 s | 1.084 GiB |
| 8 GiB cache; clear scalar blocks. | 1.670 s | 1.366 s | 1.084 GiB |

The baseline spends 98.6% of its leaf-loading time inside version prefetch. Retaining scalar blocks reduces total time by 92.4% and file-copy traffic by 96.9%. The two interventions produce the same lower read volume, which supports eviction and repeated scalar block fetches as the mechanism.

The operating system already caches these files. Both instrumented cases report zero physical filesystem input and zero major page faults. The byte column measures process read traffic, including copies from the operating system's file cache; it does not mean 34.6 GiB reaches NVMe.

## CPU sampling

A CPU profile of the slow 40M replay attributes about 43.7% of sampled cycles to byte hashing, 25.1% to kernel memory copying during reads, and 15.7% to iterator processing. The local storage backend hashes the complete file contents to construct a content fingerprint when fetching a block. Repeated application-cache misses therefore repeat this hashing as well as copying and decoding.

This backend detail limits comparisons with production object storage. Object storage can supply the content fingerprint without hashing every fetched block locally. Repeated block fetches are still costly there, but their network and server latency replaces some of the local benchmark costs. The profile does not establish a production latency estimate.

## Actual balancing replay

The investigation also completes the independent 9M copy with its embedding blocks and replays the next 1M vectors without committing. The unmodified benchmark binary completes with 10M valid unique vectors and zero reported orphans. Its insertion takes 7.62 seconds, balancing takes 22.67 seconds, and dataset loading takes 14.93 seconds. The printed checkpoint total is 45.22 seconds; the full process takes 58.73 seconds because its final diagnostic work lies outside the checkpoint timer.

A deliberately smaller cache reproduces the collapse at 9M and lets a short actual balancing cycle test the mechanism. These stress settings do not replace the approved 1 GiB benchmark settings and cannot be compared directly with the historical 40M run.

| Actual 9M→10M replay | Balance | Full process | Mean CPU usage | Reported valid unique vectors |
|---|---:|---:|---:|---:|
| Original binary; 1 GiB shared cache. | 22.67 s | 58.73 s | 8.00 cores | 10M; zero orphans. |
| Original binary; 64 MiB shared cache. | The checkpoint does not finish. | Times out after 302.07 s. | 1.92 cores | Not available. |
| Diagnostic change retains scalar blocks; 64 MiB shared cache. | 22.40 s | 58.40 s | 8.34 cores | 10M; zero orphans. |

Retaining scalar blocks restores the ordinary 1 GiB behavior even with the deliberately constrained shared cache. The timed-out case has the same low mean CPU usage as the completed 40M benchmark. This comparison connects the component-level cache mechanism to an actual parallel balancing cycle. It still does not measure the speedup of a complete 40M checkpoint.

The retained-block replay reaches a peak of 63.41 GiB versus 62.73 GiB for the ordinary 1 GiB control. The two runs perform slightly different numbers of full-precision embedding loads because parallel balancing changes work ordering. Their 0.68 GiB peak difference therefore cannot be assigned entirely to retaining scalar metadata. Recall is skipped; the completed runs' final statistics verify 10M valid unique vectors and zero reported orphans.

## Where to improve the implementation

The next change should make fetching checkpoint versions proportional to distinct metadata blocks rather than scattered IDs. One option is to populate complete compact version pages when their backing metadata blocks are read. Another option is a bounded scan that fills the compact version cache before concurrent balancing starts. Both approaches benefit from an API that releases each decoded block after copying owned version values, without invalidating borrowed data. The current streaming reader retains blocks until its pins are explicitly cleared; clearing them while borrowed row iterators remain alive would be unsafe.

A dedicated scalar block cache or retaining scalar-reader blocks provides a smaller implementation change and a useful performance control. Its memory cost must be explicit and bounded. Raising the shared cache to 8 GiB proves the mechanism in this replay, but embeddings compete with scalar metadata during actual balancing, so this alone does not guarantee the same improvement in a full checkpoint.

The current compact version cache itself is not evicting versions. At 40M dense IDs, its stored atomic version slots use approximately 80 MB plus page bookkeeping. The expensive period is populating it from disk through scattered point reads after opening a checkpoint.

## Preservation and reproducibility

The original final checkpoint remains at `/mnt/data/wikipedia-40m-current`. It is not used as a writable benchmark directory. The investigation records the name, size, modification time and inode of every file before and after its work, plus the checkpoint contents. All 109,539 original files match, totaling 177,546,237,564 bytes. The checkpoint SHA256 is `6e19351646b024612bcbca9d307c46ceca6e386ccfb2d761abd1db372d854935`.

The pinned source is `22eb704e71490d95cf715bdb8901f7d6c8a3cf34`. The original benchmark executable SHA256 is `f8ffbabb6b03c3236e73cc0d2f3d19258ef915de6721cbf7817402dc51b54252`. The independent investigation checkout is `/home/ubuntu/chroma-large-index-balancing`. Commands, source diffs, probe executable hashes, CPU profile, resource measurements, per-leaf logs and preservation manifests are in `/home/ubuntu/benchmark-data/large-index-balancing-investigation` and this local directory.

The diagnosis explains a measured loading bottleneck, not every minute of the full 40M run. The historical hardware and cache settings are unknown, and the current run omits the historical write-level minimum percentages. A full checkpoint comparison at an intermediate size is the next gate before claiming a whole-build speedup.

## Workspace and resource status

The EC2 benchmark lock is released and no investigation benchmark is running. The independent 40M component fixture remains available for the 20-second reproduction. The independent complete 9M replay copy also remains available. Together they occupy about 46 GiB, leaving about 29 GiB free on NVMe. These investigation copies can be removed when no longer needed; all original checkpoint fixtures remain available. No new paid resources are created.
