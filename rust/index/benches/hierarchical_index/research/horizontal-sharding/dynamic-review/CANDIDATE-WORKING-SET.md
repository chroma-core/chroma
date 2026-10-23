# Candidate working-set replacement plan

The admitted streaming path removes copied posting arrays but still retains every distinct live candidate under an explicit count cap. It is not M-bounded. The next optional collector can retain only the nearest M distinct minima and every tie at the cutoff, while a separate checked local-ID bitmap preserves the exact distinct-scanned count. This document specifies the collector contract. The separate frozen optional implementation and native replay are retained under candidate-working-set-v1 through candidate-working-set-v3; the default query path still uses complete minima.

## State and accounting

The candidate state has an ID-to-best-distance map and ordered distance groups containing retained IDs. A finite distance key treats positive and negative zero as the same group, matching the existing numerical tie test. NaN and infinity reject the complete request before eligibility checks, including losing duplicate copies. Each retained ID belongs to exactly one distance group; an improved duplicate removes its old group entry before insertion. There is no lazy heap with obsolete entries that can accumulate outside the resource cap.

A bitmap has one bit per admitted contiguous local-ID slot, with checked byte arithmetic and an explicit bitmap-byte budget before allocation. The bitmap is separate from the retained-candidate cap and is updated for every distinct current live ID, including candidates that are later discarded. Thus `unique_candidates_scanned` keeps its existing meaning. Under the current 100,000-row cap, a complete bitmap needs at most 12,500 payload bytes. That is a prototype bound, not a free 100B structure; local shard size and retained generation identity determine its future capacity cost.

The candidate limit accounts for insertion/pruning transients, not just final entries. A staging slot, new group and output-vector overlap need explicit allowance. If an insertion cannot fit that declared allowance, the whole operation fails even if later pruning could shrink it. It must not silently discard an equal-distance ID to fit. A production byte budget needs fixed-capacity storage or measured allocation accounting for both maps, group nodes, bitmap, staging and output; entry/key-value payload counters alone do not bound allocator bytes or RSS.

## Update rule and proof

Every current physical copy is scored with the existing residual arithmetic. A retained ID changes only when the new distance improves its minimum. An unretained ID can be ignored if its new distance exceeds the current cutoff and at least M distinct candidates are retained. Otherwise it is inserted. After insertion, the largest distance group can be removed only when at least M candidates remain without the entire group. This retains every tie at the Mth distinct distance. The cutoff never increases as scores arrive.

A discarded ID does not need a remembered distance. Its discarded minimum exceeds an earlier cutoff, which is greater than or equal to the current cutoff. That old minimum cannot become eligible later. If a later copy improves enough to cross the current cutoff, it is inserted normally. This preserves the minimum-distance result without retaining every rejected ID's score. The separate bitmap still counts the ID once.

For M=2, the sequence A:9, B:2, C:2, A:1 must return A:1, B:2 and C:2. A can be discarded after C arrives and then re-enter with its better copy. All three final IDs are required because B and C tie at the cutoff. A cap of two final entries cannot satisfy this query; it must produce a structured resource error, not a truncated result. Positive and negative zero must likewise stay in one cutoff group.

## Smallest validation matrix

Tests compare the collector against the existing complete min-distance map and inclusive cutoff oracle across deterministic row permutations, repeated IDs, late improving copies, cutoff ties and signed zero. They verify exact distinct-scanned counts even for discarded IDs. Exact capacity, one-over-capacity, bitmap-size failure and transient insertion failure reject the entire operation, with subsequent reuse or a fresh operation matching the oracle. A bounded differential fixture then checks the real persisted stale/deleted/current-copy path and the existing full coordinator trace, including external-ID tie resolution. Physical work counters stay separate from retained/wire candidates.

Worst-case ties still require O(N) candidates or a whole-request resource failure. The design makes no unconditional O(M) memory, RSS, latency, physical-host or 100B capacity claim. Startup/publication full validation and the 100,000-row prototype cap remain unchanged. The existing streaming source and its independent empty-leaf repair are preserved separately; no default search policy changes.
