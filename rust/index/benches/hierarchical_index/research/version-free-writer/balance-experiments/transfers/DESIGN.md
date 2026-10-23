# Batched balancing transfers

Balancing collects vector moves while structural workers split leaves, then applies those moves after the workers join. The joined boundary supplies a stable tree and lets each posting list receive one combined update without claiming several leaves together.

The reference is `eea4ada6f21554adb535d1ffdea95f74cb76650b`. The experiment lives in `/private/tmp/chroma-batched-balance-transfers` on `codex/batched-balance-transfers`.

## Workflow

1. Structural workers split with the existing lifetime claims and parent mutation lock. Reassignment calls queue source IDs instead of navigating or moving them. A merge queues evacuation and keeps its source attached until the joined phase.
2. The joined phase groups proposals by source and deduplicates IDs. Overlapping exclusions are combined; excluding the source forces evacuation. Missing sources supersede their proposals. Current membership is checked before loading embeddings and routing.
3. Sources prepare moves in parallel against the stable tree. Current source and destination centroids decide eligibility. Forced evacuation skips the preference comparison. Every accepted row receives a code relative to its destination centroid. Each source is scanned once and produces survivors plus outgoing buffers.
4. The coordinator combines outgoing buffers by destination. A leaf that sends and receives vectors gets one replacement containing its survivors and incoming rows. Posting buffer preparation, lazy reads, and dirty markers finish before publication.
5. Workers publish disjoint leaf replacements. A panic poisons the writer and blocks commit. Empty evacuation sources retire only after publication; final emptiness includes arrivals.

## Evacuation and quality

Evacuating sources are excluded as destinations to avoid reciprocal evacuation cycles. The largest non-evacuating leaf supplies a retained sink. If every leaf requests evacuation, the largest source is retained, with the smaller node ID breaking equal-size ties. The retained source keeps its own forced rows. An evacuation whose navigation beam contains no allowed target can use this sink; its code uses the actual sink centroid. This fallback can change placement quality and must be checked with recall measurements.

Ordinary proposals that have no allowed destination remain in their current valid source without mandatory retries. Forced work without an allowed destination remains queued and prevents commit at the round cap. Empty retirement requests survive grouping. Eight rounds can still leave valid size-only residuals, as in the reference.

## Scope and measurements

This experiment keeps the structural scheduler unchanged. Source preparation and posting publication run after structural workers join. Temporary memory includes source snapshots, source survivors, destination buffers, and changed destination postings; the experiment does not copy embeddings, but it retains shared references loaded into the writer. Peak memory must be measured alongside elapsed time.

The existing `retry_nanos` round field now includes the joined transfer phase, including parallel source preparation. It is an elapsed wall phase, not a sum of worker durations. Source entries scanned counts one source scan per joined group. Transfer batches counts source groups that actually move rows. Claim-conflict counters describe structural/direct operations; joined transfer publication requires no multi-leaf claims.

Benchmarks are owned by the parent task. This implementation task performs local correctness checks only. The intended first comparisons use Wikipedia at 1M and 3M with benchmark validation disabled during timings and saved-index audits afterward. The retained 8M snapshot can support a targeted checkpoint 9 scheduling comparison separately.
