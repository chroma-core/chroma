# Merge revalidation and centroid publication

A merge now evacuates only the vector IDs captured for that attempt. Other workers can transfer new vectors into a visible source leaf while evacuation runs. Those arrivals stay current, and ordinary size checks choose any follow-up work. An empty source is removed only through protected structural validation; interrupted retirement remains explicit, coalesced scheduler work.

Three controlled transfer interleavings reproduce the merge defects before the repair. They cover arrivals after the evacuation snapshot, changed preferred-neighbor membership before protection, and arrivals before forced evacuation starts. The fixed source preserves membership in all three cases. Unknown or unresolved transfer outcomes still stop construction.

A separate RPC response barrier reproduces a centroid publication race. A refresh that appears unnecessary must wait for its publication turn and revalidate current owner state before returning. The test edits owner routing state directly to isolate that ordering rule. An intermediate stress failure lacked the parent context needed to prove that this particular race caused it; the assertion now records that context and retains a synthetic routing artifact if visibility fails again.

The coherent source passes 150 native tests, including repeated construction with 32 workers per owner and complete centroid visibility checks. All three native command-line programs compile. Exact red and green logs, source hashes, work counts, limitations and binary hashes appear in [MERGE-REVALIDATION-FIX.json](MERGE-REVALIDATION-FIX.json). Physical correctness and 100K/3M performance measurements remain parent-owned campaign steps.
