# Existing-data investigation: where distributed balancing spends time

The existing measurements justify a checkpoint-based baseline with even placement and finer navigation and transport diagnostics. They do not isolate the cost of any single distributed mechanism. The [current plan](../../CURRENT-PLAN.md) defines the implementation and comparison workflow that follows this investigation.

## Measured findings

Corrected experiment 28 spends 19.14 seconds and 563.75 CPU-seconds balancing the final million vectors, versus 6.85 seconds and 191.66 CPU-seconds for reference21. Split counts increase approximately 2% and routing distance calculations approximately 14%. The approximately threefold CPU increase therefore calls for attribution beyond the number of splits and distance calculations.

Within experiment 28, second-million balancing runs on one owner and final-million balancing involves both owners. Navigation candidate counts remain similar at 858 million and 868 million. Owned-routing scoring takes 130.04 and 127.99 CPU-seconds. Candidate assembly, sorting and selection increase from 38.97 to 92.05 CPU-seconds; remote-reply validation adds 33.19 CPU-seconds. These phases use different trees and workloads, so this is diagnostic evidence rather than an isolated causal comparison.

Final-million balancing exchanges 10.31 GB of application-framed messages through approximately 84,715 successful request/reply exchanges. Remote-reply validation visits 317.8 million child records. These byte counts include application framing and exclude TCP/IP headers and retransmission overhead.

The transport opens one TCP connection per call, creates one server thread per accepted connection and uses JSON encoding. Serialization, allocation, connection setup and thread creation are concrete hypotheses. Their individual costs are not measured by the retained totals.

The transfer-driver timer includes navigation and overlaps nested transfer timers. It is not exclusive transfer CPU. Inclusive scopes must not be added together to estimate total cost.

Placement leaves 1.497 million and 0.503 million vectors on the owners after redistribution at 2M. Before final balancing, the counts are 2.246 million and 0.754 million. Near-even final ownership occurs only after the final redistribution. Owner 1 performs 72% of final-balance splits and consumes 67% of balance CPU. Final ownership alone therefore does not establish balanced work during insertion or balancing.

Of the 25.00-second save, parallel owner materialization takes 10.16 seconds. Copying the remote owner's 194 MB across 1,080 objects into coordinator storage takes 13.50 seconds. Synchronization timings nested inside these phases must not be added again. Experiment 29 separately establishes owner-direct durable saving and unchanged-block reuse; it uses a different storage backend and does not remove the need for a controlled construction comparison.

## Decisions and hypotheses

Even placement is the chosen policy, subject to capacity and ownership invariants. It is part of the new baseline, not an adoption experiment. A resumable full-precision writer checkpoint after drained redistribution at 2M is the prerequisite for comparing final-million runs from the same state. Compressed reader generations cannot serve this purpose.

The first optimization hypothesis is that navigation result handling performs avoidable allocations, canonical-order reconstruction and repeated passes. A fixed query replay against a frozen routing view must preserve validation, complete results, ordering and tie behavior. Only one coherent mechanism changes at a time. Transport improvements follow if measured costs support them; the current evidence alone does not select a transport rewrite.

The new baseline needs at least three lightweight timing repetitions, a separate detailed diagnostic run with profiles on both owners, and an instrumentation-overhead check. It must distinguish measured costs from inference, retain incomplete/pathological attempts, and report remaining uncertainty. The full workflow and development-speed rules are in the [current plan](../../CURRENT-PLAN.md).

## Evidence

The user-supplied investigation is grounded in [corrected28 summary](../operations/window-epoch28b/candidate-summary.json), [corrected28 raw result](../operations/window-epoch28b/3m-distributed/result.json) and [reference21 summary](../operations/window-epoch21/reference-summary.json). The final-balance elapsed and CPU figures are checked directly against the retained summary. The remaining rounded figures above preserve the supplied investigation; the upcoming diagnostic report will provide independently derived attribution rather than upgrading these comparisons into causal claims.
