# Candidates for reducing final-million insertion and balancing time

The current goal is lower insertion and balancing time from 2M to 3M, using the retained writer checkpoint. Keep both accepted CPU optimizations and validate their combination before using it as the next development baseline. The immutable epoch30 baseline remains the comparison control. The recurring follow-up stays paused. See the [current plan](../../CURRENT-PLAN.md).

## Additional insertion evidence

Phase-boundary differences from the retained detailed baseline show 13.311 seconds inserting the final million, 219.35 coordinator CPU-seconds and 158.17 peer CPU-seconds. All 15,648 navigation calls originate on the coordinator. Its inclusive navigation scope consumes 215.897 CPU-seconds; candidate processing consumes 82.949 and reply validation 28.971 CPU-seconds inside that work. The peer performs routing scoring but originates no insertion navigation calls. Application-framed traffic is 8.834960373 GB. The conditional-view candidate has the same insertion traffic, so its savings do not address this path. These are separate diagnostic runs, not replicated estimates of a causal speedup. Inclusive scopes overlap and must not be added.

The [derived insertion attribution](../operations/development30/insertion-attribution.json) subtracts before-add from after-add counters in each retained detailed result. Source inspection confirms 32 coordinator insertion workers navigate windows of 64 queries, collect destinations, then append grouped IDs through a separate worker phase. Both owners store vectors; insertion navigation orchestration is centralized. This distinction matters when interpreting utilization.

## Ranked hypotheses and discriminating measurements

1. Repeated remote navigation adds insertion latency. Separate routing, destination grouping and append elapsed time; attribute request counts, bytes and codec CPU by message kind. Measure response size per query window. Compare bounded query-window sizes with identical results and ordering; consider owner-local navigation dispatch only if the measured savings justify its additional complexity. Larger windows can increase memory, response size and stragglers, so they require measurement.
2. Waiting inside long balance jobs limits usable concurrency. Record bounded job timelines with identity, owner, node, start and finish, plus remote-call and dependency-wait spans. If the longest completion chain spends substantial time waiting while independent eligible work exists, target reservation scope, wakeups or yielding blocked work. A long job alone does not justify splitting it.
3. Per-call transport overhead increases sequential navigation latency. The listener sleeps for 1 ms after a nonblocking accept reports no connection. Measure small-request latency and accept delay before selecting event-driven acceptance or persistent connections. Existing active-handler caps already bound server concurrency; reusable workers would reduce creation churn rather than introduce the first bound. Preserve separate control capacity and prevent nested requests from exhausting a shared worker pool.
4. Repeated immutable expansion metadata drives bytes and validation cost. Attribute expansion payloads and validation by request kind first. Cache only metadata whose identity and version are proved immutable; retain per-response completeness, query association, ordering and score validation. Conditional shared-view acknowledgement does not establish that expansion replies can safely skip those checks.

Connection setup and parent-side thread creation consume relatively little measured CPU. A pooling candidate must demonstrate lower request latency or interference on the final completion chain, rather than rely only on aggregate call counts. CPU profiles alone do not measure dependency waiting.

## Separate saving work

Bounded parallel durable posting writes remain a concrete saving candidate. They do not shorten the construction-only measurements and are outside the next insertion/balance comparison. Preserve the concurrent saving implementation in the shared checkout.

## Next execution sequence

Combine and validate the two accepted changes in the isolated research source. Add the focused insertion phase measurements and bounded balance timelines, then run the exact retained 2M checkpoint. Select one mechanism using the measured delay, validate its semantics with the fixed replay and applicable native tests, and compare repeated lightweight final-million runs. Keep diagnostic runs separate from timing runs. Verify current machine identity, resource availability and budget guards before cloud execution; reuse running machines, retained inputs and compiler caches.
