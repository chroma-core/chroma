# Research goal and completion criteria

Find and demonstrate a credible design for constructing, maintaining and searching one globally evolving hierarchical SPANN tree at 100 billion vectors. Deliver a connected working proof of concept and independently reviewed measurements supporting a practical resource envelope. A failed design is a reason to revise the design, not completion of this project.

## Success criteria

1. The connected POC preserves global neighbor eligibility, complete current memberships, one current owner per vector, stable logical identities, immediate construction visibility and consistent immutable query generations through insertion, balancing, reassignment, redistribution and publication.
2. Identity, ownership lookup, routing state, vector access and publication have implemented partitioned or bounded mechanisms. Account for total coordinator and owner memory, temporary allocations and retained generations. Demonstrate that small changes do not require central whole-index assembly.
3. Representative trees demonstrate a useful recall–cost tradeoff on a fresh held-out cohort. Treat 95% recall at 100 as an aspiration, not a mandatory acceptance threshold or stopping condition. Compare reference and dynamic construction at matched recall where supported, report uncertainty and any prespecified relative-quality tolerance, and explain the practical tradeoffs at observed recall. Diagnose grouping, navigation and candidate-ranking losses separately. Preserve historical protocol outcomes and consumed query cohorts without treating an earlier 95% gate failure as failure of the broader research goal.
4. Query configurations demonstrate a useful recall/cost tradeoff under explicit memory and cache conditions. Record posting precision, query representation and rerank budget separately. Measure end-to-end and tail latency, CPU, actual fetch bytes, exact-vector work and communication; distinguish logical bytes from physical I/O.
5. Actual multi-machine execution exercises important construction and query dependencies, including ownership changes and concentrated load. Measure useful parallel work, serial coordination, dependent request rounds and maximum owner demand.
6. An independently reviewed 100B model supports an explicit operating envelope with margins for storage, total memory, build time, update load, publication, query throughput/latency, communication and skew. Separate measurements, assumptions and extrapolations. A full 100B execution is not required; essential feasibility mechanisms cannot all remain unimplemented assumptions.

## Work and decisions

Maintain INVESTIGATION-QUEUE.md as the readable, ranked investigation and experiment record throughout this project. Work on the most promising available question. After each experiment, record its result, interpretation, limitations and decision, then explicitly reconsider priorities and add newly revealed questions before selecting more work. Keep completed and adverse results visible with evidence links. Follow the fast optimized unsharded implementation by default: mirror its algorithms, data access, batching, scheduling and settings wherever compatible with the global-tree requirements. Justify each meaningful deviation by measured superiority, an explicit theory under test, or a necessary structural difference caused by sharding. Do not assume existing dynamic implementation choices are necessary. Prefer removing unnecessary differences over optimizing their overhead. Preserve correctness, bounded resource accounting and the full research success criteria.

Advance selective queries and scalable construction together. Use the next-phase report as the evidence map and CURRENT-PLAN.md as the active queue. Retain configurable one-bit and four-bit postings as candidates; investigate selective refinement only when its layout avoids fetching rejected candidates' refinement bytes. Choose tree shape and routing policy through reasoned experiments.

Choose the smallest experiment that answers the question efficiently, including millions of vectors when depth, pruning, cache pressure or throughput makes that informative. Tiny tests establish mechanisms, not large-index economics. Do not force a ladder of tiny benchmarks. Verify reference throughput rather than assume it. Each experiment names a hypothesis, relevant structural condition, source/input settings, time/cost bounds, stop rule and decision. Avoid automatic sweeps and silent deadline extensions.

Preserve global-tree semantics and initially one construction owner; capacity-driven redistribution occurs between parallel insertion/balancing cycles. Production integration, recovery, fleet hardening and garbage collection are outside scope.

Routine construction baselines must reproduce the established optimized parallel reference on the same data and hardware before comparing dynamic construction. Match insertion and balance thread counts, checkpoint cadence, metric, dimensionality, branching/split/merge settings, navigation thresholds and per-level schedules; measure effective concurrency. Experimental implementation limits must not define an artificially slow reference. Serial runs require a named thread or concurrency hypothesis. Correctness of a custom entrypoint does not establish performance representativeness. The historical serial FineWeb 2M timing describes that harness only. The relayed fast Wikipedia reference uses 32 insertion and 32 balance threads and 1M checkpoint cadence; verify its full configuration and reproduce it before using its timing quantitatively.

## Completion and stopping

Complete only when the success criteria support a credible feasible design within a stated resource envelope and independent review accepts the evidence. Rejection of an implementation or failure to reach recall does not complete the project. If promising alternatives require unavailable resources or a user decision, state the precise blocker and leave the project incomplete. Stop each experiment at its decision or resource limit and preserve adverse evidence.

Use labeled operating scenarios while application throughput, latency, build deadline and economic targets remain unspecified. Those omissions do not block source work or discriminating experiments, but an unbounded assumption of more machines or more time cannot close the final model. Refine subgoals autonomously without weakening unmet success criteria to declare completion.

## Workflow and authority

Root owns scientific decisions, goal, active plan and evidence reconciliation. Reuse existing workers and the sole cloud operator. Assign independent file ownership and parallelize at most a few useful tasks. Use short Astra high-effort assignments for difficult design and interpretation when available; use lighter models for well-specified implementation and execution. Existing agents retain their configured models unless the tool supports changing them; do not claim an override or create agents to evade limits.

Review coherent changes once before remote admission. Repeat verification only for changed claims, relevant edits or failures. Prefer existing harnesses and retained inputs. Before a cost benchmark, verify the binary’s optimization profile and emit progress at major phase boundaries. Use development builds for correctness; a timeout without phase evidence is a harness limitation, not an algorithmic conclusion. When a subgoal takes unexpectedly long, identify whether implementation, experiment runtime or coordination dominates, then simplify the approach. Check for an actionable result after each coherent task; avoid documentation and review loops that postpone measurement.

Tests of any size and research uploads are authorized within the existing $1,000 cumulative new-research allowance. Lyon authorizes self-service `cops aws login` renewal; request human assistance only if interactive authentication requires it. Preserve AWS sandbox account 079819354890 in us-east-1, Cops authentication, excluded/protected resources, independent local and remote absolute shutdown guards, the sole operator and ordinary tool approval controls. Preserve retained disks, source epochs, failed artifacts and staged work. No corpus downloads to the Mac, local Linux runtime startup or retained-root garbage collection. Use only lyon-chroma <lyon@trychroma.com> for Git actions.

## Working records

- RESEARCH-GOAL.md defines success, scope and stopping conditions.
- CURRENT-PLAN.md defines active work; its leading current section overrides dated checkpoint entries.
- CONTINUATION-STATE.json records machine-readable status and links to evidence.
- SPANN-RESEARCH-REPORT-AND-NEXT-PHASE.md maps findings and candidate designs.
- DYNAMIC-GLOBAL-TREE-DESIGN.md records construction semantics; new architecture decisions must identify which interfaces they change.
- CONTINUATION-RUNS.md records material decisions and results; dynamic-review and fleet retain immutable experiment protocols, measurements, reviews and lifecycle receipts.
- SPEND-LEDGER.json accounts for covered spending and recurring commitments without double counting.

## Revision authority

On 17 October 2026 Lyon explicitly continues the project toward finding a feasible 100B design, authorizes goal/instruction refinement, parallel appropriately sized agents and informative larger benchmarks. The prior assessed goal is preserved in dynamic-review/research-goal-assessment-checkpoint.md. Its adverse assessment remains valid evidence about that implementation, not completion of this goal.

Lyon directly authorizes research storage growth as needed within the total $1,000 spending limit. The selected immediate change is the existing sandbox root volume from 100 to 116 GiB, adding $1.28/month. STORAGE-AUTHORIZATION.json records the direct reply. Resource identity, normal tool approval, fresh shutdown guards and spending accounting still apply.
