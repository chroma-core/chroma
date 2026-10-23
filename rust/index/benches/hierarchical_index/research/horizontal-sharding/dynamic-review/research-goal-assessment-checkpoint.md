# Research goal and completion criteria

Complete the hierarchical SPANN research by determining whether the proposed globally evolving tree offers a credible path to 100 billion vectors, and deliver a working proof of concept plus independently reviewed evidence supporting that conclusion. A supported, conditionally supported or contradicted scaling conclusion is acceptable when the evidence justifies it; a favorable conclusion is not required.

## Work sequence

Work through construction, index quality, query cost and scaling in that order, revisiting an earlier phase when later evidence exposes a material problem. Finish the approved 16K comparison first if it is unfinished. Choose each subsequent experiment to resolve a specific uncertainty that could change the research conclusion or implementation. Record its hypothesis, structural or working-set requirement, input, time and cost bounds, stop rule and resulting decision before execution. Reuse retained inputs and evidence. Avoid automatic size or tuning sweeps.

## Required evidence

1. **A working proof of concept.** Demonstrate global construction and query behavior, correct current memberships, one owner per vector and preservation of logical tree identities across redistribution. Verify vector and centroid visibility and distinguish construction membership versions from immutable query versions. Clearly distinguish local ownership simulation from physical distributed execution.
2. **An explained construction cost.** Measure insertion, balancing, redistribution, publication and validation, with vector access, decoding and coordination work identified. Pair timings with work counts and establish effective concurrency. Address consequential demonstrated waste or explain why it remains. Independent randomized trees do not establish a causal speedup by themselves.
3. **A defensible quality conclusion.** Compare against the ordinary reference under matched construction conditions using a justified query cohort and a tree that exercises meaningful pruning. Prespecify the quality acceptance criterion before evaluating confirmatory results. Establish parity within that criterion or provide evidence of a limiting mechanism; failed attempts and timeouts establish neither. Separate clustering quality, ground-truth scan minima and executable recall. Exploratory reused queries with unproven corpus disjointness cannot silently become confirmatory evidence.
4. **A query-cost comparison at matched recall.** Report end-to-end latency, computation, storage reads and relevant communication costs, with cache conditions and measurement limitations explicit. Account for construction and maintenance costs as well. Distinguish logical bytes, encoded provider returns, physical I/O and actual network traffic; do not substitute one measurement for another.
5. **An independently reviewed 100B assessment.** Account for storage, memory, construction, maintenance, query work, coordination and load distribution. Separate measurements, arithmetic extrapolations and assumptions. State whether the proposed path is supported, conditionally supported or contradicted, and identify what could change that conclusion. Do not transfer throughput from another construction design or multiply isolated kernel improvements into an end-to-end forecast.

## Completion and stopping conditions

Complete the goal only when each required deliverable has inspected authoritative evidence and independent review, and remaining uncertainties do not prevent the stated conclusion. Keep an explicit distinction between established results, conditional claims and missing evidence. Completion of a benchmark, an agent turn or an earlier narrower POC acceptance does not complete this goal.

Stop an individual experiment at its prespecified decision, failure or resource limit. Preserve adverse measurements and failed artifacts. A failed experiment may motivate a new justified experiment, but does not authorize silently changing the running experiment's limits or success criteria.

If a required deliverable cannot be obtained within authorized resources or available capabilities, document the precise blocker and the evidence needed to resolve it. The research remains incomplete. Follow the goal controller's blocked-state rules; do not manufacture completion or repeatedly continue without useful work. Production integration, deployment readiness, recovery, node-failure handling, fleet hardening and garbage collection remain outside scope.

## Authority and constraints

Lyon authorizes refining this goal as subgoals change, finish or emerge: "go ahead and update the goal. you also have permission to make any changes to the goal that you see fit, as (sub)goals change or are completed or new ones are discovered that need to be added." Record material revisions and their evidence. Preserve the overall research objective; do not remove an unmet requirement merely to declare success.

Follow CURRENT-PLAN.md, CONTINUATION-STATE.json, SCALE-AUTHORIZATION.json, UPLOAD-AUTHORIZATION.json and the saved research decision criteria. Tests of any size and research uploads are authorized within the existing $1,000 cumulative spending limit and sandbox, protected/excluded resource, shutdown, identity and tool-control restrictions. There is no token budget. Reduce redundant reading, coordination, testing and review without weakening necessary safeguards.

## Controller integration

The active controller goal explicitly names this file as the authoritative definition of deliverables, success criteria and stopping conditions. Lyon added that reference directly to the displayed objective. The goal remains active with no token budget.

## Revisions

- 2026-10-17: Lyon approves the proposed outcome-based criteria and grants ongoing authority to refine subgoals. The refinement separates experiment termination, research completion and genuine blockage while preserving the overall scope and all resource restrictions.
