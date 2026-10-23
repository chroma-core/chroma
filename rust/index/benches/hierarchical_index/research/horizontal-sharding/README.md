# Hierarchical SPANN research

The project aims to build and measure one globally evolving search tree with a shared upper tree and machine-owned subtrees, then establish a credible path to 100 billion vectors.

| Resource | What it answers |
| --- | --- |
| [Design](DESIGN.md) | What are we building, what must remain true, and which architectural choices remain open? |
| [Plan](CURRENT-PLAN.md) | What should we do next, why, and what result lets us advance? |
| [Benchmarks and findings](findings/README.md) | What have we learned, how was it measured, and where is the evidence? |

## Development approach

Before planning an experiment, changing the implementation or interpreting results, follow the plan's [development principles and cycle](CURRENT-PLAN.md#development-principles).

The implementation completes physical construction on two machines for 3M vectors and validates an independent saved reader. Both owners perform substantial construction and end with nearly even ownership. Measured performance gaps against the fast reference guide the ongoing development cycle. The [findings index](findings/README.md) links the latest comparison, while the [plan](CURRENT-PLAN.md) records the current experiment and ranked questions. After each experiment, record the results and interpretation, keep or reject the change, and reprioritize. The authorized follow-up remains active; fresh guarded windows bound each remote experiment.

The [research goal](RESEARCH-GOAL.md) defines project success and stopping conditions. The [operations index](OPERATIONS.md) holds authorization, spending and execution-record links. The [journal](CONTINUATION-RUNS.md) records decisions chronologically; it is not a second work queue. The [continuation state](CONTINUATION-STATE.json) points back to the current plan.

The [archive](archive/2026-10-19-document-consolidation/README.md) preserves the documents that this consolidation replaces.
