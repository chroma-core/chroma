# Dynamic-sharding review loop

The user requests a Sol6.1 implementation worker and periodic Astra medium review. The implementation owner is `/root/shard_worker`; benchmark and 100M operational ownership stays with `/root/linux_pilot`. Read the implementation plan in `../DYNAMIC-SHARDING.md` and operational state in `../CONTINUATION-STATE.json`.

The collaboration API rejects creation of another agent because its thread limit is reached, even after an existing worker completes. The fourth concurrent reviewer therefore uses the local ephemeral agent runner with the actual `gpt-6-astra` model and medium reasoning, in read-only sandbox mode. The first successful log verifies both settings. This creates no persistent sidebar task. The initial outer-sandbox attempt cannot connect to the local app server; ordinary automatic approval review permits the read-only retry. This is not an approval-review rejection or a request for new user authorization.

## Review milestones

1. Review the early deployment and topology design. Completed report: `01-design-review.md`; feedback delivered to the implementation worker.
2. Review working whole-subtree migration and worker balancing, including real Rust reader tests, complete transfer proofs, pinned old readers, and placement-only stale-writer rejection.
3. Review working cross-boundary split, merge and reassignment, including stable identity, exact global-ID/vector preservation, reusable unchanged snapshots, and atomic routing/owner publication.
4. Review complete integration and remaining source/test limitations. Require exhaustive scalar-reference checks and same-generation transported/direct approximate parity; do not require identical approximate outputs across changed topologies.

For each new milestone, first check whether a review is already running or its report is available. Freeze a new prompt describing the concrete changed files and acceptance evidence. Include the prior review and unresolved items. Run one ephemeral read-only Astra medium review from `/private/tmp/chroma-hierarchical-spann-sharding`, with workspace-owned output and log paths. Use `codex exec --ephemeral --model gpt-6-astra -c 'model_reasoning_effort="medium"' --sandbox read-only`; pass the prompt through stdin and the final report through `-o`. Do not bypass sandbox or approval controls. Preserve failed invocations and verify the model/effort in each successful header.

Relay concrete findings to the implementation worker through collaboration, track resolution, and request the next review after material code milestones. Preserve frozen benchmark code, binaries and data. The existing ten-minute chat completion checks continue this loop and stay quiet on unchanged status.
