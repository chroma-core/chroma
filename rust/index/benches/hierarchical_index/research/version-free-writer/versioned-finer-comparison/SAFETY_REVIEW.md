# Safety review of the versioned finer scheduler

The reviewed revision is `97fa32d84be264c42db836c72f32512ff811f0fc` in `/private/tmp/chroma-versioned-finer-scheduler`, based on `1f433be7969b3d28c78e463fe33a299fa0c5fee9`. This review is read-only with respect to implementation and does not run EC2 or publish GitHub changes. The implementation owner reports 151 passing test executions across four harnesses, formatting checks, and commit hooks. This reviewer inspected the final committed diff without duplicating the suite.

## Conclusion

No blocking concurrency defect is identified in the final scheduler port; it is suitable for the bounded benchmark comparison with persisted-index audits. The source writer already recursively balances destinations outside a worker's assigned subtree, so subtree assignments do not establish exclusive mutation ownership. The port retains its version validation, merge-pair reservations, per-node map locks, and serialized tree changes. It moves the existing balancing reservation before posting loading and scrubbing, with a scope guard to release it on every return.

## Safety mechanisms inspected

A shared atomic queue position assigns each queued leaf once. A recursive balancing call can also encounter that leaf, so the existing per-node balancing set now covers the whole balancing call. The worker reads size after scrubbing and safely returns for a disappeared node. Every leaf present at a joined active-round boundary becomes a queue job, including physically in-range leaves that may contain stale versions. New leaves can be reached recursively and otherwise enter the next active round. The queue does not claim to provide the broader source/destination ownership used by the version-free writer.

Versioned reassignment takes the destination posting guard and then the version guard, checks the expected current version, and appends its replacement before releasing those guards. Competing reassignments cannot both publish a successor for the same expected version. Direct same-version merges retain their existing pair reservation and remove the source row only after registering the destination. Splitting removes a posting atomically from the map before processing it. These mechanisms remain unchanged by the queue port.

Tree changes retain the existing reentrant tree mutex. Replacing children resolves their currently reachable parent rather than trusting the parent saved at split start, and a root change is checked against the current root. The new queue increases possible sibling concurrency, but does not bypass those helpers.

Neighbor reassignment still calls scrub directly, outside the balancing reservation. Consequently the new reservation is not a universal leaf lock; correctness continues to rely on map guards and version checks for those operations. This is recorded explicitly rather than describing the port as equivalent to the version-free transfer claims.

## Comparison scope

The final port queues all leaves during an active round, preserving the versioned writer's coverage of stale-entry cleanup. A separate size predicate decides whether another round is needed. The added regression verifies that another leaf's oversized posting activates a round that also scrubs a physically healthy posting. The undersized root is excluded from merge work because it has no destination, avoiding empty round cycling.

Both implementations dynamically distribute leaf jobs, but their job populations differ intentionally: the versioned writer scans all leaves in an active round to scrub stale versions, whereas the version-free writer queues only size violations because it has no stale version rows. This retains the version-management cost rather than silently optimizing it away. Benchmark results remain an end-to-end comparison, including recursive destination balancing in the versioned writer and explicit transfer coordination in the version-free writer. Report physical and live counts separately.

## Test review

The added tests exercise a duplicate balancing entry while its leaf is reserved, reservation release after return, root-only termination, and parallel sibling split/merge rounds followed by commit and reopen. The audit checks exact-once live vector coverage, all reachable node links, matching posting/code lengths, and regenerated codes against embeddings. The final test also audits the reopened persisted index before another balance can modify it, then audits it after balancing. The reported 151 passing executions satisfy the local test gate; benchmark-scale audits are still required.
