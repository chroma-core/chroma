# 100K reassignment failure diagnosis

The candidate can reject a posting transfer because an unrelated posting edit changes a revision even when the transfer's centroid, placement and vector membership remain compatible. This is a concrete unnecessary invalidation path in the source. It is a leading explanation for the observed retry failure, rather than proof that it explains all construction work or node creation.

## Observed failure

The retained 100K run uses 32 workers, 768 dimensions, a split threshold of 2,048 and a merge threshold of 512. Its first reported construction failure exhausts the membership reassignment progress budget with `transfer routing dependency changed`. Subsequent failures include the poisoned owner's original error and are consequences of the first unresolved construction job. The receipt is [run-receipts.json](../operations/window-epoch2/100k-baseline-distributed-attempt2/run-receipts.json).

Large operation sequence numbers do not establish that the tree contains that many nodes. Node allocation and operation envelopes share the same increasing counter: `OwnerService::attempt` and `envelope` in `distributed/engine.rs:462`, and `alloc_node` in `distributed/construction.rs:271`. Failed proposals and retries consume operation IDs. The live node count, split/merge counts and retained proposal count must be measured separately before describing node growth.

## Proven invalidation mechanism

A transfer snapshots both routing revisions before fetching vectors and generating destination posting codes (`distributed/engine.rs:1613`). It then requests endpoint protection and waits for the owner region reservation. Preparation rejects any revision mismatch (`distributed/engine.rs:913`). Posting append increments that same revision (`distributed/engine.rs:749`), and source removal and destination activation increment it again (`distributed/engine.rs:1021`). These posting edits leave the centroid unchanged. A compatible transfer can therefore become rejected while it quantizes vectors or waits for protection solely because another transfer changes an unrelated vector's posting.

The source preserves revision visibility: both the live checkout and immutable `packages/source-epoch1.tar.gz` publish updated routing images after source removal and destination activation. A missing publication of the new revision is therefore ruled out for these paths. The rejection is not inherently a stale-cache correctness failure.

Reassignment retries rejected groups after navigating again, with a five-second deadline and a maximum of 1,024 iterations (`distributed/construction.rs:740`). Each retry repeats navigation, snapshotting, code preparation and reservation. Multiple workers can repeatedly invalidate each other's broad revision dependencies. The code establishes that this can happen; the receipt does not count which endpoint or intervening edit caused each rejection, so the frequency and its share of total time remain unmeasured.

## Reference behavior and correction

The unsharded writer protects the source and available destination leaves first (`writer/writer.rs:1954` and `writer/writer.rs:1978`). It then reads current source membership and centroids (`writer/writer.rs:1998`), checks current transfer eligibility and generates destination codes while that protection remains held (`writer/writer.rs:2099`). An unrelated completed membership edit does not force a centroid revision retry.

The preferred correction is to mirror that protected preparation order for a same-owner transfer, including current source membership filtering and eligibility checks. For the distributed protocol, an alternative is to bind the actual centroid, placement and leaf identity required by the codes, then validate those dependencies and source membership under protection. The existing structural revision must remain available to split preparation: dropping revision updates globally could let a split overwrite a concurrently changed posting list.

## Limits and next evidence

This diagnosis does not establish why operation IDs grow so quickly, whether the number of live nodes grows abnormally, or whether the scheduler produces redundant balancing jobs. Those questions need counts of live nodes, successful and rejected splits, attempted and completed transfers, revalidation reasons, and work completed before the first failure. A regression should interleave a transfer proposal with an unrelated posting-only edit, then show that a still-eligible transfer progresses while a changed centroid or placement still rejects or replans. Engine ownership remains with the implementation agent; this diagnosis makes no code changes and runs no benchmark.
