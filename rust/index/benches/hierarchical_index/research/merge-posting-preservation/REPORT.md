# Preserve vector postings through interrupted parallel merges

## Summary

A merge moves vector postings, the rows that record which leaf contains a vector. A vector must keep a valid source posting until a destination accepts its replacement. This PR preserves that rule when parallel balancing temporarily removes the root or a destination disappears.

The source leaf stays readable throughout transfer. Destination searches exclude that source before applying their candidate limit. A direct transfer publishes a posting with the existing version, then removes one matching source row and its code. This preserves replicas in other leaves. Reassignment publishes its new version before the old source row becomes stale. Failed transfers and concurrent additions remain in the source. Cleanup removes the source only when an atomic check finds it empty.

Each merge reserves its chosen source and destination together. Another merge that needs either leaf defers its transfer, which prevents the reproduced reciprocal pair from swapping rows without shrinking the tree. A short mutex protects reservation and release; workers transfer disjoint pairs in parallel. The reservation is released before follow-up balancing. Per-vector reassignment can still reach other active leaves, so this change does not guarantee complete balancing convergence.

This PR is stacked on #7852. It changes the prototype writer under `rust/index/benches/hierarchical_index/writer/` and keeps the persisted index format unchanged. The separate unused-navigation-tracking cleanup in #7855 is not included.

## Validation

Both local test harnesses pass 59 executions. A deterministic regression pauses the actual merge after destination selection, removes the root, then resumes transfer. It fails on the base with a missing vector and passes with this fix. Additional tests cover all destinations disappearing, a concurrent append to the source, a merge without a usable target, other replicas remaining valid, and an overlapping merge deferring without entering transfer.

All four EC2 validation builds pass on the lyon instance: two fresh 1M-vector builds for each 1,024-dimensional dataset, with 100k checkpoints, full-precision navigation and neighbor reassignment, and parallel balancing. Every run checks the exact vector-ID set and every child reference before every commit and after reopening. All 80 checks pass.

| Run | Final vector count | Before-commit checks | After-reopen checks | Result |
| --- | ---: | ---: | ---: | --- |
| msmarco-1 | 1,000,000 | 10 | 10 | Pass |
| wikipedia-1 | 1,000,000 | 10 | 10 | Pass |
| msmarco-2 | 1,000,000 | 10 | 10 | Pass |
| wikipedia-2 | 1,000,000 | 10 | 10 | Pass |

The tested runtime revision is `63be72d31`. The later commit `fb7a9dab9` clarifies one comment and changes no runtime code. These runs establish correctness for the exercised cases; validation materializes posting lists, so their timing is not an ordinary-build performance comparison.

## Tradeoffs

A merge copies the small source's IDs and versions while retaining its posting buffer. Successful direct transfers acquire the source leaf lock and search for their accepted row. Removing an entire source this way requires a quadratic number of ID comparisons; the default 512-row merge threshold bounds the ordinary cost. A future optimization can remove accepted rows in one scan while preserving concurrent additions. The pair reservation mutex is held only for booking and releasing the two leaf IDs, not during vector transfer. Ordinary navigation performs no additional candidate-filtering pass.

## EC2 settings

Common arguments are `--metric l2 --checkpoint 10 --checkpoint-size 100000 --write-navigation fp --fp-npa --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --validate-postings --save`. Wikipedia adds `--dataset wikipedia-en --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0`. MS MARCO adds `--dataset ms-marco --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4`. Each run saves to an independent checkpoint directory.

Logs, exact commands, resource measurements, and the tested revision are saved under `/home/ubuntu/benchmark-data/merge-posting-preservation/final` on EC2.
