# Fresh-index optimization: initial findings

The existing benchmark harness balances startup subbatches of 100k vectors through the first 1M indexed vectors, even when persisted checkpoints contain more vectors. The comparison baseline must retain that behavior. Earlier balancing should split those subbatches further without adding persisted checkpoints.

## Parallel first-leaf clustering

The current two-means routine evaluates four randomly chosen seed pairs and performs up to 128 assignment/recenter rounds. Each round computes two distances per vector, sums the selected distances, and accumulates both centroids serially. The first large root leaf cannot expose multiple subtree balancing tasks until that split finishes.

A focused variant can parallelize seed evaluation and per-vector distance assignment for the first root split while retaining serial distance sums and centroid accumulation. Indexed parallel writes retain the original vector order. With identical seed pairs, preserving both floating-point reduction orders should preserve the arithmetic, labels, and groups. This narrower variant avoids changing the clustering algorithm or reduction rounding before evidence warrants it.

The parallel worker pool must be bounded by the balancing phase’s worker budget and restricted to that first root-leaf split. Subsequent independent balancing tasks retain serial clustering to avoid nested CPU oversubscription. An opt-in thread cap with a serial default makes the comparison independent of startup cadence. The existing split function is in `rust/index/src/spann/utils.rs`; the writer call is in `rust/index/benches/hierarchical_index/writer/writer.rs`.

## Earlier startup balancing

An independent harness variant can start near 5k vectors and grow the add interval as leaf splits expose independent balancing tasks. A simple heuristic chooses an interval proportional to the current leaf count times the split threshold, bounded by a small initial interval and the existing 100k maximum. It aims to limit how much each leaf grows between balances while reducing repeated whole-tree work as the tree develops. These bounds are experimental; the schedule cannot be called optimal without timings and recall measurements.

The loop must finish each buffered add batch and publish its postings before balancing. It must rebuild the next navigation snapshot after each balance and retain existing commit/checkpoint boundaries. Startup scheduling should stop at the existing first-1M boundary, and reopened established trees should retain their existing behavior. A fixed interval selector allows 5k, 10k, 50k, and 100k comparisons without building a broad benchmark sweep now.

## Further option and future comparison

Combining bounded parallel first-leaf assignment with an earlier startup cadence is plausible, but each variant needs a separate comparison first. Bulk seeding many leaves is a further option with larger effects on tree shape and recall; it should not be silently introduced as a scheduling change.

No implementation or benchmarks for these fresh-index variants are completed in this initial investigation. No EC2 work is performed. Future comparisons should use the same fresh dataset prefix, metric, query set, thread limits, and saved checkpoint boundaries. They should report total add, balancing, commit, and build time together, peak memory, recall, and multiple runs; moving work between timing categories is not a speedup.
