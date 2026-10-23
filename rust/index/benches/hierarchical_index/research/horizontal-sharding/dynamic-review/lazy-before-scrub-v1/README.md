# Lazy disabled before-scrub trace preparation

The retained 8192-row measurement spends 402,423,264 ns preparing disabled before-scrub trace data and 3,076,128 ns in the separate semantic scrub, inside 2.527743228 seconds of balance. These are actor-wait-inclusive elapsed spans, not CPU fractions or a speedup prediction.

The opt-in path preserves validation while removing discarded trace construction. It requests the same physical rows, validates current-key presence through consecutive same-owner batches, and increments the same event counter. Each owner checks every requested key in order. Missing locations and owners are processed only after the preceding batch succeeds, preserving the earliest error. Duplicates remain in the batch. Serialized construction prevents intervening mutation. The enabled-trace path retains its original full event body and Reference requests.

The fixed-decision local proof checks saved postings, versions, codes, vector bits, topology, locations and enabled trace parity. Individual Reference requests fall from 438 to 46 batch requests, removing exactly 392 actor commands. Six validation cases preserve error ordering. Two Rust tests and two fake harness tests pass; the benchmark builds and an actual synthetic 64-row native entry passes its complete census. Local ARM development timings establish correctness only.

Both lazy switches default off. The new switch requires the existing before-scrub observer. A possible later experiment reuses the accepted single-arm harness on the same retained 8192 vectors, with 30 seconds per arm and total diagnostic compute, a fixed external deadline, two configured threads and observed 2 GiB/50 MiB limits. No run starts from this package. Independent stochastic runs cannot establish a causal speedup; fixed-workload command elimination is the current result.

The package preserves previous epochs and includes no corpus or executable. Linux compilation is separately bounded to 1200 seconds, offline dependencies, two jobs and a 20 GiB free-space floor. The broader reserve/parallel-add/parallel-balance/reshard design is outside this change.
