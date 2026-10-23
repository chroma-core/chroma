# Trace late remote calls, then test one latency improvement

Reuse the accepted epoch33 implementation and the retained full-precision 2M checkpoint. Run one detailed final-million diagnostic after adding bounded per-job request-kind attribution, server queue/handler durations and remote expansion chunk counts. Confirm exact 3M results. Keep coordination, notification listeners, reusable connections, bounded pools, parent-lookup caching and all saving improvements enabled.

The leading hypotheses are sequential chunks within one remote owner request batch, reply decoding/validation, and transfer/publication dependency waits. Owner fanout is already concurrent. Diagnose before selecting a candidate. Preserve inclusive timing and cross-thread attribution limits; avoid interpreting summed request time as a critical path.

Implement one focused change behind a same-binary control switch. Use targeted correctness tests and exact fixed navigation replay. Run three interleaved off/on timing pairs from the identical checkpoint without overlapping builds or other benchmarks. Run a separate candidate diagnostic, retain every failed attempt, and report uncertainty plus work counts. Validate saving if the accepted change affects construction results or durable integration. Freeze source and binary identities before every cloud run.

Reuse only the two authorized sandbox machines. Their prior guard window is closed; start one new guarded window, reuse input and compiler caches, and leave machines running under the guards after completion. Preserve all raw evidence and checkpoints. Recurring follow-up remains paused.
