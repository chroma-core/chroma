# Attribute disabled-trace work

This diagnostic changes only how a disabled owner-evaluation trace is prepared. A same-binary toggle keeps the baseline and lazy variants separate. Normal construction defaults to the baseline. Capacity planning, insertion concurrency, balancing, placement, vector references, cache policy, and predicates stay unchanged. The phased construction direction in `DYNAMIC-GLOBAL-TREE-DESIGN.md` remains future work.

An eligibility evaluation already returns the source owner's current membership version. The coordinator performs no mutation between that reply and preparing the trace. When tracing is disabled and the evaluated owner is also the coordinator's current owner, an existing returned version proves the same presence check that a second Reference command performs. The lazy variant reuses that result, increments the same event-kind counter, and omits the discarded JSON body. If the owners differ or the reply has no current record, it performs the original authoritative lookup. Enabled tracing executes the original body and lookup unchanged.

The local fixed-decision test uses one recorded construction tape for four cases: baseline and lazy, each with tracing disabled and enabled. It compares topology, locations, event counts, saved posting order, codes, versions, full vector bits, and complete saved membership. Enabled owner-evaluation trace bodies remain identical. The observed command reduction equals the exact number of owner-evaluation events. Negative cases retain validation failures for a mismatched owner location and a missing current record. This is a local correctness result, not a speed claim.

## Bounded comparison

The executable plan runs only two authoritative arms on the same retained 8,192-vector prefix: baseline and lazy. It keeps explicit full-precision navigation and neighbor predicates, 768 dimensions, and balance cuts at rows 4,096 and 8,192. It reports the exact evaluation-event count, elided and retained Reference checks, actor commands, actual vector/provider/decode counters, and phase times. The single-owner lazy run must elide one check for every evaluation event. The reference checks remain present in the baseline.

Native builds use independent random clustering decisions. Therefore, raw differences between their total actor-command counts or wall times are descriptive; they are not an exact fixed-workload attribution. The local fixed-tape test supplies the exact semantic and command-delta proof. No seed change, new cache, grouped predicate scan, or broader instrumentation is introduced.

The harness limits each direct child to 30 seconds and both children to 60 seconds total, while respecting an externally fixed deadline. Build time is separate. Metadata must remain within 50 MiB; saved-store bytes are reported separately. The prior five-cell wrapper's 140,670,998-byte failure remains preserved. Counter spans overlap and do not represent CPU fractions or physical I/O. Warm-provider zero decodes remain a valid result. Diagnostic observation overhead stays included and is not separately measured.

## Commands

Build the pinned source with the existing offline Linux toolchain and dependency cache. The recipe verifies the source manifest and safe relative IDL symlink, requires 20 GiB free, uses two build jobs, and retains the exact optimization-level-3 compiler artifact. It creates no infrastructure and downloads nothing.

```sh
python3 compile-native.py --source /mnt/spann/disabled-trace-attribution-v1/source --target /mnt/spann/hardware-scorer-v1/target --toolchain /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin --output /mnt/spann/disabled-trace-attribution-v1/build
```

Fill `attribution-spec.template.json` with the actual resulting binary descriptor and the independently authorized diagnostic deadline. Its source receipt and canonical input bindings already identify the retained restored input. Then run the existing-resource diagnostic once:

```sh
python3 run-attribution.py --spec /mnt/spann/disabled-trace-attribution-v1/attribution-spec.json
```

Local ARM development-profile checks do not establish Linux execution or performance. No upload, cloud action, resource start, or large build is performed by this package preparation.
