# Epoch12 full-balance CPU profile pair

Run one explicit fixed 100K reference arm, then one explicit fixed 100K distributed arm after root admission. Both use fresh instrumented binaries built with the same compiler flags and retained input/settings, including 32 workers. Counters and the bounded profile rendezvous do not change the algorithm. Different logical work still prevents interpreting a timing ratio alone as a causal speedup. There is no larger workload, sweep, retry or automatic advance.

## Gate and timing

The new supervisor gives only the reference/coordinator a fresh sibling directory through BENCH_PROFILE_GATE: output-path plus -profile-gate. It removes stale inherited gate variables for every process, including the idle peer. The benchmark writes ready.json immediately before the balance timer, waits at most 60 seconds for go.json, writes done.json immediately after stopping the timer, and waits at most 60 seconds for released.json. All messages bind format_version=1, phase_id=balance-0, rows=100000 and actual PID. Ready/done contain realtime_ns and monotonic_ns; done adds elapsed_seconds and completed/failed status. Waits are outside timing and respect earlier workload deadlines.

The observer starts PID-scoped perf with --delay=-1 and private control/acknowledgment FIFOs. It requires an actual enable acknowledgment before writing go. A started profiler PID alone is insufficient. After matching done, it requires disable acknowledgment, verifies profiler-group shutdown and writes its report before released. Only that complete sequence establishes full coverage. Early profiler exit, byte cap, missed done or deadline means incomplete failure, never a full-phase claim.

Sampling uses 99 Hz userspace cpu-clock:u, CLOCK_REALTIME, frame-pointer stacks and process-thread inheritance. Recording has an independent hard maximum of 120 seconds, shortened by remaining collector/job/host bounds. The whole collector uses an internal 235-second deadline and outer 238-plus-two-second forced-stop bound, at most 240 seconds. Raw data is capped at 256 MiB and remains remote; report text is capped at 1 MiB. Small pre/post timer baselines are included, so successful samples cover the whole timer but are not exclusively in-phase. CPU samples do not measure blocked time.

## Identity and privilege

The observer retains explicit sudo-n, isolated root Python, verified source bytes and source/spec hash checks. It binds PID, process group, start ticks, boot ID, executable path/hash, job/instance and inherited job marker. It signals only its separately marked profiler group. There are no workload signals, deadline extensions, sysctl, sudo-policy, dependency or security-setting changes. Preflight requires installed control/delay support and root privilege. Missing sudo or unsupported options fails before either workload launches. Recording disables unrelated build-ID cache updates.

The root must supply runtime-binaries-epoch12.json keys two_machine_spann, two_machine_reader and reference. Executables reside in /home/ubuntu/spann-two-machine-v1/epoch12/build/, with reference.bin for reference. No placeholder hashes are created. Source/build receipts establish matching flags and instrumentation-only changes separately. Both arms retain the exact epoch2 100K input and token references, with no credential/corpus copies.

## Scripts and admission

make-owner-local-spec-epoch12.py and run-one-arm-epoch12.py accept only 100k-reference or 100k-distributed, one invocation each. The root explicitly selects reference first and checks the result before selecting distributed. Reference has no peer; distributed preserves the real idle-peer arrangement and first-balance single-owner semantics. Specs, outputs, run IDs and collector directories are fresh per arm.

remote-job-epoch12.py is a separate generic-supervisor copy whose only addition is admitted gate environment/directory handling. Workload 600-second/process 590-second absolute job bounds and 660-second remaining-window/SSH admission stay unchanged. Earlier epochs and source archives stay unchanged. Existing lifecycle establishes a fresh guarded epoch12 window, exact sandbox identities, rates and budget. The runner requires atomic preflight readiness before workloads and collects final small report files after completed result. Generic process cleanup/readback stays intact. Failed profiles preserve evidence and never retry.

## Local checks

Nine pure/mock tests pass alongside the unchanged supervisor suite. They cover fixed-pair/gate scope, invalid role/path/rows, clearing inherited variables, disabled PID-only bounded recording, enable acknowledgment/error/timeout, phase identities, enable/go/done/disable/report/release ordering, and incomplete coverage on early profiler exit. The log is local-tests/epoch12-profile-preparation.log. New Python files compile in memory. No cloud, SSH, sudo, workload, profiler, Cargo or Git execution occurs during preparation. Actual installed control support and instrumented gate behavior still require reviewed checks before measurement.
