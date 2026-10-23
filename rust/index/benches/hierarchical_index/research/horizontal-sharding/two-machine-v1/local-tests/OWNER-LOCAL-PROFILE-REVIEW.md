# Owner-local results and CPU-observation review

The reservation-history result accurately describes the completed before/after pair and preserves its attribution limits. The planned CPU observation has no source blocker in this review, but installed profiler availability remains a fresh remote prerequisite. This reviewer launches no workload, profiler or cloud operation and edits only this receipt.

## Checked result claims

The epoch9 raw results and summary agree with balance time 16.949309 versus 13.292576 seconds and interval process CPU 56.38 versus 51.84 seconds. The table correctly subtracts insertion work from cumulative owner counters: balance navigation distances are 7,190,176 versus 5,879,263, and balance vector fetches are 7,808,025 versus 7,933,784. Splits, completed transferred rows, completed jobs and final leaf counts differ. The observed 21.6% elapsed reduction is descriptive evidence from independently constructed trees, not an isolated or repeatable correction speedup. The deterministic manager test supports removal of completed-history visits separately.

Both outputs validate complete unique 100K membership and saved vector/code state in fresh readers. Their final smaller-owner shares are 44.648% and 44.512%, and both trees have one internal root with depth-one leaves. The peer is empty during the measured initial-owner balance, which exchanges only 149 framed application bytes. Private-network throughput therefore does not explain this balance interval. The retained reference comparison still leaves substantial owner-local overhead; the report does not assign it all to reservation history.

The before and after source comparison changes only the reservation manager. Independent job exit receipts confirm cleanup despite the retained peer-stop helper permission errors. API closure is recorded separately. Valid benchmark results do not imply a clean launcher exit, and no rerun is required solely to erase those helper failures. The result document preserves this distinction.

## CPU observer scope and bounds

The new preparation admits only one corrected epoch9 executable and the fixed standard 100K input/settings under fresh epoch10 output and run identities. The generic workload supervisor retains its 600-second process ceiling, 590-second shared absolute job deadline and 660-second runner timeout/required remaining host interval. There is no source rebuild, input-size sweep, tuning sweep, automatic retry or next arm.

The collector verifies its own source hash from the reviewed spec and binds the observed workload to the exact spec digest, job/instance identity, process ID, boot identity, process start ticks, process group, executable path/hash and owned-job environment marker. Repeated process checks detect stale identities before and during observation. It samples userspace CPU at 99 Hz for that process and its threads; its command contains no system-wide sampling option. It never starts the workload.

A nested timeout sends the profiler SIGINT at nine seconds and SIGKILL after one further second, bounding recording to ten seconds. The collector’s own internal deadline is at most 115 seconds, reduced further by the workload and host deadlines. An outer 118-second timeout with two-second forced-stop grace bounds the complete collector invocation to 120 seconds. The profile child receives a 256 MiB file-size limit; the text-report child receives a 1 MiB limit and at most ten seconds. The raw recording remains remote. Only small reports and receipts are collected locally.

Collector cleanup verifies a separate owned-profile environment marker before signalling its process group and checks for surviving members. It never signals the workload group as profiler cleanup. Workload and peer shutdown remain the existing supervisor/runner responsibility. Interrupted or failed collectors retain their result/error where possible and do not trigger another sample automatically.

The atomic ready receipt is published only after checking installed profiler/timeout tools, required profiler options, current sampling permission configuration and the workload binary hash. The runner waits for that ready receipt before launching owner1. Missing tools or disallowed unprivileged sampling fail without sudo or permission changes. The readiness tests are mocked preparation evidence; they do not establish the remote machine’s current permissions or a successful recording.

## Interpretation limits and verification

The profiler observes one slice of the balance interval. Flushed phase events are detected by polling, so observed boundaries can overlap adjacent bookkeeping. Earlier and later CPU work remains uncovered. CPU sampling does not measure blocked time or independently quantify mutex waiting. Frame-pointer call-chain completeness depends on the retained binary; incomplete stacks must remain explicit and leaf-symbol samples may be the usable evidence. Report percentages must refer to recorded samples rather than the entire build.

The retained `epoch10-profile-preparation.log` records nine passing pure/mocked tests, including PID bindings, fixed input/artifact settings, command/time/file limits, marked-group cleanup and permission failure. Source review verifies those controls; actual attachment, usable symbols, sample count and phase coverage remain execution-result gates. The next decision is one bounded CPU observation, not an assumption that the remaining latency gap is explained.

## Reviewed SHA256 values

| File | SHA256 |
| --- | --- |
| `local-tests/RESERVATION-HISTORY-RESULT.md` | `03767374158b614307c7838c8d47dec8accc42d6d3da0b7f28d9bd15d55a9558` |
| `operations/make-owner-local-spec-epoch10.py` | `8b4e89e91b186b971e3fa0ab08eb23bd284287211aaa02ed932cf81f1189cc1e` |
| `operations/run-one-arm-epoch10.py` | `01b4d740b3a6b0a8e14abdd06a52c6ca163834c82acfefbed298e5a0efad9427` |
| `operations/remote-profile-epoch10.py` | `7c827f420019971aac10b23ab07d5d9c2a32f513e2853fe2ee69b72ae8fa3047` |
| `operations/owner_local_epoch10_test.py` | `fc946b1fe8bfb6dce044e810d13b2f8586d4373ff49f4af8124e246b8595d30c` |
