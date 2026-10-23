# Operations admission review

This review inspects the lifecycle, shutdown guard, build and generation-transfer scripts. It performs no cloud operations and starts no compiler. It checks exact task resource bindings, independent absolute shutdown enforcement, exclusive execution, immutable transfer integrity and publication ordering.

## Confirmed protections

The lifecycle controller verifies the sandbox account and exact two-instance set before starting a window. It requires stopped r6i.16xlarge instances with the task tags and preserved volume bindings. It verifies frozen artifact hashes and bounded window rates, creates a new window directory, starts an independent detached local watchdog before the API start, and confirms stopped states through the API before writing the closure receipt. An existing window directory cannot be reused for a new start.

The lifecycle controller now also reads each instance's shutdown behavior immediately before admission. It checks the returned instance identity and requires OS shutdown to stop the instance. This addresses the review finding that an OS shutdown guard could otherwise terminate an instance configured with a different shutdown action. The controller records those attribute replies before creating the execution window.

The remote guard validates the machine's account, instance ID and region through its metadata identity document. It rejects competing named task jobs, installs an absolute systemd shutdown timer and verifies that the timer is active before payload execution. The local API watchdog and remote OS timer use different shutdown mechanisms. Actual operator receipts must confirm both guards before each execution window; source inspection alone does not establish that either timer is installed.

The build script validates every file in the admitted source closure, rejects symlinks and unexpected files, checks compiler version, physical core count and disk headroom, and obtains the task's exclusive lock. It selects the exact compiler-reported benchmark artifact and records binary hashes. The operator must retain failure output and preserve existing target roots.

Generation transfer uses immutable object keys, conditional creation, checksums, exact S3 version receipts and dependency order. Payloads precede owner manifests; the generation manifest and descriptor follow them. Restore selects exact recorded versions, verifies SHA-256 and the saved reference checksum, requires a fresh namespace and parses the restored manifest graph before reporting completion. Full executable tree validation remains the fresh reader's responsibility. Corpus bytes are excluded from this transfer tool.

## Findings that require resolution or explicit limitation

| Priority | Component | Finding |
|---|---|---|
| Corrected | Build process ownership | The build supervisor now isolates a process group, sends TERM and KILL during cleanup, waits for Cargo and checks `/proc` for surviving group members. It retains the exclusive lock through the fixed host deadline if survivors remain. TERM and HUP enter controlled cleanup, and repeated TERM/HUP cannot interrupt that cleanup. This addresses the identified timeout and interrupted-build ownership gap. |
| Corrected | Workload process ownership | The remote workload supervisor centralizes cleanup in its finally block while retaining the exclusive lock. It masks TERM, INT and HUP during cleanup, checks surviving owned process-group members, and retains the lock until confirmed cleanup or the fixed host deadline. Receipt-write failures cannot escape that retention. Cleanup failure cannot produce a successful workload result. The author reports four passing pure/mock tests; this review verifies the source correction without executing remote processes. |
| Corrected | Upload resume after an ambiguous response | The format-2 content-addressed exporter handles conditional conflicts by reading and verifying actual existing bytes, length and SHA-256 before adopting an exact non-null version ID. It rejects conflicting bytes without replacement. The focused source and four-test review is recorded in CONTENT-ADDRESSED-TRANSFER-REVIEW.md. |

The 32-worker benchmark records coordinator-side client and server counters together, so requests initiated by either owner are represented once at the coordinator endpoint. Each construction phase has cumulative before and after snapshots. The figures cover framed application bytes, including JSON envelopes, length prefixes and partial failed exchanges. They exclude TCP/IP headers and retransmissions. Peer-side receipts corroborate the traffic and must not be added again to coordinator endpoint totals.

## Remaining review

The remote job runner has been inspected for reviewed argument/binary bindings, fresh machine identity, exact existing shutdown timer and action, exclusive locking, resource headroom, process-group ownership and targeted stop receipts. The identified early-lock-release finding is corrected as described above. The completed shared-tree balancing path still requires a coherent source review and the sole compiler owner's consolidated tests. No source review in this document admits physical execution or replaces the fresh identity, resource, budget, source-hash and shutdown receipts for a real window.

## Conditional publication probe

The small S3 probe creates one fresh task-owned key with a checksum and conditional creation, requires the conflicting second payload to receive HTTP 412, then checks that the current object still has the first version ID and exact bytes. It uses the fixed sandbox client and bucket, refuses to replace an existing receipt and preserves the small object. No concrete blocker appears in source review. Its actual receipt establishes conditional creation on that selected S3 adapter; it does not establish the separate local filesystem expected-generation replacement semantics. Execution remains part of the already guarded operator window, using a fresh task-owned prefix.
