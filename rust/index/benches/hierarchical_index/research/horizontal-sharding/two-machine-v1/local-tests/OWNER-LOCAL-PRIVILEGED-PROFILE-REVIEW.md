# Privileged owner-local CPU-observation review

The epoch11 preparation runs the same fixed corrected-runtime 100K observation with a narrowly scoped privileged profiler. The epoch10 preflight failure remains retained: the installed profiler is present, but the unprivileged account cannot sample with the current permission setting. Epoch11 creates a fresh guarded window and changes no kernel permission setting, workload source, executable or input. The reviewer finds no remaining source blocker in this preparation; actual successful attachment and useful samples remain execution-result gates.

## Privilege and execution binding

The runner invokes noninteractive sudo only for an absolute timeout command and the isolated absolute Python interpreter. It invokes no privileged shell, interactive login, permission change or system-wide profiler. Before executing collector code, the privileged interpreter reads the exact fixed collector path, verifies the reviewed SHA256, and compiles and executes those same in-memory bytes. This prevents a source reread between hash verification and execution. Isolated Python mode excludes user Python startup/path configuration.

The collector additionally verifies the spec bytes against the exact passed digest and its own source against the spec’s reviewed source binding. The spec admits only the retained epoch9 executable and fixed 100K input/settings. Observation binds to the workload process ID, process start ticks, boot identity, process group, executable path/hash, spec/job/instance identities and owned-workload marker. Repeated checks detect changed bindings. The actual benchmark supervisor still starts the workload as the ordinary account and retains the existing instance identity and shutdown-guard checks.

The collector requires effective root without modifying the existing profiler permission setting. Its preflight checks installed tools and required command options, including suppression of build-ID cache writes. The ready receipt is published before either workload starts. The runner rejects failed preflight or a mismatched readiness digest, then rechecks collector liveness before admitting owner1. This confines privilege to the reviewed observation while leaving workload permissions unchanged.

## Bounds, cleanup and collection

Sampling remains userspace CPU for the verified process and its threads at 99 Hz. There is no system-wide sampling flag. Normal recording stops at nine seconds, with a one-second forced-stop grace. The collector retains a 115-second internal ceiling and an outer 118-second timeout with a two-second forced-stop grace. Individual profiler files receive a 256 MiB size limit; the text-report subprocess has a 1 MiB file-size limit and at most ten seconds. Workload bounds remain the ordinary 600-second supervisor ceiling and shared 590-second absolute job deadline within the fresh independently guarded host window.

Profiler cleanup verifies a separate owned-collector marker before signalling its process group, escalates within fixed limits, and checks that no living members remain. It does not signal the workload group. Root-created collector directories explicitly receive mode 0755, and JSON receipts, text reports and diagnostic stderr receive mode 0644, making small evidence readable to ordinary-account collection. The raw recording remains remote and is explicitly excluded from collection.

The initial runner can collect workload receipts before the asynchronous profiler report finishes. The corrected runner performs a final dedicated evidence fetch only after the atomic completed collector result exists. That fetch requires a readable final report and collects only the explicit small-file allowlist. The regression checks that a missing report rejects completion and raw recording bytes are excluded. This closes the identified report-collection race without rerunning the workload.

## Tests and interpretation

The retained `epoch11-profile-preparation.log` records 12 pure/mocked tests passing in 0.020 seconds. They verify the bounded privileged argv and checked-source bootstrap, PID/spec bindings, fixed workload, recording/file limits, marked-group cleanup, evidence permissions and final-report collection. They do not prove remote sudo admission, profiler attachment, symbol quality or sample coverage. Normal remote upload and guarded-window admission remain the sole operator’s responsibility. This reviewer performs no source changes, Cargo, AWS, SSH or Git actions and edits only this receipt.

The result must report actual sampled time, phase overlap, sample count and usable symbol/call-chain coverage. The observation covers one slice of initial-owner balance and measures CPU execution rather than blocked time. The remaining latency gap stays unassigned until the sample supports narrower conclusions. No input-size sweep, rebuild, automatic retry or larger benchmark follows from this preparation.

## Reviewed SHA256 values

| File | SHA256 |
| --- | --- |
| `operations/make-owner-local-spec-epoch11.py` | `86ef153933d1eb3d9c61a6db3ebc9761d959cf1393c2e44e2317a69dd011179f` |
| `operations/remote-profile-epoch11.py` | `7d5a45e0f96d1816719a596c23d5099dc7e3e92e9bc1e56a5f76768cf5e86c0c` |
| `operations/run-one-arm-epoch11.py` | `b6f425f75c7ab14f10f67d31bae2f2349c950349ebc111efaf45ea989e686912` |
| `operations/owner_local_epoch11_test.py` | `f24274157b8e92c1040e4b0de08add191d35af417cb05516853abda627d76451` |
