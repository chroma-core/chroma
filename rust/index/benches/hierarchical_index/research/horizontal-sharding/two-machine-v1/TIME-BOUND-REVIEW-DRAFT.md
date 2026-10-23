# Epoch8 runtime bound review draft

The proposed epoch8 run repeats the fixed 3M distributed workload with a longer process bound and the exact epoch7 Linux binaries. It changes neither the algorithm nor the number of vectors, construction settings, owner capacities or input. This is local preparation for review; it launches nothing and does not extend the closed epoch7 execution window.

## Reason for the change

The epoch7 run reaches the existing absolute job deadline during the balance after adding 2M vectors. The operator reports no construction error in stderr and confirms cleanup for both processes. The recorded 1M balance takes 43.4286 seconds and the subsequent addition to 2M takes 40.9312 seconds. Input preparation also consumes the fixed process interval. These observations justify reconsidering the time bound; they do not establish the remaining completion time or guarantee that a longer run will finish.

## Fixed limits

| Workload | Process timeout ceiling | Absolute job deadline from runner admission | Required remaining host window | Runner SSH timeout |
| --- | --- | --- | --- | --- |
| Fixed 3M distributed arm, both owners | 1,800 seconds | 1,790 seconds | 1,860 seconds | 1,860 seconds |
| All 100K arms and reference arms | 600 seconds | 590 seconds | 660 seconds | 660 seconds |
| Physical correctness fixture | 600 seconds | 590 seconds | 660 seconds | 660 seconds |

The enclosing host-window ceiling remains 5,400 seconds. The remote supervisor still uses the earlier of the immutable absolute job deadline and its admitted process timeout. It verifies the independently installed absolute host shutdown timer and the exact instance/account identity. Cleanup retains exclusive writer ownership until process-group shutdown is verified or the fixed host deadline arrives. The timeout receipt label names the reviewed workload bound instead of incorrectly naming 600 seconds for every workload.

The exception applies only to kind `performance`, arm `3m-distributed`, exactly 3,000,000 rows, the fixed 32-worker construction settings and capacities, exact epoch7 performance and reader binary hashes, the exact retained epoch2 3M input descriptor path, frozen workload-plan hash and corpus digest/byte count, epoch8 output paths, and a fresh epoch8 window identity. The remote validator rejects larger time bounds for the reference, correctness, 100K workloads, changed settings or changed binaries. It also rejects a 3M job deadline more than 1,790 seconds after admission. A lower remaining interval never moves an existing deadline forward.

## Prepared files and retained inputs

The new supervisor is [remote-job-epoch8.py](operations/remote-job-epoch8.py). The new spec generator is [make-performance-spec-epoch8.py](operations/make-performance-spec-epoch8.py), and the one-arm runner is [run-one-arm-epoch8.py](operations/run-one-arm-epoch8.py). An optional fresh physical fixture runner is [run-correctness-epoch8.py](operations/run-correctness-epoch8.py). Original supervisors, epoch7 scripts and source archives remain unchanged.

The performance binary remains `/home/ubuntu/spann-two-machine-v1/epoch7/build/two_machine_spann`, SHA256 `2fd0e4a2aecb9ab0718cd7a72a6105e809dc3273d12cc7fbaa0b22e76217f864`. The reader remains `/home/ubuntu/spann-two-machine-v1/epoch7/build/two_machine_reader`, SHA256 `9787bad5d8f5ce1fbf2c4aee63f3a651d2a9069cd67b9ab8bc3505873552c356`. Correctness uses the epoch7 binary SHA256 `3a7775743b9804571c24fe3cd5bed228a1052a8097187c189b8bb56d134a57f7`. These values come from [runtime-binaries-epoch7.json](operations/runtime-binaries-epoch7.json). The reference binary and its hash remain unchanged.

The runners reuse retained epoch2 input descriptors and vector files, the retained epoch1 correctness fixture and the existing remote token path. They create no credential copy and download no corpus. Output directories, job specs, process receipts and run IDs use epoch8. The run ID incorporates the fresh window identity. Each invocation names exactly one arm; there is no `--attempt` option, automatic retry or automatic advance to another arm. Existing local or remote evidence causes admission to fail instead of being overwritten.

## Admission work still required

The root operator must create a fresh guarded window and independently verify both exact task instances, rates, remaining budget, installed binary hashes and stopped previous processes. The new local runner expects `operations/window-epoch8/start-intent.json` to contain the fresh `window_id` beginning `epoch8-` and the fixed `deadline_unix`, plus fresh `running-readback.json` host addresses. Both local and remote absolute shutdown guards must already exist. The new supervisor must undergo normal review and upload admission before remote use. No script here starts instances or installs or extends host guards.

This correction adds possible compute time, not a budget increase. The existing cumulative $1,000 ceiling still applies; the root operator must account for the planned interval using verified instance rates and promptly stop idle compute. No execution is authorized by this draft.

## Local validation

[remote_job_epoch8_test.py](operations/remote_job_epoch8_test.py) exercises both 3M owner roles, rejects extended bounds for other workloads and changed hashes/settings, checks fixed job and host deadlines, verifies fresh run identities and exact epoch7 binary reuse, and checks that the runner rejects an attempt flag. It also runs the supervisor's inherited admission, process ownership and cleanup tests. The unchanged original supervisor suite runs alongside it. All 12 local tests pass; the log is [epoch8-bound-admission.log](local-tests/epoch8-bound-admission.log). All four new scripts compile through Python's in-memory compiler. No Cargo, AWS, SSH, transfer or benchmark command is executed during this preparation.
