# Epoch8 execution-bound review

The proposed run changes the time allowance for one fixed 3M distributed workload while retaining the exact epoch7 algorithm, executable hashes, vectors, settings and capacities. The epoch7 timeout is incomplete evidence rather than a completed 3M result. It finishes ten 100K construction cycles and stops during balancing after the addition to 2M. Nothing in this review extends that closed window or predicts completion within the new bound.

The reviewer finds no remaining source blocker in the corrected epoch8 scripts. The remote supervisor permits at most 1,800 seconds only for the named 3M distributed performance arm, its two owner job identities, exact epoch7 performance/reader executable paths and hashes, fixed numeric construction settings, fixed retained input descriptor, workload-plan hash, input digest and input byte count. Correctness, reference and 100K workloads retain the 600-second process ceiling. A changed or missing exception binding falls back to that ceiling and rejects an extended spec.

## Fixed deadlines and ownership

The one-arm runner sets a new absolute 1,790-second job deadline for the 3M distributed arm, including preparation, and requires 1,860 seconds of remaining host-window time. Its SSH call uses a 1,860-second timeout. Other arms use 590 seconds, with 660 seconds of remaining host window and SSH timeout. The remote validator rejects a 3M deadline more than 1,790 seconds after admission and retains the 5,400-second host-window ceiling. The supervisor executes only until the earlier of the admitted absolute deadline and process timeout. It does not move a running process’s deadline forward.

The supervisor’s exact sandbox instance/account checks, installed shutdown-timer verification, exclusive writer lock and process-group ownership/cleanup code remain unchanged. The runner performs one explicitly selected arm, creates fresh output and evidence directories, and exposes no attempt flag or automatic retry/advance. Both owners use the same absolute job deadline. The retained token path is reused; the scripts copy no credentials or corpus. Failed stop commands and independent exit receipts remain separate evidence, and cleanup/collection errors fail the runner.

The correctness runner retains its shorter bounds and independently attempts both needed stops and receipt collections on failure. It uses the exact epoch7 binaries and retained small fixture while producing fresh epoch8 outputs. This review does not require another correctness run solely because the process supervisor permits more time; that remains an operator admission decision.

## Finding and correction

The first exception predicate omits input identity, although its generator reads the frozen plan. That allows a direct spec with another descriptor to receive the larger time allowance. The corrected predicate requires `/home/ubuntu/spann-two-machine-v1/epoch2/input/3m.json`, plan SHA256 `39f31ba042f79d29e678278a5147343f218088a6675fd9cb31746f606ccb46b4`, input SHA256 `18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba` and 9,216,000,000 input bytes. Negative tests reject changed and missing descriptor/provenance bindings for both owner roles. This closes the identified gap without widening the exception.

The retained `epoch8-bound-admission.log` records all 12 local tests passing in 0.019 seconds. The tests cover the exception, disallowed arms and settings, changed hashes and inputs, fixed deadlines, fresh window-derived run identities, rejected attempt flags, and inherited process ownership/cleanup behavior. They use pure or mocked execution and do not establish installed remote guards or actual process shutdown. This reviewer performs no Cargo, AWS, SSH or benchmark execution.

## Operator gates

Before any job, the sole operator must create a new independently guarded window, verify exact task resources and rates against the remaining cumulative budget, read back installed executable hashes, and verify previous processes are stopped. The local start-intent receipt must include the actual fresh `epoch8-` window identity and fixed deadline that match the installed guards. The current lifecycle helper’s missing window identity can be supplied as receipt metadata after start and before any job; it must describe the real guard rather than invent or extend a window. The runner rejects a closed window or an insufficient remaining interval. The reviewed supervisor still requires normal upload admission.

## Reviewed SHA256 values

| File | SHA256 |
| --- | --- |
| `TIME-BOUND-REVIEW-DRAFT.md` | `ec353c65bee0394bae64c4e6393b53b8d904a7dc90a8db25decaca00d622401b` |
| `operations/remote-job-epoch8.py` | `b9f1c55b18ad2d5dfe51fb6639a2e6b0859650abfef1cdb0ed6beaa4ce817107` |
| `operations/make-performance-spec-epoch8.py` | `dc4088527c05d0c19dbd5bc604f31f1be61201a2209478d951fe6942f703ce66` |
| `operations/run-one-arm-epoch8.py` | `22f682a3c8ebf7b3f405bf4bbee303efddc51fc753b9e763f1e21eacf35b06c4` |
| `operations/run-correctness-epoch8.py` | `7465be54d3870eefad0e07064d652c2daa8d19747b84cbb66957ce843fd57a01` |
| `operations/remote_job_epoch8_test.py` | `7a87792cdf663587996c4f4e195f8bde5205caa7a159a328ceba7cd7fd4d39d1` |
