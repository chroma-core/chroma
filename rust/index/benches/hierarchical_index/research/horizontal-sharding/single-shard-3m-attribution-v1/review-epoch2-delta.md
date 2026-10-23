# Independent review of epoch2 admission corrections

The coherent admission delta passes focused static review. No unresolved blocker is identified for a new guarded execution window. This review preserves the original review and first-window failure evidence. It does not authorize deadline reuse, infer successful remote activation or change the implementation under comparison. No cloud action or additional test is performed by this reviewer.

The storage correction verifies the preserved root EBS serial, exact 256 GiB device capacity, ext4 root filesystem and active absolute shutdown timer. It retains the existing partition-growth and filesystem-resize commands and requires at least 80 GiB of actual available filesystem space. It removes the incorrect requirement that usable filesystem total exceed 250 GiB, which does not follow from a 256 GiB block device after filesystem metadata. No retained data is removed or replaced.

The profiling helper binds both actions to the exact sandbox account, region and worker through instance metadata. Enable requires the current value 4, a new receipt and the active epoch2 timer. It records intent before setting the runtime `perf_event_paranoid` value to 2 and verifies readback. No persistent configuration file changes. The setting permits user-space profiling more generally on this dedicated worker; the actual profiling commands continue to record only their bounded child processes.

Restore binds the receipt to the exact window, sandbox account, instance, original value 4 and temporary value 2. It changes only a current value of 2, accepts an already restored value 4 idempotently and rejects unexpected values. It records the observed before/after values. This resolves the first inspected helper’s unconditional overwrite. The operator must invoke restoration before API stop on normal completion and failure, preserve the actual restoration receipt and stop promptly if a prerequisite fails. Independent machine shutdown also ends the runtime setting if remote restoration becomes unavailable; a later boot’s actual value must be verified rather than inferred from this static review.

Input restoration can read the exact retained first 100K file instead of downloading it again. That branch rejects a symlink, checks the complete byte extent and streams the same whole SHA256 verification as the pinned S3 branch. Every remaining chunk retains pinned object-version, owner, extent and complete-object checksum validation. The emitted prefix and final input identity remain unchanged. Network payload accounting excludes the locally reused chunk.

The epoch2 runtime manifest retains the identical 4,519-entry extracted source closure from epoch1 and reuses those source paths. Its build target, input materialization receipt and live profiling probe paths point to epoch2. It preserves the same input-plan hash, locked compiler, 80 GiB free-space bound and per-build timeout. A fresh execution directory, local watchdog and independent remote timer must establish a new absolute deadline before any new execution. The unchanged runner revalidates source closure and final input bytes, then executes the fixed four-run protocol; successful static review is not a substitute for those receipts.

| Reviewed artifact | SHA256 |
|---|---|
| `operations/remote-storage-preflight.py` | `b2b215b1c8e53d5da9dede121a8385ca27c9ee010a31ab2208c38a1565383093` |
| `operations/materialize-input.py` | `c5df226f42fbe552d11340c6366e2ae0a4a997d22127ae28c5f170583a24123b` |
| `operations/perf-permission.py` | `28d5826544f099bfa16339449750985024b988d1f50d046254c0ca258e1273f5` |
| `operations/remote-manifest-epoch2.json` | `39b415d45458522b71f20b0e9c58eda5f39cab7cd9bf44c352279fff5344545d` |
| `operations/input-plan.json` | `401542791f43e5bb80fd4aa4e94fef7be727db09408fac35bfde5ce78ea7756c` |

The original source/harness review, successful native validation and unchanged operational checks remain applicable. Any change to the reviewed delta requires checking that change and refreshing admission identity. The live profiler and phase parser must succeed before materialization or builds; counters alone do not satisfy this campaign’s attribution admission.
