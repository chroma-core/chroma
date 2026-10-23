# Restore the admitted two-million-row canonical input

This worker restores whole, version-pinned source objects and concatenates only the declared row prefixes into one canonical vector file. It preserves source bytes, including signed zero, without decoding or rewriting floats. The package authorizes no transfer, machine start, controller, or execution window.

## The pieces

- The admitted input plan names the exact source versions, lengths, hashes, row order, and final extent (`accepted-canonical-2m-input.json`). Its bytes are copied unchanged from the accepted plan and pinned in the worker.
- The parent supplies a fresh attempt directory, fixed absolute deadline, minimum free-disk reserve, and expected sandbox instance-role name (`restore-spec.template.json`). The null placeholders deliberately make the template nonexecutable.
- The Linux worker verifies the sandbox identity and bucket before reading any source object (`materialize.py`). Its transport-independent core exists so local tests can inject tiny fake objects; the CLI has no fake-source option.
- The completed attempt stores all whole sources, per-object proofs, the canonical file, and completion provenance. Failed attempts retain their existing files and partial canonical output.

## The flow

1. **Validate the plan and external limits.** Run `python materialize.py --spec /ABSOLUTE/filled-spec.json` only within a separately admitted Linux task. The worker rejects other platforms, unapproved plan bytes, row gaps or overlaps, wrong extents, unsafe logical paths, and a reused attempt directory. The parent directory must already exist without symlink traversal. The specification does not grant or extend an execution window.
2. **Verify the source authority.** The worker requires the exact sandbox account, expected assumed-role name, bucket and region. Every metadata/read request includes `ExpectedBucketOwner`, the content-addressed key, and exact `VersionId`. It requires exact version, full length, SHA metadata, full CRC32, and encryption headers. The SDK performs one total attempt; there is no retry, range-read shortcut, latest-version fallback, or alternate bucket.
3. **Verify each whole source before slicing.** The worker downloads the complete object into a private partial, computes its full SHA-256 and CRC32, checks exact length, and syncs it. It retains a source file only after those checks. It then rereads and rehashes the complete source while copying the declared prefix, so corruption in an unused tail still invalidates the private canonical output. All 21 full objects remain retained.
4. **Publish the exact canonical result.** After all source proofs and row extents pass, the worker syncs the canonical partial and publishes it with a no-overwrite hard link. A successful runtime emits the exact final length and SHA, the consumed plan/spec and materializer hashes, the checked identity, and per-object proofs. Consumers require complete receipt and runtime provenance; a bare file or partial is not a completion receipt. A failure before attempt creation is reported only through the enclosing task's captured error; a failure afterward also attempts to preserve `failure.json`. Forced termination can interrupt receipt writing.

## Capacity and deadline contract

The approved source objects total 6,421,573,632 bytes. The final canonical file contains exactly 2,000,000 × 768 × 4 = 6,144,000,000 bytes. Because the worker preserves the complete sources, initial free space must cover 12,565,573,632 bytes plus the externally supplied reserve. Directory/receipt metadata and other processes' disk use require reserve beyond this logical payload total. The worker checks the reserve before network work and before each write; this is not a filesystem quota or a guarantee against concurrent disk consumption.

The worker hashes during download and rereads all complete sources while slicing. That accounts for a full source receive/write pass and another 6,421,573,632-byte logical local read, plus the 6,144,000,000-byte canonical write. The final canonical SHA is computed over the exact bytes copied, not from hashes of source descriptors. It is not known until actual source payloads are restored. No real payload is fetched in this package's local tests.

A task-local alarm uses the externally supplied absolute Unix deadline without resetting it. Cooperative checks also run before and after I/O. The SDK has finite connect/read timeouts and no retries. A separate parent-owned absolute guard remains necessary for uninterruptible system calls or process termination; this worker creates no supervisor, watchdog, machine controller, or background process. Deadline expiry does not authorize a new attempt or extension.

## Local evidence and remaining prerequisites

Ten tiny fake-source tests pass. They cover exact row order and signed-zero bytes, full-source retention, unused-tail corruption, source mutation during slicing, version/owner/length/hash/metadata rejection, invalid paths and extents, reused attempts, deadline and disk guards, and Linux-only entry checks. The failed initial Path-type check and its logs are preserved under `failure-v1`; the nine-test intermediate pass is bound to `pass-v2`; the final ten-test source and logs are bound by the package manifest. Fake payload directories are temporary unittest fixtures, not real restore attempts.

The Python core uses the standard library. Real Linux transport additionally requires the parent-provided, pinned `boto3` and `botocore` environment and existing authorized instance credentials. This package installs nothing and performs no authentication flow. Actual role/bucket permissions, checksum response fields, Linux alarm behavior during SDK I/O, source availability, full payload hashes, final canonical SHA, and the external resource window remain unexecuted prerequisites. Review this package with the matched runner before any separately authorized transfer. No Rust, runner, journal, state, staged index, or accepted artifact is modified.
