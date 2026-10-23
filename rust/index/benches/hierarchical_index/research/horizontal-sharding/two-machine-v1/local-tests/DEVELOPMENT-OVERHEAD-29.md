# Development overhead after experiment 29

Reuse verified inputs, installed binaries and running machines across development attempts. Lyon explicitly requests leaving the machines running after the current experiments and will stop them later. The current 90-minute budget watchdog remains active; this is separate from the paused recurring follow-up.

The 29c controller reuses the prior corpus hash only when the same path has the expected length, the descriptor is unchanged, and the file's modification and change times both precede the start of the previous successful verification window. It records device, inode and timestamps and verifies that metadata stays stable during the check. A changed file falls back to a complete hash. Both machines take the reuse path in 29c. The workload and independent readers retain their own input-integrity checks, so cold loading still takes time.

Built executables move from the worker through the existing task S3 bucket to the peer. The transfer uses content-addressed keys, compression and a full executable SHA256 check after decompression. The reader compresses to 44,111,609 bytes and the writer to 48,902,781 bytes. No private SSH key moves between machines. The controller retains compressed SCP as a fallback if the existing Python S3 dependency is unavailable. Both 29c transfers use S3 successfully.

The interval from collected 29c build receipts to peer launch is 39.386 seconds. This includes installation, cached input checks and the successful Linux S3 adapter smoke test. It excludes cold input loading within the workload and is not a measurement of total launch-to-result time.

Publication diagnostics run through native Mac binaries with temporary Cops sandbox credentials held only in process memory. A targeted race test avoids repeated large multipart fixtures. This permits storage diagnosis while compute instances are stopped and preserves the original failures.

A saved 2M reader generation still cannot restore a construction writer. Prefer a construction checkpoint once that capability exists; do not relabel reader snapshots as resumable construction checkpoints. The full 3M workload remains necessary for the current implementation.
