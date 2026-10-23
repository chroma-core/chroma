# Native distributed-engine verification

The current source passes 140 native tests. The repeated construction fixture uses 32 workers per owner, starts with one logical tree on one owner, refines existing ownership boundaries, transfers branches over TCP, then continues construction on both owners. Ten repetitions check complete membership, exact 1-bit codes, reachability, parent and centroid visibility, and drained balance work. These are correctness checks on one Mac; they provide no physical two-machine or construction-performance result.

Fault tests exercise a lost activation acknowledgement, opposite-direction transfer callers, phase close and seal during admitted work, structural publication proof and replay, failed owner save, and fresh-process reopening. A 4096-dimensional fixture forces three complete transfer chunks under a 3-MiB frame cap.

The three command-line programs compile in the native development profile. Their exact paths and binary hashes, the source manifest, fixture settings, and test logs are recorded in [ENGINE-VERIFICATION.json](ENGINE-VERIFICATION.json). The source manifest hash is `030cf20f2011b74f82c72dcd8914b330ec2f1427042f9de005e0555c463cb2a5`. Parent owns the next native CLI smoke test, physical correctness window, and benchmark admission.
