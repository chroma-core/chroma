# Historical sharded SPANN implementation snapshot

This branch preserves the experimental implementation before the architecture redesign discussed on October 19, 2026. It includes the current source, benchmark harnesses and research utilities. It is a historical reference, not a claim that the intended shared-upper-tree and machine-owned-subtree design is complete.

The snapshot starts from commit `ab03657249240a8732109058286efb8ce31e1faf` and the retained staged index. The source directory is missing its Git pointer and many tracked files. A separate checkout restores 1979 missing tracked files from that index and overlays all 251 surviving tracked and new implementation files, verified by SHA256. The original index and source files are preserved. Generated Python bytecode and the duplicate retained unsharded reference source tree are excluded.

The preserved original index has SHA256 `92ad7530363204bd9c0dcb9c21bb6b2a5e385049e5156762b9d32aa3da90cf5c`. No implementation changes, benchmark executions or new correctness claims accompany this snapshot. Validation checks source-copy integrity; compilation and tests have not been rerun for this commit.
