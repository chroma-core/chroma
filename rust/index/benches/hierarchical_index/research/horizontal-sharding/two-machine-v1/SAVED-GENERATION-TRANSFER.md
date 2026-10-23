# Validate a saved generation on another machine

This procedure proves that a reader can reopen the completed index from saved bytes and independently verified full vectors. The reader uses no writer service, ownership executor, construction cache or writer memory. A successful local publication alone does not establish durable S3 publication or independent-machine reopen; retain separate receipts for those claims.

## Preconditions

Use the authorized sandbox account and region, the verified task instance IDs, a unique task storage prefix and a fresh execution window with independent local and remote shutdown guards. One cloud operator owns transfer and execution. Resolve the actual task bucket and source object versions from current operations receipts; this document intentionally does not invent a bucket, machine address or source version.

Construction workers must finish before saving. Close mutation admission, require all transfers, structural publications and handoffs to finish, then seal the mutation epoch. Each owner writes its own immutable routing and posting payloads. The coordinator copies those immutable objects into one generation namespace, verifies their references and root coverage, then writes the combined generation manifest and conditionally publishes its descriptor last. Preserve failed attempts and the previous published descriptor.

## Export the frozen namespace

1. Obtain the confirmed descriptor from the completed campaign and read its combined manifest. Record the save attempt, generation, input corpus descriptor and complete set of referenced owner manifests and payloads. The corpus descriptor binds dimensions, row count, byte layout, SHA-256 and immutable source locators.
2. Copy the referenced files under `objects/` and `owners/` to the unique S3 generation prefix. Preserve relative paths because the saved manifests refer to those paths. Retain successful object version IDs, lengths and integrity receipts. Owner-local staging directories, temporary files and publication lock files are not reader dependencies.
3. Upload the combined immutable generation manifest under its preserved `generations/` path only after every referenced object is confirmed. Upload the immutable save-attempt record under `attempts/` if retaining the complete local publication namespace for audit.
4. Publish the final generation descriptor only after verifying that the manifest and all dependencies exist at the intended versions. If publishing a mutable latest pointer, use the selected S3 adapter's actual conditional replacement against the expected prior generation and retain its response; independent owner latest pointers are insufficient. A plain unconditional upload is not evidence of that conditional-publication contract.
5. Read back the published descriptor and manifest and retain their exact object versions and integrity information. Do not describe the generation as durably published until this succeeds.

The independently located vector corpus need not be copied during subtree handoff. Its descriptor must point to a retained immutable corpus or pinned source object versions from which the same canonical bytes can be assembled. Retain that corpus or those source versions for the generation's lifetime.

## Restore and validate on the other machine

1. Use a fresh reader directory on the other verified task machine. Restore the exact referenced objects, owner manifests, generation manifest and descriptor from the confirmed S3 versions, preserving their relative paths. Do not copy writer memory or start a writer service. A descriptor can be passed directly; a restored local latest pointer is optional.
2. Assemble or reuse the canonical full-vector file from the generation's pinned immutable sources. The reader may use a different local filename through `--vector-path`; this changes only the filename. It must preserve the descriptor's dimensions, row count, offsets, stride, SHA-256 and source locators. The adapter streams and checks the actual corpus bytes before preloading or querying.
3. Run the prebuilt `two_machine_reader` executable with `--store`, `--descriptor`, `--expected-rows`, `--output` and, if needed, `--vector-path`. Record the executable/source hash, process and machine identity, absolute deadline, command and exit status. Do not invoke Cargo from the validation process.
4. Require the reader report to verify exact membership, all persisted 4-bit centroid codes, all persisted 1-bit posting codes and the real full-vector corpus hash. Its executable query smoke checks must return valid saved IDs and finite scores. These training-vector smoke queries establish executable visibility; they do not establish held-out recall parity.
5. Retain the result JSON, reader stdout/stderr, descriptor and manifest receipts, per-object versions, source corpus descriptor and independent instance/process receipts. Verify task compute shutdown through the API when the window closes, preserving disks and saved artifacts.

## Implemented interfaces and remaining evidence

The local POSIX adapter provides immutable payload installation, manifest-last generation save, conditional latest-pointer publication and readback. The owner service binds the complete frozen save request to its save identity, including roots, corpus descriptor, generation and destination path. A changed destination does not permit the same save identity to acquire a different body.

The benchmark runner saves actual owner-local payloads, copies them in bounded chunks and launches a separate reader process. The reader can relocate the corpus filename while verifying the generation-bound SHA-256. Local unit and subprocess tests are recorded separately from physical execution.

The cloud operator still must execute and retain the S3 publication, restoration on the other physical machine and that reader's successful validation receipts. This procedure is preparation, not evidence that those cloud actions have occurred. General machine-failure recovery, ongoing writer publication and retained-root garbage collection remain outside this milestone.

## Operator tool

The [generation transfer script](operations/generation_transfer.py) implements export and restore for the fixed authorized bucket `spann-artifacts-079819354890-20261014-ecfcca4d` in account `079819354890`, region `us-east-1`. Its `plan` mode reads local files only. Its `upload` mode requires an explicit unique prefix, receipt path and fresh absolute deadline; it checks the fresh AWS account, uses expected-bucket-owner checks, requires versioning, creates every key with `IfNoneMatch='*'`, verifies SHA-256 during upload and records exact version IDs. It publishes payloads, owner manifests, the combined manifest and the immutable descriptor in that order. It updates the local receipt after each confirmed object so a retry can verify and reuse only previously recorded exact versions.

The script's `restore` mode requires a confirmed receipt, fresh empty destination and absolute deadline. It requests each recorded version explicitly, verifies streamed SHA-256 and the saved reference checksum, installs the descriptor last, then reparses the dependency graph. Its default bounds are 1 GiB per object, 2 GiB total and 1 MiB read chunks. These are transfer bounds for saved index payloads and metadata; the script transfers no full-vector corpus. Both modes use the operator's inherited Cops-managed AWS environment or explicit profile. They do not establish the required independent shutdown guards or start instances.

The [local mock tests](operations/generation_transfer_test.py) exercise dependency order, exact-version restore, idempotent receipt-based resume, byte limits, malformed paths, source mutation and downloaded-byte corruption. They make no AWS calls and do not replace execution receipts.
