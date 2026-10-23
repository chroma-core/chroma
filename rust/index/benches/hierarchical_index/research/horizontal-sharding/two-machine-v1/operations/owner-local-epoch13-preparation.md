# Epoch13 source-leaf-claim confirmation

The prepared scripts repeat only the fixed 100K, 32-worker distributed arm with the source-leaf-claim correction and a full-balance CPU profile. They copy the working epoch12b operations, including its actual five-byte perf acknowledgment. They introduce no new guard, profiler or workload framework. Compare against the retained matched reference and prior candidate evidence; a new reference run remains an explicit optional arm, with no automatic advance or retry.

The distributed binary and reader use fresh epoch13 build paths and actual build hashes. The optional reference reuses /home/ubuntu/spann-two-machine-v1/epoch12/build/reference.bin, SHA256 8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6. Its executable is not rebuilt or copied. Gate/output/collector namespaces are fresh epoch13 even for that optional reference. Inputs, 80K owner capacity, 32 workers, thresholds, full-phase FIFO control, source/spec/PID identity, privileged collector bounds, cleanup and report collection remain unchanged from working epoch12b.

## Artifacts the root must finalize

- Wait for the engine's source freeze, meaningful native tests and independent source review. Do not package a changing checkout. Then use the existing package-observability-sources.py to create packages/source-epoch13.tar.gz and its JSON source manifest.
- Copy operations/build-manifest-epoch13-distributed.DRAFT.json to build-manifest-epoch13-distributed.json. Populate source_files from that immutable archive manifest. Set combined_review_passed only after review. The draft rejects execution. The adapter builds only distributed reader/writer, using the same compiler/flags and retained epoch12 target cache at /home/ubuntu/spann-single-shard-3m-attribution-v1/epoch2/target.
- Assemble the reviewed runtime/upload archive with the existing packaging method. Keep source, script and manifest hashes in the admission. Do not copy credentials or retained corpus. Freeze operations/admission-epoch13.json with at most 2,700 seconds and task compute cap $6.05 at the retained rates, subject to fresh rate/budget verification.
- After the actual incremental Linux build, write runtime-binaries-epoch13.json from its two_machine_spann.binary.json and two_machine_reader.binary.json SHA256 fields. Retain the exact epoch12 reference hash above. runtime-binaries-epoch13.DRAFT.json contains null candidate hashes and is deliberately inadmissible. The generator requires the final non-draft file.

## Root commands and required bindings

These are root-operator instructions, not commands executed by this preparation. Run them from two-machine-v1 after freeze and normal review. Existing lifecycle and remote guard scripts remain unchanged.

```sh
python3 package-observability-sources.py /private/tmp/chroma-hierarchical-spann-sharding/single-shard packages/source-epoch13.tar.gz
python3 operations/lifecycle.py start --window operations/window-epoch13 --admission operations/admission-epoch13.json
```

After start, obtain fresh running-readback.json addresses through the existing read-only AWS flow. Record a fresh window_id beginning epoch13- in start-intent.json without changing its deadline. Establish and independently read back both remote guards with the exact fixed deadline before payload uploads. remote-guard.sh takes SPANN_WINDOW_ID, SPANN_DEADLINE_UNIX and the exact SPANN_INSTANCE_ID environment variables. Use the existing local key and strict known-host checking; never reuse epoch12 addresses.

With SSH arguments and WORKER_IP/PEER_IP freshly bound by the root, the upload/build command forms are:

```sh
scp "${SSH_ARGS[@]}" operations/remote-build-epoch13.py operations/remote-job-epoch13.py operations/remote-profile-epoch13.py operations/build-manifest-epoch13-distributed.json packages/source-epoch13.tar.gz "ubuntu@${WORKER_IP}:/home/ubuntu/spann-two-machine-v1/epoch13/"
scp "${SSH_ARGS[@]}" operations/remote-job-epoch13.py "ubuntu@${PEER_IP}:/home/ubuntu/spann-two-machine-v1/epoch13/"
ssh "${SSH_ARGS[@]}" "ubuntu@${WORKER_IP}" "mkdir /home/ubuntu/spann-two-machine-v1/epoch13/source"
ssh "${SSH_ARGS[@]}" "ubuntu@${WORKER_IP}" "tar --no-same-owner --no-same-permissions -xzf /home/ubuntu/spann-two-machine-v1/epoch13/source-epoch13.tar.gz -C /home/ubuntu/spann-two-machine-v1/epoch13/source"
ssh "${SSH_ARGS[@]}" "ubuntu@${WORKER_IP}" "python3 /home/ubuntu/spann-two-machine-v1/epoch13/remote-build-epoch13.py --manifest /home/ubuntu/spann-two-machine-v1/epoch13/build-manifest-epoch13-distributed.json --output /home/ubuntu/spann-two-machine-v1/epoch13/build --deadline ${SPANN_DEADLINE_UNIX}"
```

SSH_ARGS, addresses and deadline are required root bindings, not guessed values. The root must create the fresh epoch13 base directories and verify each uploaded archive/script/manifest hash before extraction/build. Read the retained reference's actual hash as well. The adapter independently validates the complete extracted source closure and refuses an existing build directory. The reference is retained on the worker; transfer only the new writer/reader binaries to the peer using the existing reviewed binary-transfer method, and verify both destination hashes. After writing final runtime-binaries-epoch13.json and approving the exact runtime bindings, run the one candidate arm:

```sh
python3 operations/run-one-arm-epoch13.py 100k-distributed
python3 operations/lifecycle.py stop --window operations/window-epoch13
```

The runner retains the 600-second workload cap and 590-second absolute job bound. Both processes receive unique fresh specs; the idle peer starts only after profiler preflight is ready. The full timed balance is covered only after actual enable ACK before go, matching done, disable ACK and bounded profiler cleanup/report. Existing 120-second recording, 240-second collector, 256MiB remote raw profile and small-text report limits remain. The optional reference command replaces the arm with 100k-reference and does not start a peer. The root should use retained reference evidence unless a fresh reference answers a named unresolved question.

Eleven focused/mock and unchanged-supervisor tests pass in local-tests/epoch13-profile-preparation.log. They retain full gate ordering and five-byte ACK checks, and add retained reference/fresh candidate path checks plus rejection of unfrozen build and null runtime manifests. New scripts compile in memory. No source, shared state, ledger, Cargo, Git, AWS, SSH, upload, start or workload action occurs during this preparation.
