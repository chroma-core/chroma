# Candidate29 Linux build succeeds; initial transfer times out

All three admitted Linux targets build successfully: the benchmark writer, independent reader and opt-in storage integration test. The build receipts bind the reviewed frozen29 source. Copying the reader to the peer exceeds the launcher’s90-second transfer allowance before any S3 smoke or benchmark begins. The source and retained build remain valid; this is a delivery failure, not a candidate correctness result.

The controller stops both instances and records $0.6498940328979492 compute for the window. No durable namespace writes occur in this window. The partial peer file remains preserved. A fresh window uses the same source and build receipts, renames mismatching partial files, transfers compressed binaries with a300-second allowance, and verifies exact bytes before execution. It does not rebuild or reuse the closed deadline.

Evidence: [build receipts](../operations/window-epoch29/build-receipts.json), [closure](../operations/window-epoch29/closed.json), [fresh continuation](../operations/measure-epoch29-resume.py).
