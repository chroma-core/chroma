# CIP-0121204 Batch Ingestion

## Status

Current Status: `Under Discussion`

## Motivation

This CIP applies to single-node Chroma only.

While the primary focus of Chroma is near real-time ingestion of data, making the newly added data immediately available
for querying, there are use cases where users add large amounts of data in batches and prefer speed of ingestion over
data query availability (eventual consistency). This is especially true for the initial data ingestion.

## Public Interfaces

No changes to public interfaces are required.

## Proposed Changes

We propose the introduction of a new configuration flag `batch_ingest` that will allow users to control whether the
batch ingestion mode is enabled. The batch ingestion mode consists of the following mechanics:

- Allow data to be quickly queued in WAL without pushing it to segment indices
- Introduce a background thread that will process the WAL queue and push the data to segment indices
- Introduce an in-memory queue to do the hand-off between WAL commit and background thread processing. The benefit of
  the in-memory queue is that it preserve the existing batch processing mechanics (each client request is a single
  batch). In our test implementation we only kept the seqIds from the WAL to reduce memory footprint of the queue, at
  the expense of secondary WAL query to fetch batches. To further reduce memory requirement, we only keep the boundaries
  of the batch - min and max seqIds.

**IMPORTANT:** It is important to note that this change will act as an enabler for some use cases requiring batch
ingestion and able to tolerate eventual consistency.

### Benefits

- Our observation and experimentation indicate 5x speedup in data ingestion when batch ingestion mode is enabled( ~40m
  with batching off vs ~8m with batching on), It is important to observe that batching does not affect the data query
  availability.
- Reduction in server backpressure, as the data is queued in WAL and the client is not blocked until the data is
  ingested into segment indices. Our observations and tests show that this enables many (we tested with 20) concurrent
  clients to ingest data without any backpressure from the server.

### Considerations

- All ingestion of data will be batched, otherwise we cannot make WAL sequencing guarantees
- Eventual consistency, the segment indices will be eventually consistent with the WAL

### Further Investigation

- Concurrent collection WAL processing - We think overall performance can be improved by having a pool of background
  threads that can process WAL queue for
  different collections in parallel. The latter has not be investigated yet.
- Effects of collection delete on uncommitted WAL - We acknowledge the possibility of a unexpected behaviour when
  collection is deleted while there is uncommitted data in the WAL. We have not investigated this scenario yet. Our
  suggestion is that the background threads should handle this scenario gracefully.

## Compatibility, Deprecation, and Migration Plan

This change is backward compatible.

## Test Plan

We plan to modify unit tests to accommodate the change and use system tests to verify
this API change is backward compatible.

## Rejected Alternatives

- Distributed Chroma - while Distributed Chroma is the target solution, we believe that single-node chroma will continue
  to play an important role to our users and we want to support it as well.