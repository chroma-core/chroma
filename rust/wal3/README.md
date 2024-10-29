wal3
====

wal3 is the write-ahead (lightweight) logging library.  It implements a linearlizable log that is
built entirely on top of object storage.  It relies upon the atomicity of object storage to provide
the ability to atomically create a file if and only if it does not exist.  This allows us to create
a log entirely on top of object storage without any other sources of locking or coordination.

# Design

wal3 is designed to work on object storage.  It is intended to scale to the limits of object
storage, although the implementation currently does not.

## Interface

The log exposes separate reader and writer interfaces.  The core idea is that a log multiplexes
multiple distinct streams of data into a single log.  Each stream is identified by a stream ID.
When append completes, all data available written is readable.

```rust
pub struct LogWriter<O: ObjectStore> { ... }

impl<O: ObjectStore> LogWriter<O> {
    pub async fn initialize(options: &LogWriterOptions, object_store: &O) -> Result<(), Error>;
    pub async fn open(options: LogWriterOptions, object_store: O) -> Result<Arc<Self>, Error>;
    pub async fn close(mut self: Arc<Self>) -> Result<()>;
    pub async fn streams(self: &Arc<Self>) -> Result<Vec<StreamID>, Error>;
    pub async fn open_stream(self: &Arc<Self>, stream_id: StreamID) -> Result<(), Error>;
    pub async fn close_stream(self: &Arc<Self>, stream_id: StreamID) -> Result<(), Error>;
    pub asynv fn meta_stream(self: &Arc<Self>, stream_id: StreamID, metadata: serde_json::Value) -> Result<(), Error>;
    pub async fn append(
        self: &Arc<Self>,
        stream_id: StreamID,
        message: Message,
    ) -> Result<LogPosition, Error>;
}

pub struct Limits { ... }

pub struct LogReader<O: ObjectStore> { ...  }

impl<O: ObjectStore> LogReader<O> {
    pub async fn open(options: LogReaderOptions, object_store: O) -> Result<Self, Error>;
    pub async fn read(
        self: &Self,
        stream_id: StreamID,
        from: LogPosition,
        limits: Limits,
    ) -> Stream<Result<(LogPosition, StreamID, Message), Error>>;
}
```

The astute reader will note that this log is in process.  It is meant to be run under leader
election, with all writes routed to the log, just as one would do running a server.

## Data Structures

wal3 is built around the following data structures:

- A `Log` is a prefix in the object store that presents the abstraction of being able to append to
  the log and read from said log.
- A `Shard` is a physically isolated logical unit of the log.  A shard contains only a subset of the
  log's data and is the unit of parallelism in the log.  A log will have at least one shard; a log
  will never need more than 12 shards for throughput reasons.
- A `Fragment` or `Shard Fragment` is a single, immutable file that contains data for a shard.
- A `Manifest` is a file that contains the metadata for the log.  It contains the list of shard
  fragments that comprise the current state of the log.

The manifest ties the log together.  It transitively contains a complete reference to every file
that has been written to the log and not yet garbage collected.

### A Note about Setsums

wal3 uses a cryptographic hash to create a setsum of the data in the log.  This setsum is an
associative and commutative hash function that is used to verify the integrity of the log.  Because
of the way the hash function is constructed, it is possible to compute a new setsum from an existing
setsum and the setsum of a new fragment.  This allows us to get cryptographic-strength integrity
checking of the log.  We go into this at length in the verifiability section below.

### Manifest Structure

The Manifest is a JSON file that contains the following fields:

- setsum:  A setsum of the log data.  This is the setsum of everything in the log.  Every update to
  the log computes a new setsum and updates the manifest to reflect the checksum.
- next:  A pointer to the next manifest to be written.  This is used to create a form of consensus
  that's compatible with object storage.  The next manifest is written to the object store only if
  it doesn't already exist at the path specified by ``next''.
- prev:  A pointer to the previous manifest that was written, including its setsum.  This is
  allowed to be a dangling reference in the case of garbage collection.
- fragments:  A list of shard fragments.  Each shard fragment contains the following fields:
    - path:  The path to the shard fragment relative to the root of the log.  The full path is
      specified here so that any bugs or changes in the path layout don't invalidate past logs.
    - shard_id:  The ID of the shard.  This is a numerical identifier, coalesced to 1..=N by
      convention.
    - seq_no:  The sequence number of the fragment.  This is used to order the fragments within
      a shard.
    - start:  The lowest log position in the fragment.
    - limit:  The lowest log position after the fragment.
    - setsum:  The setsum of the log fragment.

Manifests, consequently, form a doubly-linked list.

Visually, it looks like this:

```text
   ┌──────────────────┐   ┌──────────────────┐
   │ MANIFEST.0       │   │ MANIFEST.i       │
   │            next ─│──▶│            next ─│──▶
◀──│─ prev            │◀──│─ prev            │
   └──────────────────┘   └──────────────────┘
```

The `next` field of the largest manifest is always a valid path _that doesn't yet exist_ and is
strictly greater than the current manifest.  This allows us to write the next manifest atomically.
The prefix of manifests form a form of consensus that is compatible with object storage.  The
conditional put feature of S3 allows us to write the next manifest only if it doesn't already exist,
meaning that of two log writers only one can ever extend the log and the other will get an explicit
error due to contention.

Invariants of the manifest:

- The setsum of all fragments in a manifest must add up to the setsum of the manifest.
- The delta between the setsum of prev and a manifest must exactly correspond to adding the data in
  the newly added fragments and removing the data in garbage collected fragments.  Thus, it is
  possible to take a manifest and its predecessor and compare the setsums to guarantee the manifest
  is a valid state transition.
- fragments.seq_no is sequential within a shard.
- fragment.start < fragment.limit for all fragments.
- fragment.start is strictly increasing.
- The range (shards.fragments.start, shards.fragments.limit) is disjoint for all fragments in a
  manifest.  No other shard will have overlap with log position.
- MANIFEST.0 always exists to anchor the log as existing.  It will have dangling prev and next
  pointers after garbage collection.  It is simply a marker that says that a manifest was once
  properly initialized.

### Shard Structure

Shards are logical arrangements of fragments.  The concept of a shard only exists in writers who are
configured with the number of shards to write; readers know about shards only insofar as the
manifest groups fragments by shard.  A typical shard with two fragments looks something like this:

```text
shard01
shard01/0000000000000001_1729102126112_1729102126306_679a0406f17e9791bc5b8ba3fdc10102bb9e1a1b1fa6b53cb4dcc50674f0e0c9.log
shard01/0000000000000002_1729102126716_1729102126931_c6a34ea8cdfb7b5cd03103eaf7bc2620130bc302f24cd0ed80d67d8355601de1.log
        └─shard_seq_no─┘ └─start─────┘ └─limit─────┘ └─setsum───────────────────────────────────────────────────────┘
```

Properties of this structure:

- Every shard's path names completely specify the information necessary to reconstruct a manifest
  over the shard.  Or across many shards.
- shard_seq_no is sequential within a directory.
- start < limit for all fragments.
- start is strictly increasing within a directory.
- limit of shard_seq_no i is < start of shard_seq_no i+1.

## Object Store Layout

wal3 is designed to maximize object store performance of object stores like S3 because it writes
logs in a way that scales.  Concretely, we leverage the behavior that S3 and similar services
institute rate limiting per prefix.  For example, given the following log files in an S3 bucket,
each shard and the manifest will have independent rate limits in steady state given enough traffic:

```text
shard0000/0000000000000001_1728663515079_1728663515128_d82e20fbbd232c82cbecbf82cff95fe8a79359b999b90699d38ab82a6e7bd5a3.log
shard0000/0000000000000002_1728663515130_1728663515186_fab91673f921cbf00c0f60a687ab2b7d0277ee6e28ae994cc98e96eb53dd32b3.log
shard0000/0000000000000003_1728663515186_1728663515189_2abb13c650f059aa5fd91c70a66c454118f5d7ca55f972353c819c95744debe5.log
shard0000/0000000000000004_1728663515392_1728663515550_24f06ff96ad4efb12ab06a2117a693f9a299edbad5faed1a145bcb819f222260.log
shard0000/0000000000000005_1728663515550_1728663515647_411d2ca88514ee8c041c9f94301d5e4e11be7841e689fcf3d126ad3352c200bc.log
shard0000/0000000000000007_1728663515748_1728663515749_907d0b286e6128d70cb413a7c426b5f5ecf3b2206d16a09371c391693580c74b.log
shard0000/0000000000000006_1728663515647_1728663515748_0b068270eef2daf302cc89fcd3e3547002e2bd6f5fbaf0e31a456eaad776404b.log
shard0000/0000000000000008_1728663515749_1728663515858_e3efc5726f2f9dbe51c1515cc4a66efbfdc7f8164a85a4815a48fc06969d5942.log
shardXXXX/...
manifest/MANIFEST.0
manifest/MANIFEST.1728663515390
```

This means approximately 3,500 writes per second for each shard and the manifest.  We can have as
many shards as we need, but analysis (in s3.napkin) shows that just 12 partitions is the most we'll
ever need for a single log given our size restrictions.

## Writer Arch Diagram

```text
┌─ Writer ────────────────────────────────────────────────────────────────────────┐
│  ┌─────────────────────────────┐  ┌────────────────────┐  ┌──────────────────┐  │
│  │ shard                       │  │ manifest           │  │ stream           │  │
│  │ manager                     │  │ manager            │  │ manager          │  │
│  │                             │  │                    │  │                  │  │
│  │ - new                       │  │ - new              │  │ - streams        │  │
│  │ - push_work                 │  │ - assign_timestamp │  │ - open_stream    │  │
│  │ - take_work                 │  │ - apply_delta      │  │ - close_stream   │  │
│  │ - wait_for_writable         │  │                    │  │ - stream_is_open │  │
│  │ - update_average_batch_size │  │                    │  │                  │  │
│  │ - finish_write              │  │                    │  │                  │  │
│  └─────────────────────────────┘  └────────────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

The write path is:

1.  The writer checks with the stream manager to see `stream_is_open`
2.  The writer calls `push_work` to submit work to the shard manager.  This enqueues the work.
3.  The writer calls `take_work` from the shard manager.  If there is a batch of sufficient size and
    a free shard, it will assign the work to that shard and return the work to be written.  Go to 4.
    If there is no batch, skip to step 3a.
    a.  Wait for some other task to signal that the work is ready.  Go to 6.
4.  Flush the work from take_work to object storage.  This will call assign-timestamp.
5.  The writer creates a delta to the manifest---the new fragment and its setsum---and calls
    `apply_delta` on the manifest manager.
6.  The write is durable.

## Garbage Collection

wal3 uses a multi-step garbage collection algorithm.  The garbage collector is a separate process that
runs optimistically in four phases:

1.  The garbage collector reads the manifest and produces a list of all files in the S3 prefix that
    exist, are older than the manifest, and are not in the manifest.  It writes this list to a file
    in object storage.
2.  A separate verifier process runs to check any desirable properties of the list of files to
    delete.  In particular, it will check to make sure there is no way for a reader abiding the
    protocol to get out of sync with the log if any of the files listed are deleted.  It also checks
    to see what % of the log is erased.  It outputs a list of files to erase, a list of files to
    keep, and a setsum for both.
3.  The manifest writer the list of files to delete and the setsum of the log prefix after garbage
    collection.  If everything balances, it omits the garbage-collected prefix from its next
    manifest and writes said manifest without those objects.
4.  The garbage collector, upon seeing a manifest that lists none of the files in its
    garbage-collection will then act to slowly remove the listed files before returning to step 1.

Nothing about the garbage collection algorithm necessitates being in or out of process relative to
the writer.

These first and fourth passes must be separated by an interval we call the garbage collection
interval.

## Timing Assumptions

wal3 is designed to be used in a distributed system where clocks are not synchronized.  Further, S3
and other object storage providers do not provide cross-object transactional guarantees.  This means
that our garbage collection needs to beware several timing issues.  To resolve these, we will set a
system parameter known as the garbage collection interval.  Every timing assumption should relate
some quantifiable measurement to this interval.  If we assume that these other measurements occur
sufficiently frequently and the garbage collection occurs significantly infrequently, we effectively
guarantee system safety.  Therefore:

- A writer must be sufficiently up-to-date that it has loaded a link in the manifest chain that is
  not yet garbage collected.  This is because a writer that believes it can write to MANIFEST.N must
  be sure that MANIFEST.N has never existed; if it existed and was garbage collected, the log
  breaks.  Verifiers will detect this case, but it's effectively a split brain and should be
  avoided.  To avoid this, writers must restart within the garbage collection interval.
- A reader writing a _new_ cursor, or a cursor that goes back in time must complete the operation in
  less than the garbage collection interval and then check for a concurrent garbage collection
  before it considers the operation complete.  If the reader somehow hangs between loading a log
  offset and writing the cursor for more than the garbage collection interval, the cursor will
  reference garbage collected data.  The reader will fail.

## Zero-Action Recovery

The structure of wal3 is such that it is possible to recover from a crash without any action.  Every
write to S3 leaves the log in a consistent state.  The only thing that can happen on crash is that
there is additional work for garbage collection---files that were written but not linked into the
manifest.  This is a simple matter of running the garbage collector.

## End-to-End Walkthrough of the Write Path and Garbage Collection

An end-to-end walkthrough of the write path is as follows:

0.  The writer is initialized with a set of options.  This includes the object store to write to,
    the number of shards to write, and any other configuration such as throttling.
1.  The writer reads the existing manifest.  If there is no manifest, it creates a new MANIFEST.0
2.  A client calls `writer.append` with a stream ID and a message.  The writer checks to see if the
    stream is open and then adds the work to the shard manager.
3.  If there is sufficient work available or sufficient time has passed and there is a shard that
    can be written to, the writer takes a batch of work from the shard manager and writes it to a
    single fragment.
4.  The writer then creates a delta to the manifest and applies it to the manifest manager using
    `apply_delta`.  Internally, the manifest manager allows deltas to be applied in their
    appropriate order.  It streams speculative writes to the manifest.
5.  The manifest manager batches deltas so that there is not a 1:1 of shard fragments to manifest
    deltas.  This is to reduce the number of writes to the manifest.
6.  When there is capacity to write the manifest, the manifest manager writes the manifest to the
    object store.  The write is durable and readable by all readers.

The garbage collection is a separate process that runs in the background:

0.  Read all cursors and the all manifests.
1.  From the cursors, select the minimum timestamp time across all cursors as the garbage collection
    cutoff.
2.  Write a list of shard fragments and manifests that hold data strictly less than the cutoff.
3.  Run a verifier that checks the list of shard fragments to delete.
4.  Write a new manifest prefix to /snapshot/MANIFEST.setsum that contains the setsum of the log
    prefix after garbage collection.
5.  Wait for the active writer to pick up the snapshot and reference it from a new manifest.
6.  Verify that the files listed in 3 are still safe to delete _and are no longer referenced_.
7.  Delete the files that were affirmatively verified.

The big idea is to use positive, affirmative signals to delete files.  There's a slight step of
synchronization between writer and garbage collector; an alternative design to consider would be to
have the garbage collector stomp on a manifest and let the writer pick up the pieces, but that
requires strictly more computer work to recover.

# Optimizations

wal3 is designed to be fast and efficient.  It is designed to scale to the limits of a single
machine and to have low variance in its latency profile.  This section details optimizations that
we could employ to make wal3 perform with higher throughput and/or lower latency.

## Concurrent Manifest Writes

wal3, as explained thus far, does not support concurrent writes to the manifest.  This is largely
because the manifest forms a chain of writes where each manifest writes a dangling pointer to the
next manifest to be written.  This is a form of consensus that is compatible with object storage.

However, we can support concurrent writes to the manifest by using a more complex protocol.  The
single writer to the log *knows* every write that it has in flight.  Why not simply do advanced
writes of the manifest?

The worst case is that we see something that looks like this on crash:

```text
   ┌──────────────────┐   ┌──────────────────┐                          ┌──────────────────┐
   │ MANIFEST.0       │   │ MANIFEST.i       │                          │ MANIFEST.k       │
   │            next ─│──▶│            next ─│──▶                    ──▶│            next ─│──▶
◀──│─ prev            │◀──│─ prev            │◀──                    ◀──│─ prev            │
   └──────────────────┘   └──────────────────┘                          └──────────────────┘
```

MANIFEST.k has been written, but is not connected to the chain of manifests going back to the most
recent garbage collection.  It is, therefore, garbage.

Note that this garbage is not something for other writers to deal with---only the garbage collector
will have to know about and deal with MANIFEST.k.

This impacts zero-action recovery:  On open, the log writer will have to check for garbage manifests
and prune all but the most recent one that is linked into the chain.  Without this optimization, it
is sufficient to simply accept the highest-numbered manifest.

## Bounding Manifest Recovery

Because concurrent manifest writes introduce a vulnerability to garbage manifests, we have to
recover a prefix of the log that's valid.  The naive way to do this would be to read all manifests
and find the longest chain of manifests starting with the oldest non-zero manifest.

This is inefficient and leads to unbounded work on recovery.

What we desire in this case is to bound the number of manifests we need to read to _know_ we've
recovered the latest manifest.  Here, let's take a lead from the Paxos protocol:  We will choose to
structure our protocol such that reading the 𝛼 most recent manifests is sufficient to know that we
have recovered a tail of the log.  If we have 𝛼=1, then we have the existing protocol.  If we have 𝛼=2,
then we can have at most one speculative manifest outstanding at a time.  If we recover two
manifests from the same writer at times i and j, we know that the newest of the two, j, was written
only after all the manifests prior to i were written.  Recursively, this argument holds for all
previously written manifests.  Consequently, it is sufficient to have everyone agree upon 𝛼 and
recover the 𝛼 latest manifests.

Or at least, that's the protocol for a single writer.

The actual recovery protocol is more complex:

0.  Everyone agrees upon 𝛼.
1.  No writer writes more than 𝛼 manifests concurrently, guaranteeing that the 𝛼 manifests
    outstanding link to a gap-free prefix of manifests no more than 𝛼-1 manifests ahead of what's
    durable and visible to readers.
2.  When writing a manifest, a writer embeds a globally unique identifier.
3.  When recovering the manifest, read the manifests in reverse order.  Aggregate manifests based
    upon the globally unique identifier of the writer.  If there are more than 𝛼 manifests from a
    single writer, take the longest prefix from that writer as canon.

This protocol guarantees that we can recover a prefix of the log that is guaranteed to have been
inductively constructed to always link back to the prefix of the log within 𝛼 manifests.  As stated
the protocol doesn't handle changing 𝛼.  To do that, we can do the following extension:  When
reading, track the max 𝛼 seen across all manifests.  Continue until all manifests are examined or a
sufficient suffix of max(𝛼) manifests has been discovered.  This is easy to see as safe if the above
protocol is safe.

## "Robust" Object Storage Abstraction

Object storage is generally backed by spinning disks, which incur variable latency.  This can lead
to long tails when making requests to S3.  To mitigate this, we can introduce a "robust" object
storage abstraction that wraps object storage and optimistically retries slow requests that can be
retried because they are idempotent.  Get and put-if-not-exist are both idempotent.  The slow
request's result can safely be thrown away.

This abstraction can be useful for reducing the tail latency of object store requests, but it must
be implemented carefully.  What we're essentially doing is the opposite of exponential backoff.  The
AWS SDK pages suggest that the libraries will eventually do this, so this section is more of a
stop-gap to reign in tail latency.

The guiding principles for robust object storage are:
- The number of proactive retry requests must be bounded.
- The number of outstanding requests is enforced at a higher level.

From this budget we can derive a simple algorithm:
- Allocate X retries per second.
- Allow one retry every 1/X seconds.
- Estimate some latency threshold derived from the last N requests.
- If a request exceeds the threshold and there hasn't been a retry within 1/X seconds, retry it.

# Non-Obvious Design Considerations

wal3 is designed to be a simple, linearizable log on top of object storage.  This section details
non-obvious consequences of its design.

## The Manifest Enables Parallelism

One alternative design that was discussed and passed over is to simply use a sequence number within
a shard as the sole ordering mechanism for fragments.  Under such a hypothetical, we would get a
diagram like this:

```text
┌──────────────────┐   ┌──────────────────┐
│ LOG.0            │   │ LOG.1            │
│                  │   │                  │
│                  │   │                  │
└──────────────────┘   └──────────────────┘
```

No pointers, just log files in sequential order.  This design is simpler than the log as it exists
today and also is more-obviously correct.  It is, however, not practical for our requirements.  For
this design to work, we would need to have a single speculative write outstanding at a time.  LOG.2
cannot be written speculatively unless LOG.1 is written.  This is because the setsum of LOG.2 is
dependent upon LOG.1.  If we were to write LOG.2 speculatively, we would have to write a new LOG.2
to match the exact LOG.1 that was written.

The manifest write serves to break this problem.  By writing each fragment to a separate, content
addressable file and stitching together the file from the manifest, we get the following wins:

- Within a shard, multiple fragments can be written in parallel, so long as they get applied to the
  manifest in batches consistent with their logical order.
- Across shards, multiple shards can be written in parallel, so long as they get applied to the
  manifest in batches consistent with their logical order.

In short, the manifest enables parallelism in the log.  The exact situation in which the log will
fail without a manifest looks like this:

```text
Client            Log 1         Log 2                           Storage
   │                │             │                                │
   │ 1. write(k1)   │             │                                │
   │───────────────>│             │                                │
   │ 2. write(k2)   │             │                                │
   │───────────────>│             │                                │
   │ 3. write(k3)   │             │                                │
   │────────────────│────────────>│                                │
   │ 4. write(k4)   │             │                                │
   │────────────────│────────────>│                                │
   │                │             │                                │
   │                │             │                                │
   │                │ 5. write(L1)│ with [k1]                      │
   │                │─────────────│──────────X NETWORK GLITCH      │
   │                │ 6. write(L2)│ with [k2]                      │
   │                │─────────────│───────────────────────────────>│
   │                │             │                                │
   │                │             │ 7. write(L1) with [k3]         │
   │                │             │───────────────────────────────>│
   │                │             │ 8. write(L2) with [k4]         │
   │                │             │───────────────X CAS FAIL       │
```

The log is now inconsistent and requires manual intervention to recover.  This scenario is one of
many that illustrate the difficulties of distributed consensus.

The manifest is a form of consensus.  Effectively we pipeline writes to shards optimistically and
then reveals them to readers via the manifest.  In the above protocol a reader would be responsible
for detecting the case and aborting the read with a consistency error.  In a manifest-driven
protocol, the manifest is the source of truth for what's in the log.  Anything not in the manifest
was written speculatively and can be erased.  Moreover, the manifest can be self-contained, enabling
cheap snapshots of the log.

## Why Sharding

It's not immediately obvious why we need sharding of the log.  Amazon specs out S3 as being able to
support 3,500 reads per second-prefix of S3.  This means that we can write approximately 3,500 3.6MB
files per second from a single machine.  This is a lot of throughput, so why do we need sharding?

Read-oriented throughput.

By splaying the log across many shards, we can read from many shards in parallel.  This is important
because S3 has a 5,500 read per second limit per prefix.  By sharding the log, we can read from many
shards in parallel, getting linear scaling in the number of shards.

## Manifest Compaction

The manifest is a chain of writes, each of which adds a new file to the previous write.  Look at
this another way and the number of bytes written to object storage for the manifest is quadratic in
the number of writes to the manifest.  This is a problem because each manifest write is
incrementally more expensive than the previous write.

To compensate, the manifest writer needs to periodically write a snapshot of the manifest that
contains a prefix of the manifest that it won't rewrite.  This is a form of fragmentation.

One way to handle this would be to write a snapshot every N writes and embed the snapshots.

```text
   ┌──────────────────┐   ┌──────────────────┐
   │ MANIFEST.0       │   │ MANIFEST.i       │
   │            next ─│──▶│            next ─│──▶
◀──│─ prev            │◀──│─ prev    snap    │
   └──────────────────┘   └────────────│─────┘
                                       ↓
                             ┌──────────────────┐
                             │ SNAPSHOT.x       │
                             └──────────────────┘
```

This requires writing a new snapshot everytime a new manifest that exceeds the size is written.
This would be the straight-forward way to handle this, except that it requires writing SNAPSHOT.x
before writing MANIFEST.i and that's a problem.  The manifest writer is a hot path and we don't want
to introduce an extra round trip.

Instead, we can leverage the fact that a manifest is just a list of shard fragments, a prev and next
pointer, and a setsum.  The setsum of the manifest minus the setsum of its previous manifest (both
of which are embedded within a manifest) tells the setsum of everything that is new to the manifest.
Similarly, the set of files contained within the manifest must be the same as what will roll up into
the snapshot.  This means that we can simply treat the prior manifest as a snapshot and disregard
the prev/next/snapshot pointers within the snapshot when parsing.  A manifest and a snapshot are
both valid lists of shard fragments with accompanying setsums; the fact that one contains pointers
is immaterial to this fact.

To leverage this fact, we will not write explicit snapshots, but instead store pointers to manifests
as snapshots.  When rolling a snapshot follow the following algorithm:

- Construct a new empty manifest that points to the current manifest.  We will install this as a new
  manifest.
- Copy all snapshots from the latest manifest into the new manifest.
- Refer to the latest manifest as the newest snapshot.
- Install the new manifest.  What you get looks something like this:

```text
┐   ┌──────────────────┐   ┌──────────────────┐
│   │ MANIFEST.i       │   │ MANIFEST.j       │
│──▶│            next ─│──▶│            next ─│──▶
│◀──│─ prev    snap    │◀──│─ prev  snap[x, i]│
┘   └────────────│─────┘   └─────────────│──│─┘
                 │   ↑                   │  │
                 │   └───────────────────│──┘
                 ↓                       │
       ┌──────────────────┐              │
       │ SNAPSHOT.x       │◀─────────────┘
       └──────────────────┘
```

A rats nest of pointers, but it enables writing new manifests that are constant in size without
having to write rollup snapshots of the manifest.

Effectively the manifests form a tree with one internal node and many leaves.  Each manifest is a
file of the form "manifest/MANIFEST.1730146801207827" with references of the form
"shard03/000000000000025a_1730146851138_1730146851140_544c43c0ba56f62b024644e1567ce7ce0fa5fc752929ac94001f4b81ba2f0418.log"
contained within.  Each shard fragment is currently 256 bytes, so a 1MB manifest will include at
most 4096 references to shard fragments.  The snapshot references are another 66 bytes each, so a
fully-loaded manifest will have up to ~15k snapshot references, each of which has 4096 references.
This imposes a limit of ~65M shard fragments referenced by a manifest.  A 2MB manifest will hold
~260M shard fragments, and a 4MB manifest will hold ~1B shard fragments.  8MB gets us ~4B fragments.

## Snapshotting of the Log

There is no file stored in S3 that is every mutated or overwritten in a correctly-functioning wal3
instance.

Ever.

We can make use of this structural sharing to allow cheap snapshots of the entire log that simply
incur garbage collection costs.  These snapshots can be used to enable applications to do long-lived
reads of a subset of the log without having to race with garbage collection, and without having to
stall garbage collection for everyone.  The subset to be scanned gets pinned temporarily and
addressed at the first garbage collection after the snapshot is removed.

## Zero Action Resharding

The log as designed allows for resharding by simply closing the log and reopening it.  Because the
log is assumed to be in-process and on a single machine, this is cheap and can happen without
network I/O.  This is always safe:

- The writer always writes shard fragments in a total order in the manifest.  They may be written in
  parallel, but they appear in the manifest in sequence (possibly batches at a time).
- The writer never writes something to S3 that would need to be reverted on recovery.  It may add
  garbage manifests or shard fragments, but these will always be safely garbage collected.
- Properly closing the log drains it of requests, ensuring no garbage will be generated in steady
  state.

This means that the reader is, effectively, reading from a non-sharded log stream.  The reader is
agnostic to the sharding of the log.

When a writer reshards upwards, they simply write to more shards, but refer to those shards by path
name in the manifest.  When a writer reshards downwards, they simply write to fewer shards, and
likewise refer to those shards by name in the manifest.

## Reduced Tail Latency

The implementation aggressively leverages opportunities the design affords to reduce tail latency.

For example, a snapshot of the implementation that ran a test for one minute at 10k appends per
second with a synthetic put latency of 100ms saw the following distribution of end-to-end latencies:

```text
wal3.benchmark.append_histogram_bucket{le="200.0000"} 0 1730234293090
wal3.benchmark.append_histogram_bucket{le="210.0000"} 2130 1730234293090
wal3.benchmark.append_histogram_bucket{le="220.0000"} 24885 1730234293090
wal3.benchmark.append_histogram_bucket{le="230.0000"} 73816 1730234293090
wal3.benchmark.append_histogram_bucket{le="240.0000"} 132193 1730234293090
wal3.benchmark.append_histogram_bucket{le="250.0000"} 190583 1730234293090
wal3.benchmark.append_histogram_bucket{le="260.0000"} 248792 1730234293090
wal3.benchmark.append_histogram_bucket{le="270.0000"} 306965 1730234293090
wal3.benchmark.append_histogram_bucket{le="280.0000"} 365649 1730234293090
wal3.benchmark.append_histogram_bucket{le="290.0000"} 423338 1730234293090
wal3.benchmark.append_histogram_bucket{le="300.0000"} 481530 1730234293090
wal3.benchmark.append_histogram_bucket{le="310.0000"} 539276 1730234293090
wal3.benchmark.append_histogram_bucket{le="320.0000"} 581600 1730234293090
wal3.benchmark.append_histogram_bucket{le="330.0000"} 598774 1730234293090
wal3.benchmark.append_histogram_bucket{le="340.0000"} 600072 1730234293090
wal3.benchmark.append_histogram_bucket{le="350.0000"} 600285 1730234293090
wal3.benchmark.append_histogram_bucket{le="360.0000"} 600361 1730234293090
```

This is a MAX of 360 ms, a p99 of 330ms, and a p50 of 270ms.

Let's break down how we get to these numbers.  The write path:
- Batches the data. (batch interval)
- Writes a shard fragment.  (1RTT)
- Batches the manifest deltas. (batch interval)
- Writes a manifest delta.  (1RTT)
- Waits for all prior manifests to flush. (empirical)

The batch interval is the time it takes to batch up a set of writes to a shard.  This is a
configurable value that is set to 20ms by default, and takes effect for both batching appends into
shard fragments and batching manifest deltas into manifests.  We expect that in the worst case we
will see 2RTT + 2*batch_interval + empirical.  The empirical value is unknown at time of this
writing.

# Failure Scenarios

wal3 is designed to be resilient to failure.  This section details the failure scenarios that wal3
might encounter and how to recover from them.

The only failure scenarios to consider that are unique to wal3 are a faulty writer and a faulty
garbage collector.  No other process writes to object storage, so no other process can be faulty and
cause an invalid state for readers; they only impact their own behavior.

Our model is that processes can crash and restart at any time.  A crashed process will have no way
of recovering anything except what it has previously written to object storage.

While bugs will happen, a faulty writer or garbage collector is assumed to not be maliciously,
arbitrarily faulty.  We hand-wave this situation to state that these bugs will be detectable by
non-faulty software when they influence the setsum or invariants of the log.  And if no invariants
are violated, is it a bug?

## Faulty Writer

A writer that fails will fail at any step in the process of writing to object storage.  The write
protocol is such that until a manifest is written to refer to the new fragment, the fragment is not
considered durable.  This means that a writer can crash at any time and restart, and the log will
have garbage, but not refer to the garbage.

The more malicious faulty writer scenario would be a writer writing manifests that drop fragments or
refer to something that was erroneously garbage collected.  This is a very hard problem to solve in
the general case.  In the specific case of wal3, we assume that the checksums over the log are
sufficient to detect most corruption.

## Faulty Garbage Collector

The garbage collector is a separate process that runs in the background.  It is assumed to be move
slowly and carefully.  The garbage collector can fail in two ways:

- Fail to erase data it should.  This is not a problem as it doesn't affect data durability.  Such
  bugs will be prioritized, but they are not critical.
- Erase data it shouldn't.  This is a fundamental problem to be addressed.

The garbage collector can erase data it shouldn't if it erases data that is still referenced by the
manifest that the garbage collector is collecting.

Because there's not much to be done except be careful writing this code, the garbage collector is a
three-phase process.  The first phase lists all files in object storage that are present under the
log prefix, but that are not present in the compacted manifest.  The naive way to do this would be
to list all files in the manifest in a hash map and then list all files in the log prefix and write
files not in the hash map.  We will not be clever about this.  We will simply consider every the
oldest N files (N so that there's not an unbounded number) in the bucket and write them to a file if
they are eligible for garbage collection because:

1.  The file is older than the garbage collection window.
2.  The file is not referenced transitively by any cursor.

A second pass, called a verifier, reads the output of the first pass and complains loudly if sanity
checks don't pass.  For example, if garbage collection would collect more than X% of the log it may
make sense to raise an alarm rather than go ahead deleting more than X% of the data.  Similarly, the
verifier checks that the setsums of the new log balance.

## Faulty Object Storage

The last consideration for failure is faulty object storage itself.

There's not much that can be done here except detection.

wal3 uses a cryptographic hash to verify the integrity of the log.  This hash will detect both
missing fragments and corrupted fragments.  If the hash fails, the log is corrupted and must be
recovered.  This will be a human endeavor.

## Dropped Async Tasks

In Rust, web servers and the like will drop tasks associated with dropped file handles.  If that
task were one that was driving the log foward, such an abort would cause the log to hang.  This is
unacceptable, so every file write that can block other writes if it's cancelled is carefully
scheduled on a background, uncancellable task.

# Verification

wal3 is built to be empirically verifiable.  In this section we walk through the wal3 verification
story and how to verify that a log like wal3 is correct in steady state operation.

The verification story is simple:  A log has a cryptographic checksum that can be incrementally
adjusted so that every manifest is checksummed end-to-end with a checksum nearly as strong as sha3.

Each time a new fragment is written to the log, the shard fragment gets checksummed.  This checksum
gets added to the checksum in the manifest.  Each time a shard fragment is garbage collected, the
checksum of the fragment gets removed from the manifest.

The checksum itself has the following properties:
- It is cryptographic.  While close in properties to sha3, the deviation has not been proven to not
  undermine security.
- It is incremental.  Set addition, set subtraction, set union, and set difference are all O(1)
  operations, regardless of the size of the sets of data.
- It is commutative.  The order in which fragments are added to the set does not matter.
- [setsum is its own crate](https://crates.io/crates/setsum).

This construction gives a very strong property:  In steady state it is easy to detect durability
events due to their most likely cause:  New software.  By working 100% of the time, the checksum
gives wal3 operators the ability to scrub the entire log and know that if the setsum holds, the data
is as it was written.  This gives us the ability to know the integrity of the log holds at all
times.

This is not the end of the verification story, however, as it only ensures that data at rest is not
subject to a durability event.  Data movement is how things become non-durable.  To verify that the
log is not dropping writes before they make it under the setsum, we need end-to-end verification.

End-to-end verification is simple:  Write a message to the log and then read it back.  Failure to
read the same message from the log means that something went wrong.  Reading the same message twice
means something went wrong, too.  In short, anything other than a 1:1 mapping of writes to reads
will indicate a problem.

To do this, we will construct an end-to-end, variable throughput test that we can run against wal3
to ensure that data written is readable exactly as written.

## End-to-End Verification Stress Test

wal3 supports multiple logical streams multiplexed onto a single log.  We can leverage this to
stress test the log, making some streams extremely popular and other streams extremely unpopular.
This will allow us to test the log under a variety of conditions, and more importantly allow us to
tickle failure cases through load testing.  To make this work, and to be able to verify the
integrity of the log, we will use a random number generator for each stream.  Replaying from the
same exact seed should give the same exact results, allowing a writer to write to the stream, and a
reader to independently verify the stream.

A candidate writer does the following:
- Randomly select a stream according to some skewed distribution.
- Advance the random number generator.
- Repeat until the test is over.

A candidate reader does the following:
- Read the stream from the beginning.
- For each message, discern the stream in which the message was written.
- Verify that the message is the same as the message to come from that stream's random number
  generator.

A candidate garbage collector does the following:
- Normal garbage collection process
- Before revealing the garbage to the writer, write a new map of initial states for the stream.
  - This sounds difficult, but it's taking the initial map and applying the reader algorithm to
    advance the random number generators.
  - The reader algorithm takes an initial map based upon the setsum of the garbage-collected prefix
    of the log, so when garbage collection kicks in, a reader will automatically pick the new map.

This test should be runnable in a continuous fashion for testing and debugging purposes.

# Potential Future Work and Interesting Projects

## Bounding Stream Seek

wal3 allows multiple streams to be multiplexed in the same log without any shard affinity.  This
means that a reader of a single stream---the default in the API---may have a need to seek past data
that's not in that stream.  This is a fundamental assumption of wal3:  To remove this assumption
degrades into having multiple logs and doing a handoff protocol between them---something that is
possible regardless of this assumption.

While not done today, the design of wal3 allows for publishing a bloom filter of contained streams
for each log fragment.  This would allow a reader to fetch many small metadatas rather than having
to iterate the log.
