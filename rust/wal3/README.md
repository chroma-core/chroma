wal3
====

wal3 is the write-ahead (lightweight) logging library.  It implements a linearlizable log that is
built entirely on top of object storage.  It relies upon the atomicity of object storage to provide
the If-Match header.  This allows us to create a log entirely on top of object storage without any
other sources of locking or coordination.

# Design

wal3 is designed to work on object storage.  It is intended to to be lightweight, to allow a single
machine to multiplex many logs simultaneously over a variety of paths.

## Interface

wal3 presents separate reader and writer interfaces in order to allow readers and writers to scale
separately.  Readers can read the log without blocking writers and writers can append to the log
without blocking readers.

```text
pub struct LogPosition {
    // Offset is a sequence number indicating the total number of records inserted into the log.
    pub offset: u64,
    // Timestampl
    pub timestamp; u64,
}

pub struct LogWriter { ... }

impl LogWriter {
    pub async fn open(options: LogWriterOptions) -> Result<Arc<Self>, Error>;
    pub async fn append(self: &Arc<Self>, message: Message) -> Result<LogPosition, Error>;
}

// Limits allows encoding things like offset, timestamp, and byte size limits for the read.
pub struct Limits { ... }

pub struct LogReader { ...  }

impl LogReader {
    pub async fn open(options: LogReaderOptions) -> Result<Self, Error>;
    pub async fn read(
        self: &Self,
        from: LogPosition,
        limits: Limits,
    ) -> Result<(LogPosition, Vec<Message>), Error>;
}
```

The astute reader will note that this log is in process.  It is meant to be run under leader
election, with all writes routed to the log, just as one would do running a server.  The leader
election need only be best effort---if two writers write to the log at the same time, at most one
will succeed.

## Data Structures

wal3 is built around the following data structures:

- A log is the unit of data isolation in wal3 and the unit of API instantiation.
- A `Fragment` is a single, immutable file that contains a subsequence of data for a log.
- A `Manifest` is a file that contains the metadata for the log.  It contains the list of fragments
  that comprise the current state of the log.

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
- pruned:  A setsum of the log data that has been pruned and thrown away.  The fragments of
  the log plus the `pruned` value must equal `setsum`
- fragments:  A list of fragments.  Each fragment contains the following fields:
    - path:  The path to the fragment relative to the root of the log.  The full path is specified
      here so that any bugs or changes in the path layout don't invalidate past logs.
    - fragment_seq_no:  The sequence number of the fragment.  This is used to order the fragments
      within a log.
    - start:  The lowest log position in the fragment.  Note that this embeds time and space.
    - limit:  The lowest log position after the fragment.  Note that this embeds time and space.
    - setsum:  The setsum of the log fragment.

Invariants of the manifest:

- The setsum of all fragments in a manifest plus `pruned` must add up to the `setsum` of the
  manifest.
- fragments.seq_no is sequential.
- fragment.start < fragment.limit for all fragments.
- fragment.start is strictly increasing.
- The range (fragment.start, fragment.limit) is disjoint for all fragments in a manifest.  No other
  fragment will have overlap with log position.

## Object Store Layout

wal3 is designed to maximize object store performance of object stores like S3 because it writes
logs in a way that scales.  Concretely, we leverage the behavior that S3 and similar services
institute rate limiting per prefix.  For example, given the following log files in an S3 bucket,
we will group fragments in groups of 5000 and the manifest will be in a separate prefix.

The following shows numbers every 5000.  I'd zero-pad to 16 hex digits for the sequence number and
bucket fragments in groups of 4096 so the bits align and look pretty in the seqno prefix.

```text
wal3/SeqNo=    0/0000000000000000.parquet
wal3/SeqNo=    0/0000000000000001.parquet
wal3/SeqNo=    0/0000000000000002.parquet
...
wal3/SeqNo=    0/0000000000004999.parquet
wal3/SeqNo= 5000/0000000000005000.parquet
...
wal3/SeqNo=10000/0000000000010000.parquet
...
wal3/SeqNo=15000/0000000000015000.parquet
...
wal3/Manifest/MANIFEST.json
```

## Writer Arch Diagram

```text
┌─ Writer ──────────────────────────────────────────────────┐
│  ┌─────────────────────────────┐  ┌────────────────────┐  │
│  │ fragment                    │  │ manifest           │  │
│  │ manager                     │  │ manager            │  │
│  │                             │  │                    │  │
│  │ - new                       │  │ - new              │  │
│  │ - push_work                 │  │ - assign_timestamp │  │
│  │ - take_work                 │  │ - apply_delta      │  │
│  │ - wait_for_writable         │  │                    │  │
│  │ - update_average_batch_size │  │                    │  │
│  │ - finish_write              │  │                    │  │
│  └─────────────────────────────┘  └────────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

The write path is:

2.  The writer calls `push_work` to submit work to the fragment manager.  This enqueues the work.
3.  The writer calls `take_work` from the fragment manager.  If there is a batch of sufficient size
    and a free fragment, it will assign the work to that fragment and return the work to be written.
    Go to 4.  If there is no batch, skip to step 3a.
    a.  Enqueue and wait for some other task to signal that the work is ready.  Go to 6.
4.  Flush the work from take_work to object storage.  This will call assign-timestamp on the
    manifest manager.
5.  The writer creates a delta to the manifest---the new fragment and its setsum---and calls
    `apply_delta` on the manifest manager.
6.  The write is durable.

## Timing Assumptions

wal3 is designed to be used in a distributed system where clocks are not synchronized.  Further, S3
and other object storage providers do not provide cross-object transactional guarantees.  This means
that our garbage collection needs to beware several timing issues.  To resolve these, we will set a
system parameter known as the garbage collection interval.  Every timing assumption should relate
some quantifiable measurement to this interval.  If we assume that these other measurements occur
sufficiently frequently and the garbage collection occurs significantly infrequently, we effectively
guarantee system safety.  Therefore:

- A writer must be sufficiently up-to-date that it has loaded a link in the manifest chain that is
  not yet garbage collected.  This is because a writer that believes it can write to fragment SeqNo=N
  must be sure that fragment SeqNo=N has never existed; if it existed and was garbage collected, the
  log breaks.  Verifiers will detect this case, but it's effectively a split brain and should be
  avoided.  To avoid this, writers must complete all operations within the garbage collection
  interval.
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
    and any other configuration such as throttling.
1.  The writer reads the existing manifest.  If there is no manifest, it creates a new initial
    manifest and writes it to the object store.
2.  A client calls `writer.append` with a message.  The writer adds work to the fragment manager.
3.  If there is sufficient work available or sufficient time has passed and there is a fragment that
    can be written to, the writer takes a batch of work from the fragment manager and writes it to a
    single fragment.
4.  The writer then creates a delta to the manifest and applies it to the manifest manager using
    `apply_delta`.  Internally, the manifest manager allows deltas to be applied in their
    appropriate order.  It streams speculative writes to the manifest.
5.  When there is capacity to write the manifest, the manifest manager writes the manifest to the
    object store.  The write is durable and readable by all readers.

Garbage collection is a separate process that runs in the background:

0.  Read all cursors and the all manifests.
1.  From the cursors, select the minimum timestamp time across all cursors as the garbage collection
    cutoff.
2.  Write a list of fragments that hold data strictly less than the cutoff.
3.  Run a verifier that checks the list of fragments to delete.
4.  Write a new manifest to the log, using the normal write protocol.  This will fail any other
    writer, but they will retry and succeed.
5.  Verify that the files listed in 3 are still safe to delete _and are no longer referenced_.
6.  Delete the files that were affirmatively verified.

The big idea is to use positive, affirmative signals to delete files.  There's a slight step of
synchronization between writer and garbage collector; an alternative design to consider would be to
have the garbage collector stomp on a manifest and let the writer pick up the pieces, but that
requires strictly more computer work to recover.

# Optimizations

wal3 is designed to be fast and efficient.  It is designed to scale to the limits of a single
machine and to have low variance in its latency profile.  This section details optimizations that
we could employ to make wal3 perform with higher throughput and/or lower latency.

# Non-Obvious Design Considerations

wal3 is designed to be a simple, linearizable log on top of object storage.  This section details
non-obvious consequences of its design.

## Manifest Compaction

The manifest is a chain of writes, each of which adds a new file to the previous write.  Look at
this another way and the number of bytes written to object storage for the manifest is quadratic in
the number of writes to the manifest.  This is a problem because each manifest write is
incrementally more expensive than the previous write.

To compensate, the manifest writer periodically writes a snapshot of the manifest that contains a
prefix of the manifest that it won't rewrite.  This is a form of fragmentation.

The direct way to handle this would be to write a snapshot every N writes and embed the snapshots.

```text
┌──────────────────┐
│ MANIFEST         │
│                  │
│          snap    │
└────────────│─────┘
             ↓
   ┌──────────────────┐
   │ SNAPSHOT.x       │
   └──────────────────┘
```

This requires writing a new snapshot everytime a new manifest that exceeds the size is written.
This would be the straight-forward way to handle this, except that it requires writing SNAPSHOT.x
before writing MANIFEST and a naive implementation would introduce latency.  The manifest writer
is a hot path and we don't want to introduce an extra round trip.

Instead, we are able to leverage the fact that a manifest's prefix is immutable and under control of
the writer.  The writer can write a snapshot of the manifest at any time, and then use it in the
first manifest that it starts writing after the snapshot completes.  The question then becomes what
the structure of the manifest/snapshot/fragment pointer-rich data structure looks like.

Back-of-the-envelope calculations show that a single manifest is not sufficiently large to hold a
whole log efficiently.  The same calculations show that a tree of manifests composing a single root
node with a single level of interior nodes and a single level of leaves is sufficient to capture any
log that we currently design for from a stationary perspective.

Keeping a perfectly balanced tree is hard, however.  And since the root of the multi-rooted tree is
a manifest, we rewrite the indirect pointers to the tree each time that we write a new manifest.
The bulk of this manifest is the indirect pointers to the interior nodes of the tree.

We can do better, however, by recognizing that the tree is skewed in its access pattern.  Readers
that read the whole tree will not be bothered by having to walk a tree of manifests, but readers
that are looking to do a query of the tail of the log should be able to do so without having to walk
multiple manifests.

To this end, we introduce a second level of indirection in the manifest so that we will have a root,
two levels of interior nodes, and a level of leaves.  The root will point to the interior nodes, the
first level of interior nodes point to the second level, and that level points to the leaves.

This is, strictly speaking, an optimization, but one that will allow us to scale the log to beyond
all forseeable current requirements.  20-25 pointers in the root, or 2kB are all that's needed to
capture a log that's more than a petabyte in size.  Compare that to 5k pointers or 329kB for a
single manifest.  We're dealing with kilobytes per manifest for a log that's petabytes, but when
each manifest targets < 1MB in size, the difference at write time is apparent in the latency.

Consequently, the manifest and its transitive references will be a four-level tree.

```text
root
│
├── snapshot
│   ├── snapshot
│   │   ├── fragment_1
│   │   ├── fragment_2
│   │   └── fragment_3
│   └── snapshot
│       ├── fragment_4
│       ├── fragment_5
│       └── fragment_6
├── fragment_7
├── fragment_8
└── fragment_9
```

### Interplay Between Garbage Collection and Snapshots

The manifest compaction strategy is designed to reduce the cost of writing the manifest, but it
incurs a cost for garbage collection.  To garbage collect an arbitary prefix of the log
fragment-by-fragment would require rewriting the snapshots that partially cover the prefix and
contain data that is not to be garbage collected.  This is complex.

To side-step this problem we will introduce intentional fragmentation of the manifest and snapshots
to align to the garbage collection interval.  This will guarantee that at most one interval worth of
garbage that could be compacted is left uncompacted.

### Interplay Between Snapshots and Setsum

The setsum protects the snapshot mechanism.  Each pointer to a snapshot embeds within the pointer
itself a reference to the setsum of the pointed-to snapshot.  The following example shows how to
balance setsums.

```text
┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐
│ A ││ B ││ C ││ D ││ E ││ F ││ G ││ H ││ I ││ J │
└───┘└───┘└───┘└───┘└───┘└───┘└───┘└───┘└───┘└───┘
└───────────────── setsum(A - J) ────────────────┘

└── setsum(A - D) ─┘
                    └─────── setsum(E - J) ──────┘

setsum(A - J) = setsum(A - D) + setsum(E - J)
```

To compact the manifest's pointers A-D, wal3 would write a new snapshot under `setsum(A-D)`.  Once
that snapshot is written, the manifest next manifest to write replaces the fragments A, B, C, D with
a single snapshot.A-D.  The setsum of the new manifest is setsum(A-D) + setsum(E-J), which conserves
the setsum(A-J), providing some measure of proof that integrity is assured and no data is lost from
the log when compacting.

## Snapshotting of the Log

There is no data file stored in S3 that is ever mutated or overwritten in a correctly-functioning wal3
instance.

We can make use of this structural sharing to allow cheap snapshots of the entire log that simply
incur garbage collection costs.  These snapshots can be used to enable applications to do long-lived
reads of a subset of the log without having to race with garbage collection, and without having to
stall garbage collection for everyone.  The subset to be scanned gets pinned temporarily and
addressed at the first garbage collection after the snapshot is removed.

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
considered durable.  In the event a fragment gets "orphaned" because the manifest fails, it will be
rewritten by the next valid writer.  This means that a writer can crash at any time and restart, and
the log will have garbage, but not refer to the garbage.

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

Each time a new fragment is written to the log, the fragment gets checksummed.  This checksum gets
added to the checksum in the manifest.  Each time a fragment is garbage collected, the checksum of
the fragment gets removed from the manifest.

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
