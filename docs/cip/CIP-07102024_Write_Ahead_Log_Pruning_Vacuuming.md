# CIP-07102024: Write-Ahead Log Pruning & Vacuuming

## Status

Current Status: `Under Discussion`

## Motivation

Chroma's SQLite-based write-ahead log grows infinitely over time. When ingesting large amounts of data, it's not uncommon for the SQLite database to grow to many gigabytes in size. Large databases cost more, take longer to back up, and can result in decreased query performance.

There are two separate problems:

- The database, specifically the `embeddings_queue` table, has unbounded growth.
- The SQLite `VACUUM` command, often recommended for such scenarios, takes an exclusive lock on the database and is potentially quite slow.

This CIP addresses both issues.

## Proposed Changes

After every write transaction, if `log:prune` is enabled, the `embeddings_queue` table will be pruned to remove rows that are no longer needed. Specifically, rows with a sequence ID less than the minimum sequence ID of any active subscriber will be deleted. (As long as this is done continuously, this is a relatively cheap operation.)

This does not directly reduce the disk size of the database, but allows SQLite to reuse the space occupied by the deleted rows—thus effectively bounding the disk usage of the `embeddings_queue` table by `hnsw:sync_threshold`.

## Public Interfaces

### New collection configuration parameters

**`log:prune`**:

- Default: `true`
- Usage: this exists mainly to ease migration. The only reason to set this to `false` is if your application is extremely latency-sensitive.

### New CLI command

```bash
chroma vacuum --path ./chroma_data
```

This automatically runs the pruning operation described above before running `VACUUM`. Prior to any modifications, it checks that there is enough available disk space to complete the vacuum (i.e. the free space on the disk is at least twice the size of the database).[^1]

`chroma vacuum` should be run infrequently; it may increase query performance but the degree to which it does so is currently unknown.

We should clearly document that `chroma vacuum` is not intended to be run while the Chroma server is running, maybe in the form of a confirmation prompt.

## Compatibility, Deprecation, and Migration Plan

The new `log:prune` parameter defaults to `false` on existing collections, because:

- The first pruning operation for an existing collection can be very slow.
- Some users may be relying on the WAL as a full backup.

This means existing installations will not benefit from auto-pruning until they run `chroma vacuum`. During the vacuum, `log:prune` will automatically be set to `true` on all collections.

Users should see disk space freed immediately after upgrading and running `chroma vacuum` for the first time. Subsequent runs of `chroma vacuum` will likely free up no or very little disk space as the database will be continuously auto-pruned from that point forward.

## Test Plan

Auto-pruning should be thoroughly tested with property-based testing. We should test `chroma vacuum` with concurrent write operations to confirm it behaves as expected and emits the appropriate error messages.

## Rejected Alternatives

**Only prune when running `chroma vacuum`**: instead of continuously pruning the `embeddings_queue` table, only prune it when running `chroma vacuum` or some other manual command. This alternative was rejected because Chroma should be able to automatically keep its database size in check without manual intervention.

## Appendix

### Incremental vacuum experiment

(This is kept for posterity, but is no longer relevant to the current proposal.)

Some tests were run to determine the impact of `PRAGMA incremental_vacuum` on read and write queries.

Observations:

- Parallel read queries during `PRAGMA incremental_vacuum` are not blocked.
- One or more (depending on number of threads) parallel read queries will see a large latency spike, which in most cases seems to be at least the duration of the vacuum operation.
- `PRAGMA incremental_vacuum` and write queries cannot be run in parallel (this is true in general for any query that writes data when in journaling mode).
- As a corollary to the above: if another process/thread writes and defers its commit, it can easily block the vacuum and cause it to time out.
- On a 2023 MacBook Pro M3 Pro, running `PRAGMA incremental_vacuum` on a database with ~1GB worth of free pages took around 900-1000ms.

<details>
<summary>Source code</summary>

Run this script to create `test.sqlite`, adjusting `TARGET_SIZE_BYTES` if desired:

```python
import sqlite3
import string
import random

TARGET_SIZE_BYTES = 1000000000
TEXT_COLUMN_SIZE = 32

def random_string(len):
  return ''.join(random.choices(string.ascii_uppercase + string.digits, k=len))

conn = sqlite3.connect("test.sqlite")
conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

batch_size = 10000
insert_query = "INSERT INTO test (name) VALUES (?)"
data = [(random_string(TEXT_COLUMN_SIZE),) for _ in range(batch_size)]

num_rows = TARGET_SIZE_BYTES // (TEXT_COLUMN_SIZE + 4) # int is variable width, assume average 4 bytes

for _ in range(num_rows // batch_size):
    conn.executemany(insert_query, data)
    conn.commit()

conn.close()
```

Then, run this script to test vacuuming:

```python
import multiprocessing
from multiprocessing.synchronize import Event
import sqlite3
import time
import random
import string

def random_string(len):
  return ''.join(random.choices(string.ascii_uppercase + string.digits, k=len))

def print_results(timings, vacuum_start, vacuum_end):
  if len(timings) == 0:
    return

  durations = [end - start for (start, end) in timings]

  durations.sort()
  p95 = durations[int(len(durations) * 0.95)]
  print(f"Ran {len(durations)} concurrent queries")
  print(f"Query duration 95th percentile: {p95 * 1000}ms")
  print(f"Query duration max: {durations[-1] * 1000}ms")

  num_queries_during_vacuum = sum(1 for (start, end) in timings if start >= vacuum_start and end <= vacuum_end)
  print(f"Number of queries during vacuum: {num_queries_during_vacuum}")

def query_read(ready_event: Event, shutdown_event: Event, timings_tx):
  conn = sqlite3.connect("test.sqlite")

  ready_event.set()
  timings = []
  while not shutdown_event.is_set():
    started_at = time.time()
    conn.execute("SELECT COUNT(*) FROM test")
    timings.append((started_at, time.time()))

  conn.close()
  timings_tx.send(timings)

def query_write(ready_event: Event, shutdown_event: Event, timings_tx):
  conn = sqlite3.connect("test.sqlite", check_same_thread=False)

  ready_event.set()
  timings = []
  while not shutdown_event.is_set():
    started_at = time.time()
    conn.execute("INSERT INTO test (name) VALUES (?)", (random_string(32),))
    conn.commit()
    timings.append((started_at, time.time()))

  conn.close()
  timings_tx.send(timings)


def increment_vacuum():
  conn = sqlite3.connect("test.sqlite", timeout=0, check_same_thread=False)

  conn.execute("DELETE FROM test")
  conn.commit()

  ctx = multiprocessing.get_context("spawn")
  ready_event = ctx.Event()
  shutdown_event = ctx.Event()
  (timings_tx, timings_rx) = ctx.Pipe()
  # can switch between concurrent read and writes
  # process = ctx.Process(target=query_read, args=(ready_event, shutdown_event, timings_tx), daemon=True)
  process = ctx.Process(target=query_write, args=(ready_event, shutdown_event, timings_tx), daemon=True)
  process.start()
  ready_event.wait()

  vacuum_started_at = time.time()
  r = conn.execute("PRAGMA incremental_vacuum")
  # https://stackoverflow.com/a/56412002
  r.fetchall()
  vacuum_finished_at = time.time()
  print(f"Vacuum took {(vacuum_finished_at - vacuum_started_at) * 1000}ms")

  conn.close()

  shutdown_event.set()
  process.join()

  timings = timings_rx.recv()
  print_results(timings, vacuum_started_at, vacuum_finished_at)

if __name__ == '__main__':
  increment_vacuum()
```

</details>

### Resources

- [SQLite Vacuum](https://sqlite.org/lang_vacuum.html)
- [Excellent overview of different vacuuming strategies](https://blogs.gnome.org/jnelson/2015/01/06/sqlite-vacuum-and-auto_vacuum/)

[^1]: [2.9: Transient Database Used by Vacuum](https://www.sqlite.org/tempfiles.html)
