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

Two additional things will be done after write transactions:

1. The `embeddings_queue` table will be pruned to remove rows that are no longer needed. Specifically, rows with a sequence ID less than the minimum sequence ID of any active subscriber will be deleted. (As long as this is done continuously, this is a relatively cheap operation.)
2. `PRAGMA freelist_count` will be checked to see if the number of free pages multiplied by the page size is greater than the `log:vacuum_threshold` parameter. If so, `PRAGMA incremental_vacuum` is run to free up pages, up to `log:vacuum_limit`. This adds latency to write transactions, but will not block read transactions.

### New configuration parameters

**`log:vacuum_threshold`**:

- Default: 1GB
- Unit: megabytes (Postgres' convention)
- Usage: this helps avoid excessive fragmentation—without this parameter, or if it's set to `0`, it is effectively the same as SQLite's full vacuum mode.

**`log:vacuum_limit`**:

- Default: 0
- Unit: megabytes (Postgres' convention)
- Usage: vacuuming adds latency to write transactions. This allows rough control over the added latency by only vacuuming free pages up to this limit. If set to `0`, vacuuming will always reclaim all available space. If set to a small non-zero value, it's possible that

## Public Interfaces

In addition to the configuration parameter described above, a new `chroma vacuum` command will be added to the CLI to manually perform a full vacuum of the database. Usage:

```bash
chroma vacuum --path ./chroma_data
```

This automatically runs the pruning operation described above before running `VACUUM`. Prior to any modifications, it checks that there is enough available disk space to complete the vacuum (i.e. the free space on the disk is at least twice the size of the database).[^1]

`chroma vacuum` should be run infrequently; it may increase query performance but the degree to which it does so is currently unknown.

We should clearly document that `chroma vacuum` is not intended to be run while the Chroma server is running, maybe in the form of a confirmation prompt.

## Compatibility, Deprecation, and Migration Plan

Incremental vacuuming is not available by default in SQLite, and it's a little more complicated than just flipping a setting:

> However, changing from "none" to "full" or "incremental" can only occur when the database is new (no tables have yet been created) or by running the VACUUM command.[^2]

This means existing installations will not benefit from auto-pruning until they run `chroma vacuum`.

Users should see disk space freed immediately after upgrading and running `chroma vacuum` for the first time. Subsequent runs of `chroma vacuum` will likely free up no or very little disk space as the database will be continuously auto-pruned from that point forward.

## Test Plan

Auto-pruning should be thoroughly tested with property-based testing. We should test `chroma vacuum` with concurrent write operations to confirm it behaves as expected and emits the appropriate error messages.

## Rejected Alternatives

**Only prune when running `chroma vacuum`**: instead of continuously pruning the `embeddings_queue` table, only prune it when running `chroma vacuum` or some other manual command. This alternative was rejected because Chroma should be able to automatically keep its database size in check without manual intervention.

## Appendix

### Incremental vacuum experiment

Some tests were run to determine the impact of `PRAGMA incremental_vacuum` on read and write queries.

Observations:

- Parallel read queries during `PRAGMA incremental_vacuum` are not blocked.
- One or more (depending on number of threads) parallel read queries will see a large latency spike, which in most cases seems to be at least the duration of the vacuum operation.
- `PRAGMA incremental_vacuum` and write queries cannot be run in parallel (this is true in general for any query that writes data when in journaling mode).
- As a corollary to the above: if another process/thread writes and defers its commit, it can easily block the vacuum and cause it to time out.
- On a 2023 MacBook Pro, running `PRAGMA incremental_vacuum` on a database with ~1GB worth of free pages took around 900-1000ms.

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

def print_results(timings):
  if len(timings) == 0:
    return

  timings.sort()
  p95 = timings[int(len(timings) * 0.95)]
  print(f"Ran {len(timings)} concurrent queries")
  print(f"Query duration 95th percentile: {p95 * 1000}ms")
  print(f"Query duration max: {timings[-1] * 1000}ms")

def query_read(ready_event: Event, shutdown_event: Event):
  conn = sqlite3.connect("test.sqlite")

  ready_event.set()
  timings = []
  while not shutdown_event.is_set():
    started_at = time.time()
    conn.execute("SELECT COUNT(*) FROM test")
    duration = (time.time() - started_at)
    timings.append(duration)

  conn.close()
  print_results(timings)

def query_write(ready_event: Event, shutdown_event: Event):
  conn = sqlite3.connect("test.sqlite", check_same_thread=False)
  cur = conn.cursor()

  ready_event.set()
  timings = []
  while not shutdown_event.is_set():
    started_at = time.time()
    cur.execute("INSERT INTO test (name) VALUES (?)", (random_string(32),))
    duration = (time.time() - started_at)
    timings.append(duration)

  conn.close()
  print_results(timings)


def increment_vacuum():
  conn = sqlite3.connect("test.sqlite", check_same_thread=False)

  conn.execute("DELETE FROM test")
  conn.commit()

  ctx = multiprocessing.get_context("spawn")
  ready_event = ctx.Event()
  shutdown_event = ctx.Event()
  # can switch between concurrent read and writes
  process = ctx.Process(target=query_read, args=(ready_event, shutdown_event,), daemon=True)
  # process = ctx.Process(target=query_write, args=(ready_event, shutdown_event,), daemon=True)
  process.start()
  ready_event.wait()

  started_at = time.time()
  r = conn.execute("PRAGMA incremental_vacuum")
  # https://stackoverflow.com/a/56412002
  r.fetchall()
  finished_at = time.time()

  print(f"Vacuum took {(finished_at - started_at) * 1000}ms")
  conn.close()

  shutdown_event.set()
  process.join()

if __name__ == '__main__':
  increment_vacuum()
```

</details>

### Resources

- [SQLite Vacuum](https://sqlite.org/lang_vacuum.html)
- [Excellent overview of different vacuuming strategies](https://blogs.gnome.org/jnelson/2015/01/06/sqlite-vacuum-and-auto_vacuum/)

[^1]: [2.9: Transient Database Used by Vacuum](https://www.sqlite.org/tempfiles.html)
[^2]: https://www.sqlite.org/pragma.html#pragma_auto_vacuum
