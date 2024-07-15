# CIP-07102024: Write-Ahead Log Pruning & Vacuuming

## Status

Current Status: `Under Discussion`

## Motivation

Chroma's SQLite-based write-ahead log grows infinitely over time. When ingesting large amounts of data, it's not uncommon for the SQLite database to grow to many gigabytes in size. Large databases cost more, take longer to back up, and can result in decreased query performance.

There are two separate problems:

- The database, specifically the `embeddings_queue` table, has unbounded growth.
- The SQLite `VACUUM` command, often recommended for such scenarios, is a blocking and potentially slow operation. Read and write operations are both blocked during a `VACUUM`.[^1]

This CIP addresses both issues.

## Proposed Changes

A new configuration parameter will be added, `log:vacuum_threshold`. It defaults to 1GB. Following Postgres' convention, the unit is megabytes. This helps avoid excessive fragmentation—without this parameter, or if it's set to `0`, it is effectively the same as SQLite's full vacuum mode.

Two additional things will be done after write transactions:

1. The `embeddings_queue` table will be pruned to remove rows that are no longer needed. Specifically, rows with a sequence ID less than the minimum sequence ID of any active subscriber will be deleted.
2. `PRAGMA freelist_count` will be checked to see if the number of free pages multiplied by the page size (`PRAGMA page_size`) is greater than the `log:vacuum_threshold` parameter. If so, `PRAGMA incremental_vacuum` is run to free up pages. This is a non-blocking operation.

Tentatively this will be done after every write transaction, assuming the added latency is minimal. If this is not the case, it will be run on some interval.

## Public Interfaces

In addition to the configuration parameter described above, a new `chroma vacuum` command will be added to the CLI to manually perform a full vacuum of the database. Usage:

```bash
chroma vacuum --path ./chroma_data
```

This automatically runs the pruning operation described above before running `VACUUM`. Prior to any modifications, it checks that there is enough available disk space to complete the vacuum (i.e. the free space on the disk is at least twice the size of the database).[^2]

`chroma vacuum` should be run infrequently; it may increase query performance but the degree to which it does so is currently unknown.

We should clearly document that `chroma vacuum` is not intended to be run while the Chroma server is running, maybe in the form of a confirmation prompt.

## Compatibility, Deprecation, and Migration Plan

Incremental vacuuming is not available by default in SQLite, and it's a little more complicated than just flipping a setting:

> However, changing from "none" to "full" or "incremental" can only occur when the database is new (no tables have yet been created) or by running the VACUUM command.[^3]

This means existing installations will not benefit from auto-pruning until they run `chroma vacuum`.

Users should see disk space freed immediately after upgrading and running `chroma vacuum` for the first time. Subsequent runs of `chroma vacuum` will likely free up no or very little disk space as the database will be continuously auto-pruned from that point forward.

## Test Plan

Auto-pruning should be thoroughly tested with property-based testing. We should test `chroma vacuum` with concurrent write operations to confirm it behaves as expected and emits the appropriate error messages.

## Rejected Alternatives

**Only prune when running `chroma vacuum`**: instead of continuously pruning the `embeddings_queue` table, only prune it when running `chroma vacuum` or some other manual command. This alternative was rejected because Chroma should be able to automatically keep its database size in check without manual intervention.

## Resources

- [Excellent overview of different vacuuming strategies](https://blogs.gnome.org/jnelson/2015/01/06/sqlite-vacuum-and-auto_vacuum/)

[^1]: [SQLite Vacuum](https://sqlite.org/lang_vacuum.html)
[^2]: [2.9: Transient Database Used by Vacuum](https://www.sqlite.org/tempfiles.html)
[^3]: https://www.sqlite.org/pragma.html#pragma_auto_vacuum
