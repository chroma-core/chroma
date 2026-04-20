# Memory monitoring runbook

Purpose: figure out **why** a long-running bench was killed (no panic,
just `signal: 9, SIGKILL: kill`). On Linux that almost always means the
OOM killer fired. The kernel does not deliver a catchable signal first,
so the only way to debug is to (a) watch RSS while the bench runs and
(b) confirm post-mortem via the kernel log.

The bench itself prints a per-checkpoint `Process mem:` line; this
runbook covers the host-side commands you'd run in another terminal to
watch in real time.

## Find the bench PID

`pgrep -f hierarchical_spann_profile_quantized` matches **everything**
with that string in its command line: the `cargo` wrapper, the actual
binary, any `watch`/`pgrep`/your shell history search, etc. That breaks
`top -p ...` and pollutes `ps` output.

Match only the compiled binary:

```bash
pgrep -f 'target/release/deps/hierarchical_spann_profile_quantized'
```

Save it to a variable for reuse:

```bash
BENCH_PID=$(pgrep -f 'target/release/deps/hierarchical_spann_profile_quantized')
echo "$BENCH_PID"
```

If `BENCH_PID` is empty the bench isn't running (or the binary path
differs — adjust the pattern).

## System memory snapshot

```bash
free -h
```

Live (refreshes every second):

```bash
free -h -s 1
```

Just the `MemAvailable` number the kernel actually uses for OOM
decisions:

```bash
grep -E '^(MemTotal|MemAvailable|SwapTotal|SwapFree):' /proc/meminfo
```

## Process snapshot (single shot)

```bash
ps -o pid,vsz,rss,pmem,thcount,etime,cmd -p "$BENCH_PID"
```

Columns: `vsz` virtual size (KB), `rss` resident set (KB), `pmem` % of
system RAM, `thcount` thread count, `etime` elapsed wall time.

Exact RSS / peak RSS / virtual size from `/proc` (matches the fields
the bench's `Process mem:` line reads):

```bash
grep -E '^Vm(RSS|HWM|Size|Peak):' /proc/$BENCH_PID/status
```

- `VmRSS`: current resident set
- `VmHWM`: peak RSS since process start (high water mark, never decreases)
- `VmSize`: current virtual size
- `VmPeak`: peak virtual size

## Live monitoring

`top` for just the bench (note: `top -p` requires a single
comma-separated PID list, not multiple `-p` args):

```bash
top -p "$BENCH_PID"
```

If `pgrep` returned multiple PIDs you wanted to watch, comma-join them:

```bash
top -p "$(pgrep -df, -f 'target/release/deps/hierarchical_spann_profile_quantized')"
```

Combined live monitor (system free + the bench process), one terminal:

```bash
watch -n 1 '
  echo "=== free ===";
  free -h;
  echo;
  echo "=== process ===";
  PID=$(pgrep -f "target/release/deps/hierarchical_spann_profile_quantized");
  if [ -n "$PID" ]; then
    ps -o pid,vsz,rss,pmem,thcount,etime,cmd -p $PID;
  else
    echo "(not running)";
  fi
'
```

The pattern is narrow enough that `watch` and `pgrep` do not match
themselves.

## OOM post-mortem

After a `SIGKILL`, confirm it was the OOM killer.

`dmesg -T` requires `CAP_SYSLOG` on most distros. If you see
`dmesg: read kernel buffer failed: Operation not permitted`:

```bash
sudo dmesg -T | grep -iE 'killed process|out of memory|oom_reaper|hierarchical_spann' | tail -20
```

No-sudo alternative via the systemd journal (works on Ubuntu EC2):

```bash
journalctl -k --since "1 hour ago" | grep -iE 'killed process|out of memory|oom_reaper|hierarchical_spann' | tail -20
```

Or scoped to today:

```bash
journalctl -k --since today | grep -iE 'killed process|oom' | tail -20
```

A kill line looks like:

```
kernel: Out of memory: Killed process 2216421 (hierarchical_sp) total-vm:302456984kB, anon-rss:295738680kB, file-rss:0kB, shmem-rss:0kB, UID:1000 pgtables:585820kB oom_score_adj:0
```

`anon-rss` at the moment of kill = how much physical memory the bench
was holding. Compare to the last `Process mem:` line printed by the
bench to see whether RSS spiked between log flushes.

## Interpreting the bench's `Process mem:` line

```
  Process mem: start 12.1GB -> balanced 38.4GB -> committed 41.2GB -> reopened 13.8GB | cp peak 42.7GB | lifetime peak 42.7GB | sys avail 84GB
```

- `start`: RSS at start of this checkpoint (after the previous reopen
  released).
- `balanced`: RSS after `add` + `balance_index_parallel`. This is the
  writer's working-set peak.
- `committed`: RSS after `flush()` returned. Blockfile cache may grow
  here.
- `reopened`: RSS after `drop(writer)` and `HierarchicalSpannWriter::open`.
  This **should** drop near `start`. If not, you have a leak across
  reopens (writer destructor isn't fully releasing materialized data,
  or the blockfile cache is holding everything).
- `cp peak`: max RSS observed by the background sampler during this
  checkpoint. Catches spikes between phase-boundary samples.
- `lifetime peak`: `VmHWM` — process-lifetime peak, never decreases.
- `sys avail`: `MemAvailable` from `/proc/meminfo`. When this trends
  toward zero across checkpoints, an OOM kill is imminent.

Cross-check `cp peak` against the writer's `Lazy IO total` line on the
preceding `--- Checkpoint ---` block — anything significantly above
that is blockfile cache, commit overhead, or allocator slack, and is a
candidate for further investigation.

## Common gotchas

- **`top -p`** takes one PID or one comma-separated list, not multiple
  `-p` flags and not whitespace-separated PIDs. Use `pgrep -df,` to
  produce the right format.
- **`pgrep -f` matches itself** when it appears in `watch '... pgrep
  ...'` because the watch shell argv contains the search string. Use a
  more specific pattern (the binary path under `target/release/deps/`)
  or `pgrep -f ... | grep -v $$`.
- **`dmesg`** is restricted on most modern kernels (`kernel.dmesg_restrict=1`).
  Use `sudo dmesg` or `journalctl -k`.
- **`free` "used" vs "available"**: prefer `available`. The kernel
  treats reclaimable page cache (`buff/cache`) as available, so `used`
  overstates pressure.
- **No swap**: on these EC2 boxes `SwapTotal: 0`, so the OOM killer has
  zero buffer once `MemAvailable` hits a few hundred MB. The bench's
  `sys avail` field is the leading indicator.
