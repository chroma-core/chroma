"""Compare two sets of machine-count runs (1, 2, 4 and 8 machines).

For each machine count and configuration this checks that both sets return
the same IDs and distances for every query in the one-at-a-time pass, then
prints, per set: the share of queries that navigated in parallel, warm median latency and navigation time, and for the second
32-thread pass its throughput, median latency, the median wait for the
slowest remote part's request thread, the bench process's cores busy, and
each machine's cores busy and mean runnable threads from the per-second CPU
sampler (cpu-<role>.log, when present).

Usage: python3 fleet_compare.py BEFORE_DIR AFTER_DIR
  where each DIR holds fleet-100m-{M}machines run directories.
"""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

TICKS = 100  # USER_HZ


def load(root, m):
    d = Path(root) / f'fleet-100m-{m}machines'
    run = d / f'fleet-100m-{m}machines'
    summary = json.loads((run / 'summary.json').read_text())
    recs = defaultdict(list)
    for line in (run / 'queries.jsonl').read_text().splitlines():
        r = json.loads(line)
        recs[(r['config'], r['pass'])].append(r)
    passes = defaultdict(list)
    for e in summary['results']:
        passes[e['config']['name']].append(e['summary'])
    cpu = {}
    for f in d.glob('cpu-*.log'):
        rows = []
        for line in f.read_text().splitlines():
            parts = line.split()
            if len(parts) < 12 or parts[1] != 'cpu':
                continue
            ticks = [int(x) for x in parts[2:12]]
            rows.append((float(parts[0]), sum(ticks), ticks[3] + ticks[4], int(parts[-1])))
        cpu[f.stem.removeprefix('cpu-')] = rows
    return summary, recs, passes, cpu


def window(rows, start, end):
    inside = [r for r in rows if start <= r[0] <= end]
    if len(inside) < 2:
        return None
    (t0, tot0, idle0, _), (t1, tot1, idle1, _) = inside[0], inside[-1]
    busy = ((tot1 - tot0) - (idle1 - idle0)) / TICKS / (t1 - t0)
    return round(busy, 1), round(st.mean(r[3] for r in inside), 1)


def slowest_spawn(rs):
    v = [max([p['client_spawn_ns'] for p in r['owners']['parts'] if p['remote']] or [0]) / 1e6 for r in rs]
    return round(st.median(v), 1)


def main():
    before, after = sys.argv[1], sys.argv[2]
    for m in [1, 2, 4, 8]:
        sets = {'before': load(before, m), 'after': load(after, m)}
        print(f'== {m} machines')
        for name in sets['after'][2]:
            wb = sets['before'][1][(name, 'warm')]
            wa = sets['after'][1][(name, 'warm')]
            same = sum(a['ids'] == b['ids'] and a['distances'] == b['distances'] for a, b in zip(wa, wb))
            print(f'  {name}: identical IDs and distances for {same} of {len(wa)} warm queries')
            for label, (summary, recs, passes, cpu) in sets.items():
                cold, warm, par = passes[name]
                rs = recs[(name, 'cold')][len(recs[(name, 'cold')]) // 2:]
                pc = par.get('process_cpu') or {}
                machines = ''
                if 'started_unix' in par:
                    machines = ' '.join(f'{role}:{window(rows, par["started_unix"], par["finished_unix"])}'
                                        for role, rows in sorted(cpu.items()))
                share = lambda rows: (f'{sum(r["owners"].get("navigation_parallel", False) for r in rows) / len(rows):.2f}'
                                      if rows and 'navigation_parallel' in rows[0]['owners'] else '-')
                parallel = f'parallel nav warm {share(recs[(name, "warm")])} 32-thread {share(rs)} | '
                print(f'    {label:6} {parallel}warm p50 {warm["latency_ms"]["p50"]:.2f} nav {warm["navigation_ms"]["p50"]:.2f}'
                      f' | 32-thread {1000 / par["pass_seconds"]:.0f} q/s p50 {par["latency_ms"]["p50"]:.1f}'
                      f' nav {par["navigation_ms"]["p50"]:.1f} slowest-spawn {slowest_spawn(rs)}'
                      f' | bench cores {pc.get("cores_busy", float("nan")):.1f} invol/s'
                      f' {pc.get("involuntary_switches", 0) / par["pass_seconds"]:.0f} | {machines}')


if __name__ == '__main__':
    main()
