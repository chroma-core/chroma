"""Aggregate throughput when every machine coordinates queries (fleet.py spread).

For each machine count M and configuration:
- the load pass's common window runs from the latest machine's start to the
  earliest machine's finish; aggregate throughput counts the queries on all
  machines that started and finished inside it, divided by its length;
- load-pass latency is the median and p99 of those queries;
- each machine's cores busy and mean runnable threads come from its
  per-second CPU sampler over the same window;
- the one-at-a-time pass's IDs and distances are compared, per query ID, with
  the single-coordinator run of the same reader family at the same M
  (runs/fleet-100m-{M}machines), which must match exactly.

Usage: python3 spread_analyze.py [--markdown]
"""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
TICKS = 100


def cpu_window(path, start, end):
    rows = []
    for line in path.read_text().splitlines():
        p = line.split()
        if len(p) >= 12 and p[1] == 'cpu':
            t = [int(x) for x in p[2:12]]
            rows.append((float(p[0]), sum(t), t[3] + t[4], int(p[-1])))
    inside = [r for r in rows if start <= r[0] <= end]
    if len(inside) < 2:
        return None, None
    a, b = inside[0], inside[-1]
    return ((b[1] - a[1]) - (b[2] - a[2])) / TICKS / (b[0] - a[0]), st.mean(r[3] for r in inside)


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


def main():
    markdown = '--markdown' in sys.argv
    rows = []
    base = {}
    for m in [1, 2, 4, 8]:
        d = HERE / 'runs' / f'fleet-spread-{m}machines'
        if not d.exists():
            continue
        run = json.loads((d / 'run.json').read_text())
        roles = run['roles']
        summaries, recs = {}, {}
        for role in roles:
            rd = d / role / f'fleet-spread-{m}machines'
            summaries[role] = {(e['config']['name'], e['pass']): e['summary']
                               for e in json.loads((rd / 'summary.json').read_text())['results']}
            recs[role] = defaultdict(list)
            for line in (rd / 'queries.jsonl').read_text().splitlines():
                r = json.loads(line)
                recs[role][(r['config'], r['pass'])].append(r)
        single = defaultdict(dict)
        sd = HERE / 'runs' / f'fleet-100m-{m}machines' / f'fleet-100m-{m}machines' / 'queries.jsonl'
        if sd.exists():
            for line in sd.read_text().splitlines():
                r = json.loads(line)
                if r['pass'] == 'warm':
                    single[r['config']][r['query_id']] = (r['ids'], r['distances'])
        for name in [k[0] for k in summaries[roles[0]] if k[1] == 'load']:
            loads = [summaries[r][(name, 'load')] for r in roles]
            start = max(s['started_unix'] for s in loads)
            end = min(s['finished_unix'] for s in loads)
            skew = max(s['started_unix'] for s in loads) - min(s['started_unix'] for s in loads)
            inside = [q for r in roles for q in recs[r][(name, 'load')]
                      if q['finished_unix'] - q['seconds'] >= start and q['finished_unix'] <= end]
            qps = len(inside) / (end - start)
            lat = [q['seconds'] * 1e3 for q in inside]
            cpu = {r: cpu_window(d / r / 'cpu.log', start, end) for r in roles}
            busy = [c[0] for c in cpu.values() if c[0] is not None]
            runq = [c[1] for c in cpu.values() if c[1] is not None]
            warm = [q for r in roles for q in recs[r][(name, 'warm')]]
            same = sum(1 for q in warm if single[name].get(q['query_id']) == (q['ids'], q['distances']))
            if m == 1:
                base[name] = qps
            rows.append(dict(config=name, machines=m, aggregate_qps=qps, speedup=qps / base.get(name, qps),
                             window_s=end - start, skew_ms=skew * 1e3, load_p50_ms=st.median(lat), load_p99_ms=pct(lat, 0.99),
                             warm_p50_ms=st.median(q['seconds'] * 1e3 for q in warm),
                             recall=st.mean(q['recall_fraction'] for q in warm),
                             same_ids=f'{same}/{len(warm)}' if single[name] else '-',
                             cores_busy=f'{min(busy):.1f}-{max(busy):.1f}' if busy else '-',
                             runnable=f'{min(runq):.0f}-{max(runq):.0f}' if runq else '-',
                             failed=run.get('failed')))
    cols = ['config', 'machines', 'aggregate_qps', 'speedup', 'window_s', 'skew_ms', 'load_p50_ms', 'load_p99_ms',
            'warm_p50_ms', 'recall', 'same_ids', 'cores_busy', 'runnable', 'failed']
    fmt = lambda v, c=None: (f'{v:.4f}' if c == 'recall' else f'{v:,.1f}') if isinstance(v, float) else str(v)
    rows.sort(key=lambda r: (r['config'], r['machines']))
    if markdown:
        print('| ' + ' | '.join(cols) + ' |')
        print('| ' + ' | '.join('---' for _ in cols) + ' |')
        for r in rows:
            print('| ' + ' | '.join(fmt(r[c], c) for c in cols) + ' |')
    else:
        for r in rows:
            print('  '.join(f'{c}={fmt(r[c], c)}' for c in cols))


if __name__ == '__main__':
    main()
