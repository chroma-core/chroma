"""Find the cheapest configuration at each recall level, per corpus size.

A bench run sweeps a grid of beam widths and rerank depths. For one cost
measure, a configuration is on the frontier when no cheaper configuration has
equal or higher recall. The cost of reaching a recall target is then read off
the frontier in two ways: the cheapest measured configuration that reaches
the target, and a log-linear interpolation between the two frontier points
that bracket it (labelled as interpolated). A bootstrap over queries gives a
95% interval for both; queries are resampled once per round and shared by
every configuration, so configurations stay paired.

Costs per query come from the warm-pass records:
  leaves        leaves scanned
  vectors       posting codes scored
  centroids     centroid codes scored during navigation
  reranked      full vectors exact-scored
  cpu_ms        process CPU (the query ran alone)
  latency_ms    wall time
  cold_mb       bytes a fully cold query would read

Usage: python3 frontier.py LABEL=RESULTS_DIR[+RESULTS_DIR...] [LABEL=...]
           [--targets 0.90,0.95] [--k 10,100] [--rounds 400] [--markdown]
"""
import argparse
import json
import math
import random
from collections import defaultdict
from pathlib import Path

COSTS = {
    'leaves': lambda r: r['work']['leaves'],
    'vectors': lambda r: r['work']['vectors'],
    'centroids': lambda r: r['work']['navigation']['distances'],
    'reranked': lambda r: r['work']['reranked'],
    'cpu_ms': lambda r: (r['cpu_seconds'] or 0) * 1e3,
    'latency_ms': lambda r: r['seconds'] * 1e3,
    'cold_mb': lambda r: (r['bytes'].get('cold_equivalent') or 0) / 1e6,
}


def load(results):
    """config name -> (k, per-query recall list, per-query cost dict), warm pass only.
    `results` may join several results directories with '+'; their configurations
    are pooled, so they must run the same queries."""
    if '+' in results:
        out = {}
        for part in results.split('+'):
            for name, d in load(part).items():
                out[name if name not in out else f'{name}@{part}'] = d
        return out
    by_config = defaultdict(list)
    for line in (Path(results) / 'queries.jsonl').open():
        r = json.loads(line)
        if r['pass'] == 'warm':
            by_config[r['config']].append(r)
    configs = {c['config']['name']: c['config'] for c in json.load(open(Path(results) / 'summary.json'))['results']}
    out = {}
    for name, rows in by_config.items():
        rows.sort(key=lambda r: r['query_index'])
        out[name] = dict(k=configs[name]['k'], recall=[r['recall_fraction'] for r in rows],
                         cost={m: [f(r) for r in rows] for m, f in COSTS.items()})
    return out


def frontier(points):
    """points: [(cost, recall, name)] -> frontier sorted by cost."""
    best, out = -1.0, []
    for cost, recall, name in sorted(points):
        if recall > best:
            out.append((cost, recall, name))
            best = recall
    return out


def at_target(front, target):
    measured = next(((c, n) for c, r, n in front if r >= target), (math.nan, None))
    interp = math.nan
    for (c0, r0, _), (c1, r1, _) in zip(front, front[1:]):
        if r0 < target <= r1 and c0 > 0 and c1 > 0:
            t = (target - r0) / (r1 - r0)
            interp = math.exp(math.log(c0) + t * (math.log(c1) - math.log(c0)))
            break
    if front and front[0][1] >= target:
        interp = front[0][0]
    return measured, interp


def means(data, idx=None):
    """config -> (k, mean recall, {metric: mean cost}) over the query indices."""
    out = {}
    for name, d in data.items():
        sel = idx if idx is not None else range(len(d['recall']))
        n = len(sel)
        out[name] = (d['k'], sum(d['recall'][i] for i in sel) / n,
                     {m: sum(c[i] for i in sel) / n for m, c in d['cost'].items()})
    return out


def evaluate(mean, k, metric, target):
    pts = [(cost[metric], recall, name) for name, (kk, recall, cost) in mean.items() if kk == k]
    return at_target(frontier(pts), target)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('runs', nargs='+')
    p.add_argument('--targets', default='0.90,0.95')
    p.add_argument('--k', default='10,100')
    p.add_argument('--metrics', default=','.join(COSTS))
    p.add_argument('--rounds', type=int, default=400)
    p.add_argument('--markdown', action='store_true')
    a = p.parse_args()
    rng = random.Random(7)
    rows = []
    for spec in a.runs:
        label, path = spec.split('=', 1)
        data = load(path)
        nq = len(next(iter(data.values()))['recall'])
        full = means(data)
        boots = [means(data, [rng.randrange(nq) for _ in range(nq)]) for _ in range(a.rounds)]
        for k in [int(x) for x in a.k.split(',')]:
            for target in [float(x) for x in a.targets.split(',')]:
                for metric in a.metrics.split(','):
                    (measured, name), interp = evaluate(full, k, metric, target)
                    bm, bi = [], []
                    for mean in boots:
                        (m, _), i = evaluate(mean, k, metric, target)
                        bm.append(m)
                        bi.append(i)
                    rows.append(dict(size=label, k=k, target=target, metric=metric, cheapest=name,
                                     measured=measured, measured_ci=_ci(bm), interpolated=interp,
                                     interpolated_ci=_ci(bi)))
    cols = ['size', 'k', 'target', 'metric', 'cheapest', 'measured', 'measured_ci', 'interpolated', 'interpolated_ci']
    if a.markdown:
        print('| ' + ' | '.join(cols) + ' |')
        print('| ' + ' | '.join('---' for _ in cols) + ' |')
    for r in rows:
        vals = [_fmt(r[c]) for c in cols]
        print(('| ' + ' | '.join(vals) + ' |') if a.markdown else '\t'.join(vals))


def _ci(values):
    v = sorted(x for x in values if not math.isnan(x))
    if len(v) < 0.95 * len(values) or not v:
        return 'unattained in some rounds'
    return f'{v[int(0.025 * len(v))]:.4g}–{v[int(0.975 * len(v)) - 1]:.4g}'


def _fmt(x):
    if isinstance(x, float):
        return 'unattained' if math.isnan(x) else f'{x:.4g}'
    return str(x)


if __name__ == '__main__':
    main()
