"""Count how many machines each query would contact under a given placement.

A placement assigns every leaf of the routing tree to one of M parts. Under
subtree placement, the leaves are laid out in depth-first order, so each
subtree's leaves are consecutive, and the order is cut into M runs of equal
posting bytes. Under random placement, each leaf goes to a part chosen by a
hash of its ID. For each query, the bench's diagnostic walk recorded the
leaves the beam chose (`--record-leaves`); the parts holding those leaves are
the machines the query would contact.

The script also reports how widely each query's leaves spread across the
tree: the number of distinct ancestors at every depth.

Inputs come from one bench results directory: `tree.jsonl` (written with
`--dump-tree`) and `queries.jsonl`. Only warm-pass records are read.

Usage: python3 fanout.py RESULTS_DIR [--parts 2,4,8,...] [--markdown]
"""
import argparse
import hashlib
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load_tree(path):
    nodes = {}
    for line in path.open():
        n = json.loads(line)
        nodes[n['id']] = n
    children = defaultdict(list)
    root = None
    for n in nodes.values():
        if n['parent'] is None:
            root = n['id']
        else:
            children[n['parent']].append(n['id'])
    order, stack = [], [root]
    while stack:
        node = stack.pop()
        if nodes[node]['leaf']:
            order.append(node)
        else:
            stack.extend(sorted(children[node], reverse=True))
    return nodes, order


def subtree_parts(nodes, order, m):
    total = sum(nodes[l]['posting_bytes'] or 0 for l in order)
    part, seen, out = 0, 0, {}
    for leaf in order:
        # Advance when this leaf's midpoint passes the next equal-bytes boundary.
        size = nodes[leaf]['posting_bytes'] or 0
        while part < m - 1 and seen + size / 2 > total * (part + 1) / m:
            part += 1
        out[leaf] = part
        seen += size
    return out


def random_parts(order, m):
    return {l: int(hashlib.sha256(l.encode()).hexdigest(), 16) % m for l in order}


def ancestors(nodes, leaf):
    path = []
    node = nodes[leaf]['parent']
    while node is not None:
        path.append(node)
        node = nodes[node]['parent']
    return list(reversed(path))


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('results')
    p.add_argument('--parts', default='2,4,8,16,32,64,128,256,512,1024')
    p.add_argument('--markdown', action='store_true')
    a = p.parse_args()
    d = Path(a.results)
    nodes, order = load_tree(d / 'tree.jsonl')
    queries = defaultdict(list)
    for line in (d / 'queries.jsonl').open():
        r = json.loads(line)
        if r['pass'] == 'warm' and r.get('leaf_ids'):
            queries[r['config']].append(r['leaf_ids'])
    depth_of = {}
    rows = []
    for config, qs in sorted(queries.items()):
        beam = statistics.mean(len(q) for q in qs)
        spread = defaultdict(list)
        for q in qs:
            by_depth = defaultdict(set)
            for leaf in q:
                if leaf not in depth_of:
                    depth_of[leaf] = ancestors(nodes, leaf)
                for depth, anc in enumerate(depth_of[leaf]):
                    by_depth[depth].add(anc)
            for depth, s in by_depth.items():
                spread[depth].append(len(s))
        spread_text = ', '.join(f'depth {k}: {statistics.mean(v):.1f}' for k, v in sorted(spread.items()) if k > 0)
        for m in [int(x) for x in a.parts.split(',')]:
            if m > len(order):
                continue
            for policy, parts in (('subtree', subtree_parts(nodes, order, m)), ('random', random_parts(order, m))):
                contacted = [len({parts[l] for l in q}) for q in qs]
                busiest = [max(_counts(q, parts).values()) / len(q) for q in qs]
                rows.append(dict(config=config, policy=policy, parts=m, leaves_per_part=len(order) / m,
                                 beam=beam, contacted_mean=statistics.mean(contacted),
                                 contacted_p50=pct(contacted, 0.5), contacted_p99=pct(contacted, 0.99),
                                 contacted_fraction=statistics.mean(contacted) / m,
                                 busiest_share=statistics.mean(busiest)))
        print(f'{config}: {len(qs)} queries, {beam:.0f} leaves each; distinct ancestors per query: {spread_text}')
    cols = ['config', 'policy', 'parts', 'leaves_per_part', 'beam', 'contacted_mean', 'contacted_p50',
            'contacted_p99', 'contacted_fraction', 'busiest_share']
    if a.markdown:
        print('| ' + ' | '.join(cols) + ' |')
        print('| ' + ' | '.join('---' for _ in cols) + ' |')
    for r in rows:
        vals = [f'{r[c]:.2f}' if isinstance(r[c], float) else str(r[c]) for c in cols]
        print(('| ' + ' | '.join(vals) + ' |') if a.markdown else '\t'.join(vals))


def _counts(leaves, parts):
    out = defaultdict(int)
    for l in leaves:
        out[parts[l]] += 1
    return out


if __name__ == '__main__':
    main()
