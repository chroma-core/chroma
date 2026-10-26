"""Routing-only stand-in for a large hierarchical SPANN tree.

At 100B vectors the routing tree has about 70M leaves, two more levels than at
100M, and its internal levels are searched at a few percent of their width.
No affordable real index reaches that shape. This stand-in builds one:
`--points` real corpus vectors play the role of leaf centroids, and a top-down
hierarchical k-means groups them under internal nodes of about `--fanout`
children each, so the tree has the depth and level widths of a real tree with
that many leaves. Internal centroids are the exact means of their members.

Navigation follows the reader: starting at the root, each level scores the
children of the kept nodes by exact squared distance and keeps the nearest
`beam` (per level, `--beams` sets internal levels and `--leaf-beam` the last).
For each query, the truth is its exact nearest `k` stand-in leaves. The
reported reach at a level is the share of those true leaves whose ancestor at
that level was kept; reach at the leaf level is the share found.

Placement counts: leaves are laid out in depth-first order and cut into M
parts of equal leaf count, as subtree placement would assign them to M
machines; each query's chosen leaves give the parts it would contact.

This measures hierarchical navigation against the flat ranking of leaf
centroids, at the real tree's depth. It does not model posting lists, so it
says nothing about how well vectors are grouped into leaves.

Usage: python routing_standin.py --corpus /data/fineweb100m.f32 --points 70000000
           --queries /data/queries/dev1000.jsonl --output DIR [--threads 32]
"""
import argparse
import json
import os
import time
from pathlib import Path

import numpy as np

DIM = 768


def assign_blocks(src, members, c, block=65536):
    """Nearest centroid for each member row, plus each centroid's member mean,
    streaming member rows from `src` in blocks (no full copy)."""
    cn = (c * c).sum(1)
    lab = np.empty(len(members), dtype=np.int64)
    sums = np.zeros((len(c), src.shape[1]), dtype=np.float64)
    for i in range(0, len(members), block):
        b = np.asarray(src[members[i:i + block]], dtype=np.float32)
        l = np.argmin(cn[None, :] - 2 * b @ c.T, axis=1)
        lab[i:i + block] = l
        for j in np.unique(l):
            sums[j] += b[l == j].sum(0)
    counts = np.bincount(lab, minlength=len(c))
    means = (sums / np.maximum(counts, 1)[:, None]).astype(np.float32)
    return lab, counts, means


def kmeans(src, members, k, rng, iters=8, sample=200_000):
    """Lloyd's k-means trained on a sample of the members, then one streamed
    assignment of every member. Returns (labels, counts, member means)."""
    k = max(1, min(k, len(members)))
    if k == 1:
        return assign_blocks(src, members, np.asarray(src[members[:1]], dtype=np.float32))
    pick = members if len(members) <= sample else np.sort(rng.choice(members, sample, replace=False))
    s = np.asarray(src[pick], dtype=np.float32)
    c = s[rng.choice(len(s), k, replace=False)].copy()
    for _ in range(iters):
        lab, counts, c2 = assign_blocks(s, np.arange(len(s)), c)
        empty = counts == 0
        c = c2
        if empty.any():
            c[empty] = s[rng.choice(len(s), int(empty.sum()), replace=False)]
    return assign_blocks(src, members, c)


def build_tree(x, fanout, rng):
    """Top-down hierarchical k-means. Returns levels from the root's children
    down to the leaves; each has `parent` (index into the level above) and
    `centroid` (exact member mean; None for the leaves, which are the points
    themselves in their original order)."""
    n = len(x)
    depth = max(1, int(np.ceil(np.log(n) / np.log(fanout) - 1e-9)))
    groups = [np.arange(n)]
    levels = []
    for d in range(1, depth):
        target = fanout ** (depth - d)  # members per node at this level
        parents, centroids, next_groups = [], [], []
        for gi, members in enumerate(groups):
            k = max(1, int(round(len(members) / target)))
            lab, counts, means = kmeans(x, members, k, rng)
            order = np.argsort(lab, kind='stable')
            bounds = np.searchsorted(lab[order], np.arange(len(means) + 1))
            for j in range(len(means)):
                if counts[j] == 0:
                    continue
                parents.append(gi)
                centroids.append(means[j])
                next_groups.append(members[order[bounds[j]:bounds[j + 1]]])
        levels.append(dict(parent=np.array(parents), centroid=np.array(centroids, dtype=np.float32)))
        groups = next_groups
        print(json.dumps(dict(level=d, nodes=len(parents))), flush=True)
    leaf_parent = np.empty(n, dtype=np.int64)
    for gi, members in enumerate(groups):
        leaf_parent[members] = gi
    levels.append(dict(parent=leaf_parent, centroid=None))
    return levels


def children_index(levels):
    """For each level, CSR arrays mapping a node of the level above to its children."""
    out = []
    for lv in levels:
        p = lv['parent']
        order = np.argsort(p, kind='stable')
        n_up = (p.max() + 1) if len(p) else 0
        start = np.searchsorted(p[order], np.arange(n_up + 1))
        out.append((order, start))
    return out


def navigate(q, x, levels, kids, beams):
    """Beam search; returns the kept node indices at every level (last = leaves)."""
    kept_per_level = []
    frontier = None
    for d, lv in enumerate(levels):
        if d == 0:
            cand = np.arange(len(lv['parent']))
        else:
            order, start = kids[d]
            cand = np.concatenate([order[start[f]:start[f + 1]] for f in frontier])
        cent = lv['centroid'][cand] if lv['centroid'] is not None else np.asarray(x[np.sort(cand)])
        if lv['centroid'] is None:
            cand = np.sort(cand)
        dist = ((cent - q) ** 2).sum(1)
        b = min(beams[d], len(cand))
        top = np.argpartition(dist, b - 1)[:b] if b < len(cand) else np.arange(len(cand))
        frontier = cand[top]
        kept_per_level.append(frontier)
    return kept_per_level


def ancestors_at_levels(leaf_ids, levels):
    """Ancestor index of each leaf at every level; the last entry is the leaf."""
    chain = [leaf_ids]
    for d in range(len(levels) - 1, 0, -1):
        chain.append(levels[d]['parent'][chain[-1]])
    return list(reversed(chain))


def exact_topk(x, queries, k, block=262144):
    qn = (queries * queries).sum(1)
    best_d = np.full((len(queries), k), np.inf, dtype=np.float32)
    best_i = np.zeros((len(queries), k), dtype=np.int64)
    for i in range(0, len(x), block):
        b = np.asarray(x[i:i + block], dtype=np.float32)
        d = qn[:, None] + (b * b).sum(1)[None, :] - 2 * queries @ b.T
        kk = min(k, d.shape[1])
        part = np.argpartition(d, kk - 1, axis=1)[:, :kk]
        cd = np.take_along_axis(d, part, 1)
        allc = np.concatenate([best_d, cd], 1)
        alli = np.concatenate([best_i, part + i], 1)
        sel = np.argpartition(allc, k - 1, axis=1)[:, :k]
        best_d = np.take_along_axis(allc, sel, 1)
        best_i = np.take_along_axis(alli, sel, 1)
    return best_i


def dfs_leaf_order(levels):
    """Leaves in depth-first order of the tree (children in index order)."""
    order = np.arange(len(levels[0]['parent']))
    for lv in levels[1:]:
        p = lv['parent']
        rank = np.empty(len(order), dtype=np.int64)
        rank[order] = np.arange(len(order))
        order = np.lexsort((np.arange(len(p)), rank[p]))
    return order


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--corpus', required=True)
    ap.add_argument('--points', type=int, required=True)
    ap.add_argument('--offset', type=int, default=0, help='first corpus row used as a stand-in leaf')
    ap.add_argument('--queries', required=True)
    ap.add_argument('--output', required=True)
    ap.add_argument('--fanout', type=int, default=63)
    ap.add_argument('--k', default='10,100')
    ap.add_argument('--internal-beams', default='512,2048,8192')
    ap.add_argument('--leaf-beams', default='128,512,2048')
    ap.add_argument('--parts', default='10,20,50,100,200')
    ap.add_argument('--query-limit', type=int, default=1000)
    ap.add_argument('--seed', type=int, default=7)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    rng = np.random.default_rng(a.seed)
    began = time.time()
    rows = os.path.getsize(a.corpus) // (DIM * 4)
    x = np.memmap(a.corpus, dtype='<f4', mode='r', shape=(rows, DIM))[a.offset:a.offset + a.points]
    queries = np.array([json.loads(l)['vector'] for l in open(a.queries)][:a.query_limit], dtype=np.float32)
    ks = [int(v) for v in a.k.split(',')]
    t = time.time()
    truth = exact_topk(x, queries, max(ks))
    truth_seconds = time.time() - t
    t = time.time()
    levels = build_tree(x, a.fanout, rng)
    build_seconds = time.time() - t
    kids = children_index(levels)
    widths = [len(lv['parent']) for lv in levels]
    leaf_anc = ancestors_at_levels(np.arange(a.points), levels)
    order = dfs_leaf_order(levels)
    pos = np.empty(a.points, dtype=np.int64)
    pos[order] = np.arange(a.points)
    results = []
    for ib in [int(v) for v in a.internal_beams.split(',')]:
        for lb in [int(v) for v in a.leaf_beams.split(',')]:
            beams = [ib] * (len(levels) - 1) + [lb]
            reach = {k: np.zeros(len(levels)) for k in ks}
            scored = np.zeros(len(levels))
            contacted = {m: [] for m in [int(v) for v in a.parts.split(',')]}
            t = time.time()
            for qi, q in enumerate(queries):
                kept = navigate(q, x, levels, kids, beams)
                for d in range(len(levels)):
                    keep = np.zeros(widths[d], dtype=bool)
                    keep[kept[d]] = True
                    for k in ks:
                        reach[k][d] += keep[leaf_anc[d][truth[qi, :k]]].mean()
                leaves = kept[-1]
                for m in contacted:
                    contacted[m].append(len(np.unique(pos[leaves] * m // a.points)))
            nav_seconds = (time.time() - t) / len(queries)
            row = dict(points=a.points, levels=widths, internal_beam=ib, leaf_beam=lb,
                       reach={f'k{k}': [round(v / len(queries), 4) for v in reach[k]] for k in ks},
                       contacted_mean={m: float(np.mean(v)) for m, v in contacted.items()},
                       contacted_p99={m: float(np.percentile(v, 99)) for m, v in contacted.items()},
                       seconds_per_query=nav_seconds)
            results.append(row)
            print(json.dumps(row), flush=True)
    report = dict(points=a.points, offset=a.offset, fanout=a.fanout, levels=widths, queries=len(queries),
                  truth_seconds=truth_seconds, build_seconds=build_seconds, seconds=time.time() - began,
                  results=results)
    (out / 'report.json').write_text(json.dumps(report, indent=1))


if __name__ == '__main__':
    main()
