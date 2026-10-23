"""Plan one write destination; this module never registers or publishes a vector.

The caller pins a complete topology and supplies the writer's level policy.
Expansion tasks contain selected parents only; every returned child participates
in one global selection. A physical worker is an execution detail, not a beam.
"""
from dataclasses import dataclass
import math


@dataclass(frozen=True)
class Node:
    owner: str
    node_id: int
    is_leaf: bool
    distance: float

    @property
    def key(self):
        return self.owner, self.node_id


def destination(roots, expand, select_count, *, topology_hash, max_levels=128, distance_key=None):
    """Return a single selected leaf and a replayable navigation trace.

    expand(owner, parent_ids, topology_hash) returns (parent_id, Node) pairs.
    select_count(level, sorted_nodes) returns the authoritative GLOBAL write
    beam size, including tau and any level overrides. Level one is the upper
    root's children, matching the Rust writer's first scored child level.
    Distances must come from the same metric/precision as the pinned writer.
    Equal-distance routing uses qualified node identity as a deterministic
    extension: the current Rust unstable distance-only sort has no tie promise.
    """
    if not topology_hash or type(max_levels) is not int or max_levels < 1:
        raise ValueError('a pinned topology and positive depth bound are required')
    order_key = lambda n: ((distance_key(n.distance) if distance_key is not None else n.distance), n.key)
    candidates = list(roots)
    seen = set()
    leaves = []
    trace = []
    for level in range(1, max_levels + 1):
        keys = set()
        for node in candidates:
            if (not isinstance(node, Node) or not isinstance(node.owner, str)
                    or not node.owner or type(node.node_id) is not int or node.node_id < 0
                    or type(node.is_leaf) is not bool or not math.isfinite(node.distance)):
                raise ValueError('invalid navigation child')
            if node.key in keys or node.key in seen:
                raise ValueError('duplicate child or navigation cycle')
            keys.add(node.key)
        ordered = sorted(candidates, key=order_key)
        if not ordered:
            raise ValueError('empty navigation frontier')
        count = select_count(level, tuple(ordered))
        if type(count) is not int or not 1 <= count <= len(ordered):
            raise ValueError('invalid global write beam')
        selected = ordered[:count]
        trace.append(dict(level=level, candidates=tuple(ordered), selected=tuple(selected)))
        grouped = {}
        for node in selected:
            seen.add(node.key)
            if node.is_leaf:
                leaves.append(node)
            else:
                grouped.setdefault(node.owner, []).append(node.node_id)
        if not grouped:
            # max_replicas=1: exactly one destination after all selected branches.
            return min(leaves, key=order_key), trace
        candidates = []
        for owner, parents in sorted(grouped.items()):
            rows = list(expand(owner, tuple(parents), topology_hash))
            covered = set()
            for parent, child in rows:
                if parent not in parents or not isinstance(child, Node) or child.owner != owner:
                    raise ValueError('expansion crosses ownership or returns an unrequested parent')
                covered.add(parent)
                candidates.append(child)
            if covered != set(parents):
                raise ValueError('selected parent has no children')
    raise ValueError('navigation exceeds depth bound')
