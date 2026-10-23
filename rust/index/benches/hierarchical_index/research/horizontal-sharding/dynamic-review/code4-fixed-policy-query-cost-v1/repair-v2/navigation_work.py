"""Validate query work without treating the optional traversal trace as a scalar.

The caller must bind saved_internal_nodes to the exact admitted saved generation.
This helper neither reads a snapshot nor establishes that generation binding.
"""

LEVEL_CAP = 128
LEVEL_FIELDS = frozenset((
    "level", "expanded_internal", "offered_internal", "offered_leaf",
    "retained_internal", "retained_leaf", "discarded_internal", "discarded_leaf",
))
NAVIGATION_FIELDS = frozenset((
    "expanded_internal_nodes", "centroid_distance_evaluations", "navigation_levels",
    "peak_child_frontier", "selected_leaves",
))


def _counter(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer, excluding booleans")
    return value


def validate_query_work_counts(counts, *, saved_internal_nodes, require_trace=False):
    """Return scalar counts and an independently checked structural summary.

    A missing/null trace is compatible with legacy or untraced responses, but
    cannot pass structural admission. Set require_trace=True for that admission.
    A present malformed/incomplete trace always raises, even if not required.
    No input is mutated. No node IDs or additional per-query sets are collected.
    """
    saved_total = _counter(saved_internal_nodes, "saved_internal_nodes")
    if type(require_trace) is not bool:
        raise ValueError("require_trace must be a boolean")
    if type(counts) is not dict or len(counts) > 64:
        raise ValueError("counts must be an object with at most64 fields")
    scalars = {}
    for name, value in counts.items():
        if type(name) is not str or not name or len(name) > 128:
            raise ValueError("invalid scalar counter name")
        if name != "navigation_level_trace":
            scalars[name] = _counter(value, name)
    if not NAVIGATION_FIELDS.issubset(scalars):
        raise ValueError("missing navigation scalar counters")
    if scalars["expanded_internal_nodes"] > saved_total:
        raise ValueError("expanded internals exceed saved generation total")
    trace = counts.get("navigation_level_trace")
    if trace is None:
        if require_trace:
            raise ValueError("complete navigation_level_trace required")
        return {"scalar_counts": scalars, "trace_observed": False,
                "internal_pruning": None, "reason": "trace absent or null"}
    if type(trace) is not dict or set(trace) != {"complete", "level_cap", "levels"}:
        raise ValueError("invalid navigation trace schema")
    if trace["complete"] is not True:
        raise ValueError("incomplete or malformed navigation trace")
    if type(trace["level_cap"]) is not int or trace["level_cap"] != LEVEL_CAP:
        raise ValueError("navigation trace must retain the existing128-level cap")
    levels = trace["levels"]
    if type(levels) is not list or len(levels) > LEVEL_CAP:
        raise ValueError("invalid bounded level records")
    if len(levels) != scalars["navigation_levels"]:
        raise ValueError("level count differs from navigation_levels")
    expanded = evaluations = discarded_internal = discarded_leaf = retained_leaves = peak = 0
    next_expansions = 1
    for index, level in enumerate(levels, 1):
        if type(level) is not dict or set(level) != LEVEL_FIELDS:
            raise ValueError("invalid per-level schema")
        for name, value in level.items():
            _counter(value, f"level{index}.{name}")
        if level["level"] != index or level["expanded_internal"] != next_expansions:
            raise ValueError("nonsequential level or retained-to-expanded mismatch")
        for kind in ("internal", "leaf"):
            if level[f"offered_{kind}"] != level[f"retained_{kind}"] + level[f"discarded_{kind}"]:
                raise ValueError(f"{kind} partition does not reconcile")
        offered = level["offered_internal"] + level["offered_leaf"]
        if offered == 0:
            raise ValueError("recorded expansion level has no offered children")
        expanded += level["expanded_internal"]
        evaluations += offered
        discarded_internal += level["discarded_internal"]
        discarded_leaf += level["discarded_leaf"]
        retained_leaves += level["retained_leaf"]
        peak = max(peak, offered)
        next_expansions = level["retained_internal"]
    if levels and next_expansions:
        raise ValueError("trace ends before retained internals expand")
    if not levels and saved_total:
        raise ValueError("internal saved tree requires an expansion trace")
    if expanded != scalars["expanded_internal_nodes"] or evaluations != scalars["centroid_distance_evaluations"]:
        raise ValueError("cumulative expansion or evaluation count mismatch")
    if peak != scalars["peak_child_frontier"]:
        raise ValueError("peak offered frontier does not reconcile")
    if levels and retained_leaves != scalars["selected_leaves"]:
        raise ValueError("selected leaves do not reconcile")
    if not levels and scalars["selected_leaves"] not in (0, 1):
        raise ValueError("root-only traversal selects at most one leaf")
    return {"scalar_counts": scalars, "trace_observed": True,
            "internal_pruning": discarded_internal > 0 and expanded < saved_total,
            "discarded_internal": discarded_internal, "discarded_leaf": discarded_leaf,
            "expanded_internal": expanded, "saved_internal_nodes": saved_total,
            "centroid_evaluations": evaluations, "level_records": len(levels)}
