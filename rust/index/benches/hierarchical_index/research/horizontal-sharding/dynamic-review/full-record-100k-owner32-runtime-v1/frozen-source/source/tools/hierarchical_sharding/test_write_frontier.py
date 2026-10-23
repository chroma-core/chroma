import unittest
from .write_frontier import Node, destination


class WriteFrontierTest(unittest.TestCase):
    def test_upper_top_one_misses_better_leaf_and_global_beam_recovers_it(self):
        # Squared L2 in one dimension: q=0, upper centroids a=1/b=2,
        # descendant leaf centroids a=3/b=0.125. These are actual distances.
        squared_l2 = lambda x: (x - 0.0) ** 2
        roots = [Node('a', 1, False, squared_l2(1)), Node('b', 1, False, squared_l2(2))]
        calls = []
        def expand(owner, parents, pin):
            calls.append((owner, parents, pin))
            return [(1, Node(owner, 2, True, squared_l2(3) if owner == 'a' else squared_l2(0.125)))]
        top_one, _ = destination(roots, expand, lambda level, nodes: 1, topology_hash='pinned')
        self.assertEqual(top_one.owner, 'a')
        calls.clear()
        global_best, trace = destination(roots, expand, lambda level, nodes: len(nodes), topology_hash='pinned')
        self.assertEqual(global_best, Node('b', 2, True, squared_l2(0.125)))
        self.assertEqual(calls, [('a', (1,), 'pinned'), ('b', (1,), 'pinned')])
        self.assertEqual(len(trace[1]['selected']), 2)

    def test_next_level_prunes_globally_and_keeps_reached_leaves(self):
        roots = [Node('a', 0, True, 0.2), Node('b', 0, False, 1.0)]
        def expand(owner, parents, pin):
            return [(0, Node('b', 1, True, 0.3)), (0, Node('b', 2, True, 0.4))]
        leaf, trace = destination(roots, expand, lambda level, nodes: 2 if level == 1 else 1, topology_hash='p')
        self.assertEqual(leaf.owner, 'a')
        self.assertEqual(len(trace[1]['selected']), 1)

    def test_bad_remote_expansion_cannot_choose_a_destination(self):
        for rows in ([], [(0, Node('other', 1, True, 0.0))],
                     [(3, Node('a', 1, True, 0.0))],
                     [(0, Node('a', 0, False, 0.0))]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                destination([Node('a', 0, False, 1.0)], lambda *args: rows,
                            lambda level, nodes: 1, topology_hash='p')

    def test_ties_have_one_stable_owner_and_policy_is_global(self):
        roots = [Node('b', 0, True, 0.0), Node('a', 0, True, 0.0)]
        observed = []
        def policy(level, nodes):
            observed.append((level, tuple(n.owner for n in nodes)))
            return 2
        leaf, _ = destination(roots, None, policy, topology_hash='p')
        self.assertEqual(leaf.owner, 'a')
        self.assertEqual(observed, [(1, ('a', 'b'))])
