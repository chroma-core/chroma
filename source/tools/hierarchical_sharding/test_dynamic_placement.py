import unittest
from .dynamic_placement import plan_placement

class PlacementTests(unittest.TestCase):
    def test_balances_whole_subtrees_deterministically_with_hard_limits(self):
        subtrees=[dict(subtree_id=str(i),storage_bytes=x,memory_bytes=x,load=x) for i,x in enumerate((8,7,3,2))]
        workers=[dict(worker_id=w,storage_bytes=10,memory_bytes=10,load=10) for w in ('a','b')]
        plan=plan_placement(subtrees,workers)
        self.assertEqual(plan,plan_placement(list(reversed(subtrees)),list(reversed(workers))))
        self.assertEqual(sorted(x['load'] for x in plan['estimated_resources'].values()),[10,10])
        with self.assertRaises(ValueError):plan_placement(subtrees,workers[:1])
    def test_invalid_inputs_fail_closed(self):
        for invalid in (float('nan'),-1,True):
            with self.assertRaises(ValueError):plan_placement([dict(subtree_id='s',storage_bytes=invalid,memory_bytes=1,load=1)],[dict(worker_id='w',storage_bytes=10,memory_bytes=10,load=10)])

    def test_retained_generation_resources_are_charged_before_planning(self):
        subtree=dict(subtree_id='s',storage_bytes=6,memory_bytes=1,load=1)
        worker=dict(worker_id='w',storage_bytes=10,memory_bytes=10,load=10,retained_storage_bytes=5)
        with self.assertRaisesRegex(ValueError,'cannot admit'):plan_placement([subtree],[worker])
        worker['retained_storage_bytes']=4
        self.assertEqual(plan_placement([subtree],[worker])['estimated_resources']['w']['storage_bytes'],10)

    def test_heterogeneous_workers_respect_serving_subtree_count_limit(self):
        rows=[dict(subtree_id=str(i),storage_bytes=1,memory_bytes=1,load=1) for i in range(129)]
        workers=[dict(worker_id='a',storage_bytes=1000000,memory_bytes=1000000,load=1000000),dict(worker_id='b',storage_bytes=1000,memory_bytes=1000,load=1000)]
        plan=plan_placement(rows,workers)
        self.assertEqual(list(plan['owners'].values()).count('a'),128)
        self.assertEqual(list(plan['owners'].values()).count('b'),1)
