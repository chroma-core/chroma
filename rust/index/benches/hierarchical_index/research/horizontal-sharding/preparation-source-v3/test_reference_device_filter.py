import unittest
import numpy as np
from .reference_device_filter import NumpyIntervals,RunningFilter,check_endpoint_conformance


class DeviceFilterTests(unittest.TestCase):
    def test_endpoints(self):
        self.assertTrue(check_endpoint_conformance(NumpyIntervals(),lambda x:x,lambda x:x))

    def test_early_prefix_and_all_ties(self):
        run=RunningFilter(5)
        for n in (2,1,7):
            self.assertEqual(run.select(np.ones(n,dtype=np.float32),0.).tolist(),list(range(n)))

    def test_current_block_updates_before_filter(self):
        run=RunningFilter(1)
        self.assertEqual(run.select(np.array([100,101],dtype=np.float32),0.).tolist(),[0])
        self.assertEqual(run.select(np.array([2,1,3],dtype=np.float32),0.).tolist(),[1])

    def test_full_reference_survives_blocks(self):
        rng=np.random.default_rng(91)
        truth=rng.normal(size=101);errors=.01
        approximate=(truth+rng.uniform(-errors/2,errors/2,len(truth))).astype(np.float32)
        run=RunningFilter(7);kept=[]
        for start in range(0,len(truth),3):
            kept.extend((start+run.select(approximate[start:start+3],errors)).tolist())
        expected=np.lexsort((np.arange(len(truth)),truth))[:7]
        actual=np.array(kept)[np.lexsort((np.array(kept),truth[kept]))[:7]]
        np.testing.assert_array_equal(actual,expected)

    def test_invalid_contract(self):
        for error in (-1.,float('inf'),np.float32(.1)):
            with self.assertRaises(ValueError):RunningFilter(1).select(np.ones(2,dtype=np.float32),error)
        with self.assertRaises(ValueError):RunningFilter(1).select(np.ones(2,dtype=np.float64),0.)
        with self.assertRaises(ValueError):RunningFilter(1).select(np.array([np.nan],dtype=np.float32),0.)
