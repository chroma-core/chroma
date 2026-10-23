import unittest
import numpy as np
from .reference_bounds import fp32_ip_distance_error,interval_candidates,validate_and_rerank


class BoundTests(unittest.TestCase):
    def test_random_nonunit_and_near_tie_match_full_f64(self):
        rng=np.random.default_rng(77);x=rng.normal(size=(300,768)).astype(np.float32);q=rng.normal(size=768).astype(np.float32)
        x[1]=x[0];x[2]=np.nextafter(x[0],np.float32(np.inf));ids=np.arange(len(x),dtype=np.uint64)[::-1]
        approximate=(np.float32(1)-x@q).astype(np.float32)
        exact=1-x.astype(np.float64)@q.astype(np.float64);errors=fp32_ip_distance_error(np.linalg.norm(q.astype(np.float64)),np.linalg.norm(x.astype(np.float64),axis=1),768)
        self.assertTrue(np.all(np.abs(exact-approximate)<=errors))
        result=validate_and_rerank(x,q,approximate,ids,20);order=np.lexsort((ids,exact))[:20]
        self.assertEqual(result['ids'],ids[order].tolist());np.testing.assert_allclose(result['distances'],exact[order],rtol=1e-14,atol=1e-14)

    def test_all_boundary_ties_are_retained_before_global_id_sort(self):
        x=np.ones((50,8),dtype=np.float32);q=np.ones(8,dtype=np.float32);ids=np.arange(50,dtype=np.uint64)[::-1]
        result=validate_and_rerank(x,q,np.full(50,-7,dtype=np.float32),ids,3)
        self.assertEqual(result['retained_candidates'],50);self.assertEqual(result['ids'],[0,1,2])

    def test_shape_integer_ids_and_subnormal_inputs_rejected(self):
        with self.assertRaises(ValueError):validate_and_rerank([[1]],[1],[0,10],[7],1)
        with self.assertRaises(ValueError):validate_and_rerank([[1],[2]],[1],[0,-1],[1.9,2.9],1)
        with self.assertRaises(ValueError):validate_and_rerank([[1],[2]],[1],[0,-1],[-1,2],1)
        x=np.array([[1e-40,0],[0,5e-38]],dtype=np.float32);q=np.array([1e38,1e35],dtype=np.float32)
        with self.assertRaisesRegex(ValueError,'subnormal'):validate_and_rerank(x,q,[1,.995],[0,1],1)

    def test_invalid_contract_inputs_fail_closed(self):
        for scores,errors,k in [([1,np.nan],.1,1),([1,2],-.1,1),([1,2],.1,3)]:
            with self.assertRaises(ValueError):interval_candidates(scores,errors,k)
        with self.assertRaises(ValueError):fp32_ip_distance_error(1,[1],100000000)
        with self.assertRaisesRegex(ValueError,'overflow'):fp32_ip_distance_error(1e30,[1e30],768)
        with self.assertRaisesRegex(ValueError,'overflow'):fp32_ip_distance_error(1e308,[1e308],768)

if __name__=='__main__':unittest.main()
