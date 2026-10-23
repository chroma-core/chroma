import tempfile,unittest
from pathlib import Path
import numpy as np
from .reference_batched_filter import BatchedRunningFilter,NumpyBatchedIntervals,check_batched_conformance,batched_memory_projection
from .reference_batched_engine import batched_reference
from .reference_device_engine import screened_reference

class Scorer:
    def __init__(self):self.calls=0
    def begin_block(self,x):self.x=x
    def score(self,q):self.calls+=1;return (1-q@self.x.T).astype(np.float32)

class BatchedTests(unittest.TestCase):
    def scan(self,x,q,k,scorer=None,**options):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder);cut=len(x)//2;x[:cut].tofile(p/'a');x[cut:].tofile(p/'b');chunks=[(p/'a',0,cut),(p/'b',cut,len(x)-cut)]
            queries=[dict(query_id=str(i),vector=v.tolist()) for i,v in enumerate(q)]
            rows,metrics=batched_reference(chunks,queries,x.shape[1],k,scorer or Scorer(),expected_rows=len(x),corpus_block=3,query_block=options.pop('query_block',3),**options)
            for row,v in zip(rows,q):
                distances=1-np.einsum('ij,j->i',x.astype(np.float64),v.astype(np.float64),optimize=False);order=np.lexsort((np.arange(len(x)),distances))[:k]
                self.assertEqual(row['ids'],order.tolist());np.testing.assert_array_equal(row['distances'],distances[order])
            old,_=screened_reference(chunks,queries,x.shape[1],k,Scorer(),corpus_block=3,query_block=3)
            self.assertEqual(rows,old)
            return metrics
    def test_random_full_oracle_old_epoch_and_prefix_less_than_k(self):
        rng=np.random.default_rng(71)
        for queries in (1,3,7,33):
            self.scan(rng.normal(size=(41,16)).astype('<f4'),rng.normal(size=(queries,16)).astype('<f4'),20,query_block=32)
    def test_all_ties_remain_uncapped_across_batches(self):
        m=self.scan(np.ones((41,16),dtype='<f4'),np.ones((7,16),dtype='<f4'),5)
        self.assertEqual(m['retained_candidates_per_query'],[41]*7);self.assertEqual(m['maximum_uncapped_coordinates'],9);self.assertEqual(m['coordinate_transfers'],42)
    def test_near_precision_and_cancellation(self):
        x=np.ones((31,8),dtype='<f4');x[:,0]=np.linspace(-1,1,len(x));x[1,1]=np.nextafter(np.float32(1),np.float32(2));q=np.array([[1.,-1,1,-1,1,-1,1,-1],[1+2**-12]*8],dtype='<f4')
        self.scan(x,q,10)
    def test_full_checks_include_rejected_rows(self):
        class Bad(Scorer):
            def score(self,q):
                out=super().score(q);out[:,-1]=1e10;return out
        with self.assertRaisesRegex(ValueError,'envelope'):
            self.scan(np.ones((31,8),dtype='<f4'),np.ones((3,8),dtype='<f4'),1,Bad())
    def test_adversarial_signed_envelope_errors_preserve_oracle(self):
        from .reference_bounds import fp32_ip_distance_error
        class Noisy(Scorer):
            def score(self,q):
                exact=1-np.einsum('qd,nd->qn',q.astype(np.float64),self.x.astype(np.float64),optimize=False)
                bound=fp32_ip_distance_error(float(np.linalg.norm(self.x.astype(np.float64),axis=1).max()),np.linalg.norm(q.astype(np.float64),axis=1),q.shape[1])
                signs=np.where(np.arange(len(self.x))%2,1.,-1.)
                return (exact+.75*bound[:,None]*signs).astype(np.float32)
        rng=np.random.default_rng(911)
        self.scan(rng.normal(size=(41,16)).astype('<f4'),rng.normal(size=(7,16)).astype('<f4'),10,Noisy())
    def test_low_precision_internal_scorer_fails_full_envelope(self):
        class Quantized(Scorer):
            def score(self,q):return (1-q.astype(np.float16).astype(np.float32)@self.x.astype(np.float16).astype(np.float32).T).astype(np.float32)
        with self.assertRaisesRegex(ValueError,'envelope'):
            self.scan(np.full((7,768),1+2**-12,dtype='<f4'),np.full((3,768),1+2**-12,dtype='<f4'),1,Quantized())
    def test_invalid_arithmetic_and_declared_count_fail_before_scoring(self):
        x=np.ones((31,8),dtype='<f4');q=np.ones((3,8),dtype='<f4');s=Scorer();x[0,0]=np.nextafter(np.float32(0),np.float32(1))
        with self.assertRaisesRegex(ValueError,'source arithmetic'):self.scan(x,q,1,s)
        self.assertEqual(s.calls,0)
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'x';np.ones((2,8),dtype='<f4').tofile(path)
            with self.assertRaisesRegex(ValueError,'coverage'):batched_reference([(path,0,2)],[dict(query_id='x',vector=[1.]*8)],8,1,s,expected_rows=3)
        self.assertEqual(s.calls,0)
    def test_overflow_rejected_before_score(self):
        s=Scorer()
        with self.assertRaisesRegex(ValueError,'overflow'):self.scan(np.full((31,8),1e30,dtype='<f4'),np.full((3,8),1e30,dtype='<f4'),1,s)
        self.assertEqual(s.calls,0)
    def test_batched_history_and_endpoint_bits_match_scalar(self):
        self.assertTrue(check_batched_conformance(NumpyBatchedIntervals(),np.asarray,np.asarray))
    def test_current_upper_updates_before_filter_and_changing_bounds(self):
        f=BatchedRunningFilter(2,2)
        a=f.select(np.array([[5.],[4.]],dtype=np.float32),np.array([0.,1.],dtype=np.float64));self.assertEqual(a.tolist(),[[0,0],[1,0]])
        b=f.select(np.array([[0.,1.,2.],[0.,1.,2.]],dtype=np.float32),np.zeros(2,dtype=np.float64));self.assertEqual(b.tolist(),[[0,0],[0,1],[1,0],[1,1]])
    def test_memory_projection_bounds_batched_ties(self):
        p=batched_memory_projection(768,32768,32,11000,100)
        self.assertGreaterEqual(p['terms']['matrix_temporaries_and_uncapped_coordinates'],32*32768*(8*3+4+16));self.assertLess(p['projected_peak_bytes'],6*1024**3)
        with self.assertRaises(ValueError):batched_memory_projection(768,32768,33,11000,100)
    def test_invalid_bounds_shape_dtype_and_scores(self):
        f=BatchedRunningFilter(2,2)
        for errors in [np.ones(1,dtype=np.float64),np.ones(2,dtype=np.float32),np.array([0.,np.nan])]:
            with self.assertRaises(ValueError):f.select(np.ones((2,3),dtype=np.float32),errors)
        with self.assertRaises(ValueError):f.select(np.ones((2,3),dtype=np.float64),np.ones(2,dtype=np.float64))
if __name__=='__main__':unittest.main()
