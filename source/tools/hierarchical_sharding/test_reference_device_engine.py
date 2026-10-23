import tempfile,unittest
from pathlib import Path
import numpy as np
from .reference_device_engine import screened_reference,preflight_chunks,main

class Scorer:
    def begin_block(self,x):self.x=x
    def score(self,q):return (1-q@self.x.T).astype(np.float32)

class DeviceEngineTests(unittest.TestCase):
    def run_case(self,x,k,scorer=None):
        q=x[:3];queries=[dict(query_id=str(i),vector=v.tolist()) for i,v in enumerate(q)]
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'x';x.tofile(path)
            rows,metrics=screened_reference([(path,0,len(x))],queries,x.shape[1],k,scorer or Scorer(),corpus_block=3,query_block=2)
        for row,v in zip(rows,q):
            exact=1-np.einsum('ij,j->i',x.astype(np.float64),v.astype(np.float64),optimize=False)
            order=np.lexsort((np.arange(len(x)),exact))[:k]
            self.assertEqual(row['ids'],order.tolist());np.testing.assert_array_equal(row['distances'],exact[order])
        self.assertEqual(metrics['full_score_checks'],len(x)*len(q))
        return metrics

    def test_random_and_prefix_smaller_than_k(self):
        self.run_case(np.random.default_rng(81).normal(size=(71,16)).astype('<f4'),20)

    def test_all_ties_uncapped(self):
        metrics=self.run_case(np.ones((71,16),dtype='<f4'),20)
        self.assertEqual(metrics['retained_candidates_per_query'],[71]*3)

    def test_full_check_includes_rejected_rows(self):
        class Bad(Scorer):
            def score(self,q):
                out=super().score(q);out[:,-1]=1e10;return out
        with self.assertRaisesRegex(ValueError,'envelope'):
            self.run_case(np.ones((71,16),dtype='<f4'),1,Bad())

    def test_subnormal_rejected_before_score(self):
        x=np.ones((71,16),dtype='<f4');x[-1,0]=np.nextafter(np.float32(0),np.float32(1))
        with self.assertRaisesRegex(ValueError,'source arithmetic'):
            self.run_case(x,1)

    def test_preflight_requires_declared_count(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'x';np.ones((4,3),dtype='<f4').tofile(path)
            self.assertEqual(preflight_chunks([(path,0,4)],3,4),4)
            with self.assertRaises(ValueError):preflight_chunks([(path,0,4)],3,5)

    def test_failure_report_before_cuda_initialization(self):
        import json,sys
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);prepared=root/'input.json';prepared.write_text('{}');out=root/'out'
            with patch.object(sys,'argv',['engine','--prepared',str(prepared),'--queries','absent','--expected','absent','--output',str(out)]):
                with self.assertRaises(ValueError):main()
            report=json.loads((out/'report.json').read_text())
            self.assertEqual(report['status'],'failed');self.assertTrue(report['runtime']['initialization_failed'])
