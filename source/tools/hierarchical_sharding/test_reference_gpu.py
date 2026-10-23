import tempfile,unittest,json
from unittest.mock import patch
from pathlib import Path
import numpy as np
from .reference_gpu import screened_reference,pathology_checks,main
from .tree_manifest import artifact


class CpuScorer:
    def begin_block(self,x):self.x=x
    def score(self,q):return np.float32(1)-q@self.x.T


class ScreenTests(unittest.TestCase):
    def test_streaming_screen_matches_complete_f64_with_ties_and_chunk_boundaries(self):
        rng=np.random.default_rng(11);x=rng.normal(size=(111,32)).astype('<f4');x[10:15]=x[3];queries=[dict(query_id=str(i),vector=q.tolist()) for i,q in enumerate(x[:7])]
        with tempfile.TemporaryDirectory() as tmp:
            chunks=[]
            for start,end in [(0,33),(33,111)]:
                path=Path(tmp)/str(start);path.write_bytes(x[start:end].tobytes());chunks.append((path,start,end-start))
            rows,metrics=screened_reference(chunks,queries,32,10,CpuScorer(),corpus_block=17,query_block=3,full_score_queries=7)
            for row,q in zip(rows,queries):
                d=1-np.einsum('ij,j->i',x.astype(np.float64),np.asarray(q['vector']),optimize=False);order=np.lexsort((np.arange(len(x)),d))[:10]
                self.assertEqual(row['ids'],order.tolist());np.testing.assert_allclose(row['distances'],d[order],atol=1e-14)
            self.assertEqual(metrics['full_score_checks'],len(x)*len(queries))

    def test_internal_low_precision_fp32_output_is_rejected(self):
        class Quantized(CpuScorer):
            def begin_block(self,x):self.x=x.astype(np.float16).astype(np.float32)
            def score(self,q):return np.float32(1)-q.astype(np.float16).astype(np.float32)@self.x.T
        with self.assertRaises(ValueError):pathology_checks(Quantized())
        self.assertEqual(len(pathology_checks(CpuScorer())),4)

    def test_initialization_failure_is_reported_and_short_prefix_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);vectors=root/'vectors';np.ones((2,768),dtype='<f4').tofile(vectors)
            tree=dict(metric='ip',dimension=768,global_row_count=2,source=dict(chunks=[dict(start_id=0,row_count=2,vectors=artifact(vectors,root))]))
            prepared=root/'input.json';prepared.write_text(json.dumps(tree));queries=root/'queries';queries.write_text(json.dumps(dict(query_id='q',vector=[1]*768))+'\n');expected=root/'expected';expected.write_text(json.dumps(dict(query_id='q',ids=[0],distances=[-767]))+'\n')
            args=['reference_gpu','--prepared',str(prepared),'--queries',str(queries),'--expected',str(expected),'--output',str(root/'out'),'--k','1']
            with patch('sys.argv',args),patch('tools.hierarchical_sharding.reference_gpu.CudaScorer',side_effect=ValueError('init failed')):
                with self.assertRaises(ValueError):main()
            report=json.loads((root/'out/report.json').read_text());self.assertEqual(report['status'],'failed');self.assertTrue(report['runtime']['initialization_failed'])
            tree['global_row_count']=3;prepared.write_text(json.dumps(tree));args[args.index(str(root/'out'))]=str(root/'out2')
            with patch('sys.argv',args),patch('tools.hierarchical_sharding.reference_gpu.CudaScorer') as scorer:
                with self.assertRaisesRegex(ValueError,'declared'):main()
                scorer.assert_not_called()

    def test_bad_kernel_dtype_fails_closed(self):
        class Bad(CpuScorer):
            def score(self,q):return super().score(q).astype(np.float16)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'x';np.ones((10,2),dtype='<f4').tofile(path)
            with self.assertRaisesRegex(ValueError,'strict-FP32'):screened_reference([(path,0,10)],[dict(query_id='q',vector=[1,1])],2,1,Bad())

if __name__=='__main__':unittest.main()
