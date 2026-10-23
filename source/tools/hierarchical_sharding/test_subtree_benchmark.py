import json,pathlib,tempfile,time,unittest
from unittest.mock import patch
from .subtree_benchmark import load_process,choose_setting,measure,journal_outcomes

class JournalTest(unittest.TestCase):
    def test_failed_client_preserves_completed_query_and_error(self):
        class Reader:
            calls=0
            closed=False
            def verify_roots(self):pass
            def query(self,*args,**kwargs):
                self.calls+=1
                if self.calls==2:raise RuntimeError('connection closed')
                return dict(ids=[1],actual_hosts=['host'],global_candidate_count=1)
            def close(self):self.closed=True
        reader=Reader()
        with tempfile.TemporaryDirectory() as directory:
            journal=pathlib.Path(directory)/'client.jsonl'
            with patch('tools.hierarchical_sharding.subtree_benchmark.TreeCoordinator.from_files',return_value=reader):
                with self.assertRaisesRegex(RuntimeError,'connection closed'):
                    load_process(('tree','placement',[dict(query_id='q',vector=[1])],2,1000,time.monotonic()-.001,1,0,str(journal)))
            records=[json.loads(v) for v in journal.read_text().splitlines()]
            self.assertEqual([r['status'] for r in records],['ok','error','client_failure'])
            self.assertEqual(records[0]['ids'],[1]);self.assertTrue(reader.closed)

class ReadinessTest(unittest.TestCase):
    def test_client_waits_for_declared_load_barrier(self):
        class Reader:
            def verify_roots(self):pass
            def query(self,*args,**kwargs):raise RuntimeError('deliberate post-barrier failure')
            def close(self):pass
        class Queue:
            def __init__(self):self.rows=[]
            def put(self,row):self.rows.append(row)
        class Go:
            def wait(self,timeout):return True
        class Value:
            def __init__(self,value):self.value=value
        queue=Queue()
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'client.jsonl'
            with patch('tools.hierarchical_sharding.subtree_benchmark.TreeCoordinator.from_files',return_value=Reader()):
                with self.assertRaisesRegex(RuntimeError,'deliberate post-barrier'):
                    load_process(('tree','placement',[dict(query_id='q',vector=[1])],2,1000,None,1,0,str(path),queue,Go(),Value(time.monotonic()-.01),Value(False)))
            rows=[json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([r['status'] for r in rows],['ready','error','client_failure']);self.assertEqual(queue.rows[0]['status'],'ready')

class OutcomeTest(unittest.TestCase):
    def test_partial_failed_client_successes_are_counted(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)/'journal';p.write_text('\n'.join(json.dumps(dict(status=status)) for status in ('ready','ok','error','client_failure'))+'\n{partial')
            counts,successes=journal_outcomes([p]);self.assertEqual(counts,dict(ready=1,ok=1,error=1,client_failure=1,invalid=1));self.assertEqual(len(successes),1)

def _spawn_barrier_client(args):
    class Reader:
        def verify_roots(self):pass
        def query(self,*args,**kwargs):
            time.sleep(.01);return dict(ids=[1],actual_hosts=['fixture'],global_candidate_count=1)
        def close(self):pass
    with patch('tools.hierarchical_sharding.subtree_benchmark.TreeCoordinator.from_files',return_value=Reader()):return load_process(args)

class SpawnBarrierTest(unittest.TestCase):
    def test_real_spawn_clients_wait_until_all_roots_are_ready(self):
        import multiprocessing,concurrent.futures
        with tempfile.TemporaryDirectory() as d,multiprocessing.get_context('spawn').Manager() as manager:
            ready=manager.Queue();go=manager.Event();start=manager.Value('d',0.0);abort=manager.Value('b',False)
            paths=[pathlib.Path(d)/str(i) for i in range(2)]
            with concurrent.futures.ProcessPoolExecutor(max_workers=2,mp_context=multiprocessing.get_context('spawn')) as pool:
                futures=[pool.submit(_spawn_barrier_client,('tree','placement',[dict(query_id='q',vector=[1])],2,1000,None,.1,i,str(paths[i]),ready,go,start,abort)) for i in range(2)]
                self.assertEqual({ready.get(timeout=15)['client'] for _ in range(2)},{0,1})
                for path in paths:self.assertEqual([json.loads(v)['status'] for v in path.read_text().splitlines()],['ready'])
                start.value=time.monotonic()+.05;go.set();records=[f.result(timeout=15) for f in futures]
                self.assertTrue(all(records));self.assertTrue(all(r['finished_monotonic']>=start.value for rows in records for r in rows))

class SelectionTest(unittest.TestCase):
    def test_fastest_setting_must_meet_frozen_calibration_target(self):
        rows=[dict(beam_width=64,candidate_budget=1000,recall_at_100=.94,mean_s=.01),dict(beam_width=128,candidate_budget=1000,recall_at_100=.96,mean_s=.03),dict(beam_width=256,candidate_budget=4000,recall_at_100=.99,mean_s=.1)]
        self.assertEqual(choose_setting(rows,.95)['beam_width'],128)
        self.assertEqual(choose_setting(rows,1)['beam_width'],256)

class CalibrationFailureTest(unittest.TestCase):
    def test_failed_setting_retains_query_error_and_cannot_be_selected(self):
        class Reader:
            def verify_roots(self):pass
            def query(self,*args,**kwargs):raise RuntimeError('response exceeds payload limit')
            def close(self):pass
        with tempfile.TemporaryDirectory() as directory:
            output=pathlib.Path(directory)/'failed.jsonl'
            with patch('tools.hierarchical_sharding.subtree_benchmark.TreeCoordinator.from_files',return_value=Reader()):
                result=measure('tree','placement',[dict(query_id='q',vector=[1])],[dict(query_id='q',ids=[1])],1024,4000,output)
            self.assertEqual(result['status'],'failed');self.assertNotIn('recall_at_100',result)
            self.assertIn('payload limit',output.read_text());self.assertIsNone(choose_setting([result],.95))

if __name__=='__main__':unittest.main()
