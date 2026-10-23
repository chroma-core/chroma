import json,pathlib,tempfile,time,unittest
from unittest.mock import patch
from .subtree_benchmark import load_process

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

if __name__=='__main__':unittest.main()
