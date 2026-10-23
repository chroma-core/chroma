import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch,MagicMock
import controller
from manifest_bridge import EXPECTED_CONFIG

class ControllerTests(unittest.TestCase):
    def fixture(self,root):
        q=root/'queries';queries=[dict(query_id=f'q{i}',vector=[0.0]*768) for i in range(16)];q.write_text(''.join(json.dumps(x)+'\n' for x in queries))
        return dict(output=str(root/'out'),deadline_unix=time.time()+899,rows=2000000,policy=dict(navigation='fp',beam=32,tau=2.0,k=100,rerank_factor=4),binary=dict(path='newbinary',sha256='new'),manifest=dict(sha256='s'),query_file=dict(path=str(q)),vectors=dict(path='v'),query_ids=[x['query_id'] for x in queries])
    def process(self,target,args,timeout):
        if target is controller._bridge_process:Path(args[1]).write_text(json.dumps(dict(config=EXPECTED_CONFIG)))
    def child(self,argv,output,timeout,stdin=None):
        if '--exact' in argv:
            job=json.loads(Path(argv[-1]).read_text());Path(job['output']).write_text(json.dumps([dict(query_id=f'q{i}',ids=list(range(100))) for i in range(16)]));return
        request=[json.loads(x) for x in stdin.splitlines()];budget=dict(rows=2000000,fp_nodes=4096,fp_payload_bytes=16777216,gt_ids=100,beam=32,k=100,rerank_factor=4,candidate_budget=400,dimension=768,metric='ip',tau=2.0,navigation='fp',construction_config=EXPECTED_CONFIG)
        rows=[dict(depth=3,nodes=1780,leaves=1752,budgets=budget,fp=dict(vector_access_complete=False))]
        for q in request:rows.append(dict(query_id=q['query_id'],snapshot_sha256='s',gt=dict(complete=True,ground_truth_count=100,reached_ids=list(range(100)),retained_ids=list(range(100)),returned_ids=list(range(100)),navigation_missing=0,trim_missing=0,exact_missing=0),ids=list(range(100)),distance_bits=[0]*100,work=dict(navigation_level_trace=dict(complete=True,levels=[])),selected_leaves=64,posting_rows=65000))
        if self.badreply:rows[1]['query_id']='bad-warmup'
        output.write_text(''.join(json.dumps(x)+'\n' for x in rows))
    def execute(self,failure=None,badreply=False):
        self.badreply=badreply
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);spec=self.fixture(root)
            def process(target,args,timeout):
                if failure=='bridge' and target is controller._bridge_process:raise TimeoutError('bridge timeout')
                self.process(target,args,timeout)
            def child(argv,output,timeout,stdin=None):
                if failure=='gt' and '--exact' in argv:raise TimeoutError('GT timeout')
                if failure=='reader' and '--bridge' in argv:raise TimeoutError('reader timeout')
                self.child(argv,output,timeout,stdin)
            with patch.object(controller,'OUTPUT_PREFIX',str(root)+'/'),patch.object(controller,'bounded_process',side_effect=process),patch.object(controller,'limited_child',side_effect=child):
                value=controller.run(spec)
            self.assertTrue((root/'out/result.json').is_file())
            return value
    def test_complete_mock_and_all_warmup_binding(self):
        self.assertEqual(self.execute()['status'],'completed')
        self.assertEqual(self.execute(badreply=True)['status'],'failed')
    def test_each_timed_stage_keeps_failure_receipt(self):
        for stage in ('bridge','gt','reader'):
            with self.subTest(stage=stage):self.assertEqual(self.execute(failure=stage)['status'],'failed')
    def test_spawn_failure_still_reconciles_child(self):
        proc=MagicMock();proc.pid=123;proc.is_alive.return_value=True;proc.join.side_effect=[RuntimeError('join failure'),None]
        ctx=MagicMock();ctx.Process.return_value=proc
        with patch.object(controller.multiprocessing,'get_context',return_value=ctx),self.assertRaises(RuntimeError):controller.bounded_process(lambda:None,(),.01)
        proc.kill.assert_called_once();self.assertEqual(proc.join.call_count,2)
if __name__=='__main__':unittest.main()
