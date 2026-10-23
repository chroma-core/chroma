"""The serial posting policy preserves the complete coordinator contract."""
import json
import os
import unittest
from .subtree_transport import validate_streaming_limits

LIMITS=dict(max_leaf_entries=100000,max_total_entries=1000000,max_total_posting_bytes=64*1024*1024,max_unique_candidates=100000)
class Limits(unittest.TestCase):
    def test_limits_are_explicit_strict_and_owned(self):
        result=validate_streaming_limits(LIMITS);self.assertEqual(result,LIMITS);self.assertIsNot(result,LIMITS)
        for row in (dict(LIMITS,max_unique_candidates=True),dict(LIMITS,max_total_entries=0),dict(LIMITS,extra=1)):
            with self.assertRaises(ValueError):validate_streaming_limits(row)

    def test_worker_silently_ignoring_streaming_policy_is_rejected(self):
        from .subtree_transport import PinnedReader,TreeError
        from unittest.mock import Mock
        pinned=PinnedReader.__new__(PinnedReader);pinned.descriptor={'snapshot_sha256':'hash'};pinned.snapshot={'generation':1,'shard_id':'a'};pinned.streaming_limits=dict(LIMITS);pinned.candidate_limits=None
        pinned.reader=Mock();pinned.reader.query.return_value={}
        with self.assertRaises(TreeError):pinned.call(dict(subtree_id='a',snapshot_sha256='hash',payload=dict(op='score_selected_leaves')))

@unittest.skipUnless(os.environ.get('SPANN_STREAM_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'reviewed native streaming reader required')
class NativeStreaming(unittest.TestCase):
    def test_full_coordinator_ties_repeat_and_whole_request_budget_failure(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .test_dynamic_trie_query import logical_trace
        from .dynamic_runtime import DeploymentReader
        from .subtree_transport import TreeError
        with evidence_case('streaming-full-query') as root:
            _,pointer,_,_=make_parent(root,os.environ['SPANN_DYNAMIC_BINARY'],page_rows=4,trie_fanout=4);readers={};raw=[];proof=[]
            def capture(reader,mode):
                for sid,pinned in reader.coordinator.backend.readers.items():
                    original=pinned.reader.query
                    def wrapped(payload,*args,_call=original,_sid=sid,**kwargs):
                        result=_call(payload,*args,**kwargs);raw.append(dict(mode=mode,subtree_id=_sid,request=payload,response=result));return result
                    pinned.reader.query=wrapped
            try:
                for mode in ('retain','stream'):
                    readers[mode]=DeploymentReader(pointer,os.environ['SPANN_STREAM_BINARY'],worker_id_mode='local-only',profile_memory=True,streaming_limits=LIMITS if mode=='stream' else None);capture(readers[mode],mode)
                for query,budget in (([4.5,1,0,0],1),([4.5,1,0,0],1),([8.5,1,0,0],16)):
                    rows={mode:reader.query(query,min(4,budget),beam_width=None,candidate_budget=budget) for mode,reader in readers.items()};self.assertEqual(logical_trace(rows['retain']),logical_trace(rows['stream']))
                    for op in rows['stream']['worker_operations']:
                        if op['op']=='score_selected_leaves':
                            metrics=op['result']['streaming_metrics'];self.assertEqual(metrics['owned_posting_array_bytes'],0);self.assertEqual(op['result']['retained_postings']['materialized_leaves'],0)
                    proof.append(dict(query=query,budget=budget,results=rows))
                head=pointer.read_bytes();limited=DeploymentReader(pointer,os.environ['SPANN_STREAM_BINARY'],worker_id_mode='local-only',profile_memory=True,streaming_limits=dict(LIMITS,max_unique_candidates=1));readers['limited']=limited;capture(limited,'limited')
                with self.assertRaises(TreeError):limited.query([8.5,1,0,0],4,beam_width=None,candidate_budget=16)
                self.assertEqual(pointer.read_bytes(),head)
                for sid,pinned in limited.coordinator.backend.readers.items():
                    response=pinned.reader.query(dict(op='describe_root',generation=pinned.snapshot['generation'],profile_memory=True,request_id='after-error'))
                    self.assertEqual(response['retained_postings']['posting_block_pins'],0);self.assertEqual(response['retained_postings']['vector_block_pins'],0);self.assertEqual(response['retained_postings']['materialized_leaves'],0)
                again=readers['stream'].query([8.5,1,0,0],4,beam_width=None,candidate_budget=16);self.assertEqual(logical_trace(again),logical_trace(proof[-1]['results']['stream']))
            finally:
                for reader in readers.values():reader.close()
                (root/'full-trace-and-budget.json').write_text(json.dumps(dict(queries=proof,raw_operations=raw,scope='one borrowed posting per scoring operation, provider/RSS/quantizer scratch excluded'),indent=2)+'\n')
