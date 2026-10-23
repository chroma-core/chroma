"""Explicit opt-in candidate limits; no silent policy fallback."""
import json
import os
import unittest
from unittest.mock import Mock
from .subtree_transport import validate_candidate_limits,PinnedReader,TreeError
from .test_streaming_policy import LIMITS
CANDIDATE_LIMITS=dict(max_retained_entries=16,max_staging_entries=1,max_cutoff_ties=16,max_output_entries=16,max_seen_bitmap_bytes=16)
class Policy(unittest.TestCase):
    def test_strict_policy_and_required_streaming(self):
        from .dynamic_runtime import DeploymentReader
        for row in (dict(CANDIDATE_LIMITS,max_staging_entries=2),dict(CANDIDATE_LIMITS,max_seen_bitmap_bytes=True),dict(CANDIDATE_LIMITS,unknown=1)):
            with self.assertRaises(ValueError):validate_candidate_limits(row)
        with self.assertRaises(ValueError):DeploymentReader('/not-opened','/not-opened',candidate_limits=CANDIDATE_LIMITS)
    def test_old_worker_ignoring_candidate_policy_is_rejected(self):
        pinned=PinnedReader.__new__(PinnedReader);pinned.descriptor={'snapshot_sha256':'hash'};pinned.snapshot={'generation':1,'shard_id':'a'};pinned.streaming_limits=dict(LIMITS);pinned.candidate_limits=dict(CANDIDATE_LIMITS);pinned.reader=Mock()
        metrics=dict(physical_entries=0,physical_posting_payload_bytes=0,current_entries_scored=0,peak_borrowed_leaf_entries=0,owned_posting_array_bytes=0,peak_candidate_entries=0,peak_candidate_capacity=0,peak_candidate_capacity_payload_bytes=0,residual_vector_capacity_bytes=0)
        pinned.reader.query.return_value=dict(streaming_limits=LIMITS,streaming_metrics=metrics)
        with self.assertRaisesRegex(TreeError,'candidate policy'):pinned.call(dict(subtree_id='a',snapshot_sha256='hash',payload=dict(op='score_selected_leaves')))

@unittest.skipUnless(os.environ.get('SPANN_CANDIDATE_BINARY') and os.environ.get('SPANN_RETAINED_STREAM_POINTER'),'admitted reader and retained fixture required')
class NativePolicy(unittest.TestCase):
    def test_full_trace_ties_repeated_and_explicit_tie_overflow(self):
        from .test_dynamic_write import evidence_case
        from .dynamic_runtime import DeploymentReader
        from .test_dynamic_trie_query import logical_trace
        with evidence_case('candidate-full-query') as root:
            pointer=os.environ['SPANN_RETAINED_STREAM_POINTER'];binary=os.environ['SPANN_CANDIDATE_BINARY'];readers={};proof=[];raw=[]
            def capture(reader,mode):
                for sid,pinned in reader.coordinator.backend.readers.items():
                    original=pinned.reader.query
                    def wrapped(payload,*args,_call=original,_sid=sid,**kwargs):
                        response=_call(payload,*args,**kwargs);raw.append(dict(mode=mode,subtree_id=_sid,request=payload,response=response));return response
                    pinned.reader.query=wrapped
            try:
                for mode in ('all-minima','working-set'):
                    readers[mode]=DeploymentReader(pointer,binary,timeout=5,worker_id_mode='local-only',profile_memory=True,streaming_limits=LIMITS,candidate_limits=CANDIDATE_LIMITS if mode=='working-set' else None);capture(readers[mode],mode)
                for query,budget in (([4.5,1,0,0],1),([4.5,1,0,0],1),([8.5,1,0,0],16)):
                    rows={mode:reader.query(query,min(4,budget),beam_width=None,candidate_budget=budget,timeout=5) for mode,reader in readers.items()};self.assertEqual(logical_trace(rows['all-minima']),logical_trace(rows['working-set']));proof.append(dict(query=query,budget=budget,results=rows))
                head=__import__('pathlib').Path(pointer).read_bytes();limited=DeploymentReader(pointer,binary,timeout=5,worker_id_mode='local-only',profile_memory=True,streaming_limits=LIMITS,candidate_limits=dict(CANDIDATE_LIMITS,max_cutoff_ties=1));readers['limited']=limited;capture(limited,'limited')
                with self.assertRaises(TreeError):limited.query([4.5,1,0,0],1,beam_width=None,candidate_budget=1,timeout=5)
                self.assertEqual(__import__('pathlib').Path(pointer).read_bytes(),head)
                failed=next(row for row in raw if row['mode']=='limited' and 'error' in row['response']);pinned=limited.coordinator.backend.readers[failed['subtree_id']]
                for key in ('materialized_leaves','posting_block_pins','vector_block_pins'):self.assertEqual(failed['response']['retained_postings'][key],0)
                # Retry the same Rust process with a new explicit per-request
                # policy; the pinned adapter itself is never reconfigured.
                request=dict(failed['request'],candidate_limits=CANDIDATE_LIMITS,request_id='retry-with-valid-tie-budget');retry=pinned.reader.query(request,timeout=5);self.assertNotIn('error',retry)
                self.assertEqual(retry['streaming_metrics']['candidate_working_set']['unique_scanned'],retry['unique_candidates_scanned'])
                for key in ('materialized_leaves','posting_block_pins','vector_block_pins'):self.assertEqual(retry['retained_postings'][key],0)
            finally:
                for reader in readers.values():reader.close()
                (root/'full-trace-and-tie-failure.json').write_text(json.dumps(dict(queries=proof,raw_operations=raw,scope='same global beam/M, exact logical traces; count/capacity payload metrics exclude allocator/BTree/output overlap/RSS'),indent=2)+'\n')
