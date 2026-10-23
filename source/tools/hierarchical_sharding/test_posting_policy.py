"""Opt-in posting release through the full pinned coordinator path."""
import json
import os
import unittest
from unittest.mock import patch
from .dynamic_runtime import DeploymentReader
from .subtree_transport import PinnedReader

class PolicyArgs(unittest.TestCase):
    def test_unknown_external_policy_and_nonboolean_fail_before_open(self):
        for flags in (dict(evict_postings_after_operation=1),dict(profile_memory=1),dict(backend=object(),evict_postings_after_operation=True)):
            with self.assertRaises(ValueError):DeploymentReader('/not-opened','/not-opened',**flags)

@unittest.skipUnless(os.environ.get('SPANN_EVICT_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted retained native readers required')
class RealPolicy(unittest.TestCase):
    def test_full_local_id_query_traces_ties_repeated_queries_and_arrays(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .test_dynamic_trie_query import logical_trace
        with evidence_case('posting-policy-full-coordinator') as root:
            parent,pointer,_,_=make_parent(root,os.environ['SPANN_DYNAMIC_BINARY'],page_rows=4,trie_fanout=4)
            binary=os.environ['SPANN_EVICT_BINARY'];readers={};rows=[]
            try:
                for mode in ('retain','evict'):
                    readers[mode]=DeploymentReader(pointer,binary,worker_id_mode='local-only',evict_postings_after_operation=mode=='evict',profile_memory=True)
                # Half-way coordinates create exact-distance ties; repeat the
                # same query to expose the release/re-materialization tradeoff.
                for query,budget in (([4.5,1,0,0],1),([4.5,1,0,0],1),([8.5,1,0,0],16),([8.5,1,0,0],16)):
                    results={mode:reader.query(query,4,beam_width=None,candidate_budget=budget) for mode,reader in readers.items()}
                    self.assertEqual(logical_trace(results['retain']),logical_trace(results['evict']))
                    for operation in results['evict']['worker_operations']:
                        metrics=operation['result']['retained_postings']
                        self.assertEqual(metrics['materialized_leaves'],0);self.assertEqual(metrics['payload_bytes'],0)
                    rows.append(dict(query=query,candidate_budget=budget,results=results))
                self.assertTrue(any(op['result']['retained_postings']['evicted_leaves'] for row in rows for op in row['results']['evict']['worker_operations']))
                self.assertTrue(any(row['results']['evict']['wire_candidate_contributions']>row['results']['evict']['candidate_contributions'] for row in rows))
                metrics={mode:reader.mapping_metrics() for mode,reader in readers.items()}
                self.assertTrue(all(m['mapped_flat_files']==0 for m in metrics.values()))
                (root/'full-query-parity.json').write_text(json.dumps(dict(queries=rows,mapper_metrics=metrics,scope='same global beam/M, exact logical query traces; memory counters exclude RSS/provider/transient bytes'),indent=2)+'\n')
            finally:
                for reader in readers.values():reader.close()
