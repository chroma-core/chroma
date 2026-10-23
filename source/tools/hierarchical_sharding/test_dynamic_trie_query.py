"""Real coordinator ID resolution uses the pinned trie, with a flat-map oracle."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from .test_dynamic_write import make_parent,evidence_case
from .dynamic_runtime import DeploymentReader
from .dynamic_deployment import resolved_tree


def logical_trace(result):
    # Timing/transport/cache counters are intentionally outside logical parity.
    keys=('ids','distances','tree_sha256','generation','policy','beam_width','global_candidate_budget','global_candidate_count','candidate_contributions','wire_candidate_contributions','reached_leaves','levels','selected_candidates')
    return {key:result[key] for key in keys}


@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native reader required')
class TrieQuery(unittest.TestCase):
    def test_actual_query_trace_matches_flat_oracle_without_coordinator_flat_map_access(self):
        with evidence_case('trie-query-parity') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,_,_=make_parent(root,binary,page_rows=4,trie_fanout=4)
            actual=DeploymentReader(pointer,binary);oracle=DeploymentReader(pointer,binary,mapping_mode='flat-oracle')
            try:
                self.assertEqual(actual.coordinator.mappings,{})
                flatpaths={str(Path(s['local_to_global']['path']).resolve()) for s in resolved_tree(parent)['subtrees']}
                original=Path.open;proof=[]
                def reject_flat(path,*args,**kwargs):
                    if str(path.resolve()) in flatpaths:raise AssertionError('coordinator query reopened a flat-map oracle')
                    return original(path,*args,**kwargs)
                for query,beam,budget,k in [([0,0,0,0],None,16,16),([7,1,0,0],2,8,4),([17,1,0,0],None,16,8)]:
                    expected=oracle.query(query,k,beam_width=beam,candidate_budget=budget)
                    before=actual.mapping_metrics()
                    with patch.object(Path,'open',new=reject_flat):observed=actual.query(query,k,beam_width=beam,candidate_budget=budget)
                    self.assertEqual(logical_trace(observed),logical_trace(expected))
                    after=actual.mapping_metrics();self.assertGreater(after['lookup_calls'],before['lookup_calls'])
                    self.assertLessEqual(after['peak_cache_nodes'],32);self.assertLessEqual(after['peak_cache_owned_bytes'],262144)
                    proof.append(dict(query=query,k=k,beam=beam,budget=budget,actual=logical_trace(observed),oracle=logical_trace(expected),before=before,after=after))
                cold=actual.mapping_metrics()
                actual.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
                warm=actual.mapping_metrics();self.assertEqual(warm['metadata_disk_reads'],cold['metadata_disk_reads'])
                (root/'query-parity-and-mapper-metrics.json').write_text(json.dumps(dict(queries=proof,cold=cold,warm=warm,worker_limitation='Rust worker still verifies/emits global IDs with its retained flatmap; only coordinator runtime mapping replaced'),indent=2)+'\n')
            finally:actual.close();oracle.close()


if __name__=='__main__':unittest.main()
