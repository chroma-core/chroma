"""Map every boundary tie before the external-ID cutoff; reject malformed rows."""
import copy
import json
import os
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from . import ownership_trie as trie
from .subtree_transport import TreeCoordinator,TreeError


class LocalProtocol(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=trie.create({'a':[2,0,1]},Path(self.temp.name).resolve()/'initial',page_rows=2)
        self.mapper=trie.PinnedReader(trie.audit(root)[0])
        self.coordinator=TreeCoordinator(dict(subtrees=[dict(subtree_id='a',vector_count=3)]),{},None,id_mapper=self.mapper,local_ids_only=True)
        self.payload=dict(op='score_selected_leaves',candidate_budget=2)
        self.result=dict(id_mode='local-only',candidates=[dict(local_id=i,distance=1.) for i in range(3)],actual_candidate_count=3,candidate_budget=2,unique_candidates_scanned=3)

    def test_all_boundary_ties_map_before_global_id_cutoff(self):
        result=self.coordinator._resolve_local_result('a',self.payload,self.result)
        self.assertEqual([row['id'] for row in result['candidates']],[0,1])
        self.assertEqual([row['local_id'] for row in result['candidates']],[1,2])
        self.assertEqual(result['actual_candidate_count'],2);self.assertEqual(result['wire_candidate_count_before_mapping'],3)

    def test_invalid_discarded_row_and_mixed_mode_accounting_reject(self):
        mutations=[lambda r:r['candidates'][-1].update(local_id=True),lambda r:r['candidates'][-1].update(id=99),lambda r:r['candidates'][-1].update(distance=float('nan')),lambda r:r['candidates'][-1].update(local_id=0),lambda r:r.update(actual_candidate_count=True),lambda r:r.update(candidate_budget=True),lambda r:r.update(id_mode='global')]
        for mutation in mutations:
            result=copy.deepcopy(self.result);mutation(result)
            with self.assertRaises((ValueError,TreeError)):self.coordinator._resolve_local_result('a',self.payload,result)

    def test_exact_rerank_requires_complete_selected_local_set(self):
        payload=dict(op='rerank_selected',local_ids=[1,2])
        result=dict(id_mode='local-only',results=[dict(local_id=2,distance=.5),dict(local_id=1,distance=.25)],exact_vectors_scored=2)
        actual=self.coordinator._resolve_local_result('a',payload,result)
        self.assertEqual([r['id'] for r in actual['results']],[1,0])
        result['results'].pop()
        with self.assertRaises(TreeError):self.coordinator._resolve_local_result('a',payload,result)

    def test_local_only_owner_reduction_rejects_before_reader_start(self):
        from .subtree_server import Host
        with patch('tools.hierarchical_sharding.subtree_server.PinnedReader') as reader:
            with self.assertRaisesRegex(ValueError,'not admitted with reduction'):
                Host(dict(owner_candidate_reduction=True,subtrees={'a':dict(local_ids_only=True)}))
            reader.assert_not_called()


@unittest.skipUnless(os.environ.get('SPANN_LOCAL_ONLY_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'independently admitted local-only native reader required')
class RealLocalProtocol(unittest.TestCase):
    def test_real_local_only_queries_match_global_oracle_and_preserve_boundary_ties(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .test_dynamic_trie_query import logical_trace
        from .dynamic_runtime import DeploymentReader
        with evidence_case('local-only-query-parity') as root:
            old_binary=os.environ['SPANN_DYNAMIC_BINARY'];binary=os.environ['SPANN_LOCAL_ONLY_BINARY']
            parent,pointer,_,_=make_parent(root,old_binary,page_rows=4,trie_fanout=4)
            actual=DeploymentReader(pointer,binary,worker_id_mode='local-only');oracle=DeploymentReader(pointer,old_binary,mapping_mode='flat-oracle');proof=[]
            try:
                for query,k,budget in [([0,0,0,0],16,16),([4.5,1,0,0],1,1),([17,1,0,0],4,8)]:
                    observed=actual.query(query,k,beam_width=None,candidate_budget=budget)
                    expected=oracle.query(query,k,beam_width=None,candidate_budget=budget)
                    a=logical_trace(observed);b=logical_trace(expected)
                    # Boundary ties can add wire rows, never logical M/work.
                    awire=a.pop('wire_candidate_contributions');bwire=b.pop('wire_candidate_contributions')
                    self.assertEqual(a,b)
                    if budget==1:self.assertGreater(awire,bwire)
                    self.assertTrue(all(w['result'].get('id_mode')=='local-only' for w in observed['worker_operations']))
                    proof.append(dict(query=query,k=k,budget=budget,actual=observed,oracle=expected))
                self.assertEqual(actual.coordinator.mappings,{})
                self.assertIn('flat maps startup/admission oracle only',actual.mapping_metrics()['scope'])
                (root/'parity.json').write_text(json.dumps(dict(queries=proof,mapper_metrics=actual.mapping_metrics()),indent=2)+'\n')
            finally:actual.close();oracle.close()

    def test_real_local_reader_opens_scores_and_reranks_without_flat_map_file(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .server import Reader
        with evidence_case('local-only-missing-map') as root:
            old_binary=os.environ['SPANN_DYNAMIC_BINARY'];binary=os.environ['SPANN_LOCAL_ONLY_BINARY']
            parent,_,_,_=make_parent(root,old_binary,page_rows=4,trie_fanout=4)
            directory=root/'private-missing-map';shutil.copytree(parent['owners']['s0']['checkpoint_dir'],directory)
            manifest=json.loads((directory/'manifest-1.json').read_text());mapping=directory/manifest['id_mapping']['path']
            mapping.rename(root/'removed-map.u64le')
            argv=[binary,'read','--checkpoint-dir',str(directory),'--generation','1'];reader=Reader(argv+['--local-ids-only'],10);responses=[]
            def call(op,**kwargs):
                response=reader.query(dict(op=op,generation=1,request_id=str(len(responses)),**kwargs));responses.append(response)
                self.assertEqual(response.get('id_mode'),'local-only');self.assertNotIn('error',response);return response
            try:
                described=call('describe_root');pending=[described['root']];leaves=[]
                while pending:
                    internal=[]
                    for node in pending:
                        if node['is_leaf']:leaves.append(node['node_id'])
                        else:internal.append(node['node_id'])
                    pending=call('expand',nodes=internal,query=[4.5,1,0,0])['children'] if internal else []
                scored=call('score_selected_leaves',nodes=leaves,query=[4.5,1,0,0],candidate_budget=1)
                self.assertEqual({r['local_id'] for r in scored['candidates']},{3,4});self.assertTrue(all('id' not in r for r in scored['candidates']))
                exact=call('rerank_selected',local_ids=[3,4],query=[4.5,1,0,0]);self.assertEqual([r['distance'] for r in exact['results']],[.25,.25])
                self.assertTrue(all('id' not in r for r in exact['results']))
            finally:reader.close()
            global_mode=subprocess.run(argv,input=json.dumps(dict(op='describe_root',request_id='global',generation=1))+'\n',capture_output=True,text=True,timeout=10)
            self.assertNotEqual(global_mode.returncode,0)
            (root/'missing-map-proof.json').write_text(json.dumps(dict(argv=argv,local_responses=responses,global_mode_returncode=global_mode.returncode,global_mode_stderr=global_mode.stderr),indent=2)+'\n')


if __name__=='__main__':unittest.main()
