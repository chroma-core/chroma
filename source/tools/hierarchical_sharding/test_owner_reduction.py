"""Validate all contributions before exact owner-budget reduction."""
import copy
import hashlib
import json
from pathlib import Path
import random
import struct
import tempfile
import unittest
import threading
from .owner_reduction import PinnedMap,reduce_owner_batch,validate_summary
from .test_subtree_transport import fixture


class Mapping:
    generation=1;metric='l2';dimension=1;source_id='source'
    def __init__(self,ids):self.ids=ids;self.count=len(ids)
    def get(self,local):
        if type(local) is not int or not 0<=local<len(self.ids):raise ValueError('outside local range')
        return self.ids[local]


def batch(values,budget):
    requests=[];responses=[];maps={}
    for sid,rows in values.items():
        ids=[row[0] for row in rows];maps[sid]=Mapping(ids)
        candidates=[dict(local_id=i,id=ident,distance=distance) for i,(ident,distance) in enumerate(rows)]
        requests.append(dict(subtree_id=sid,snapshot_sha256=sid,payload=dict(op='score_selected_leaves',query=[0.],candidate_budget=budget)))
        responses.append(dict(subtree_id=sid,snapshot_sha256=sid,result=dict(generation=1,candidates=candidates,actual_candidate_count=len(candidates),candidate_budget=budget,unique_candidates_scanned=len(candidates))))
    return requests,responses,maps


class ReductionTests(unittest.TestCase):
    def test_owner_cutoff_uses_external_ids_and_preserves_work_counts(self):
        requests,responses,maps=batch(dict(a=[(90,0.),(2,0.)],b=[(8,0.),(1,0.)]),2)
        output,metrics=reduce_owner_batch(requests,responses,maps)
        self.assertEqual(sorted(c['id'] for r in output for c in r['result']['candidates']),[1,2])
        self.assertEqual(metrics['subtree_contributions_before_reduction'],4);self.assertEqual(metrics['wire_candidates_returned'],2)
        self.assertEqual(metrics['live_unique_candidates_scored'],4)
        self.assertEqual(sum(r['result']['candidate_contributions_before_owner_reduction'] for r in output),4)
        self.assertEqual(len(responses[0]['result']['candidates']),2)

    def test_same_id_duplicates_take_minimum_before_budget(self):
        requests,responses,maps=batch(dict(a=[(5,10.),(6,2.)],b=[(7,3.)]),3)
        duplicate=dict(local_id=0,id=5,distance=1.)
        responses[0]['result']['candidates'].append(duplicate);responses[0]['result']['actual_candidate_count']=3
        output,metrics=reduce_owner_batch(requests,responses,maps)
        hits=sorted((c['id'],c['distance']) for r in output for c in r['result']['candidates'])
        self.assertEqual(hits,[(5,1.),(6,2.),(7,3.)]);self.assertEqual(metrics['owner_unique_candidates_before_reduction'],3)

    def test_invalid_discardable_rows_and_mixed_policies_fail_before_cutoff(self):
        original=batch(dict(a=[(1,0.)],b=[(2,99.)]),1)
        for change in ('bad_id','bad_local','nan','query','budget','generation','metric','source','duplicate_ownership'):
            requests,responses,maps=copy.deepcopy(original)
            if change=='bad_id':responses[1]['result']['candidates'][0]['id']=999
            elif change=='bad_local':responses[1]['result']['candidates'][0]['local_id']=9
            elif change=='nan':responses[1]['result']['candidates'][0]['distance']=float('nan')
            elif change=='query':requests[1]['payload']['query']=[1.]
            elif change=='budget':requests[1]['payload']['candidate_budget']=2
            elif change=='generation':responses[1]['result']['generation']=2
            elif change=='metric':maps['b'].metric='ip'
            elif change=='source':maps['b'].source_id='other'
            else:maps['b'].ids=[1];responses[1]['result']['candidates'][0]['id']=1
            with self.subTest(change=change),self.assertRaises(ValueError):reduce_owner_batch(requests,responses,maps)

    def test_random_partition_prefix_proof_and_global_ties(self):
        rng=random.Random(88)
        for _ in range(200):
            budget=rng.randrange(1,12);values={str(i):[] for i in range(4)}
            for ident in range(60):values[str(rng.randrange(4))].append((ident,float(rng.randrange(5))))
            all_rows=[r for rows in values.values() for r in rows]
            # Existing subtree contribution is already its exact top M.
            values={sid:sorted(rows,key=lambda r:(r[1],r[0]))[:budget] for sid,rows in values.items()}
            requests,responses,maps=batch(values,budget);output,_=reduce_owner_batch(requests,responses,maps)
            actual=sorted([(c['id'],c['distance']) for r in output for c in r['result']['candidates']],key=lambda r:(r[1],r[0]))
            self.assertEqual(actual,sorted(all_rows,key=lambda r:(r[1],r[0]))[:budget])

    def test_coordinator_frontiers_selected_candidates_exact_results_and_work_match(self):
        baseline,_=fixture(a=(1,-1),b=(2,-2));reduced,backend=fixture(a=(1,-1),b=(2,-2))
        original=backend.call_many
        def call(items,tree_hash,timeout):
            rows,traces=original(items,tree_hash,timeout)
            if items[0]['payload']['op']=='score_selected_leaves':
                for item,row in zip(items,rows):
                    row['result'].update(actual_candidate_count=len(row['result']['candidates']),candidate_budget=item['payload']['candidate_budget'],unique_candidates_scanned=len(row['result']['candidates']))
                maps={sid:Mapping(value['map']) for sid,value in backend.rows.items()}
                rows,_=reduce_owner_batch(items,rows,maps)
            return rows,traces
        backend.call_many=call
        a=baseline.query([0],2,beam_width=None,candidate_budget=2);b=reduced.query([0],2,beam_width=None,candidate_budget=2)
        for field in ('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions'):
            self.assertEqual(a[field],b[field],field)
        self.assertEqual(a['wire_candidate_contributions'],4);self.assertEqual(b['wire_candidate_contributions'],2)

    def test_global_generation_two_allows_unchanged_and_changed_subtree_pins(self):
        outputs=[]
        for enabled in (False,True):
            coordinator,backend=fixture(a=(1,-1),b=(1,-1))
            coordinator.tree['tree_generation']=2
            coordinator.subtrees['a']['generation']=1
            coordinator.subtrees['b']['generation']=2
            from .tree_manifest import content_hash
            coordinator.tree_hash=content_hash(coordinator.tree)
            original=backend.call_many
            def call(items,tree_hash,timeout):
                rows,traces=original(items,tree_hash,timeout)
                for item,row in zip(items,rows):
                    row['result']['generation']=coordinator.subtrees[item['subtree_id']]['generation']
                if items[0]['payload']['op']=='score_selected_leaves':
                    for item,row in zip(items,rows):
                        row['result'].update(actual_candidate_count=len(row['result']['candidates']),candidate_budget=item['payload']['candidate_budget'],unique_candidates_scanned=len(row['result']['candidates']))
                    if enabled:
                        maps={sid:Mapping(value['map']) for sid,value in backend.rows.items()}
                        for sid,mapping in maps.items():mapping.generation=coordinator.subtrees[sid]['generation']
                        rows,summary=reduce_owner_batch(items,rows,maps)
                        validate_summary(rows,summary)
                return rows,traces
            backend.call_many=call
            outputs.append(coordinator.query([0],2,beam_width=None,candidate_budget=2))
        for field in ('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions'):
            self.assertEqual(outputs[0][field],outputs[1][field],field)
        self.assertEqual(outputs[1]['ids'],[0,1])
        self.assertEqual(outputs[1]['candidate_contributions'],4)
        self.assertEqual([r['wire_candidate_contributions'] for r in outputs],[4,2])

    def test_boolean_generation_counts_and_policy_are_rejected(self):
        for field in ('generation','actual_candidate_count','candidate_budget'):
            requests,responses,maps=batch(dict(a=[(1,0.)]),1);responses[0]['result'][field]=True
            with self.subTest(field=field),self.assertRaises(ValueError):reduce_owner_batch(requests,responses,maps)
        requests,responses,maps=batch(dict(a=[(1,0.)],b=[(2,1.)]),1);requests[1]['payload']['candidate_budget']=True
        with self.assertRaises(ValueError):reduce_owner_batch(requests,responses,maps)
        requests,responses,maps=batch(dict(a=[(1,0.)],b=[(2,1.)]),1)
        requests[0]['payload']['query']=[1.];requests[1]['payload']['query']=[True]
        with self.assertRaises(ValueError):reduce_owner_batch(requests,responses,maps)

    def test_summary_scoring_total_must_match_exact_subtree_sum(self):
        requests,responses,maps=batch(dict(a=[(1,0.)],b=[(2,1.)]),1)
        output,summary=reduce_owner_batch(requests,responses,maps);summary['live_unique_candidates_scored']=999
        with self.assertRaises(ValueError):validate_summary(output,summary)

    def test_summary_mismatch_rejects(self):
        requests,responses,maps=batch(dict(a=[(1,0.)],b=[(2,1.)]),1)
        output,summary=reduce_owner_batch(requests,responses,maps);validate_summary(output,summary)
        summary['wire_candidates_returned']+=1
        with self.assertRaises(ValueError):validate_summary(output,summary)

    def test_optional_http_adapter_matches_direct_global_selection(self):
        from .subtree_server import Host,make_server
        from .subtree_transport import HttpBackend
        from .tree_manifest import content_hash
        direct,_=fixture(a=(1,-1),b=(2,-2));remote,backend=fixture(a=(1,-1),b=(2,-2))
        class Reader:
            def call(self,item,timeout):
                response=backend.call_many([item],content_hash(remote.tree),timeout)[0][0]
                if item['payload']['op']=='score_selected_leaves':
                    result=response['result'];result.update(actual_candidate_count=len(result['candidates']),candidate_budget=item['payload']['candidate_budget'],unique_candidates_scanned=len(result['candidates']))
                return response
        host=Host.__new__(Host);host.configuration=dict(tree_sha256=content_hash(remote.tree),owner_id='owner',timeout=5);host.readers={sid:Reader() for sid in ('a','b')};host.reduction_maps={sid:Mapping(value['map']) for sid,value in backend.rows.items()}
        server=make_server(host);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            remote.backend=HttpBackend(dict(owners={sid:dict(owner_id='owner',endpoint='http://127.0.0.1:'+str(server.server_port)) for sid in ('a','b')}))
            expected=direct.query([0],2,beam_width=None,candidate_budget=2);actual=remote.query([0],2,beam_width=None,candidate_budget=2)
            for key in ('ids','distances','levels','selected_candidates','candidate_contributions'):self.assertEqual(expected[key],actual[key])
            self.assertEqual(actual['wire_candidate_contributions'],2);self.assertTrue(any('owner_reduction' in t for t in actual['transport']))
        finally:server.shutdown();server.server_close();thread.join()

    def test_pinned_map_hash_count_and_local_range(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'id-map-1.u64le';path.write_bytes(struct.pack('<QQ',9,2))
            snapshot=dict(vector_count=2,generation=1,metric='ip',dimension=1,source_id='s',id_mapping=dict(path=path.name,count=2,bytes=16,sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
            mapping=PinnedMap(directory,snapshot)
            try:
                self.assertEqual(mapping.get(1),2)
                with self.assertRaises(ValueError):mapping.get(2)
            finally:mapping.close()
            snapshot['generation']=True
            with self.assertRaises(ValueError):PinnedMap(directory,snapshot)
            snapshot['generation']=1
            snapshot['id_mapping']['sha256']='bad'
            with self.assertRaisesRegex(ValueError,'checksum'):PinnedMap(directory,snapshot)

if __name__=='__main__':unittest.main()
