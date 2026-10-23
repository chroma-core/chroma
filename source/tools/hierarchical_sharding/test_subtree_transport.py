import copy
import unittest
import threading
from .subtree_transport import TreeCoordinator,TreeError


class FakeBackend:
    def __init__(self,graph,rows):
        self.graph,self.rows=graph,rows
        self.calls=[]

    def call_many(self,items,tree_hash,timeout):
        output=[]
        for item in items:
            sid=item['subtree_id'];payload=item['payload'];op=payload['op'];self.calls.append(copy.deepcopy(item))
            result=dict(generation=1)
            if op=='expand':
                result['children']=[dict(parent_id=n,node_id=child,is_leaf=leaf,distance=d)
                                    for n in payload['nodes'] for child,leaf,d in self.graph[sid][n]]
            elif op=='score_selected_leaves':
                ids=[local for node in payload['nodes'] for local in self.rows[sid]['leaves'][node]]
                ids=sorted(set(ids),key=lambda local:(self.rows[sid]['vectors'][local]**2,self.rows[sid]['map'][local]))[:payload['candidate_budget']]
                result['candidates']=[dict(local_id=local,id=self.rows[sid]['map'][local],distance=self.rows[sid]['vectors'][local]**2) for local in ids]
            elif op=='rerank_selected':
                result['results']=[dict(local_id=local,id=self.rows[sid]['map'][local],distance=self.rows[sid]['vectors'][local]**2) for local in payload['local_ids']]
            else:raise AssertionError(op)
            output.append(dict(subtree_id=sid,snapshot_sha256=item['snapshot_sha256'],result=result))
        return output,[dict(owner_id='fake',actual_host='fake',roundtrips=len({i['subtree_id'] for i in items}),operation_count=len(items))]


def fixture(a=(0,.1),b=(10,11)):
    rows={sid:dict(vectors=list(values),map=[2*n,2*n+1],leaves={1:[0],2:[1]}) for n,(sid,values) in enumerate((('a',a),('b',b)))}
    graph={sid:{0:[(1,True,values[0]**2),(2,True,values[1]**2)]} for sid,values in [('a',a),('b',b)]}
    tree=dict(tree_generation=1,dimension=1,metric='l2',source=dict(row_count=4),
              upper_root=dict(children=[dict(subtree_id=sid,centroid_index=n) for n,sid in enumerate(('a','b'))]),
              subtrees=[dict(subtree_id=sid,vector_count=2,generation=1,snapshot=dict(sha256=sid),root_ref=dict(kind='internal',id=0)) for sid in ('a','b')])
    backend=FakeBackend(graph,rows)
    coordinator=TreeCoordinator(tree,dict(centroids=[[0],[0]]),backend,{sid:value['map'] for sid,value in rows.items()})
    return coordinator,backend


class Tests(unittest.TestCase):
    def test_one_global_beam_keeps_two_best_children_on_same_owner(self):
        coordinator,backend=fixture()
        result=coordinator.query([0],2,beam_width=2,candidate_budget=2)
        self.assertEqual(result['ids'],[0,1])
        self.assertEqual([n['subtree_id'] for n in result['levels'][1]['selected']],['a','a'])
        self.assertTrue(all(level['selected_count']<=2 for level in result['levels']))
        scored={item['subtree_id'] for item in backend.calls if item['payload']['op']=='score_selected_leaves'}
        self.assertEqual(scored,{'a'})

    def test_one_global_candidate_budget_across_owners(self):
        coordinator,backend=fixture(a=(0,10),b=(.1,11))
        result=coordinator.query([0],1,beam_width=2,candidate_budget=1)
        self.assertEqual(result['candidate_contributions'],2)
        self.assertEqual(result['global_candidate_count'],1)
        reranked=[item for item in backend.calls if item['payload']['op']=='rerank_selected']
        self.assertEqual([(r['subtree_id'],r['payload']['local_ids']) for r in reranked],[('a',[0])])

    def test_exhaustive_matches_independent_scalar_oracle_and_ties(self):
        coordinator,_=fixture(a=(1,-1),b=(2,-2))
        result=coordinator.query([0],4,beam_width=None,candidate_budget=4)
        vectors=[1,-1,2,-2]
        exact=sorted(enumerate(vectors),key=lambda row:(row[1]**2,row[0]))
        self.assertEqual(result['ids'],[row[0] for row in exact])
        self.assertEqual(result['distances'],[row[1]**2 for row in exact])
        bounded=coordinator.query([0],2,beam_width=2,candidate_budget=2)
        self.assertEqual(bounded['ids'],[0,1])

    def test_mixed_depth_leaves_accumulate(self):
        coordinator,backend=fixture(a=(0,.1),b=(.2,.3))
        coordinator.tree['source']['row_count']=3
        coordinator.subtrees['a']['root_ref']=dict(kind='leaf',id=0)
        coordinator.subtrees['a']['vector_count']=1
        coordinator.mappings={'a':[0],'b':[1,2]}
        backend.rows['a']=dict(vectors=[0],map=[0],leaves={0:[0]})
        backend.rows['b']['map']=[1,2]
        backend.graph['b']={0:[(1,True,.04),(2,False,.09)],2:[(3,True,.09)]}
        backend.rows['b']['leaves']={1:[0],3:[1]}
        result=coordinator.query([0],3,beam_width=2,candidate_budget=3)
        self.assertEqual(result['ids'],[0,1,2])
        self.assertEqual(len(result['reached_leaves']),3)
        self.assertEqual([l['selected_count'] for l in result['levels']],[2,2,1])

    def test_http_batches_match_direct_global_frontiers(self):
        from .subtree_server import make_server,Host
        from .subtree_transport import HttpBackend
        direct,fake=fixture()
        host_servers=[]
        placement={'owners':{}}
        try:
            for sid in ('a','b'):
                host=Host.__new__(Host)
                host.configuration={'tree_sha256':direct.tree_hash,'owner_id':sid,'timeout':1}
                class Reader:
                    def call(self,item,timeout=None):
                        return fake.call_many([item],direct.tree_hash,timeout)[0][0]
                host.readers={sid:Reader()}
                server=make_server(host,port=0)
                thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
                host_servers.append((server,thread))
                placement['owners'][sid]={'owner_id':sid,'endpoint':'http://127.0.0.1:'+str(server.server_port)}
            remote=TreeCoordinator(direct.tree,direct.model,HttpBackend(placement),direct.mappings)
            for width in (1,2,None):
                expected=direct.query([0],1,beam_width=width,candidate_budget=4)
                actual=remote.query([0],1,beam_width=width,candidate_budget=4)
                for field in ('ids','distances','levels','selected_candidates','reached_leaves'):
                    self.assertEqual(actual[field],expected[field])
                if width==1:
                    self.assertEqual({r['owner_id'] for r in actual['transport']},{'a'})
                self.assertTrue(all(level['selected_count']<=width for level in actual['levels']) if width is not None else True)
        finally:
            for server,thread in host_servers:server.shutdown();server.server_close();thread.join()

    def test_invalid_child_reference_fails_closed(self):
        coordinator,backend=fixture()
        original=backend.call_many
        def faulty(*args,**kwargs):
            rows,traces=original(*args,**kwargs)
            if rows and rows[0]['result'].get('children'):
                rows[0]['result']['children'][0]['parent_id']=999
            return rows,traces
        backend.call_many=faulty
        with self.assertRaises(TreeError):coordinator.query([0],1,beam_width=2,candidate_budget=2)

    def test_missing_exact_candidate_and_stale_snapshot_fail_closed(self):
        for defect in ('missing','stale'):
            coordinator,backend=fixture()
            original=backend.call_many
            def faulty(*args,**kwargs):
                rows,traces=original(*args,**kwargs)
                if defect=='stale':rows[0]['snapshot_sha256']='wrong'
                elif rows and 'results' in rows[0]['result']:rows[0]['result']['results']=[]
                return rows,traces
            backend.call_many=faulty
            with self.assertRaises(TreeError):coordinator.query([0],1,beam_width=2,candidate_budget=2)


if __name__=='__main__':unittest.main()
