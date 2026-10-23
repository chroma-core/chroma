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
                host.configuration={'tree_sha256':direct.tree_hash,'owner_id':sid,'timeout':1,'profile_host':True}
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
                self.assertTrue(all(all(v>=0 for v in t['adapter_timing'].values()) for t in actual['transport']))
                self.assertTrue(all(t['client_timing']['response_bytes']>0 for t in actual['transport']))
                self.assertTrue(all(0<t['request_bytes']<=4*1024*1024 for t in actual['transport']))
                self.assertTrue(all(t['response_bytes']==t['client_timing']['response_bytes'] for t in actual['transport']))
                if width==1:
                    self.assertEqual({r['owner_id'] for r in actual['transport']},{'a'})
                self.assertTrue(all(level['selected_count']<=width for level in actual['levels']) if width is not None else True)
        finally:
            for server,thread in host_servers:server.shutdown();server.server_close();thread.join()

    def test_oversized_owner_request_fails_before_network(self):
        from unittest.mock import patch
        from .subtree_transport import HttpBackend
        backend=HttpBackend({'owners':{'a':{'owner_id':'host','endpoint':'http://unused'}}})
        item={'subtree_id':'a','payload':{'op':'expand','query':['x'*(4*1024*1024)]}}
        with patch('urllib.request.urlopen') as request:
            with self.assertRaises(TreeError):backend.call_many([item],'tree',1)
            request.assert_not_called()

    def test_requested_memory_profile_fails_closed_when_worker_omits_metrics(self):
        import types
        from .subtree_transport import PinnedReader
        pinned=PinnedReader.__new__(PinnedReader)
        pinned.descriptor={'snapshot_sha256':'snapshot','profile_memory':True}
        pinned.snapshot={'generation':4,'shard_id':'slot'}
        captured=[]
        def response(payload,**_kwargs):
            captured.append(payload)
            return dict(request_id=payload['request_id'],generation=4,shard_id='slot')
        pinned.reader=types.SimpleNamespace(query=response)
        item={'subtree_id':'s','snapshot_sha256':'snapshot','payload':{'op':'describe_root'}}
        with self.assertRaises(TreeError):pinned.call(item)
        self.assertTrue(captured[-1]['profile_memory'])
        pinned.descriptor['profile_memory']=False
        self.assertEqual(pinned.call(item)['result']['generation'],4)
        self.assertNotIn('profile_memory',captured[-1])

    def test_owner_admission_returns_structured_503_and_releases_capacity(self):
        import json
        import types
        import urllib.error
        import urllib.request
        from .subtree_server import make_server
        host=types.SimpleNamespace(configuration={'timeout':1,'max_inflight_batches':1},batch=lambda payload:{'accepted':True})
        server=make_server(host,port=0)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        request=urllib.request.Request('http://127.0.0.1:'+str(server.server_port)+'/batch',data=b'{}')
        try:
            server.slots.acquire()
            try:
                for size in [2,1024*1024,4*1024*1024]:
                    rejected=urllib.request.Request(request.full_url,data=b' '*size)
                    with self.assertRaises(urllib.error.HTTPError) as failed:
                        urllib.request.urlopen(rejected,timeout=1)
                    self.assertEqual(failed.exception.code,503)
                    self.assertEqual(json.loads(failed.exception.read())['code'],'owner_busy')
                    failed.exception.close()
            finally:
                server.slots.release()
            with urllib.request.urlopen(request,timeout=1) as response:
                self.assertTrue(json.loads(response.read())['accepted'])
        finally:
            server.shutdown();server.server_close();thread.join()
        host.configuration['max_inflight_batches']=0
        with self.assertRaises(ValueError):make_server(host,port=0)

    def test_owner_overload_drain_has_a_deadline_for_incomplete_requests(self):
        import socket
        import types
        import time
        from .subtree_server import make_server
        host=types.SimpleNamespace(configuration={'timeout':1,'max_inflight_batches':1})
        server=make_server(host,port=0)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();server.slots.acquire()
        try:
            began=time.monotonic()
            with socket.create_connection(server.server_address,timeout=1) as connection:
                connection.sendall(b'POST /batch HTTP/1.1\r\nContent-Length: 4194304\r\n\r\n')
                self.assertIn(b'503 Service Unavailable',connection.recv(4096))
            self.assertLess(time.monotonic()-began,.8)
        finally:
            server.slots.release();server.shutdown();server.server_close();thread.join()

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
