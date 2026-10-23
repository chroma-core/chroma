"""The actual shared-query codec rejects ambiguity before any child call."""
import copy
import json
import unittest
from unittest.mock import patch
from .batch_query import FORMAT,encode_batch
from .subtree_server import Host
from .subtree_transport import HttpBackend,TreeError


def fixture():
    calls=[];host=Host.__new__(Host);host.configuration=dict(tree_sha256='tree',owner_id='adapter',timeout=5,shared_query_batches=True);host.reduction_maps={}
    class Reader:
        descriptor={'snapshot_sha256':'snapshot'};snapshot={'dimension':3}
        def call(self,item,timeout):calls.append(item);return dict(subtree_id=item['subtree_id'],snapshot_sha256='snapshot',result={})
    host.readers={sid:Reader() for sid in ('a','b')}
    items=[dict(subtree_id=sid,snapshot_sha256='snapshot',payload=dict(op='expand',query=[1.,-0.,3.],nodes=[1])) for sid in ('a','b')]
    payload=dict(tree_sha256='tree',request_id='request',requests=items,timeout_s=5.)
    return host,calls,payload


class SharedQueryContract(unittest.TestCase):
    def test_default_bytes_exact_and_expansion_preserves_fields_and_negative_zero(self):
        host,calls,payload=fixture();self.assertEqual(encode_batch(payload),json.dumps(payload,allow_nan=False).encode())
        body=encode_batch(payload,True);expanded=json.loads(body);self.assertNotEqual(body,encode_batch(payload))
        reply=host.batch(expanded);self.assertEqual(reply['query_format'],FORMAT)
        self.assertEqual(json.dumps(sorted(calls,key=lambda x:x['subtree_id']),sort_keys=True),json.dumps(payload['requests'],sort_keys=True))
        self.assertIn('-0.0',json.dumps(calls));self.assertTrue(all('query' not in row['payload'] for row in expanded['requests']))
        self.assertEqual(payload['requests'][0]['payload']['query'],[1.,-0.,3.])

    def test_client_rejects_mixed_query_values_types_and_signed_zero(self):
        for value in ([2.,-0.,3.],[1.,0.,3.],[1,-0.,3.],[True,-0.,3.],[float('nan'),0.,3.]):
            _,_,payload=fixture();payload['requests'][1]['payload']['query']=value
            with self.assertRaises(ValueError):encode_batch(payload,True)
        with self.assertRaises(ValueError):HttpBackend({},share_query=1)
        with self.assertRaises(ValueError):Host(dict(shared_query_batches=1,subtrees={}))
        _,_,payload=fixture();payload['requests'][1]['payload']['query']=[4.,5.,6.]
        placement=dict(owners={sid:dict(owner_id=sid,endpoint='http://unused') for sid in ('a','b')})
        with patch('urllib.request.urlopen') as network,self.assertRaises(ValueError):HttpBackend(placement,True).call_many(payload['requests'],'tree',5)
        network.assert_not_called()

    def test_complete_server_validation_precedes_every_dispatch(self):
        mutations=[lambda p:p.update(query_format='unknown'),lambda p:p.pop('shared_query'),lambda p:p.update(shared_query=True),lambda p:p.update(shared_query=[True,0.,3.]),lambda p:p.update(shared_query=[1.,float('inf'),3.]),lambda p:p.update(shared_query=[1.]),lambda p:p['requests'][1]['payload'].update(query=[1.,0.,3.]),lambda p:p['requests'][1].update(snapshot_sha256='other'),lambda p:p['requests'][1].update(subtree_id='unknown'),lambda p:p['requests'][1].update(subtree_id='a'),lambda p:p['requests'][1]['payload'].update(op='unknown'),lambda p:p.update(extra=True)]
        for mutation in mutations:
            host,calls,payload=fixture();shared=json.loads(encode_batch(payload,True));mutation(shared)
            with self.assertRaises((ValueError,TreeError)):host.batch(shared)
            self.assertEqual(calls,[])
        host,calls,payload=fixture();host.readers['b'].snapshot={'dimension':4}
        with self.assertRaises(ValueError):host.batch(json.loads(encode_batch(payload,True)))
        self.assertEqual(calls,[])

    def test_disabled_server_and_no_cross_batch_state(self):
        host,calls,payload=fixture();host.configuration['shared_query_batches']=False
        with self.assertRaises(ValueError):host.batch(json.loads(encode_batch(payload,True)))
        self.assertEqual(calls,[]);host.configuration['shared_query_batches']=True
        host.batch(json.loads(encode_batch(payload,True)));first=copy.deepcopy(calls);calls.clear()
        for item in payload['requests']:item['payload']['query']=[4.,5.,6.]
        host.batch(json.loads(encode_batch(payload,True)));self.assertTrue(all(item['payload']['query']==[4.,5.,6.] for item in calls));self.assertEqual(first[0]['payload']['query'],[1.,-0.,3.])
        # A child cannot mutate another child's query list.
        calls[0]['payload']['query'][0]=99.;self.assertEqual(calls[1]['payload']['query'][0],4.)

    def test_client_rejects_missing_or_wrong_format_echo(self):
        _,_,payload=fixture();placement=dict(owners={sid:dict(owner_id='adapter',endpoint='http://unused') for sid in ('a','b')})
        for echo in (None,'old'):
            class Stream:
                headers={}
                def __enter__(self):return self
                def __exit__(self,*_):pass
                def read(self,n):return self.body
            def response(request,timeout):
                stream=Stream();sent=json.loads(request.data);reply=dict(tree_sha256='tree',request_id=sent['request_id'],owner_id='adapter',responses=[{},{}])
                if echo is not None:reply['query_format']=echo
                stream.body=json.dumps(reply).encode();return stream
            with patch('urllib.request.urlopen',response),self.assertRaisesRegex(TreeError,'format echo'):HttpBackend(placement,True).call_many(payload['requests'],'tree',5)
