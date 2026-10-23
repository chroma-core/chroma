"""Control traffic cannot escape the pinned loopback tunnel or exceed its cap."""
import io
import unittest
from .dynamic_remote import AgentClient


class Response(io.BytesIO):
    def __init__(self,data):super().__init__(data);self.headers={}


class ClientAdmission(unittest.TestCase):
    def test_endpoint_and_timeout_admission(self):
        for endpoint in ('https://127.0.0.1:1','http://localhost:1','http://172.31.0.1:1','http://user@127.0.0.1:1','http://127.0.0.1:1/path','http://127.0.0.1:1?x=1'):
            with self.subTest(endpoint=endpoint),self.assertRaises(ValueError):AgentClient(endpoint,'token','run')
        with self.assertRaises(ValueError):AgentClient('http://127.0.0.1:1','token','run',31)
    def test_control_limits_and_remote_errors(self):
        client=AgentClient('http://127.0.0.1:1','token','run')
        client.request=lambda *args,**kwargs:Response(b'{"ready":true}')
        self.assertEqual(client.rpc({'op':'info'}),{'ready':True})
        with self.assertRaisesRegex(ValueError,'request exceeds'):client.rpc({'large':'x'*(256*1024)})
        client.request=lambda *args,**kwargs:Response(b' '*(256*1024+1))
        with self.assertRaisesRegex(ValueError,'response exceeds'):client.rpc({'op':'info'})
        client.request=lambda *args,**kwargs:Response(b'{"error":"not ready"}')
        with self.assertRaisesRegex(ValueError,'not ready'):client.rpc({'op':'info'})


if __name__=='__main__':unittest.main()

class BackendPins(unittest.TestCase):
    def test_grouping_preserves_order_and_generation_pins(self):
        from .dynamic_remote import RemoteBackend
        class Client:
            def __init__(self,worker):self.worker=worker;self.calls=[]
            def rpc(self,request):
                self.calls.append(request)
                if request['op']!='batch':return {'ok':True}
                payload=request['payload']
                return dict(tree_sha256=payload['tree_sha256'],request_id=payload['request_id'],owner_id=self.worker,responses=[dict(sid=item['subtree_id']) for item in payload['requests']])
        clients={worker:Client(worker) for worker in ('a','b')}
        backend=RemoteBackend(clients,{'s0':'a','s1':'b','s2':'a'},{'a':'generation1','b':'generation2'},'tree')
        rows,traces=backend.call_many([{'subtree_id':sid} for sid in ('s1','s0','s2')],'tree',2)
        self.assertEqual([row['sid'] for row in rows],['s1','s0','s2']);self.assertEqual(sum(row['operation_count'] for row in traces),3)
        backend.close()
        for worker,client in clients.items():
            self.assertEqual([request['op'] for request in client.calls],['pin','batch','unpin'])
            self.assertTrue(all(request['generation_id']==('generation1' if worker=='a' else 'generation2') for request in client.calls))
        with self.assertRaises(ValueError):backend.call_many([],'tree',2)
