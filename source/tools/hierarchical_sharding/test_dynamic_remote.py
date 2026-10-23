"""Control traffic cannot escape the pinned loopback tunnel or exceed its cap."""
import io
import unittest
from .dynamic_remote import AgentClient


class Response(io.BytesIO):
    def __init__(self,data):super().__init__(data);self.headers={};self.request_nonce='nonce'


class ClientAdmission(unittest.TestCase):
    def test_endpoint_and_timeout_admission(self):
        for endpoint in ('https://127.0.0.1:1','http://localhost:1','http://172.31.0.1:1','http://user@127.0.0.1:1','http://127.0.0.1:1/path','http://127.0.0.1:1?x=1'):
            with self.subTest(endpoint=endpoint),self.assertRaises(ValueError):AgentClient(endpoint,'token','run',worker_id='worker',instance_id='instance')
        with self.assertRaises(ValueError):AgentClient('http://127.0.0.1:1','token','run',31,worker_id='worker',instance_id='instance')
    def test_control_limits_and_remote_errors(self):
        client=AgentClient('http://127.0.0.1:1','token','run',worker_id='worker',instance_id='instance')
        client.request=lambda *args,**kwargs:Response(b'{"run_id":"run","request_id":"nonce","worker_id":"worker","instance_id":"instance","result":{"ready":true}}')
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

class AgentHTTPProtocol(unittest.TestCase):
    def test_authenticated_loopback_envelope_and_wrong_identity_rejection(self):
        import threading
        from .dynamic_agent import make_server
        class Agent:
            run_id='run';worker_id='worker';token='f'*64;identity={'instanceId':'instance'}
            def __init__(self):self.lock=threading.RLock();self.seen=set()
            def execute(self,payload):
                if payload['op']=='prepare-import':return dict(ticket='ticket')
                return dict(operation=payload['op'])
            def receive(self,ticket,stream,length):
                if ticket!='ticket' or length!=3 or stream.read(length)!=b'raw':raise ValueError('upload differs')
                return dict(imported=True)
        agent=Agent();server=make_server(agent);thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            endpoint='http://127.0.0.1:'+str(server.server_address[1])
            client=AgentClient(endpoint,agent.token,'run',worker_id='worker',instance_id='instance')
            self.assertEqual(client.rpc({'op':'info'}),{'operation':'info'})
            import tempfile
            from pathlib import Path
            from .tree_manifest import file_hash
            with tempfile.TemporaryDirectory() as temp:
                archive=Path(temp)/'fixture.tar';archive.write_bytes(b'raw')
                imported=client.upload('replica',archive,dict(bytes=3,sha256=file_hash(archive)),dict(subtree_id='s0',generation=1,snapshot=dict(sha256='snapshot')))
                self.assertEqual(imported,{'imported':True})
            wrong=AgentClient(endpoint,agent.token,'run',worker_id='wrong',instance_id='instance')
            with self.assertRaisesRegex(ValueError,'identity'):wrong.rpc({'op':'info'})
            import urllib.error
            wrong=AgentClient(endpoint,'bad','run',worker_id='worker',instance_id='instance')
            with self.assertRaises(urllib.error.HTTPError):wrong.rpc({'op':'info'})
        finally:server.shutdown();server.server_close();thread.join(2)
