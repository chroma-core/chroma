"""Bounded authenticated transport through an already pinned SSH tunnel.

The caller owns tunnel supervision and verified host identities. This client
only accepts a loopback endpoint and never retries state-changing operations.
"""
import json
from pathlib import Path
import urllib.parse
import urllib.request
import uuid
from .dynamic_archive import DEFAULT_LIMITS,import_checkpoint
from .tree_manifest import canonical_bytes,file_hash


class AgentClient:
    def __init__(self,endpoint,token,run_id,timeout=30,*,worker_id,instance_id):
        parsed=urllib.parse.urlsplit(endpoint)
        if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.path not in ('','/') or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('agent transport requires a plain loopback SSH-forward endpoint')
        if not 0<timeout<=30:raise ValueError('bounded request timeout required')
        self.worker_id=worker_id;self.instance_id=instance_id
        self.endpoint=endpoint.rstrip('/');self.token=token;self.run_id=run_id;self.timeout=timeout
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def request(self,path,data=None,method='POST'):
        request=urllib.request.Request(self.endpoint+path,data=data,method=method,headers={'Authorization':'Bearer '+self.token,'X-Run-ID':self.run_id,'X-Request-ID':uuid.uuid4().hex,'Content-Type':'application/json'})
        response=self.opener.open(request,timeout=self.timeout)
        response.request_nonce=request.headers['X-request-id']
        return response
    def rpc(self,payload):
        encoded=canonical_bytes(payload)
        if len(encoded)>256*1024:raise ValueError('agent control request exceeds admission')
        with self.request('/rpc',encoded) as response:
            raw=response.read(256*1024+1);nonce=response.request_nonce
        if len(raw)>256*1024:raise ValueError('agent control response exceeds admission')
        value=json.loads(raw)
        if not isinstance(value,dict) or 'error' in value:raise ValueError('agent rejected operation: '+str(value.get('error') if isinstance(value,dict) else 'invalid response'))
        if value.get('run_id')!=self.run_id or value.get('request_id')!=nonce or value.get('worker_id')!=self.worker_id or value.get('instance_id')!=self.instance_id or not isinstance(value.get('result'),dict):raise ValueError('agent authenticated response identity differs')
        return value['result']
    def download(self,export_id,receipt,target):
        expected=receipt['bytes']
        if type(expected) is not int or not 0<expected<=DEFAULT_LIMITS['wire_bytes']:raise ValueError('export wire size exceeds admission')
        target=Path(target)
        with self.request('/export/'+export_id,method='GET') as response,target.open('xb') as output:
            if int(response.headers.get('Content-Length','-1'))!=expected:raise ValueError('export content length differs')
            remaining=expected
            while remaining:
                chunk=response.read(min(65536,remaining))
                if not chunk:raise ValueError('truncated agent export')
                output.write(chunk);remaining-=len(chunk)
            if response.read(1):raise ValueError('oversized agent export')
        if file_hash(target)!=receipt['sha256']:raise ValueError('export digest differs')
        return target
    def upload(self,replica,archive,receipt,subtree):
        archive=Path(archive)
        if archive.stat().st_size!=receipt['bytes'] or archive.stat().st_size>DEFAULT_LIMITS['wire_bytes'] or file_hash(archive)!=receipt['sha256']:raise ValueError('sealed upload archive differs')
        admitted=self.rpc(dict(op='prepare-import',replica_id=replica,receipt=receipt,subtree_id=subtree['subtree_id'],generation=subtree['generation'],snapshot_sha256=subtree['snapshot']['sha256']))
        # The complete archive is at most32MiB and is streamed from a file.
        request=urllib.request.Request(self.endpoint+'/import/'+admitted['ticket'],method='POST',headers={'Authorization':'Bearer '+self.token,'X-Run-ID':self.run_id,'X-Request-ID':uuid.uuid4().hex,'Content-Length':str(receipt['bytes'])})
        with archive.open('rb') as source:
            request.data=source
            request.add_header('Content-Length',str(receipt['bytes']))
            with self.opener.open(request,timeout=self.timeout) as response:raw=response.read(256*1024+1)
        if len(raw)>256*1024:raise ValueError('import metadata exceeds admission')
        result=json.loads(raw)
        if result.get('run_id')!=self.run_id or result.get('request_id')!=request.headers['X-request-id'] or result.get('worker_id')!=self.worker_id or result.get('instance_id')!=self.instance_id or not isinstance(result.get('result'),dict):raise ValueError('import response identity differs')
        return result['result']
    def mirror(self,replica,expected_inventory,target,archive):
        exported=self.rpc(dict(op='export',replica_id=replica,expected_inventory_sha256=expected_inventory))
        self.download(exported['export_id'],exported['receipt'],archive)
        import_checkpoint(archive,target,exported['receipt'])
        return exported['receipt']


class RemoteBackend:
    """Keep generation-specific worker readers pinned for this reader lifetime."""
    def __init__(self,clients,subtree_workers,generations,tree_hash,*,deployment_sha256):
        self.clients=clients;self.subtree_workers=dict(subtree_workers);self.generations=dict(generations)
        self.deployment_sha256=deployment_sha256
        self.tree_hash=tree_hash;self.reader_id=uuid.uuid4().hex;self.pinned=[];self.closed=False
        try:
            for worker in sorted(set(self.subtree_workers.values())):
                clients[worker].rpc(dict(op='pin',generation_id=self.generations[worker],reader_id=self.reader_id));self.pinned.append(worker)
        except BaseException:
            self.close();raise
    def call_many(self,items,tree_hash,timeout):
        import concurrent.futures
        import time
        if self.closed or tree_hash!=self.tree_hash:raise ValueError('reader is closed or pinned tree differs')
        if not 0<timeout<=30:raise ValueError('bounded batch timeout required')
        groups={}
        for index,item in enumerate(items):groups.setdefault(self.subtree_workers[item['subtree_id']],[]).append((index,item))
        if len(groups)>2:raise ValueError('two-host fixture admission exceeded')
        deadline=time.monotonic()+timeout
        def call(worker,group):
            started=time.monotonic();request_id=uuid.uuid4().hex
            payload=dict(tree_sha256=tree_hash,request_id=request_id,requests=[item for _,item in group],timeout_s=max(.001,deadline-started))
            result=self.clients[worker].rpc(dict(op='batch',generation_id=self.generations[worker],reader_id=self.reader_id,payload=payload))
            if result.get('tree_sha256')!=tree_hash or result.get('request_id')!=request_id or result.get('owner_id')!=worker:raise ValueError('remote pinned response identity differs')
            responses=result.get('responses')
            if not isinstance(responses,list) or len(responses)!=len(group):raise ValueError('remote batch is incomplete')
            if time.monotonic()>deadline:raise TimeoutError('remote batch exceeded deadline')
            return group,responses,dict(owner_id=worker,actual_host=self.clients[worker].instance_id,agent_run_id=self.clients[worker].run_id,roundtrips=1,operation_count=len(group),elapsed_s=time.monotonic()-started)
        rows=[None]*len(items);traces=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(call,worker,group) for worker,group in sorted(groups.items())]
            for future in futures:
                group,responses,trace=future.result()
                for (index,_),response in zip(group,responses):rows[index]=response
                traces.append(trace)
        return rows,traces
    def close(self):
        errors=[]
        for worker in self.pinned:
            try:self.clients[worker].rpc(dict(op='unpin',generation_id=self.generations[worker],reader_id=self.reader_id))
            except Exception as error:errors.append(str(error))
        self.pinned=[];self.closed=True
        if errors:raise ValueError('remote generation unpin requires reconciliation: '+'; '.join(errors))


class RemoteBuilder:
    """Execute a tiny lower-tree build remotely and retain its full local mirror.

    Full mirrors keep existing publication content checks intact. They are a
    correctness-fixture mechanism, not a storage design for large collections.
    """
    def __init__(self,clients,assignments,health=None,attempt=None):
        self.clients=clients;self.assignments=dict(assignments);self.built={};self.health=health;self.attempt=attempt
    def __call__(self,binary,vectors,mapping,directory,sid,generation,dimension,metric,source_id,boundary_hash,timeout=60,checkpoint_rows=10000,construction=None):
        import base64
        import copy
        from .tree_manifest import artifact
        if self.health is not None:self.health()
        directory=Path(directory)
        if directory.exists():raise ValueError('fresh mirrored build output required')
        count=Path(mapping).stat().st_size//8
        if not 1<=count<=128 or not 1<=dimension<=16:raise ValueError('tiny remote rebuild admission exceeded')
        worker=self.assignments[sid];client=self.clients[worker];replica='build-'+uuid.uuid4().hex
        if self.attempt is not None:self.attempt.reserve(worker,replica,sid,generation,directory)
        result=client.rpc(dict(op='build',replica_id=replica,subtree_id=sid,generation=generation,dimension=dimension,row_count=count,metric=metric,source_id=source_id,boundary_hash=boundary_hash,construction=construction,vectors_b64=base64.b64encode(Path(vectors).read_bytes()).decode(),map_b64=base64.b64encode(Path(mapping).read_bytes()).decode()))
        if result['subtree']['subtree_id']!=sid or result['subtree']['generation']!=generation:raise ValueError('remote build identity differs')
        client.mirror(replica,result['owner']['inventory_sha256'],directory,Path(str(directory)+'.transfer.tar'))
        subtree=copy.deepcopy(result['subtree']);subtree['snapshot']=artifact(directory/Path(subtree['snapshot']['path']).name);subtree['local_to_global']=artifact(directory/Path(subtree['local_to_global']['path']).name)
        if subtree['snapshot']['sha256']!=result['subtree']['snapshot']['sha256'] or subtree['local_to_global']['sha256']!=result['subtree']['local_to_global']['sha256']:raise ValueError('mirrored build identity differs')
        if self.health is not None:self.health()
        self.built[sid]=dict(worker_id=worker,replica_id=replica,metadata=result)
        return subtree


class RemoteGeneration:
    """Open all destination readers before the authority can publish cutover."""
    def __init__(self,record,clients):
        self.record=record;self.clients=clients;self.generations={};self.subtree_workers={};self.closed=False
        groups={}
        for subtree in record['tree']['subtrees']:
            sid=subtree['subtree_id'];owner=record['owners'][sid];remote=owner['remote'];worker=remote['worker_id']
            if worker!=owner['worker_id'] or remote.get('instance_id')!=clients[worker].instance_id or remote.get('agent_run_id')!=clients[worker].run_id:raise ValueError('remote physical owner differs')
            self.subtree_workers[sid]=worker
            groups.setdefault(worker,[]).append(dict(subtree_id=sid,replica_id=remote['replica_id'],snapshot_sha256=subtree['snapshot']['sha256']))
        if not 1<=len(groups)<=2:raise ValueError('one or two fixture workers required')
        try:
            for worker,items in sorted(groups.items()):
                generation='generation-'+uuid.uuid4().hex
                clients[worker].rpc(dict(op='prepare-generation',generation_id=generation,tree_sha256=record['tree_sha256'],subtrees=items))
                self.generations[worker]=generation
        except BaseException:
            self.close();raise
    def backend(self):
        if self.closed:raise ValueError('serving generation is closed')
        return RemoteBackend(self.clients,self.subtree_workers,self.generations,self.record['tree_sha256'],deployment_sha256=__import__('hashlib').sha256(canonical_bytes(self.record)).hexdigest())
    def probe(self,sid,owner,subtree):
        if self.closed or owner!=self.record['owners'][sid]:raise ValueError('probe does not match prepared deployment')
        worker=self.subtree_workers[sid];client=self.clients[worker];remote=owner['remote']
        metadata=client.rpc(dict(op='metadata',replica_id=remote['replica_id']))
        if metadata['owner']['inventory_sha256']!=owner['inventory_sha256'] or metadata['subtree']['snapshot']['sha256']!=subtree['snapshot']['sha256']:raise ValueError('remote sealed destination differs')
        request_id=uuid.uuid4().hex
        payload=dict(tree_sha256=self.record['tree_sha256'],request_id=request_id,timeout_s=30,requests=[dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root'))])
        response=client.rpc(dict(op='probe-generation',generation_id=self.generations[worker],payload=payload))
        if response.get('request_id')!=request_id or response.get('tree_sha256')!=self.record['tree_sha256'] or response.get('owner_id')!=worker:raise ValueError('remote serving probe identity differs')
        rows=response.get('responses',[])
        if len(rows)!=1 or rows[0].get('subtree_id')!=sid or rows[0].get('snapshot_sha256')!=subtree['snapshot']['sha256']:raise ValueError('remote serving probe is incomplete')
        root=rows[0]['result']
        if root.get('metric')!=self.record['tree']['metric'] or root.get('dimension')!=self.record['tree']['dimension'] or root.get('shard_id')!=sid:raise ValueError('remote reader interpretation differs')
        return dict(snapshot_sha256=subtree['snapshot']['sha256'],generation=root['generation'],root_ref=dict(kind=root['root']['kind'],id=root['root']['id']),vector_count=root['vector_count'],map_sha256=root['id_mapping']['sha256'])
    def close(self):
        errors=[]
        for worker,generation in self.generations.items():
            try:self.clients[worker].rpc(dict(op='retire',generation_id=generation))
            except Exception as error:errors.append(str(error))
        if errors:raise ValueError('remote serving retirement requires reconciliation: '+'; '.join(errors))
        self.generations={};self.closed=True
