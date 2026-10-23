"""Lease-supervised loopback worker agent for the bounded two-host fixture.

The outer host supervisor owns this agent and every Rust child. The listener
must be reached through pinned SSH forwarding; it never binds a VPC address.
"""
import argparse
import base64
import hashlib
import hmac
import json
from pathlib import Path
import re
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from .dynamic_archive import DEFAULT_LIMITS,export_checkpoint,import_checkpoint
from .dynamic_content import VERIFIED_WRITERS
from .dynamic_runtime import build_lower,inventory,seal_owner,descriptor,PRODUCER_SOURCE_SHA256
from .subtree_transport import PinnedReader,TreeError
from .subtree_server import Host
from .tree_manifest import artifact,canonical_bytes,content_hash,file_hash,_atomic_write


def name(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,95}',value):raise ValueError('safe bounded logical handle required')
    return value


def actual_identity():
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request=urllib.request.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    token=opener.open(request,timeout=2).read(1024).decode()
    request=urllib.request.Request('http://169.254.169.254/latest/dynamic/instance-identity/document',headers={'X-aws-ec2-metadata-token':token})
    return json.loads(opener.open(request,timeout=2).read(16384))


class Agent:
    def __init__(self,config,identity_reader=actual_identity):
        self.config=config;self.run_id=name(config['run_id']);self.worker_id=name(config['worker_id'])
        self.identity=identity_reader()
        for key in ('instanceId','accountId','region','privateIp'):
            if self.identity.get(key)!=config['expected_identity'].get(key):raise ValueError('actual worker identity mismatch: '+key)
        self.binary=Path(config['binary']);binary_hash=file_hash(self.binary)
        if binary_hash!=config['binary_sha256'] or binary_hash not in VERIFIED_WRITERS or config['producer_sha256']!=PRODUCER_SOURCE_SHA256:raise ValueError('worker binary or producer source epoch mismatch')
        token_path=Path(config['token_file'])
        if token_path.stat().st_mode&0o077:raise ValueError('agent token must be private')
        self.token=token_path.read_text().strip()
        if not re.fullmatch(r'[0-9a-f]{64}',self.token):raise ValueError('256-bit private token required')
        scope=Path(config['scope_root']);self.root=scope/'agent-data'
        if not scope.is_absolute() or scope.resolve()!=scope or '..' in scope.parts:raise ValueError('canonical absolute owned scope required')
        self.root.mkdir(mode=0o700,exist_ok=False)
        for folder in ('replicas','inputs','exports','incoming'): (self.root/folder).mkdir()
        self.replicas={};self.reservations={};self.generations={};self.exports={};self.imports={};self.seen=set();self.lock=threading.RLock();self.builder=threading.BoundedSemaphore(1);self.stop=threading.Event()
        from .dynamic_write_session import AgentWriteSessions
        self.write_sessions=AgentWriteSessions(self)
        self.log=self.root/'agent.jsonl';self.record('started',identity=self.identity,binary_sha256=binary_hash,producer_sha256=PRODUCER_SOURCE_SHA256)
    def record(self,phase,**fields):
        with self.lock,self.log.open('ab') as stream:
            stream.write(canonical_bytes(dict(phase=phase,epoch=time.time(),run_id=self.run_id,worker_id=self.worker_id,**fields)));stream.flush()
    def reserve_replica(self,replica,operation):
        replica=name(replica)
        with self.lock:
            if replica in self.reservations or replica in self.replicas or (self.root/'replicas'/replica).exists() or len(self.reservations)>=32:
                raise ValueError('fresh bounded replica reservation required')
            self.reservations[replica]=dict(operation=operation,state='reserved')
        return replica
    def metadata(self,replica):
        row=self.replicas[name(replica)];subtree=row['subtree'];directory=Path(row['owner']['checkpoint_dir'])
        if content_hash(inventory(directory))!=row['owner']['inventory_sha256']:raise ValueError('sealed replica changed')
        return dict(replica_id=replica,subtree=subtree,owner=row['owner'],manifest_b64=base64.b64encode(Path(subtree['snapshot']['path']).read_bytes()).decode(),map_b64=base64.b64encode(Path(subtree['local_to_global']['path']).read_bytes()).decode(),inventory=inventory(directory))
    def register(self,replica,directory,sid,generation,snapshot_sha=None):
        manifest=Path(directory)/f'manifest-{generation}.json';snapshot=json.loads(manifest.read_text())
        if snapshot['shard_id']!=sid or snapshot['generation']!=generation:raise ValueError('replica slot or generation differs')
        if snapshot_sha is not None and file_hash(manifest)!=snapshot_sha:raise ValueError('imported manifest identity differs')
        subtree=dict(subtree_id=sid,generation=generation,vector_count=snapshot['vector_count'],snapshot=artifact(manifest),local_to_global=artifact(Path(directory)/snapshot['id_mapping']['path']),root_ref=snapshot['root_ref'],routing_boundary_hash=snapshot['routing_boundary_hash'],ownership_revision=content_hash(dict(source_id=snapshot['source_id'],subtree_id=sid,map_sha256=snapshot['id_mapping']['sha256'])))
        self.replicas[replica]=dict(subtree=subtree,owner=seal_owner(directory,subtree,self.worker_id));return self.metadata(replica)
    def withhold_private_vectors(self,replica):
        """Retain raw blocks outside an unpublished replica for a fault test."""
        import struct
        from .dynamic_runtime import durable_namespace
        replica=name(replica)
        with self.lock:
            if any(replica in generation.get('replica_ids',set()) for generation in self.generations.values()):raise ValueError('cannot fault a prepared or pinned serving replica')
            row=self.metadata(replica);directory=Path(row['owner']['checkpoint_dir']);subtree=row['subtree']
            snapshot=json.loads((directory/f"manifest-{subtree['generation']}.json").read_text())
            root=(directory/'data'/'sparse_index'/snapshot['block_ids']['vector_data_id']).read_bytes()
            blocks=[path for path in (directory/'data'/'block').iterdir() if uuid.UUID(path.name).bytes in root or path.name.encode() in root]
            if not blocks:raise ValueError('vector root has no identified raw blocks')
            retained=self.root/'fault-retained'/replica;retained.mkdir(parents=True,exist_ok=False)
            moved=[]
            for path in blocks:
                moved.append(dict(name=path.name,sha256=file_hash(path),bytes=path.stat().st_size));path.rename(retained/path.name)
            durable_namespace(retained);durable_namespace(directory/'data'/'block');self.reservations[replica]['state']='faulted'
        reader=PinnedReader(descriptor(self.binary,directory,subtree),30)
        try:
            item=dict(subtree_id=subtree['subtree_id'],snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='describe_root'));reader.call(item)
            dimension=snapshot['dimension'];first=(directory/'construction-input.f32').read_bytes()[:dimension*4];query=list(struct.unpack('<'+'f'*dimension,first))
            item['payload']=dict(op='rerank_selected',query=query,local_ids=list(range(subtree['vector_count'])))
            try:reader.call(item)
            except TreeError as error:
                self.record('private-vector-blocks-withheld',replica_id=replica,blocks=moved)
                return dict(root_opened=True,rerank_failed=True,diagnostic=str(error)[:2048],retained_blocks=moved)
            raise ValueError('withheld vector blocks did not fail exact rerank')
        finally:reader.close()
    def execute(self,payload):
        op=payload.get('op')
        if op=='prepare-selected-write':return self.write_sessions.prepare(payload)
        if op=='selected-write-command':return self.write_sessions.command(payload)
        if op=='selected-write-status':return self.write_sessions.status(payload)
        if op=='close-selected-write':return self.write_sessions.close_one(payload)
        if op=='info':return dict(identity=self.identity,binary_sha256=file_hash(self.binary),producer_sha256=PRODUCER_SOURCE_SHA256)
        if op=='build':
            replica=name(payload['replica_id']);sid=name(payload['subtree_id']);dimension=payload['dimension'];count=payload['row_count']
            if type(dimension) is not int or not 1<=dimension<=16 or type(count) is not int or not 1<=count<=128:raise ValueError('fixture build dimensions exceed admission')
            if not self.builder.acquire(blocking=False):raise ValueError('one bounded build at a time')
            try:self.reserve_replica(replica,'build')
            except BaseException:self.builder.release();raise
            try:
                inputs=self.root/'inputs'/replica;inputs.mkdir();vp=inputs/'vectors.f32';mp=inputs/'map.u64le'
                vectors=base64.b64decode(payload['vectors_b64'],validate=True);mapping=base64.b64decode(payload['map_b64'],validate=True)
                if len(vectors)!=count*dimension*4 or len(mapping)!=count*8:raise ValueError('fixture vector/map size mismatch')
                vp.write_bytes(vectors);mp.write_bytes(mapping)
                subtree=build_lower(self.binary,vp,mp,self.root/'replicas'/replica,sid,payload['generation'],dimension,payload['metric'],payload['source_id'],payload['boundary_hash'],timeout=30,construction=payload['construction'])
                self.replicas[replica]=dict(subtree=subtree,owner=seal_owner(self.root/'replicas'/replica,subtree,self.worker_id));self.record('built',replica_id=replica,snapshot_sha256=subtree['snapshot']['sha256'])
                with self.lock:self.reservations[replica]['state']='ready'
                return self.metadata(replica)
            except BaseException:
                with self.lock:self.reservations[replica]['state']='failed'
                raise
            finally:self.builder.release()
        if op=='metadata':return self.metadata(payload['replica_id'])
        if op=='withhold-private-vectors':return self.withhold_private_vectors(payload['replica_id'])
        if op=='export':
            replica=name(payload['replica_id']);row=self.metadata(replica)
            if row['owner']['inventory_sha256']!=payload['expected_inventory_sha256']:raise ValueError('source replica is not the pinned inventory')
            with self.lock:
                if len(self.exports)>=32:raise ValueError('export count exceeds admission')
                handle=uuid.uuid4().hex;self.exports[handle]=None
            target=self.root/'exports'/(handle+'.tar');receipt=export_checkpoint(row['owner']['checkpoint_dir'],target)
            with self.lock:self.exports[handle]=target
            return dict(export_id=handle,receipt=receipt)
        if op=='prepare-import':
            replica=name(payload['replica_id'])
            from .dynamic_archive import checked_inventory
            checked_inventory(payload['receipt']['files'],DEFAULT_LIMITS)
            if type(payload['receipt'].get('bytes')) is not int or not 0<payload['receipt']['bytes']<=DEFAULT_LIMITS['wire_bytes']:raise ValueError('wire import exceeds admission')
            self.reserve_replica(replica,'import')
            ticket=uuid.uuid4().hex;self.imports[ticket]=dict(replica_id=replica,receipt=payload['receipt'],subtree_id=name(payload['subtree_id']),generation=payload['generation'],snapshot_sha256=payload['snapshot_sha256'],state='waiting')
            return dict(ticket=ticket)
        if op=='prepare-generation':
            handle=name(payload['generation_id']);items=payload['subtrees']
            with self.lock:
                if handle in self.generations or len(self.generations)>=8 or not 1<=len(items)<=8:raise ValueError('fresh bounded serving generation required')
                self.generations[handle]=dict(host=None,pins=set(),inflight=0,replica_ids={item['replica_id'] for item in items})
            if len({row['subtree_id'] for row in items})!=len(items):
                with self.lock:del self.generations[handle]
                raise ValueError('duplicate generation subtree')
            host=Host.__new__(Host);host.configuration=dict(tree_sha256=payload['tree_sha256'],owner_id=self.worker_id,timeout=30);host.readers={};host.reduction_maps={}
            try:
                for item in items:
                    row=self.metadata(item['replica_id']);subtree=row['subtree']
                    if subtree['subtree_id']!=item['subtree_id'] or subtree['snapshot']['sha256']!=item['snapshot_sha256']:raise ValueError('serving generation snapshot mismatch')
                    reader=PinnedReader(descriptor(self.binary,row['owner']['checkpoint_dir'],subtree),30);host.readers[item['subtree_id']]=reader
                    reader.call(dict(subtree_id=item['subtree_id'],snapshot_sha256=item['snapshot_sha256'],payload=dict(op='describe_root')))
                with self.lock:self.generations[handle]['host']=host
                return dict(generation_id=handle,subtrees=len(items))
            except BaseException:
                host.close()
                with self.lock:self.generations.pop(handle,None)
                raise
        if op=='pin':
            handle=name(payload['generation_id']);reader=name(payload['reader_id'])
            with self.lock:
                generation=self.generations[handle]
                if generation['host'] is None:raise ValueError('serving generation is not ready')
                if len(generation['pins'])>=64 and reader not in generation['pins']:raise ValueError('reader pin count exceeds admission')
                generation['pins'].add(reader)
            return dict(pinned=True)
        if op=='unpin':
            with self.lock:self.generations[name(payload['generation_id'])]['pins'].discard(name(payload['reader_id']))
            return dict(unpinned=True)
        if op in ('batch','probe-generation'):
            handle=name(payload['generation_id'])
            with self.lock:
                generation=self.generations[handle]
                if generation['host'] is None:raise ValueError('serving generation is not ready')
                if op=='batch' and name(payload['reader_id']) not in generation['pins']:raise ValueError('query does not hold a generation pin')
                generation['inflight']+=1
            try:return generation['host'].batch(payload['payload'])
            finally:
                with self.lock:generation['inflight']-=1
        if op=='retire':
            handle=name(payload['generation_id'])
            with self.lock:
                generation=self.generations[handle]
                if generation['host'] is None:raise ValueError('serving generation is not ready')
                if generation['pins'] or generation['inflight']:raise ValueError('cannot retire a pinned or active serving generation')
                generation['host'].close();del self.generations[handle]
            return dict(retired=True)
        if op=='shutdown':self.stop.set();return dict(stopping=True)
        raise ValueError('unsupported agent operation')
    def receive(self,ticket,stream,length):
        ticket=name(ticket);row=self.imports[ticket]
        with self.lock:
            if row['state']!='waiting' or length!=row['receipt']['bytes']:raise ValueError('fresh import ticket and exact wire size required')
            row['state']='receiving'
        target=self.root/'incoming'/(ticket+'.tar');remaining=length
        try:
            with target.open('xb') as output:
                while remaining:
                    chunk=stream.read(min(65536,remaining))
                    if not chunk:raise ValueError('truncated import body')
                    output.write(chunk);remaining-=len(chunk)
                output.flush();__import__('os').fsync(output.fileno())
            directory=self.root/'replicas'/row['replica_id'];import_checkpoint(target,directory,row['receipt']);result=self.register(row['replica_id'],directory,row['subtree_id'],row['generation'],row['snapshot_sha256']);row['state']='ready';self.reservations[row['replica_id']]['state']='ready';self.record('imported',replica_id=row['replica_id']);return result
        except BaseException:
            row['state']='failed';self.reservations[row['replica_id']]['state']='failed';raise
    def close(self):
        self.write_sessions.close()
        for generation in self.generations.values():
            if generation['host'] is not None:generation['host'].close()
        self.generations.clear();self.record('closed')


def make_server(agent,port=0):
    class Handler(BaseHTTPRequestHandler):
        def authorized(self):
            return self.headers.get('X-Run-ID')==agent.run_id and hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+agent.token)
        def reply(self,status,value):
            encoded=canonical_bytes(value)
            if len(encoded)>256*1024:status=503;encoded=canonical_bytes(dict(error='agent metadata response exceeds admission'))
            self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(encoded)));self.send_header('Connection','close');self.end_headers();self.wfile.write(encoded)
        def do_POST(self):
            request_id=self.headers.get('X-Request-ID','')
            try:
                self.connection.settimeout(30)
                if not self.authorized():self.reply(403,dict(error='agent authentication failed'));return
                name(request_id)
                with agent.lock:
                    if request_id in agent.seen or len(agent.seen)>=10000:raise ValueError('duplicate request nonce or request admission exceeded')
                    agent.seen.add(request_id)
                length=int(self.headers.get('Content-Length','-1'))
                if self.path.startswith('/import/'):
                    if not 0<length<=DEFAULT_LIMITS['wire_bytes']:raise ValueError('import wire size exceeds admission')
                    result=agent.receive(self.path.split('/')[-1],self.rfile,length)
                elif self.path=='/rpc':
                    if not 0<length<=256*1024:raise ValueError('control request exceeds admission')
                    raw=self.rfile.read(length)
                    if len(raw)!=length:raise ValueError('truncated control request')
                    result=agent.execute(json.loads(raw))
                else:raise ValueError('unsupported agent path')
                self.reply(200,dict(run_id=agent.run_id,request_id=request_id,worker_id=agent.worker_id,instance_id=agent.identity['instanceId'],result=result))
            except Exception as error:self.reply(503,dict(run_id=agent.run_id,request_id=request_id,error=type(error).__name__+': '+str(error)[:2048]))
        def do_GET(self):
            try:
                self.connection.settimeout(30)
                if not self.authorized():self.reply(403,dict(error='agent authentication failed'));return
                handle=name(self.path.removeprefix('/export/'))
                if not self.path.startswith('/export/'):raise ValueError('unsupported agent path')
                target=agent.exports[handle];size=target.stat().st_size
                if size>DEFAULT_LIMITS['wire_bytes']:raise ValueError('export wire size exceeds admission')
                self.send_response(200);self.send_header('Content-Length',str(size));self.send_header('Connection','close');self.end_headers()
                with target.open('rb') as source:
                    while chunk:=source.read(65536):self.wfile.write(chunk)
            except Exception as error:
                if not self.wfile.closed:
                    try:self.reply(503,dict(error=str(error)[:2048]))
                    except (BrokenPipeError,ConnectionResetError):pass
        def log_message(self,*args):pass
    class Server(ThreadingHTTPServer):
        daemon_threads=True
        request_queue_size=8
        slots=threading.BoundedSemaphore(8)
        def process_request(self,request,address):
            if not self.slots.acquire(blocking=False):
                body=b'{"error":"agent admission is busy"}\n'
                try:request.settimeout(.25);request.sendall(b'HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body)
                except OSError:pass
                self.shutdown_request(request);return
            try:super().process_request(request,address)
            except BaseException:self.slots.release();raise
        def process_request_thread(self,request,address):
            try:super().process_request_thread(request,address)
            finally:self.slots.release()
    return Server(('127.0.0.1',port),Handler)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True);args=parser.parse_args();config=json.loads(Path(args.config).read_text());agent=Agent(config);server=make_server(agent,config['port'])
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    _atomic_write(Path(config['ready_path']),canonical_bytes(dict(port=server.server_address[1],run_id=agent.run_id,worker_id=agent.worker_id,identity=agent.identity)))
    try:
        while not agent.stop.wait(.1):pass
    finally:server.shutdown();server.server_close();thread.join(timeout=2);agent.close()

if __name__=='__main__':main()
