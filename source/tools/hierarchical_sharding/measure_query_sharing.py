"""Actual codec/native parity plus explicitly reconstructed fixed16 bodies."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import struct
import tarfile
import time
from unittest.mock import patch
from .batch_query import encode_batch,expand_batch
from .dynamic_runtime import descriptor
from .measure_rerank_pins import inventory
from .subtree_server import Host
from .subtree_transport import HttpBackend,TreeCoordinator
from .test_dynamic_trie_query import logical_trace
from .tree_manifest import checked_artifact,content_hash,load_model


def fixed16(trace_path,query_path,model_path):
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();model=json.loads(model_path.read_bytes())
    if len(model['perquery'])!=16:raise ValueError('exact16 query model required')
    if sha(trace_path)!=model['source_trace_sha256'] or sha(query_path)!=model['query_file_sha256']:raise ValueError('fixed16 source identity differs')
    queries={};selected={row['query_id'] for row in model['perquery']}
    with query_path.open('rb') as source:
        for raw in source:
            row=json.loads(raw)
            if row['query_id'] in selected:queries[row['query_id']]=(row['vector'],raw)
    if set(queries)!=selected:raise ValueError('fixed16 query selection differs')
    rows=[];raw_lines=[]
    with tarfile.open(trace_path) as archive:
        source=archive.extractfile('fineweb10m-heldout-v2/test-cohost.jsonl')
        for expected in model['perquery']:
            raw=source.readline(8*1024*1024+1)
            if len(raw)>8*1024*1024:raise ValueError('trace row budget')
            record=json.loads(raw);result=record['result']
            if record['query_id']!=expected['query_id']:raise ValueError('fixed16 order differs')
            query,query_raw=queries[record['query_id']];raw_lines.append(query_raw);operations=result['worker_metrics'];cursor=0;pairs=[]
            for batch_id,batch in enumerate(result['transport']):
                count=batch['operation_count'];items=[]
                for slot,operation in enumerate(operations[cursor:cursor+count]):
                    metrics=operation['metrics'];op=operation['op'];payload=dict(op=op,query=query)
                    # Historical requests/UUID/deadline and node identities are
                    # unavailable. Use declared deterministic placeholders with
                    # real recorded counts; measure NEW serializer bytes only.
                    if op=='expand':payload['nodes']=list(range(metrics['expanded_nodes']))
                    elif op=='score_selected_leaves':payload.update(nodes=list(range(metrics['leaves_scanned'])),candidate_budget=1000)
                    elif op=='rerank_selected':payload['local_ids']=list(range(metrics['exact_vectors_scored']))
                    else:raise ValueError('unexpected archived operation')
                    items.append(dict(subtree_id='slot-'+str(slot),snapshot_sha256='0'*64,payload=payload))
                if len(items)!=count:raise ValueError('operation partition differs')
                cursor+=count;payload=dict(tree_sha256='0'*64,request_id='reconstructed-'+str(batch_id),requests=items,timeout_s=30.)
                legacy=encode_batch(payload);shared=encode_batch(payload,True)
                # Verify shared codec roundtrip using actual operation fields;
                # mock descriptors attest only this synthetic serialization.
                from types import SimpleNamespace
                readers={item['subtree_id']:SimpleNamespace(descriptor={'snapshot_sha256':'0'*64},snapshot={'dimension':len(query)}) for item in items}
                expanded,_=expand_batch(json.loads(shared),True,readers)
                if json.dumps(expanded,sort_keys=True)!=json.dumps(items,sort_keys=True):raise ValueError('synthetic operation roundtrip differs')
                pairs.append(dict(legacy_bytes=len(legacy),shared_bytes=len(shared),legacy_sha256=hashlib.sha256(legacy).hexdigest(),shared_sha256=hashlib.sha256(shared).hexdigest(),operations=count))
            if cursor!=len(operations) or cursor!=expected['subtree_operations']:raise ValueError('fixed16 operation count differs')
            rows.append(dict(query_id=record['query_id'],trace_line_sha256=hashlib.sha256(raw).hexdigest(),query_line_sha256=hashlib.sha256(query_raw).hexdigest(),pairs=pairs,legacy_bytes=sum(p['legacy_bytes'] for p in pairs),shared_bytes=sum(p['shared_bytes'] for p in pairs)))
    if hashlib.sha256(b''.join(raw_lines)).hexdigest()!=model['selection_raw_lines_sha256']:raise ValueError('exact query lines differ')
    return dict(scope='actual new serializer on reconstructed operation envelopes/counts; placeholder node/local IDs, subtree/snapshot/tree/request IDs/deadlines and batch membership; not historical wire bodies',trace_sha256=sha(trace_path),query_sha256=sha(query_path),model_sha256=sha(model_path),queries=rows,mean_legacy_bytes=sum(r['legacy_bytes'] for r in rows)/16,mean_shared_bytes=sum(r['shared_bytes'] for r in rows)/16)


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','binary','manifest','expected-inventory','output','fixed16-trace','fixed16-queries','fixed16-model'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);parser.add_argument('--expected-fixed16-model-sha256',required=True);a=parser.parse_args();root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2];sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    if sha(a.fixed16_model)!=a.expected_fixed16_model_sha256 or before!=json.loads(a.expected_inventory.read_bytes()) or sha(a.binary)!=a.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('source/input/binary admission differs')
    receipt=json.loads((root/'receipt.json').read_bytes());deployment=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes())['deployment'];tree=deployment['tree']
    if tree['dimension']!=32 or tree['source']['row_count']!=98 or len(tree['subtrees'])!=2:raise ValueError('exact retained fixture required')
    mappings={s['subtree_id']:[row[0] for row in struct.iter_unpack('<Q',checked_artifact(s['local_to_global'],'.').read_bytes())] for s in tree['subtrees']}
    configuration=dict(tree_sha256=content_hash(tree),owner_id='adapter',timeout=5,shared_query_batches=True,subtrees={s['subtree_id']:descriptor(a.binary,deployment['owners'][s['subtree_id']]['checkpoint_dir'],s) for s in tree['subtrees']})
    placement=dict(owners={s['subtree_id']:dict(owner_id='adapter',endpoint='http://inprocess') for s in tree['subtrees']});out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    result=dict(status='failed',scope='actual HTTP serializer + in-process adapter + native readers, no listener/network latency proof');bodies=[];native=[];proof=[];host=None;started=time.monotonic();mode='setup';children=[]
    previous_env={k:os.environ.get(k) for k in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS')};os.environ.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
    def stop(*_):raise TimeoutError('query sharing five-minute work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(300)
    class Stream:
        headers={}
        def __init__(self,body):self.body=body
        def __enter__(self):return self
        def __exit__(self,*_):pass
        def read(self,n):return self.body[:n]
    def dispatch(request,timeout):
        if request.full_url!='http://inprocess/batch':raise ValueError('unexpected endpoint')
        sent=json.loads(request.data);expanded,format_name=expand_batch(sent,True,host.readers)
        baseline=dict(tree_sha256=sent['tree_sha256'],request_id=sent['request_id'],requests=expanded,timeout_s=sent['timeout_s']);legacy=encode_batch(baseline);shared=encode_batch(baseline,True)
        bodies.append(dict(mode=mode,actual_body=request.data.decode(),legacy_body=legacy.decode(),shared_body=shared.decode(),legacy_bytes=len(legacy),shared_bytes=len(shared),paired_scope='same exact operation fields/envelope and numeric values'))
        return Stream(json.dumps(host.batch(sent),allow_nan=False).encode())
    try:
        host=Host(configuration)
        for sid,pinned in host.readers.items():
            children.append(pinned.reader.process);original=pinned.reader.query
            def observed(payload,*args,_sid=sid,_call=original,**kwargs):
                response=_call(payload,*args,**kwargs);native.append(dict(mode=mode,subtree_id=_sid,request=copy.deepcopy(payload),response=response));return response
            pinned.reader.query=observed
        coordinators={name:TreeCoordinator(tree,load_model(tree),HttpBackend(placement,share_query=name=='shared'),mappings) for name in ('legacy','shared')}
        for query,budget,width in (([47.4,1.,-0.]+[0.]*29,1,None),([48.,4097.,-0.]+[0.]*29,16,None),([48.,4097.,-0.]+[0.]*29,16,None)):
            rows={}
            for mode in ('legacy','shared'):
                rows[mode]=coordinators[mode].query(query,min(4,budget),beam_width=width,candidate_budget=budget,timeout=5)
            if logical_trace(rows['legacy'])!=logical_trace(rows['shared']):raise ValueError('complete logical query trace differs')
            proof.append(dict(query=query,budget=budget,results=rows))
        result['fixed16']=fixed16(a.fixed16_trace,a.fixed16_queries,a.fixed16_model)
        # Child close is owned here, once per child; failed close is never retried.
        owned=host;host=None;owned.close();result['cleanup']=[dict(pid=p.pid,returncode=p.returncode) for p in children]
        if any(p.returncode is None for p in children):raise ValueError('native child cleanup unknown')
        rss=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss;rss_bytes=rss if os.uname().sysname=='Darwin' else rss*1024
        if rss_bytes>2*1024**3:raise ValueError('observed child highwater budget')
        result.update(status='passed',queries=proof,terminal_children_maxrss_bytes=rss_bytes,actual_pairs=dict(legacy_bytes=sum(b['legacy_bytes'] for b in bodies),shared_bytes=sum(b['shared_bytes'] for b in bodies),scope='paired samework bodies, includes both trace modes; not historical realdata bytes'),resource_scope='2configuredthreads/child, observed waited-child2GiB/50MiBoutput; no CPUquota/hardmemorycap')
    except BaseException as error:result['error']=repr(error);raise
    finally:
        if host is not None:
            owned=host;host=None
            try:owned.close();result['cleanup']=[dict(pid=p.pid,returncode=p.returncode) for p in children]
            except BaseException as error:result.update(status='failed-cleanup',cleanup_error=repr(error))
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        for key,value in previous_env.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        result['seconds']=time.monotonic()-started;(out/'bodies.json').write_text(json.dumps(bodies,indent=2)+'\n');(out/'native.json').write_text(json.dumps(native,indent=2)+'\n')
        data=json.dumps(result,indent=2)+'\n';files=[p for p in out.rglob('*') if p.is_file()]
        if len(data.encode())+sum(p.stat().st_size for p in files)>50*1024*1024:raise ValueError('observed output budget exceeded')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('query sharing did not reconcile')

if __name__=='__main__':main()
