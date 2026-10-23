"""Paired eager versus reached-node routing against one fixed admitted parent."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import struct
import subprocess
import time
from .measure_rerank_pins import inventory
from .measure_shared_store import store_delta
from .run_poc_lazy_write import copied
from .server import Reader
from .tree_manifest import artifact,checked_artifact,content_hash,load_model


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','output','binary','manifest','expected-inventory','bootstrap'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);a=parser.parse_args();root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2];sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    if before!=json.loads(a.expected_inventory.read_bytes()) or sha(a.binary)!=a.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('source/input/binary differs')
    parent=json.loads((root/'parent.json').read_bytes());receipt=json.loads((root/'receipt.json').read_bytes());reference=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes());expected=json.loads(checked_artifact(reference['writer_execution'],'.').read_bytes());bootstrap=json.loads(a.bootstrap.read_bytes())
    if bootstrap['status']!='passed' or bootstrap['phases']['bootstrap-parent96']['value']['tree_sha256']!=parent['tree_sha256']:raise ValueError('prior full semantic bootstrap required')
    export=json.loads(checked_artifact(reference['materialized_source_binding']['export'],'.').read_bytes());rows=export['captured']['rows'];tail=b''.join(struct.pack('<'+'I'*len(r['embedding_bits']),*r['embedding_bits']) for r in rows)
    if [r['global_id'] for r in rows]!=[96,97] or len(tail)!=256:raise ValueError('actual append source differs')
    old=parent['canonical_vectors'];grown=reference['deployment']['canonical_vectors']
    with Path(grown['path']).open('rb') as f:f.seek(12288);actual=f.read(256)
    if actual!=tail:raise ValueError('materialized bits differ')
    view=dict(assumption='fixed-immutable-input-poc',dimension=32,rows=98,spans=[dict(path=old['path'],global_start=0,rows=96,source_offset_bytes=0,bytes=12288,sha256=old['sha256']),dict(path=grown['path'],global_start=96,rows=2,source_offset_bytes=12288,bytes=256,sha256=hashlib.sha256(tail).hexdigest())]);model=load_model(parent['tree']);owners=[dict(subtree_id=s['subtree_id'],checkpoint_dir=parent['owners'][s['subtree_id']]['checkpoint_dir'],generation=s['generation'],manifest_sha256=s['snapshot']['sha256'],centroid=model['centroids'][s['centroid_index']]) for s in sorted(parent['tree']['subtrees'],key=lambda s:s['subtree_id'])]
    if [o['subtree_id'] for o in owners]!=['a','b']:raise ValueError('exact owner pair required')
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n');started=time.monotonic();result=dict(status='failed',assumption='fixed immutable audited no-balancing add-only parent',bootstrap_semantic=dict(path=str(a.bootstrap),sha256=sha(a.bootstrap),scope='prior all-owner semantic audit separately charged'),modes={});readers=[];cleanup=[];raw=[];old_env={k:os.environ.get(k) for k in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS')};os.environ.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
    def stop(*_):raise TimeoutError('five-minute lazy routing deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(300)
    def close(r):
        readers.remove(r)
        try:r.close();cleanup.append(dict(pid=r.process.pid,returncode=r.process.returncode))
        except BaseException as error:cleanup.append(dict(pid=r.process.pid,error=repr(error)));raise
    def query(directory,generation,count,label):
        r=Reader([str(a.binary.resolve()),'read','--checkpoint-dir',str(directory),'--generation',str(generation)],5);readers.append(r);request=dict(op='rerank_selected',generation=generation,request_id=label,query=[0.]*32,local_ids=list(range(count)));response=r.query(request,timeout=5);raw.append(dict(request=request,response=response));close(r)
        if 'error' in response or response['exact_vectors_scored']!=count:raise ValueError('reader parity operation failed')
        return sorted((row['id'],row['local_id'],row['distance']) for row in response['results'])
    def execute(directory,name,command,timeout):
        with (directory/(name+'.stdout')).open('xb') as so,(directory/(name+'.stderr')).open('xb') as se:p=subprocess.run(command,stdout=so,stderr=se,timeout=timeout)
        if p.returncode or (directory/(name+'.stdout')).stat().st_size>1024*1024:raise ValueError(name+' native failed')
        return json.loads((directory/(name+'.stdout')).read_bytes())
    try:
        original=Path(owners[1]['checkpoint_dir']);old_store=inventory(original);oracle_old=query(original,1,48,'parent-old');groups={r['subtree_id']:r['ids'] for r in expected['owners']};admission=None
        for mode in ('eager','lazy'):
            directory=out/mode;directory.mkdir();store=out/('owned-'+mode)/'b';copied_bytes=0;copied_files=0
            for path in sorted(original.rglob('*')):
                if path.is_file():copied_bytes+=copied(path,store/path.relative_to(original));copied_files+=1
            if inventory(store)!=old_store:raise ValueError('owned bootstrap differs')
            spec=dict(assumption='fixed-immutable-input-poc',counted_parent=True,parent_sha256=content_hash(parent),source_id=parent['tree']['source']['source_id'],dimension=32,metric=parent['tree']['metric'],old_count=96,view=view,owners=owners,output=str(directory/'writer'),shared_stores={'b':str(store)})
            if mode=='eager':spec['emit_routing_bootstrap']=True
            else:spec['routing_admission']=admission
            spec_path=directory/'spec.json';spec_path.write_text(json.dumps(spec)+'\n');reply=execute(directory,'writer',[str(a.binary.resolve()),'poc-lazy-global-writes','--spec',str(spec_path)],30)
            if reply['input_spec_sha256']!=sha(spec_path) or reply['routes']!=expected['routes'] or reply['mutable_writers_opened']!=(2 if mode=='eager' else 1) or len(reply['saved'])!=1 or any(v!={'copied_bytes':0,'copied_files':0} for v in reply['copies'].values()):raise ValueError('routes/selected mutable owner set differs')
            if mode=='eager':
                admission=reply['routing_bootstrap'];(out/'routing-bootstrap.json').write_text(json.dumps(admission,indent=2)+'\n')
                if admission['parent_sha256']!=content_hash(parent) or [o['subtree_id'] for o in admission['owners']]!=['a','b']:raise ValueError('bootstrap binding differs')
            manifest_path=store/'manifest-2.json';manifest=json.loads(manifest_path.read_bytes());mapping=store/manifest['id_mapping']['path']
            if [v[0] for v in struct.iter_unpack('<Q',mapping.read_bytes())]!=groups['b']:raise ValueError('map mismatch')
            selected=[(r['leaf_id'],r['local_id']) for r in reply['routes']];audit=dict(assumption='fixed-immutable-input-poc',checkpoint_dir=str(store),generation=2,manifest_sha256=sha(manifest_path),map=str(mapping),map_sha256=sha(mapping),view=view,selected=selected,subtree_id='b',source_id=spec['source_id'],metric=spec['metric'],routing_boundary_hash=manifest['routing_boundary_hash'],root_ref=manifest['root_ref']);path=directory/'audit.json';path.write_text(json.dumps(audit)+'\n');checked=execute(directory,'audit',[str(a.binary.resolve()),'poc-audit-snapshot','--spec',str(path)],10)
            if checked['global_ids']!=groups['b'] or checked['selected']!=[list(v) for v in selected] or checked['coverage']['duplicate_current_entries']!=0:raise ValueError('saved owner differs')
            after=inventory(store);delta=store_delta(old_store,after,True);(directory/'after-store.json').write_text(json.dumps(after,indent=2)+'\n')
            if query(store,1,48,mode+'-old')!=oracle_old:raise ValueError('old generation differs')
            new=query(store,2,50,mode+'-new')
            if [row for row in new if row[1]<48]!=oracle_old:raise ValueError('old saved prefix differs')
            result['modes'][mode]=dict(write=reply,saved_audit=checked,store_delta=delta,old_results=oracle_old,new_results=new,owned_store_bootstrap=dict(copied_bytes=copied_bytes,copied_files=copied_files,scope='separate setup for each independently measured mode'))
        if result['modes']['eager']['new_results']!=result['modes']['lazy']['new_results']:raise ValueError('complete rerank parity differs')
        rss=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss;rss_bytes=rss if os.uname().sysname=='Darwin' else rss*1024
        if rss_bytes>2*1024**3:raise ValueError('observed child RSS exceeded')
        result.update(status='passed',terminal_children_maxrss_bytes=rss_bytes,routing_bootstrap_sha256=sha(out/'routing-bootstrap.json'),remaining_costs=['flat boundary scoring O(owners*dimension)','owner manifest/root metadata O(owners)','selected writer still opens full selected topology','selected full map/construction vectors/audit','record/legacy scans separate'],scope='mechanism only; bootstrap/full selected costs separate; no real-data latency claim; pin/cache/provider decode/allocator/RSS are not routing payload bounds')
    except BaseException as error:result['error']=repr(error);raise
    finally:
        for r in list(readers):
            try:close(r)
            except BaseException as error:result.update(status='failed-cleanup',cleanup_error=repr(error))
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        for key,value in old_env.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        if any(type(row.get('returncode')) is not int for row in cleanup):result['status']='failed-cleanup'
        result.update(seconds=time.monotonic()-started,cleanup=cleanup);(out/'raw-readers.json').write_text(json.dumps(raw,indent=2)+'\n');data=json.dumps(result,indent=2)+'\n'
        if len(data.encode())+sum(p.stat().st_size for p in out.rglob('*') if p.is_file())>50*1024*1024:raise ValueError('observed output budget')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('lazy routing did not reconcile')

if __name__=='__main__':main()
