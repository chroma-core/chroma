"""Paired actual append saves with a separately charged owned-store bootstrap."""
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
from .run_poc_lazy_write import copied
from .server import Reader
from .tree_manifest import artifact,checked_artifact,content_hash,load_model


def object_key(key):return key.startswith('block/') or key.startswith('sparse_index/')


def store_delta(before,after,preserve_all=False):
    if any(key not in after or after[key]!=value for key,value in before.items() if preserve_all or object_key(key)):raise ValueError('old store bytes or keys changed')
    new={key:value for key,value in after.items() if key not in before};objects={key:value for key,value in new.items() if object_key(key)}
    return dict(new_uuid_objects=objects,new_uuid_object_count=len(objects),new_uuid_object_bytes=sum(row['bytes'] for row in objects.values()),new_nonobject_files={key:value for key,value in new.items() if not object_key(key)},modified_old_nonobjects={key:after.get(key) for key,value in before.items() if not object_key(key) and after.get(key)!=value},scope='retained new UUID-key file bytes, not physical write traffic; renamed legacy sidecars/head aliases are not new index objects')


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','output','binary','manifest','expected-inventory','bootstrap'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);a=parser.parse_args();root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2];sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    if before!=json.loads(a.expected_inventory.read_bytes()) or sha(a.binary)!=a.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('source/input/binary admission differs')
    parent=json.loads((root/'parent.json').read_bytes());receipt=json.loads((root/'receipt.json').read_bytes());reference=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes());expected=json.loads(checked_artifact(reference['writer_execution'],'.').read_bytes());bootstrap=json.loads(a.bootstrap.read_bytes())
    if bootstrap['status']!='passed' or bootstrap['phases']['bootstrap-parent96']['value']['tree_sha256']!=parent['tree_sha256']:raise ValueError('exact full parent audit required')
    export=json.loads(checked_artifact(reference['materialized_source_binding']['export'],'.').read_bytes());rows=export['captured']['rows'];tail=b''.join(struct.pack('<'+'I'*len(r['embedding_bits']),*r['embedding_bits']) for r in rows)
    if [r['global_id'] for r in rows]!=[96,97] or [r['record_offset_id'] for r in rows]!=[97,98] or len(tail)!=256:raise ValueError('actual materializer source differs')
    old=parent['canonical_vectors'];grown=reference['deployment']['canonical_vectors']
    with Path(grown['path']).open('rb') as source:source.seek(12288);actual=source.read(256)
    if actual!=tail:raise ValueError('source vector bits differ')
    view=dict(assumption='fixed-immutable-input-poc',dimension=32,rows=98,spans=[dict(path=old['path'],global_start=0,rows=96,source_offset_bytes=0,bytes=12288,sha256=old['sha256']),dict(path=grown['path'],global_start=96,rows=2,source_offset_bytes=12288,bytes=256,sha256=hashlib.sha256(tail).hexdigest())]);model=load_model(parent['tree']);owners=[dict(subtree_id=s['subtree_id'],checkpoint_dir=parent['owners'][s['subtree_id']]['checkpoint_dir'],generation=s['generation'],manifest_sha256=s['snapshot']['sha256'],centroid=model['centroids'][s['centroid_index']]) for s in sorted(parent['tree']['subtrees'],key=lambda s:s['subtree_id'])]
    if len(owners)!=2 or [o['subtree_id'] for o in owners]!=['a','b']:raise ValueError('exact two-owner fixture required')
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n');result=dict(status='failed',assumption='fixed immutable parent POC; shared store is experiment-owned, not a provider immutability guarantee',bootstrap_audit=dict(path=str(a.bootstrap),sha256=sha(a.bootstrap),scope='prior full audit separately charged'),modes={});raw=[];readers=[];cleanup=[];started=time.monotonic();previous_env={k:os.environ.get(k) for k in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS')};os.environ.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
    def stop(*_):raise TimeoutError('five-minute shared-store work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(300)
    def opened(directory,generation):
        reader=Reader([str(a.binary.resolve()),'read','--checkpoint-dir',str(directory),'--generation',str(generation)],5);readers.append(reader);return reader
    def close(reader):
        readers.remove(reader)
        try:reader.close();cleanup.append(dict(pid=reader.process.pid,returncode=reader.process.returncode))
        except BaseException as error:cleanup.append(dict(pid=reader.process.pid,error=repr(error)));raise
    def query(reader,generation,count,label):
        request=dict(op='rerank_selected',generation=generation,request_id=label,query=[0.]*32,local_ids=list(range(count)));response=reader.query(request,timeout=5);raw.append(dict(request=request,response=response))
        if 'error' in response or response['exact_vectors_scored']!=count:raise ValueError('complete old/new reader operation failed')
        return sorted((r['id'],r['local_id'],r['distance']) for r in response['results'])
    try:
        source_b=Path(owners[1]['checkpoint_dir']);old_store=inventory(source_b);shared=out/'owned-shared-store/b';bootstrap_counts=dict(copied_bytes=0,copied_files=0)
        for path in sorted(source_b.rglob('*')):
            if path.is_dir():continue
            bootstrap_counts['copied_bytes']+=copied(path,shared/path.relative_to(source_b));bootstrap_counts['copied_files']+=1
        shared_before=inventory(shared)
        if shared_before!=old_store:raise ValueError('owned bootstrap copy differs')
        (out/'shared-before.json').write_text(json.dumps(shared_before,indent=2)+'\n');result['bootstrap_copy']=dict(**bootstrap_counts,scope='once-per-owned-store setup; first-use copy cost is not eliminated')
        pinned=opened(shared,1);old_results=query(pinned,1,48,'old-pinned-before')
        oracle_groups={r['subtree_id']:r['ids'] for r in expected['owners']}
        for mode in ('clone','shared'):
            directory=out/mode;directory.mkdir();spec=dict(assumption='fixed-immutable-input-poc',counted_parent=True,parent_sha256=content_hash(parent),source_id=parent['tree']['source']['source_id'],dimension=32,metric=parent['tree']['metric'],old_count=96,view=view,owners=owners,output=str(directory/'writer'))
            if mode=='shared':spec['shared_stores']={'b':str(shared)}
            spec_path=directory/'spec.json';spec_path.write_text(json.dumps(spec)+'\n')
            with (directory/'writer.stdout').open('xb') as stdout,(directory/'writer.stderr').open('xb') as stderr:process=subprocess.run([str(a.binary.resolve()),'poc-lazy-global-writes','--spec',str(spec_path)],stdout=stdout,stderr=stderr,timeout=30)
            if process.returncode or (directory/'writer.stdout').stat().st_size>1024*1024:raise ValueError('actual writer failed: '+mode)
            reply=json.loads((directory/'writer.stdout').read_bytes())
            if reply['routes']!=expected['routes'] or len(reply['saved'])!=1 or reply['copies']['a']!={'copied_bytes':0,'copied_files':0}:raise ValueError('same global routes/selected-owner set differ')
            if reply['copies']['b']!=(bootstrap_counts if mode=='clone' else dict(copied_bytes=0,copied_files=0)):raise ValueError('actual clone accounting differs')
            checkpoint=Path(reply['saved'][0]['checkpoint_dir']);manifest_path=checkpoint/'manifest-2.json';manifest=json.loads(manifest_path.read_bytes());mapping=checkpoint/manifest['id_mapping']['path']
            if [v[0] for v in struct.iter_unpack('<Q',mapping.read_bytes())]!=oracle_groups['b']:raise ValueError('saved map differs')
            selected=[(r['leaf_id'],r['local_id']) for r in reply['routes']];audit=dict(assumption='fixed-immutable-input-poc',checkpoint_dir=str(checkpoint),generation=2,manifest_sha256=sha(manifest_path),map=str(mapping),map_sha256=sha(mapping),view=view,selected=selected,subtree_id='b',source_id=spec['source_id'],metric=spec['metric'],routing_boundary_hash=manifest['routing_boundary_hash'],root_ref=manifest['root_ref']);audit_path=directory/'audit.json';audit_path.write_text(json.dumps(audit)+'\n')
            with (directory/'audit.stdout').open('xb') as stdout,(directory/'audit.stderr').open('xb') as stderr:auditor=subprocess.run([str(a.binary.resolve()),'poc-audit-snapshot','--spec',str(audit_path)],stdout=stdout,stderr=stderr,timeout=10)
            if auditor.returncode:raise ValueError('saved semantic audit failed')
            checked=json.loads((directory/'audit.stdout').read_bytes())
            if checked['global_ids']!=oracle_groups['b'] or checked['selected']!=[list(v) for v in selected] or checked['coverage']['duplicate_current_entries']!=0:raise ValueError('exact saved versions/vectors/leaves differ')
            after=inventory(checkpoint);delta=store_delta(old_store,after,preserve_all=mode=='shared')
            execution=checkpoint/'generation-2' if mode=='shared' else checkpoint
            delta['execution_input_vector_bytes']=(execution/'construction-input.f32').stat().st_size;delta['execution_input_map_bytes']=(execution/'construction-input.u64le').stat().st_size;delta['execution_proof_bytes']=(execution/'compactor-execution.json').stat().st_size;delta['published_map_bytes']=mapping.stat().st_size;delta['generation_manifest_bytes']=manifest_path.stat().st_size
            (directory/'after-store.json').write_text(json.dumps(after,indent=2)+'\n')
            result['modes'][mode]=dict(write=reply,saved_audit=checked,store_delta=delta,exitcodes=[process.returncode,auditor.returncode])
        if query(pinned,1,48,'old-pinned-after')!=old_results:raise ValueError('old pinned reader changed')
        fresh_old=opened(shared,1)
        if query(fresh_old,1,48,'old-fresh-after')!=old_results:raise ValueError('old generation cannot reopen exactly')
        close(fresh_old);fresh_new=opened(shared,2);new_results=query(fresh_new,2,50,'new-fresh-after');close(fresh_new)
        if [r for r in new_results if r[1]<48]!=old_results:raise ValueError('old row prefix changed')
        close(pinned);shared_after=inventory(shared);store_delta(shared_before,shared_after,True);(out/'shared-after.json').write_text(json.dumps(shared_after,indent=2)+'\n')
        if any(key.startswith('retained-') for key in set(shared_after)-set(shared_before)):raise ValueError('shared save renamed old sidecars')
        rss=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss;rss_bytes=rss if os.uname().sysname=='Darwin' else rss*1024
        if rss_bytes>2*1024**3:raise ValueError('observed waited-child highwater budget')
        result.update(status='passed',terminal_children_maxrss_bytes=rss_bytes,old_reader_results=old_results,new_reader_results=new_results,scope='one actual96→98 pair; bootstrap charged, no first-use end-to-end copy advantage claimed',remaining_costs=['same new dirty object writes in BOTH modes','full selected-owner construction vectors/map/proof and semantic audit','all-owner metadata open/per-row widths','record/legacy complete validation separate'],complete_vector_view=dict(parent_sha256=content_hash(parent),rows=98,canonical_view=view,unchanged_owner_a=parent['owners']['a'],new_owner_b=dict(checkpoint_dir=str(shared),snapshot=artifact(shared/'manifest-2.json'),map=artifact(shared/manifest['id_mapping']['path'])),no_head_update=True)
    except BaseException as error:result['error']=repr(error);raise
    finally:
        for reader in list(readers):
            try:close(reader)
            except BaseException as error:result.update(status='failed-cleanup',cleanup_error=repr(error))
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        for key,value in previous_env.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        if any(type(row.get('returncode')) is not int for row in cleanup):result['status']='failed-cleanup'
        result.update(seconds=time.monotonic()-started,cleanup=cleanup);(out/'raw-readers.json').write_text(json.dumps(raw,indent=2)+'\n');data=json.dumps(result,indent=2)+'\n';files=[p for p in out.rglob('*') if p.is_file()]
        if len(data.encode())+sum(p.stat().st_size for p in files)>50*1024*1024:raise ValueError('observed output budget')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('shared store experiment did not reconcile')

if __name__=='__main__':main()
