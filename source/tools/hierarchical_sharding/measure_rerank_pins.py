"""Fixed exact-rerank diagnostic on retained multi-block owners only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import time
from .server import Reader
from .tree_manifest import checked_artifact


def inventory(root):
    files={};total=0
    for path in sorted(root.rglob('*')):
        if path.is_symlink():raise ValueError('fixture link is unsupported')
        if path.is_dir():continue
        if not path.is_file():raise ValueError('fixture contains a nonregular object')
        total+=path.stat().st_size
        if len(files)>=2048 or total>8*1024*1024:raise ValueError('fixture inventory budget exceeded')
        files[str(path.relative_to(root))]=dict(bytes=path.stat().st_size,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return files


def ids_for(count,size):
    return [0] if size==1 else [i*(count-1)//(size-1) for i in range(size)]


def logical(response):
    if 'error' in response:raise ValueError('unexpected native response error: '+response['error'])
    return dict(results=sorted(response['results'],key=lambda value:(value['id'],value['local_id'])),exact_vectors_scored=response['exact_vectors_scored'])


def main():
    parser=argparse.ArgumentParser()
    for name in ('fixture','binary','manifest','output'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True)
    args=parser.parse_args();repo=Path(__file__).resolve().parents[2];fixture=args.fixture.resolve();output=args.output.resolve()
    if output==fixture or fixture in output.parents or output in fixture.parents:raise ValueError('output overlaps fixture')
    hashes=json.loads(args.manifest.read_bytes())['files'];digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    if digest(args.binary)!=args.expected_binary_sha256:raise ValueError('reviewed reader binary differs')
    if {p:digest(repo/p) for p in hashes}!=hashes:raise ValueError('reviewed source changed')
    before=inventory(fixture);receipt=json.loads((fixture/'receipt.json').read_bytes());deployment=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes())['deployment']
    if deployment['tree']['dimension']!=32 or deployment['tree']['source']['row_count']!=98 or len(deployment['owners'])!=2:raise ValueError('exact retained 98x32 two-owner fixture required')
    if any(len(row['root']['block_ids'])<2 for row in receipt['saved_blocks']):raise ValueError('retained root inventory lacks diversity')
    output.mkdir(exist_ok=False,parents=True);(output/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    result=dict(status='failed',binary=dict(path=str(args.binary.resolve()),sha256=digest(args.binary)),scope='reader-held references and estimated Arrow payload only; no cache/decode/RSS bound')
    raw=[];cleanup=[];children=[];started=time.monotonic()
    def check():
        if time.monotonic()-started>90 or len(json.dumps(raw,indent=2).encode())>8*1024*1024:raise ValueError('observed diagnostic budget exceeded')
    def stop(*_):raise TimeoutError('rerank diagnostic work deadline exceeded')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(90)
    def opened(slot,mode):
        argv=[str(args.binary.resolve()),'read','--checkpoint-dir',deployment['owners'][slot['subtree_id']]['checkpoint_dir'],'--generation',str(slot['generation'])]
        if mode!='reference':argv+=['--profile-rerank-pins']
        if mode=='serial-release':argv+=['--release-rerank-vectors']
        reader=Reader(argv,5);children.append((reader,slot['subtree_id'],mode));return reader
    def close(reader):
        entry=next(row for row in children if row[0] is reader)
        children.remove(entry)  # A rejected cleanup is terminal, never retried.
        try:reader.close()
        except BaseException as error:
            cleanup.append(dict(owner=entry[1],mode=entry[2],error=repr(error)));result['status']='failed-cleanup';raise
        cleanup.append(dict(owner=entry[1],mode=entry[2],returncode=reader.process.returncode))
    def request(reader,slot,mode,ids,query=None,error=False):
        check();payload=dict(op='rerank_selected',generation=slot['generation'],request_id=str(len(raw)),query=query or [0.]*32,local_ids=ids)
        response=reader.query(payload,timeout=5);raw.append(dict(owner=slot['subtree_id'],mode=mode,request=payload,response=response));check()
        if mode=='reference' and ('vector_pin_observation' in response or 'exact_rerank_policy' in response):raise ValueError('default response unexpectedly enables diagnostics')
        if mode!='reference':
            expected='serial-release' if mode=='serial-release' else 'default-concurrent-retain'
            if response.get('exact_rerank_policy')!=expected:raise ValueError('native policy echo differs')
            stats=response['vector_pin_observation']
            if stats['current_count']!=0 or stats['current_estimated_payload_bytes']!=0:raise ValueError('operation leaves vector pins')
            if mode=='serial-release' and stats['peak_count']>max(stats['baseline_count'],1):raise ValueError('serial reference peak exceeds baseline/one-block contract')
        if error:
            if 'non-finite exact distance' not in response.get('error',''):raise ValueError('expected post-fetch error missing')
            return response
        return logical(response)
    try:
        owners=[]
        for slot in deployment['tree']['subtrees']:
            count=slot['vector_count'];sizes=[n for n in (1,2,4,8,16,32,count) if n<=count];sets=[ids_for(count,n) for n in sizes]+[ids_for(count,count),[0]]
            reference=opened(slot,'reference');control=[request(reference,slot,'reference',ids) for ids in sets]
            baseline=opened(slot,'baseline');actual=[request(baseline,slot,'baseline',ids) for ids in sets]
            if actual!=control:raise ValueError('instrumentation changes exact result')
            huge=[1e30]+[0.]*31;request(baseline,slot,'baseline',sets[2],huge,error=True)
            if request(baseline,slot,'baseline',sets[2])!=control[2]:raise ValueError('baseline error retry differs')
            stats=[row['response']['vector_pin_observation'] for row in raw if row['owner']==slot['subtree_id'] and row['mode']=='baseline' and 'error' not in row['response']]
            growth=max(row['peak_count'] for row in stats)>stats[0]['peak_count'] and max(row['peak_count'] for row in stats)>1
            close(baseline)
            owner=dict(owner=slot['subtree_id'],baseline_growth=growth,baseline_peaks=[row['peak_count'] for row in stats])
            # Only observed retention growth justifies activating the separately
            # reviewed serial release branch. No corpus expansion is allowed.
            if growth:
                serial=opened(slot,'serial-release');bounded=[request(serial,slot,'serial-release',ids) for ids in sets]
                if bounded!=control:raise ValueError('serial release changes exact result')
                request(serial,slot,'serial-release',sets[2],huge,error=True)
                if request(serial,slot,'serial-release',sets[2])!=control[2]:raise ValueError('serial error retry differs')
                close(serial);owner['bounded_executed']=True
            else:owner['bounded_executed']=False
            if request(reference,slot,'reference',sets[-2])!=control[-2]:raise ValueError('independent pinned reader changed')
            close(reference);owners.append(owner)
        result.update(status='passed',owners=owners,decision='bounded-release-validated' if all(row['bounded_executed'] for row in owners) else 'insufficient-retention-growth')
    except BaseException as error:result['error']=repr(error);raise
    finally:
        for reader,owner,mode in list(children):
            try:close(reader)
            except BaseException as error:cleanup.append(dict(owner=owner,mode=mode,error=repr(error)));result['status']='failed-cleanup'
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        try:
            after=inventory(fixture);result['inventory_unchanged']=after==before
            (output/'after.json').write_text(json.dumps(after,indent=2)+'\n')
            result['end_source_hashes']={p:digest(repo/p) for p in hashes}
            if after!=before or result['end_source_hashes']!=hashes:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        if any(type(row.get('returncode')) is not int for row in cleanup):result['status']='failed-cleanup'
        result.update(seconds=time.monotonic()-started,cleanup=cleanup)
        (output/'raw.json').write_text(json.dumps(raw,indent=2)+'\n');(output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    if result['status']!='passed':raise ValueError('diagnostic did not reconcile')


if __name__=='__main__':main()
