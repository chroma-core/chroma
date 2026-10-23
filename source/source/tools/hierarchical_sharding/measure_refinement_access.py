"""Fixed-candidate access accounting, not a latency or OS-cold benchmark."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import time
from .measure_rerank_pins import inventory, ids_for, logical
from .server import Reader
from .tree_manifest import checked_artifact


def totals(response):
    observation=response['vector_pin_observation']
    if observation['access_overflow']:raise ValueError('access cardinality exceeded')
    fields=('reader_requests','pin_hits','pin_misses','provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes','obtain_ns','decode_ns')
    values={field:sum(row[field] for row in observation['block_access'].values()) for field in fields}
    values.update(required_blocks=len(set(observation['point_blocks'].values())),mapped_ids=len(observation['point_blocks']),rerank_operation_ns=response['rerank_operation_ns'])
    if values['reader_requests']!=values['pin_hits']+values['pin_misses'] or values['provider_obtains']!=values['provider_hits']+values['provider_misses'] or values['decode_attempts']!=values['storage_callbacks']:raise ValueError('counter partition differs')
    return values


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','binary','manifest','expected-inventory','output'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);a=parser.parse_args();root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2]
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    if before!=json.loads(a.expected_inventory.read_bytes()) or sha(a.binary)!=a.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('admitted inputs differ')
    receipt=json.loads((root/'receipt.json').read_bytes());deployment=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes())['deployment']
    if deployment['tree']['dimension']!=32 or deployment['tree']['source']['row_count']!=98 or len(deployment['owners'])!=2:raise ValueError('exact retained fixture required')
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n');raw=[];children=[];cleanup=[];result=dict(status='failed',assumption='fresh provider process is not OS-cold',root_open='excluded from per-operation block counters',cache='existing default plain Foyer, no disk-cache reconstruction claim');started=time.monotonic()
    env={key:os.environ.get(key) for key in ('TOKIO_WORKER_THREADS','RAYON_NUM_THREADS')};os.environ.update(TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
    def stop(*_):raise TimeoutError('five-minute work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(300)
    def close(reader):
        children.remove(reader)
        try:reader.close();cleanup.append(dict(pid=reader.process.pid,returncode=reader.process.returncode))
        except BaseException as error:cleanup.append(dict(pid=reader.process.pid,error=repr(error)));raise
    def opened(slot,mode):
        argv=[str(a.binary.resolve()),'read','--checkpoint-dir',deployment['owners'][slot['subtree_id']]['checkpoint_dir'],'--generation',str(slot['generation'])]
        if mode!='reference':argv+=['--profile-rerank-pins']
        if mode=='serial-release':argv+=['--release-rerank-vectors']
        reader=Reader(argv,5);children.append(reader);return reader
    def ask(reader,slot,mode,ids,phase):
        response=reader.query(dict(op='rerank_selected',generation=slot['generation'],request_id=str(len(raw)),query=[0.]*32,local_ids=ids),timeout=5)
        row=dict(owner=slot['subtree_id'],mode=mode,phase=phase,ids=ids,response=response);raw.append(row)
        if len(json.dumps(raw).encode())>8*1024*1024:raise ValueError('raw response limit')
        if mode!='reference':
            row['totals']=totals(response)
            if row['totals']['reader_requests']!=len(ids) or row['totals']['mapped_ids']!=len(ids):raise ValueError('actual lookup mapping differs')
            if response['vector_pin_observation']['current_count']!=0:raise ValueError('terminal pins remain')
        return logical(response)
    try:
        summary=[]
        for slot in deployment['tree']['subtrees']:
            count=slot['vector_count'];ids=ids_for(count,min(count,32))
            reference=opened(slot,'reference');oracle=ask(reference,slot,'reference',ids,'independent-oracle');close(reference)
            # Each mode has a separate provider/cache lifetime. Root opening is
            # outside the per-operation counters; the first vector pass is fresh.
            for mode in ('default','serial-release'):
                reader=opened(slot,mode)
                for phase in ('fresh-provider','warm-repeat'):
                    if ask(reader,slot,mode,ids,phase)!=oracle:raise ValueError('exact output differs')
                close(reader)
                phases=[row for row in raw if row['owner']==slot['subtree_id'] and row['mode']==mode]
                cold,warm=[row['totals'] for row in phases]
                summary.append(dict(owner=slot['subtree_id'],mode=mode,fresh=cold,warm=warm,storage_origin_decode_amplification=cold['decode_successes']/cold['required_blocks'] if cold['required_blocks'] else None))
        maxrss=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        # Darwin reports bytes; Linux reports KiB. This terminal child highwater
        # is observed OS accounting, not an instantaneous enforced memory cap.
        rss_bytes=maxrss if os.uname().sysname=='Darwin' else maxrss*1024
        if rss_bytes>2*1024**3:raise ValueError('observed child highwater exceeds2GiB')
        consequential=any(row['fresh']['decode_successes']>=2*row['fresh']['required_blocks'] and row['fresh']['decode_successes']-row['fresh']['required_blocks']>=4 for row in summary if row['mode']=='default')
        result.update(status='passed',summary=summary,terminal_children_maxrss_bytes=rss_bytes,decision='substantial-cold-duplicate-decoding-observed' if consequential else 'stop-no-substantial-duplicate-decoding',threshold='default fresh storage-origin decodes>=2xrequired blocks and>=4extra, mechanism criterion only',cpu_scope='Tokio/Rayon2threads, no OS CPU quota',memory_scope='terminal waited-child OS maxrss observed<=2GiB; no hard memory cap')
    except BaseException as error:result['error']=repr(error);raise
    finally:
        for reader in list(children):
            try:close(reader)
            except BaseException as error:result.update(status='failed-cleanup',cleanup_error=repr(error))
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        for key,value in env.items():
            if value is None:os.environ.pop(key,None)
            else:os.environ[key]=value
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        if any(type(row.get('returncode')) is not int for row in cleanup):result['status']='failed-cleanup'
        result.update(seconds=time.monotonic()-started,cleanup=cleanup);(out/'raw.json').write_text(json.dumps(raw,indent=2)+'\n')
        files=[p for p in out.rglob('*') if p.is_file()];result['output_bytes']=sum(p.stat().st_size for p in files)
        if result['output_bytes']>50*1024*1024:result['status']='failed-observed-output-budget'
        (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    if result['status']!='passed':raise ValueError('measurement did not reconcile')

if __name__=='__main__':main()
