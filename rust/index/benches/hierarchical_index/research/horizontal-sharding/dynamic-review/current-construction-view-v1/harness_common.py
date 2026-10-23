"""Run five tiny local native cells; no network, lifecycle, or corpus transfer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

CELLS=('serial-serial','parallel-serial','serial-parallel','parallel-parallel','authoritative')

def require(ok, message):
    if not ok: raise ValueError(message)

def descriptor(path):
    path=Path(path).resolve(strict=True);h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
    return dict(path=str(path),bytes=path.stat().st_size,sha256=h.hexdigest())

def verify(item):
    require(descriptor(item['path'])==item,'descriptor mismatch: '+item['path'])
    return Path(item['path'])

def write(path, value):
    with path.open('x') as stream:json.dump(value,stream,indent=2);stream.write('\n')

def run(spec):
    # External owner fixes the absolute deadline; this script never extends it.
    require(time.time()<spec['deadline_unix'],'expired external deadline')
    require(type(spec['threads']) is int and 1<spec['threads']<=8,'parallel threads must be2..8')
    require(spec['rows']==8192 and spec['dimension']==768 and spec['balance_every']==4096,'fixed tiny diagnostic extent')
    require(spec['arm_seconds']==30 and spec['total_seconds']==120,'fixed diagnostic compute budget')
    binary=verify(spec['binary']);receipt=json.loads(verify(spec['restore_receipt']).read_text())
    require(receipt['status']=='complete' and receipt['canonical']==spec['vectors'],'restored source binding')
    require(receipt['rows']==2000000 and receipt['dimension']==768 and receipt['expected_bytes']==6144000000,'source cohort')
    source=Path(spec['vectors']['path']);require(source.is_file() and source.stat().st_size==6144000000,'source extent')
    output=Path(spec['output']);output.mkdir(exist_ok=False)
    preparation=time.monotonic()
    with source.open('rb') as stream:prefix=stream.read(8192*768*4)
    require(len(prefix)==8192*768*4,'prefix truncated');prefix_sha=hashlib.sha256(prefix).hexdigest();del prefix
    write(output/'preparation.json',dict(prefix_bytes=8192*768*4,prefix_sha256=prefix_sha,elapsed_seconds=time.monotonic()-preparation,source_receipt=spec['restore_receipt'],source_binding='Accepted restored immutable input; fullfileSHA not recomputed by this diagnostic; onlyprefix read andhashed'))
    env=dict(os.environ,RAYON_NUM_THREADS=str(spec['threads']),TOKIO_WORKER_THREADS=str(spec['threads']),OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
    started=time.monotonic();results=[];failure=None
    try:
        for cell in CELLS:
            remaining=min(120-(time.monotonic()-started),spec['deadline_unix']-time.time())
            require(remaining>0,'total diagnostic deadline')
            native=dict(vectors=spec['vectors'],prefix_sha256=prefix_sha,rows=8192,dimension=768,metric='ip',config=spec['config'],cell=cell,threads=spec['threads'],balance_every=4096,output=str(output/cell))
            path=output/(cell+'-spec.json');write(path,native)
            command=[str(binary),'build-speed-diagnostic','--spec',str(path)]
            before=time.monotonic()
            with (output/(cell+'.stdout')).open('xb') as out,(output/(cell+'.stderr')).open('xb') as err:
                try:p=subprocess.run(command,stdout=out,stderr=err,env=env,timeout=min(30,remaining))
                except subprocess.TimeoutExpired:
                    write(output/(cell+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-before));raise
            process=dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-before)
            write(output/(cell+'-process.json'),process);require(p.returncode==0,'native cell failed: '+cell)
            value=json.loads((output/cell/'result.json').read_text())
            require(json.loads((output/(cell+'.stdout')).read_text())==value,'native stdout/result mismatch')
            require(value['status']=='passed' and value['spec']==native and value['spec_sha256']==descriptor(path)['sha256'],'native output binding')
            require(value['census']['unique_live_rows']==8192 and value['census']['duplicate_current_entries']==0,'census')
            results.append(dict(cell=cell,result=descriptor(output/cell/'result.json'),process=process))
    except Exception as error:failure=str(error)
    finally:
        # Logs are not transferred here. Report observed aggregate evidence size.
        total=sum(p.stat().st_size for p in output.rglob('*') if p.is_file())
        result=dict(status='passed' if failure is None else 'failed',error=failure,results=results,compute_elapsed_seconds=time.monotonic()-started,evidence_bytes=total,binary=spec['binary'],prefix_sha256=prefix_sha,scope='Samebinary/config/prefix/cuts; stochastic independenttrees. Build/preparation separate; cachewarm afterwrites, no coldOS/physicalIO claim; diagnostic observer overhead included and unmeasured separately. Directchild timeout doesnot guarantee detacheddescendants; native uses owned localactors only.')
        if total>50*1024**2:result.update(status='failed',error='observed evidence exceeds50MiB')
        write(output/'result.json',result)
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--spec',required=True);args=parser.parse_args()
    result=run(json.loads(Path(args.spec).read_text()));print(json.dumps(result));raise SystemExit(result['status']!='passed')
