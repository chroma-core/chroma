"""Bounded exploratory controller. Requires a separately accepted new native reader."""
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import multiprocessing
import subprocess
import time
from manifest_bridge import checked, bridge
from admission import validate_summary
OUTPUT_PREFIX = "/mnt/spann/reference-2m-selectivity-v1/"


def write(path,value):
    with path.open('x') as stream:json.dump(value,stream,indent=2);stream.write('\n')

def limited_child(argv,output,timeout,stdin=None):
    def limits():
        resource.setrlimit(resource.RLIMIT_AS,(32*1024**3,32*1024**3))
        resource.setrlimit(resource.RLIMIT_FSIZE,(64*1024**2,64*1024**2))
    env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    with output.open('xb') as stdout,output.with_suffix('.stderr').open('xb') as stderr:
        proc=subprocess.run(argv,input=stdin,stdout=stdout,stderr=stderr,timeout=timeout,env=env,preexec_fn=limits)
    if proc.returncode:raise ValueError('bounded child failed')
    if output.stat().st_size>64*1024**2:raise ValueError('child output cap')

def exact_child(job_path):
    sys.path.insert(0,str(Path(__file__).parent/'engine'))
    from hierarchical_sharding.exact_blocked import exact_blocked
    job=json.loads(Path(job_path).read_text())
    queries=[json.loads(x) for x in checked(job['queries']).read_text().splitlines()]
    gt=exact_blocked(job['vectors']['path'],queries,768,k=100,metric='ip',corpus_block=8192,query_block=16)
    write(Path(job['output']),gt)

def _bridge_process(pin,output):write(Path(output),bridge(pin))

def bounded_process(target,args,timeout):
    # Timeout is computed by caller before spawn; cleanup also covers spawn/join errors.
    proc=multiprocessing.get_context('spawn').Process(target=target,args=args)
    try:
        proc.start();proc.join(timeout)
        if proc.is_alive():raise ValueError('bounded process timeout')
        if proc.exitcode:raise ValueError('bounded process failed')
    finally:
        if proc.pid is not None and proc.is_alive():proc.kill();proc.join()


def run(spec):
    out=Path(spec['output'])
    if not out.is_absolute() or out.exists() or not str(out).startswith(OUTPUT_PREFIX):
        raise ValueError('fresh bounded output path required')
    out.mkdir(exist_ok=False)
    start=time.monotonic();result=dict(status='failed',scope='Consumed16 text queries, existing2M reference; leaf/candidate economics only.')
    try:
        if not 180<spec['deadline_unix']-time.time()<=1500:raise ValueError('fixed external deadline/reserve')
        if spec['rows']!=2000000 or spec['policy']!={'navigation':'fp','beam':32,'tau':2.0,'k':100,'rerank_factor':4}:
            raise ValueError('fixed policy or rows differ')
        if spec['binary']['sha256']=='67439496d6d5eb33d7210a347a90b68f934ce7dac012acc35fa99ef485a7d528':raise ValueError('known incompatible retained binary')
        end=min(time.time()+240,spec['deadline_unix']-180)
        def remaining(cap):
            value=min(cap,end-time.time())
            if value<=0:raise ValueError('absolute science deadline/reserve')
            return value
        # All file hashing and bridge scans run in bounded children.
        bounded_process(verify_inputs,(spec,),remaining(60))
        binary=Path(spec['binary']['path'])
        bounded_process(_bridge_process,(spec['manifest'],str(out/'bridge.json')),remaining(60))
        snapshot=json.loads((out/'bridge.json').read_text())
        query_path=Path(spec['query_file']['path']);queries=[json.loads(v) for v in query_path.read_text().splitlines()]
        if len(queries)!=16 or len({v['query_id'] for v in queries})!=16:raise ValueError('query cohort differs')
        if [q['query_id'] for q in queries]!=spec['query_ids']:raise ValueError('query order differs')
        write(out/'gt-job.json',dict(queries=spec['query_file'],vectors=spec['vectors'],output=str(out/'gt.json')))
        limited_child([sys.executable,str(Path(__file__).resolve()),'--exact',str(out/'gt-job.json')],out/'gt-process.stdout',remaining(90))
        gt=json.loads((out/'gt.json').read_text())
        if len(gt)!=16 or any(g['query_id']!=q['query_id'] or len(g['ids'])!=100 or len(set(g['ids']))!=100 or any(type(i)is not int or not 0<=i<2000000 for i in g['ids']) for g,q in zip(gt,queries)):
            raise ValueError('GT identity/count/range')
        req=[dict(query_id=q['query_id'],query=q['vector'],ground_truth=g['ids']) for q,g in zip(queries,gt)]
        payload=''.join(json.dumps(q)+'\n' for q in req+req).encode()
        if len(payload)>2*1024**2:raise ValueError('request cap')
        limited_child([str(binary),'--bridge',str(out/'bridge.json')],out/'reader.jsonl',remaining(30),payload)
        replies=[json.loads(line) for line in (out/'reader.jsonl').read_text().splitlines()]
        if len(replies)!=33:raise ValueError('startup and reply count')
        startup=replies[0]
        expected_budget=dict(rows=2000000,fp_nodes=4096,fp_payload_bytes=16777216,gt_ids=100,beam=32,k=100,rerank_factor=4,candidate_budget=400,dimension=768,metric='ip',tau=2.0,navigation='fp',construction_config=snapshot['config'])
        if startup.get('budgets')!=expected_budget:raise ValueError('native echoed settings differ')
        if not 1<=startup['depth']<=16 or startup['nodes']!=1780 or startup['leaves']!=1752:raise ValueError('actual shape differs')
        allrows=[dict(query_id=q['query_id'],**validate_summary(v,q['query_id'],g['ids'],spec['manifest']['sha256'])) for q,g,v in zip(queries+queries,gt+gt,replies[1:])]
        rows=allrows[16:]
        # Revalidate all pinned roots/maps/config inputs after requests under the same remaining deadline.
        bounded_process(_bridge_process,(spec['manifest'],str(out/'bridge-after.json')),remaining(60))
        if json.loads((out/'bridge-after.json').read_text())!=snapshot:raise ValueError('snapshot changed')
        result.update(status='completed',rows=rows,startup=startup,centroid_vector_access_complete=startup['fp'].get('vector_access_complete',False),traffic_claim='No total traffic claim from incomplete bounded access maps.',mean_recall=sum(100-r['navigation_loss']-r['trim_loss']-r['exact_loss'] for r in rows)/1600)
    except Exception as error:result['error']=repr(error)
    finally:
        result['elapsed_seconds']=time.monotonic()-start
        write(out/'result.json',result)
    return result

def verify_inputs(spec):
    for key in ('binary','manifest','query_file','vectors'):checked(spec[key])

if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--exact':exact_child(sys.argv[2])
    else:
        spec=json.loads(Path(sys.argv[1]).read_text())
        template=json.loads((Path(__file__).parent/'runtime-spec.template.json').read_text())
        if set(spec)!=set(template) or any(spec[k]!=v for k,v in template.items() if k not in ('deadline_unix','output')):raise ValueError('frozen runtime spec differs')
        build=json.loads(Path(spec['build_receipt']).read_text())
        if build['returncode']!=0 or build['compiler_sha256']!='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1' or build['source_manifest_sha256']!=hashlib.sha256((Path(__file__).parent/'source-manifest.json').read_bytes()).hexdigest():raise ValueError('native build binding differs')
        for name,pin in json.loads((Path(__file__).parent/'package-manifest.json').read_text())['files'].items():
            checked(dict(pin,path=str(Path(__file__).parent/name)))
        spec=dict(spec,binary=build['binary']);result=run(spec)
        print(json.dumps(result));raise SystemExit(0 if result['status']=='completed' else 1)
