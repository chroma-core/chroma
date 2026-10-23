"""Run only hash-bound publication stages under an immutable external deadline."""
import json,subprocess,sys,time
from pathlib import Path
from .tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write

PRODUCERS={'publication_launch.py','publication_queries.py','publication_gpu_job.py','publication_reference.py','reference_device_engine.py','reference_device_filter.py','reference_bounds.py','reference_gpu.py','fineweb_queries.py','fineweb_prepare.py','tree_manifest.py','benchmark.py'}

KEYS=('prepared1m','prepared10m','queries200','expected1m','expected10m','texts11000','old_texts200','model_metadata','code_metadata','runtime_lock')


def validate_job(job,base,now=None):
    now=time.time() if now is None else now
    start=job['started_epoch'];stop=job['stop_epoch']
    if not 0<stop-start<=3600 or not start<=now<stop-300:raise ValueError('fixed at-most60min external compute window required')
    if set(job['inputs'])!=set(KEYS):raise ValueError('concrete publication input set differs')
    paths={key:str(checked_artifact(desc,base)) for key,desc in job['inputs'].items()}
    producers=job.get('producer_sources',[])
    if len(producers)!=len(PRODUCERS) or {Path(p['path']).name for p in producers}!=PRODUCERS:raise ValueError('exact executed producer set required')
    for source in producers:
        actual=checked_artifact(source,base)
        if actual.resolve()!=(Path(__file__).parent/actual.name).resolve():raise ValueError('producer binding is not the executed module path')
    full=snapshot_chunks(paths['prepared10m'],10000000)
    prefix=snapshot_chunks(paths['prepared1m'],1000000)
    if prefix!=full[:len(prefix)]:raise ValueError('1M source is not the exact10M prefix')
    bound=[]
    for desc in job.get('source_chunks',[]):
        actual=checked_artifact(desc,base)
        bound.append((str(actual.resolve()),desc['sha256'],desc['bytes']))
    if bound!=full:raise ValueError('source bindings do not cover exact prepared chunks')
    return paths


def snapshot_chunks(path,expected_rows):
    path=Path(path);tree=json.loads(path.read_text())
    if tree.get('global_row_count')!=expected_rows or tree.get('dimension')!=768 or tree.get('metric')!='ip':raise ValueError('prepared source dimensions/metric/count differ')
    result=[];end=0
    for chunk in tree['source']['chunks']:
        desc=chunk['vectors'];count=chunk['row_count']
        if type(count) is not int or count<1 or chunk['start_id']!=end or desc['bytes']!=count*768*4:raise ValueError('source coverage/size mismatch')
        actual=checked_artifact(desc,path.parent);result.append((str(actual.resolve()),desc['sha256'],desc['bytes']));end+=count
    if end!=expected_rows:raise ValueError('source does not cover declared prefix')
    return result


def commands(paths,root,stop,python=sys.executable):
    root=Path(root)
    def engine(size):return [python,'-m','tools.hierarchical_sharding.reference_device_engine','--prepared',paths['prepared'+size],'--queries',paths['queries200'],'--expected',paths['expected'+size],'--output',str(root/('integration'+size))]
    embed=[python,'-m','tools.hierarchical_sharding.publication_gpu_job']
    values=dict(texts=paths['texts11000'],output=str(root/'embedding11000'),model_metadata=paths['model_metadata'],code_metadata=paths['code_metadata'],cache=str(root/'model-cache'),lock=paths['runtime_lock'],old_texts=paths['old_texts200'],old_vectors=paths['queries200'],stop_epoch=str(stop))
    for key,value in values.items():embed+=['--'+key.replace('_','-'),value]
    refs=[python,'-m','tools.hierarchical_sharding.publication_reference']
    values=dict(prepared=paths['prepared10m'],queries=str(root/'embedding11000/queries.jsonl'),receipt1m=str(root/'integration1m/report.json'),receipt10m=str(root/'integration10m/report.json'),cpu1m=paths['expected1m'],cpu10m=paths['expected10m'],output=str(root/'reference11000'),stop_epoch=str(stop))
    for key,value in values.items():refs+=['--'+key.replace('_','-'),value]
    return [('integration1m',engine('1m')),('integration10m',engine('10m')),('embedding11000',embed),('reference11000',refs)]


def launch(config,output):
    config=Path(config);root=Path(output);root.mkdir(parents=True,exist_ok=False)
    report=dict(status='running',stages=[]);started=time.monotonic()
    try:
        job=json.loads(config.read_text());paths=validate_job(job,config.parent)
        report['config']=artifact(config)
        for name,argv in commands(paths,root,job['stop_epoch']):
            remaining=job['stop_epoch']-time.time()-300
            if remaining<1:raise TimeoutError('no work may use the preservation reserve')
            begin=time.monotonic()
            with (root/(name+'.log')).open('xb') as log:
                try:subprocess.run(argv,check=True,timeout=remaining,stdout=log,stderr=subprocess.STDOUT)
                except BaseException as error:
                    report['stages'].append(dict(name=name,status='failed',error=repr(error),wall_s=time.monotonic()-begin));raise
            result=json.loads((root/name/'report.json').read_text())
            if result.get('status')!='passed':raise ValueError('stage returns without a passed report')
            report['stages'].append(dict(name=name,status='passed',wall_s=time.monotonic()-begin,report=artifact(root/name/'report.json',root)))
            _atomic_write(root/'report.json',canonical_bytes(report))
        report.update(status='passed',wall_s=time.monotonic()-started);_atomic_write(root/'complete.json',canonical_bytes(report));return report
    except BaseException as error:report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
    finally:_atomic_write(root/'report.json',canonical_bytes(report))


def main():
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',required=True);p.add_argument('--output',required=True);a=p.parse_args();launch(a.config,a.output)

if __name__=='__main__':main()
