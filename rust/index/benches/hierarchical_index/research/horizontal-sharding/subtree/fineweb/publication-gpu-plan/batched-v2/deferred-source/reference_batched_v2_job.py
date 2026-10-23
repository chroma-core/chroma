"""Hash-bound bounded GPU reference job; no staging, provisioning or model work.

An external resource guard invokes the parent launcher. A single child performs
input validation before CUDA, with parent-enforced phase and absolute deadlines.
Only the fixed 1M/10M inputs are admitted; this is no larger-scale admission.
"""
import argparse,datetime,hashlib,importlib.metadata,json,os,platform,re,resource,signal,subprocess,sys,time
from pathlib import Path
import numpy as np
from .tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write

PRODUCERS=frozenset(['reference_batched_v2_job.py','reference_batched_v2_engine.py','reference_batched_v2_filter.py','reference_bounds.py','reference_gpu.py','reference_device_engine.py','reference_device_filter.py','publication_reference.py','tree_manifest.py','benchmark.py'])
INPUTS=frozenset(['prepared1m','prepared10m','queries200','expected1m','expected10m','embeddings11000','embedding_report','embedding_batches','texts11000','frozen11000','runtime_lock','staging_receipt'])
SIZES=(1000000,10000000)
POOL_COUNTS=(200,1000,10000)
PHASE_LIMITS=dict(inputs=900,initialize=180,integration1m=300,integration10m=600,oracle=180,pilot32=180,pilot128=300,pilot256=450)


def save(path,value):_atomic_write(Path(path),canonical_bytes(value))
def jsonl(path):return [json.loads(line) for line in Path(path).read_text().splitlines()]
def ids(rows):
    values=[r['query_id'] for r in rows]
    if any(not isinstance(v,str) or not v for v in values) or len(set(values))!=len(values):raise ValueError('unique nonempty query identities required')
    return values


def compare_answers(actual,expected,k=100,*,bitwise=False):
    from .publication_reference import compare
    if len(actual)!=len(expected) or ids(actual)!=ids(expected):raise ValueError('complete ordered query answer coverage required')
    compare(actual,expected,k)
    if bitwise:
        for a,b in zip(actual,expected):
            if not np.array_equal(np.asarray(a['distances'],dtype=np.float64).view(np.uint64),np.asarray(b['distances'],dtype=np.float64).view(np.uint64)):raise ValueError('scalar/batched rerank bits differ')


def basic_job(job,base,now=None):
    now=time.time() if now is None else now
    start,stop=job['started_epoch'],job['stop_epoch']
    if any(type(v) not in (int,float) or not np.isfinite(v) for v in (start,stop)) or not 0<stop-start<=3600 or not start<=now<stop-300:raise ValueError('fixed at-most60min job deadline required')
    if set(job['inputs'])!=INPUTS or set(job['phase_limits_s'])!=set(PHASE_LIMITS) or any(type(job['phase_limits_s'][k]) is not int or not 1<=job['phase_limits_s'][k]<=v for k,v in PHASE_LIMITS.items()):raise ValueError('exact fixed input/phase policy required')
    if job.get('k')!=100 or type(job['k']) is not int or job.get('corpus_block')!=32768 or job.get('query_block')!=32 or job.get('max_scale_rows')!=10000000:raise ValueError('fixed1M/10M reference settings required')
    if job.get('pool_policy')!=['calibration1000','test10000'] or job.get('preservation_reserve_s')!=300:raise ValueError('fixed disjoint pool policy/reserve required')
    root=Path(job['output'])
    if not root.is_absolute() or root.resolve()!=root or '..' in root.parts:raise ValueError('canonical owned output required')
    names=[Path(d['path']).name for d in job['producer_sources']]
    if len(names)!=len(PRODUCERS) or set(names)!=PRODUCERS:raise ValueError('exact executed producer set required')
    for desc in job['producer_sources']:
        p=checked_artifact(desc,base)
        if p.resolve()!=Path(__file__).with_name(p.name).resolve():raise ValueError('producer path is not the executed module')
    if type(job.get('output_archive_allowance_s')) is not int or not 60<=job['output_archive_allowance_s']<=300:raise ValueError('bounded output/archive allowance required')
    return root


def source_chunks(prepared,expected_rows):
    path=Path(prepared);tree=json.loads(path.read_text())
    if tree.get('metric')!='ip' or tree.get('dimension')!=768 or tree.get('global_row_count')!=expected_rows:raise ValueError('fixed preserved FineWeb corpus differs')
    result=[];descs=[];end=0
    for row in tree['source']['chunks']:
        count=row['row_count']
        if type(count) is not int or count<1 or type(row['start_id']) is not int or row['start_id']!=end:raise ValueError('dense source coverage differs')
        p=checked_artifact(row['vectors'],path.parent)
        if p.stat().st_size!=count*768*4:raise ValueError('source dimensions differ')
        result.append((p,end,count));descs.append((str(p.resolve()),row['vectors']['bytes'],row['vectors']['sha256']));end+=count
    if end!=expected_rows:raise ValueError('source count differs')
    return result,descs


def bound_descriptors(declared,expected,base):
    values=[]
    for d in declared:
        p=checked_artifact(d,base);values.append((str(p.resolve()),d['bytes'],d['sha256']))
    if values!=expected:raise ValueError('descriptor set does not cover exact executed data')


def validate_inputs(job,base):
    paths={key:checked_artifact(desc,base) for key,desc in job['inputs'].items()}
    one,a=source_chunks(paths['prepared1m'],SIZES[0]);full,b=source_chunks(paths['prepared10m'],SIZES[1])
    if a!=b[:len(a)]:raise ValueError('1M bytes are not exact10M prefix')
    bound_descriptors(job['source_chunks'],b,base)
    stage=json.loads(paths['staging_receipt'].read_text())
    if stage.get('status')!='passed' or stage.get('stop_epoch')!=job['stop_epoch'] or stage.get('instance_id')!=job['expected_identity']['instanceId'] or type(stage.get('unique_payload_bytes')) is not int or not 0<stage['unique_payload_bytes']<=34*1024**3 or type(stage.get('charged_retry_payload_bytes')) is not int or not 0<=stage['charged_retry_payload_bytes']<=34*1024**3 or stage.get('staging_install_wall_s',float('inf'))>900:raise ValueError('bounded immutable staging receipt differs')
    old=jsonl(paths['queries200']);new=jsonl(paths['embeddings11000']);texts=jsonl(paths['texts11000']);frozen=json.loads(paths['frozen11000'].read_text())
    old_ids=ids(old);new_ids=ids(new);text_ids=ids(texts);cal=frozen['calibration_ids'];test=frozen['test_ids'];nold,ncal,ntest=POOL_COUNTS
    if len(old_ids)!=nold or len(cal)!=ncal or len(test)!=ntest or len(set(cal))!=ncal or len(set(test))!=ntest or set(cal)&set(test) or set(old_ids)&set(cal+test) or new_ids!=cal+test or text_ids!=new_ids:raise ValueError('frozen disjoint query pools differ')
    for rows in (old,new):
        for row in rows:
            v=np.asarray(row['vector'],dtype=np.float32)
            if v.shape!=(768,) or not np.isfinite(v).all():raise ValueError('invalid pinned query values')
    report=json.loads(paths['embedding_report'].read_text());batch=json.loads(paths['embedding_batches'].read_text())
    if report.get('status')!='passed' or report.get('queries')!=ncal+ntest:raise ValueError('completed embedding provenance required')
    for key,actual in [('source_text',job['inputs']['texts11000']),('output',job['inputs']['embeddings11000'])]:
        if (report[key]['bytes'],report[key]['sha256'])!=(actual['bytes'],actual['sha256']):raise ValueError('embedding provenance bytes differ')
    expected_chunks=[];cursor=0
    for chunk in batch['chunks']:
        d=chunk['vectors'];p=checked_artifact(d,paths['embedding_batches'].parent);qids=chunk['query_ids'];values=np.fromfile(p,dtype='<f4')
        if chunk['first']!=cursor or not qids or qids!=new_ids[cursor:cursor+len(qids)] or len(values)!=len(qids)*768:raise ValueError('embedding chunk coverage differs')
        values=values.reshape(-1,768)
        if not np.isfinite(values).all() or not np.array_equal(values.view(np.uint32),np.asarray([r['vector'] for r in new[cursor:cursor+len(qids)]],dtype=np.float32).view(np.uint32)):raise ValueError('embedding JSON/chunk bits differ')
        expected_chunks.append((str(p.resolve()),d['bytes'],d['sha256']));cursor+=len(qids)
    if cursor!=ncal+ntest:raise ValueError('embedding chunks incomplete')
    bound_descriptors(job['embedding_chunks'],expected_chunks,base)
    expected=[jsonl(paths['expected1m']),jsonl(paths['expected10m'])]
    for values in expected:
        if ids(values)!=old_ids:raise ValueError('complete old CPU reference identities differ')
        compare_answers(values,values)
    if batch.get('status')!='passed':raise ValueError('completed embedding batches required')
    return dict(paths=paths,chunks=[one,full],old=old,new=new,expected=expected,calibration_count=ncal,test_count=ntest)


def actual_identity():
    import urllib.request
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}));r=urllib.request.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'});token=opener.open(r,timeout=3).read(1024).decode();r=urllib.request.Request('http://169.254.169.254/latest/dynamic/instance-identity/document',headers={'X-aws-ec2-metadata-token':token});return json.loads(opener.open(r,timeout=3).read(16384))


def validate_runtime(lock):
    required={}
    for line in Path(lock).read_text().splitlines():
        match=re.match(r'^([A-Za-z0-9_.-]+)==([^ \\]+)',line)
        if match:required[match.group(1)]=match.group(2)
    if not required or required.get('numpy')!='2.2.6' or required.get('torch') not in ('2.5.1','2.5.1+cu124'):raise ValueError('reviewed numeric runtime lock required')
    actual={name:importlib.metadata.version(name) for name in required}
    for name,wanted in required.items():
        if actual[name]!=wanted and not (name=='torch' and wanted=='2.5.1' and actual[name]=='2.5.1+cu124'):raise ValueError('installed runtime differs: '+name)
    if sys.version_info[:2]!=(3,10) or platform.machine()!='x86_64':raise ValueError('pinned LinuxPython3.10x86_64 runtime required')
    return actual


def operational_gate(job,job_path,job_sha):
    actual=actual_identity()
    if any(actual.get(k)!=v for k,v in job['expected_identity'].items()) or actual.get('accountId')!='079819354890' or actual.get('region')!='us-east-1' or actual.get('instanceType')!='g6.2xlarge':raise ValueError('new sandbox L4 identity differs')
    if actual.get('instanceId') in ('i-03ddedeaccb5cba8f','i-0aa5faca97d41442b'):raise ValueError('terminated GPU reuse prohibited')
    timer=job['stop_timer_unit']
    if not isinstance(timer,str) or not timer.startswith('spann-') or not timer.endswith('.timer') or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789-.' for c in timer):raise ValueError('safe independent stop timer required')
    show=subprocess.run(['systemctl','show',timer,'--property=ActiveState','--property=NextElapseUSecRealtime'],capture_output=True,text=True,check=True,timeout=5).stdout;props=dict(line.split('=',1) for line in show.splitlines() if '=' in line)
    when=datetime.datetime.strptime(props['NextElapseUSecRealtime'],'%a %Y-%m-%d %H:%M:%S %Z').replace(tzinfo=datetime.timezone.utc).timestamp()
    if props.get('ActiveState')!='active' or abs(when-job['stop_epoch'])>1:raise ValueError('actual independent deadline timer differs')
    guard=Path(job['guard_config']);parent=Path('/proc')/str(os.getppid())/'cmdline';argv=parent.read_bytes().split(b'\0')
    if b'tools.hierarchical_sharding.resource_guard' not in argv or str(guard).encode() not in argv:raise ValueError('actual resource-guard parent required')
    cfg=json.loads(guard.read_text());limits=cfg['limits'];root=Path(job['output'])
    if cfg['concurrency']!=1 or cfg['timeout_s']>3600 or limits['rss_cap_bytes']>16*1024**3 or limits['disk_free_floor_bytes']<64*1024**3 or not any(Path(s['path'])==root.parent for s in cfg['scopes']):raise ValueError('whole owned-root external guard differs')
    wanted=[sys.executable,'-m','tools.hierarchical_sharding.reference_batched_v2_job','--job',str(job_path),'--job-sha256',job_sha]
    if cfg['commands']!=[wanted]:raise ValueError('guard does not bind actual immutable job argv')
    return dict(identity=actual,guard=artifact(guard),timer_properties=props)


def phase(root,name,limit):
    save(root/'phase.json',dict(name=name,started_epoch=time.time(),limit_s=limit))


def scan_pair(data,index,scorer,root):
    from .reference_device_engine import screened_reference
    from .reference_device_filter import TorchIntervals
    from .reference_batched_v2_engine import batched_reference
    count=SIZES[index];queries=data['old'];report={};outputs=[]
    for name in ('scalar','batched'):
        started=time.monotonic();scorer.torch.cuda.reset_peak_memory_stats()
        if name=='scalar':rows,m=screened_reference(data['chunks'][index],queries,768,100,scorer,backend=TorchIntervals(),to_numpy=scorer.to_numpy)
        else:rows,m=batched_reference(data['chunks'][index],queries,768,100,scorer,expected_rows=count,backend=scorer.backend,to_numpy=scorer.to_numpy)
        if m['corpus_rows']!=count or m['full_score_checks']!=3*count:raise ValueError('actual full-score coverage differs')
        compare_answers(rows,data['expected'][index]);outputs.append(rows)
        report[name]=dict(metrics=m,wall_s=time.monotonic()-started,runtime=scorer.metadata());save(root/(name+'.json'),dict(answers=rows,**report[name]))
    compare_answers(outputs[0],outputs[1],bitwise=True);return report


def execute_child(job,base,root):
    from .reference_batched_v2_engine import CudaBatchedScorer,batched_reference
    from .reference_batched_v2_filter import batched_memory_projection
    from .reference_device_filter import admit_memory
    from .reference_gpu import pathology_checks
    from .publication_reference import independent_oracle
    report=dict(status='running',stages={},python=platform.python_version(),numpy=np.__version__);scorer=None
    try:
        input_limit=min(job['phase_limits_s']['inputs'],job['started_epoch']+900-time.time())
        if input_limit<=0:raise TimeoutError('staging/install/validation900s allowance exhausted')
        phase(root,'inputs',input_limit);began=time.monotonic();data=validate_inputs(job,base);report['input_validation_wall_s']=time.monotonic()-began;report['installed_runtime']=validate_runtime(data['paths']['runtime_lock'])
        phase(root,'initialize',job['phase_limits_s']['initialize']);began=time.monotonic();scorer=CudaBatchedScorer();report['initialization_conformance_wall_s']=time.monotonic()-began
        projection=batched_memory_projection(768,32768,32,len(data['new']),100);report['memory_admission']=admit_memory(projection,int(scorer.torch.cuda.mem_get_info()[0]))
        class Full:
            def begin_block(self,x):scorer.begin_block(x)
            def score(self,q):return scorer.to_numpy(scorer.score(q))
        report['pathologies']=pathology_checks(Full());save(root/'child-report.json',report)
        for index,name in enumerate(('integration1m','integration10m')):
            phase(root,name,job['phase_limits_s'][name]);report['stages'][name]=scan_pair(data,index,scorer,root/name);save(root/'child-report.json',report)
        phase(root,'oracle',job['phase_limits_s']['oracle']);begin=time.monotonic();oracle=independent_oracle(data['chunks'][1],data['new'][:3],768,100);oracle_s=time.monotonic()-begin;save(root/'oracle.json',dict(answers=oracle,wall_s=oracle_s,method='independent full-source f64 elementwise sum/global lexsort'))
        rates=[]
        for count in (32,128,256):
            name='pilot'+str(count);phase(root,name,job['phase_limits_s'][name]);begin=time.monotonic();rows,m=batched_reference(data['chunks'][1],data['new'][:count],768,100,scorer,expected_rows=SIZES[1],backend=scorer.backend,to_numpy=scorer.to_numpy)
            if m['full_score_checks']!=3*SIZES[1] or len(rows)!=count:raise ValueError('pilot coverage differs')
            compare_answers(rows[:3],oracle);elapsed=time.monotonic()-begin;rates.append(elapsed/count);report['stages'][name]=dict(metrics=m,wall_s=elapsed,runtime=scorer.metadata());save(root/(name+'.json'),dict(answers=rows,**report['stages'][name]));save(root/'child-report.json',report)
        for name,queries in [('calibration1000',data['new'][:data['calibration_count']]),('test10000',data['new'][data['calibration_count']:])]:
            if name=='test10000' and report['stages'].get('calibration1000',{}).get('status')!='passed':
                report['stages'][name]=dict(status='pending_calibration_incomplete');continue
            remaining=job['stop_epoch']-time.time()-300;estimate=1.5*(max(rates[-2:])*len(queries)+oracle_s+job['output_archive_allowance_s'])
            if estimate>remaining:
                report['stages'][name]=dict(status='pending_budget_declined',estimated_s=estimate,remaining_s=remaining);continue
            phase(root,name,remaining);begin=time.monotonic();expected=independent_oracle(data['chunks'][1],queries[:3],768,100);save(root/(name+'-oracle.json'),dict(answers=expected));rows,m=batched_reference(data['chunks'][1],queries,768,100,scorer,expected_rows=SIZES[1],backend=scorer.backend,to_numpy=scorer.to_numpy)
            if m['corpus_rows']!=SIZES[1] or m['full_score_checks']!=3*SIZES[1] or len(rows)!=len(queries):raise ValueError('pool actual coverage differs')
            compare_answers(rows[:3],expected);compare_answers(rows,rows);save(root/(name+'.json'),dict(status='passed',answers=rows,metrics=m,wall_s=time.monotonic()-begin,runtime=scorer.metadata()));report['stages'][name]=dict(status='passed',output=artifact(root/(name+'.json')));save(root/'child-report.json',report)
        report.update(status='passed_mechanics_pool_states_explicit',runtime=scorer.metadata());return report
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:
        try:report['runtime']=scorer.metadata() if scorer else dict(not_initialized=True)
        except BaseException as error:report['runtime']=dict(metadata_error=repr(error))
        report['lifetime_peak_RSS_KiB']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss;save(root/'child-report.json',report)


def launch(job_path,expected_sha):
    job_path=Path(job_path).resolve()
    if artifact(job_path)['sha256']!=expected_sha:raise ValueError('launch job SHA differs')
    job=json.loads(job_path.read_text());root=basic_job(job,job_path.parent);root.mkdir(parents=True,exist_ok=False);report=dict(status='running',job=artifact(job_path));process=None
    try:
        report['operational_admission']=operational_gate(job,job_path,expected_sha);save(root/'report.json',report)
        argv=[sys.executable,'-m','tools.hierarchical_sharding.reference_batched_v2_job','--job',str(job_path),'--job-sha256',expected_sha,'--execute-child']
        with (root/'child.log').open('xb') as log:
            process=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
            while process.poll() is None:
                if time.time()>=job['stop_epoch']-300:raise TimeoutError('fixed preservation reserve reached')
                p=root/'phase.json'
                if p.exists():
                    current=json.loads(p.read_text())
                    if time.time()-current['started_epoch']>current['limit_s']:raise TimeoutError('phase deadline exceeded: '+current['name'])
                time.sleep(.1)
        if process.returncode!=0:raise RuntimeError('GPU child failed; retain all phase evidence')
        child=json.loads((root/'child-report.json').read_text())
        if child['status']!='passed_mechanics_pool_states_explicit':raise ValueError('missing successful mechanics report')
        report.update(status=child['status'],child_report=artifact(root/'child-report.json'));return report
    except BaseException as error:report.update(status='failed',error=repr(error));raise
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=5)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5)
        save(root/'report.json',report)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True);p.add_argument('--job-sha256',required=True);p.add_argument('--execute-child',action='store_true');a=p.parse_args()
    if a.execute_child:
        path=Path(a.job).resolve()
        if artifact(path)['sha256']!=a.job_sha256:raise ValueError('child job changed')
        parent=Path('/proc')/str(os.getppid())/'cmdline';argv=parent.read_bytes().split(b'\0')
        if b'tools.hierarchical_sharding.reference_batched_v2_job' not in argv or b'--execute-child' in argv or str(path).encode() not in argv or a.job_sha256.encode() not in argv:raise ValueError('actual admitted launcher parent required')
        job=json.loads(path.read_text());basic_job(job,path.parent);execute_child(job,path.parent,Path(job['output']))
    else:launch(a.job,a.job_sha256)
if __name__=='__main__':main()
