#!/usr/bin/env python3
"""Run the frozen two-arm protocol on the already guarded approved worker."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

INPUT = None
INPUT_SHA = None

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''): h.update(chunk)
    return h.hexdigest()

def write(path, value): path.write_text(json.dumps(value,indent=2)+'\n')

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--manifest',required=True,type=Path)
    p.add_argument('--run-dir',required=True,type=Path)
    p.add_argument('--deadline',required=True,type=int)
    p.add_argument('--window-id',required=True)
    args=p.parse_args()
    global INPUT, INPUT_SHA
    if not args.window_id.replace('-','').isalnum(): raise RuntimeError('Bad window identifier')
    out=args.run_dir.resolve()
    out.mkdir(parents=True,exist_ok=False)
    manifest=json.loads(args.manifest.read_text())
    materialized=json.loads(Path(manifest['input_manifest']).read_text())
    if materialized.get('status')!='passed' or materialized.get('bytes')!=9216000000 or materialized.get('rows')!=3000000 or materialized.get('plan_sha256')!=manifest['input_plan_sha256']: raise RuntimeError('Unadmitted materialized input')
    INPUT=Path(materialized['path']); INPUT_SHA=materialized['sha256']
    probe=json.loads(Path(manifest['perf_probe']).read_text())
    if probe.get('supported') is not True: raise RuntimeError('CPU profiling probe must pass before builds')
    perf=probe['perf_path']
    if manifest.get('combined_review_passed') is not True: raise RuntimeError('Reviewed freeze required')
    def admit(seconds):
        if time.time()+seconds+60 >= args.deadline: raise RuntimeError('Insufficient absolute window; no extension')
        subprocess.run(['systemctl','is-active','--quiet',f'spann-single-shard-3m-{args.window_id}-stop.timer'],check=True)
    def command(name,argv,cwd=None,seconds=180,env=None):
        admit(seconds)
        write(out/f'{name}.command.json',{'argv':argv,'cwd':str(cwd) if cwd else None,'timeout_seconds':seconds,'started_unix':time.time()})
        started=time.time()
        try:
            with (out/f'{name}.stdout').open('wb') as stdout,(out/f'{name}.stderr').open('wb') as stderr:
                r=subprocess.run(['/usr/bin/timeout','--signal=TERM','--kill-after=10',str(seconds),'/usr/bin/time','-v']+argv,cwd=cwd,env=env,stdout=stdout,stderr=stderr,timeout=seconds+15)
            code=r.returncode
        except subprocess.TimeoutExpired:
            code=124
        write(out/f'{name}.process.json',{'exit_code':code,'wall_seconds':time.time()-started,'finished_unix':time.time()})
        if code: raise RuntimeError(f'{name} failed with exit {code}; no retry')
    admit(30)
    hardware=subprocess.check_output(['lscpu','-p=CPU,CORE,SOCKET'],text=True)
    cores={tuple(line.split(',')[1:]) for line in hardware.splitlines() if line and not line.startswith('#')}
    if len(cores)!=32: raise RuntimeError(f'Expected32physicalcores, got{len(cores)}')
    if INPUT.stat().st_size!=9216000000 or sha(INPUT)!=INPUT_SHA: raise RuntimeError('Retained input differs')
    artifacts=manifest['source_files']
    if manifest.get('source_closure_complete') is not True or not artifacts:
        raise RuntimeError('Complete extracted-source closure manifest is required')
    for item in artifacts:
        if sha(Path(item['path']))!=item['sha256']: raise RuntimeError(f'Source hash mismatch: {item["path"]}')
    reference=Path(manifest['reference_source']).resolve()
    candidate=Path(manifest['candidate_source']).resolve()
    listed={str(Path(item['path']).resolve()) for item in artifacts}
    for source in [reference,candidate]:
        actual=set()
        for entry in source.rglob('*'):
            rel=entry.relative_to(source)
            if any(part in {'.git','target'} for part in rel.parts): continue
            if entry.is_symlink(): raise RuntimeError(f'Unadmitted source symlink: {entry}')
            if entry.is_file(): actual.add(str(entry.resolve()))
        expected={v for v in listed if Path(v).is_relative_to(source)}
        if actual!=expected:
            raise RuntimeError(f'Extracted source closure mismatch in {source}: unlisted={len(actual-expected)}, missing={len(expected-actual)}')
    target=Path(manifest['cargo_target_dir']).resolve()
    target.mkdir(parents=True,exist_ok=True)
    free=shutil.disk_usage(target).free
    if free < manifest.get('required_free_bytes',80*1024**3): raise RuntimeError('Insufficient preserved-volume free capacity')
    toolchain=manifest.get('rust_toolchain','1.92.0')
    if toolchain != '1.92.0': raise RuntimeError('Initial matched protocol requires frozen Rust1.92.0')
    env=dict(os.environ,PATH='/home/ubuntu/.cargo/bin:'+os.environ.get('PATH',''),RUSTUP_TOOLCHAIN=toolchain,CARGO_TARGET_DIR=str(target),RUSTFLAGS='-C target-cpu=native -C force-frame-pointers=yes -C debuginfo=1')
    compiler=subprocess.check_output(['rustc','--version'],env=env,text=True).strip()
    if not compiler.startswith('rustc 1.92.0 '): raise RuntimeError(f'Unexpected compiler: {compiler}')
    env.pop('RUSTC_WRAPPER',None)
    env.pop('RUSTC_WORKSPACE_WRAPPER',None)
    env.pop('CARGO_ENCODED_RUSTFLAGS',None)
    with (out/'host.txt').open('wb') as fd:
        for argv in [['date','-u'],['uname','-a'],['lscpu'],['free','-b'],['df','-B1','/',str(target),'/dev/shm'],['lsblk','-o','NAME,SERIAL,SIZE,FSTYPE,MOUNTPOINTS'],['rustc','--version'],['cargo','--version']]:
            subprocess.run(argv,env=env,stdout=fd,stderr=subprocess.STDOUT,check=True)
    write(out/'environment.json',{'selected_env':{k:env.get(k) for k in ['PATH','RUSTUP_TOOLCHAIN','CARGO_TARGET_DIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','CARGO_PROFILE_RELEASE_OPT_LEVEL','CARGO_PROFILE_RELEASE_DEBUG']},'physical_cores':len(cores),'input_sha256':INPUT_SHA,'input_bytes':INPUT.stat().st_size,'manifest_sha256':sha(args.manifest),'deadline_unix':args.deadline,'cache_policy':'No OS cache flush; bounded full input read before every fresh process; provider/decoded caches begin fresh per process; prior build/validation OS cache exposure recorded.'})
    # All builds and timing run under one external flock, held by remote-run.sh.
    for arm,source,flags in [('reference',reference,[]),('candidate',candidate,['--','--cfg','single_shard_owner'])]:
        command(f'build-{arm}',['cargo','rustc','-p','chroma-index','--release','--locked','--bench','single_shard_attribution','--message-format=json']+flags,cwd=source,seconds=manifest.get('build_timeout_seconds',900),env=env)
        # Cargo's successful compiler-artifact message identifies this exact
        # target and source; filesystem age/glob fallbacks are not permitted.
        executable_artifacts=[]
        finished=False
        for line in (out/f'build-{arm}.stdout').read_text().splitlines():
            try: event=json.loads(line)
            except json.JSONDecodeError: continue
            if event.get('reason')=='build-finished': finished=event.get('success') is True
            if event.get('reason')!='compiler-artifact' or not event.get('executable'): continue
            target_info=event.get('target',{})
            if target_info.get('name')!='single_shard_attribution' or 'bench' not in target_info.get('kind',[]): continue
            src=Path(target_info['src_path']).resolve()
            if src!=source/'rust/index/benches/single_shard_attribution.rs':
                raise RuntimeError(f'Artifact source belongs to wrong arm: {src}')
            if 'chroma-index' not in event.get('package_id',''):
                raise RuntimeError('Artifact package differs from chroma-index')
            executable_artifacts.append(event)
        if not finished or len(executable_artifacts)!=1:
            raise RuntimeError(f'Expected one exact successful benchmark artifact, got{len(executable_artifacts)}')
        artifact=executable_artifacts[0]
        binary=Path(artifact['executable']).resolve()
        if target/'release'/'deps' not in binary.parents or not binary.is_file() or not os.access(binary,os.X_OK):
            raise RuntimeError('Cargo artifact lies outside admitted release target or is not executable')
        write(out/f'{arm}.cargo-artifact.json',artifact)
        saved=out/f'{arm}.bin'
        shutil.copy2(binary,saved)
        write(out/f'{arm}.binary.json',{'source_executable':str(binary),'retained_executable':str(saved),'sha256':sha(saved),'source_dir':str(source),'cfg':flags})
    for index,arm in enumerate(['reference','candidate','candidate','reference'],1):
        admit(800)
        required_disk=((5-index)*12+12)*1024**3
        disk_free=shutil.disk_usage(out).free
        write(out/f'{index:02d}-{arm}.disk-admission.json',{'available_bytes':disk_free,'required_bytes':required_disk,'basis':'12GiB per remaining saved index plus12GiB preserved margin; profile/source/build retained; no deletion.'})
        if disk_free<required_disk: raise RuntimeError('Insufficient preserved disk capacity for remaining fixed runs')
        meminfo={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in Path('/proc/meminfo').read_text().splitlines()}
        available=int(meminfo['MemAvailable'].split()[0])*1024
        minimum=50*1024**3
        memory_receipt={'MemAvailable_bytes':available,'required_available_bytes':minimum,'basis':'Fixed3M:9.216GBinput plusconstructionvectors,caches,temporarypublication,and32workerheadroom, roundedto50GiBavailable. This is admission, not an implemented boundedcache.','size_adaptation':False,'admitted':available>=minimum}
        write(out/f'{index:02d}-{arm}.memory-admission.json',memory_receipt)
        if available<minimum: raise RuntimeError('Less than50GiB memory available; fixed workload rejected')
        # Cache preparation reads the exact pinned9.216GBinput once.
        warm_start=time.time()
        with INPUT.open('rb') as fd:
            read=0
            while True:
                data=fd.read(8*1024*1024)
                if not data: break
                read+=len(data)
        if read!=9216000000: raise RuntimeError('Input warmup extent changed')
        name=f'{index:02d}-{arm}'
        write(out/f'{name}.cache.json',{'read_bytes':read,'warm_read_seconds':time.time()-warm_start,'drop_caches':False,'fresh_process':True,'fresh_output_directory':True})
        command(name,[perf,'record','--freq','99','--event','cpu-clock:u','--clockid','CLOCK_REALTIME','--call-graph','fp','--output',str(out/f'{name}.perf.data'),'--',str(out/f'{arm}.bin'),'--input',str(INPUT),'--output-dir',str(out/f'{name}-index'),'--rows','3000000','--dim','768','--workers','32','--input-sha256',INPUT_SHA],seconds=600,env=env)
        command(name+'-perf-report',[perf,'report','--stdio','--no-children','--percent-limit','0.1','--call-graph','graph,0.5,caller','--input',str(out/f'{name}.perf.data')],seconds=60,env=env)
        command(name+'-perf-phases',['python3',str(Path(__file__).with_name('perf-phase-summary.py')),'--perf',perf,'--profile',str(out/f'{name}.perf.data'),'--events',str(out/f'{name}-index'/'events.jsonl'),'--output',str(out/f'{name}.perf-phases.json')],seconds=130,env=env)
    write(out/'complete.json',{'finished_unix':time.time(),'order':['reference','candidate','candidate','reference'],'workload_rows':3000000,'workers':32,'all_processes_passed':True,'root_must_collect_and_api_stop':True})

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print(f'STOP: {exc}',flush=True)
        raise
