"""Dedicated-host integration helpers; never provisions hosts or selects production."""
import argparse
import concurrent.futures
import json
import pathlib
import shlex
import subprocess
import sys
import time
from .build import run_jobs
from .tree_manifest import checked_artifact,assemble_tree,publish,canonical_bytes,content_hash,artifact
from .subtree_transport import TreeCoordinator,LocalBackend,TreeError


def ssh(host,code):
    result=subprocess.run(['ssh','-o','BatchMode=yes',host,'python3','-'],input=code,text=True,capture_output=True,timeout=300)
    if result.returncode:raise RuntimeError(host+': '+result.stderr)
    return result.stdout


def copy_to(host,source,destination):
    ssh(host,'import pathlib\npathlib.Path('+repr(str(pathlib.Path(destination).parent))+').mkdir(parents=True,exist_ok=True)\n')
    subprocess.run(['scp','-q',str(source),host+':'+destination],check=True,timeout=300)


def build_case(prepared_path,case_path,hosts,generation=1,resume_case=None,threads=32,small=False,jobs_per_host=1,binary="/mnt/spann/hierarchical_spann_shard-verified-v2",measured_binary="/mnt/spann/measured_subtree_worker"):
    started=time.monotonic();prepared_path=pathlib.Path(prepared_path);prepared=json.loads(prepared_path.read_text())
    case=pathlib.Path(case_path);case.mkdir(parents=True,exist_ok=False)
    previous=json.loads(pathlib.Path(resume_case).read_text()) if resume_case else None
    entries=[];jobs=[];transfer_started=time.monotonic()
    # Largest-first scheduling changes physical placement, never ownership slots.
    loads={host:0 for host in hosts}
    placement={}
    for slot in sorted(prepared['subtrees'],key=lambda row:(-row['vector_count'],row['subtree_id'])):
        if previous:host=previous['entries'][slot['subtree_id']]['host']
        else:host=min(hosts,key=lambda h:(loads[h],hosts.index(h)))
        loads[host]+=slot['vector_count'];placement[slot['subtree_id']]=host
    for slot in prepared['subtrees']:
        sid=slot['subtree_id'];host=placement[sid]
        root='/mnt/spann/'+case.name
        if previous:index=previous['entries'][sid]['index']
        else:index=root+'/indexes/'+sid
        lower_generation=generation if slot['append_row_count'] else previous['entries'][sid]['generation']
        entry=dict(host=host,index=index,generation=lower_generation,slot=slot)
        entries.append((sid,entry))
        if not slot['append_row_count']:continue
        vectors=checked_artifact(slot['vectors'],prepared_path.parent);mapping=checked_artifact(slot['local_to_global'],prepared_path.parent)
        remote_vectors=root+'/inputs/'+sid+'.f32';remote_map=root+'/inputs/'+sid+'.u64le'
        copy_to(host,vectors,remote_vectors);copy_to(host,mapping,remote_map)
        verification=ssh(host,'import hashlib,json\nprint(json.dumps({p:hashlib.file_digest(open(p,"rb"),"sha256").hexdigest() for p in '+repr([remote_vectors,remote_map])+'}))\n')
        digests=json.loads(verification)
        if digests[remote_vectors]!=slot['vectors']['sha256'] or digests[remote_map]!=slot['local_to_global']['sha256']:raise ValueError('remote prepared input digest mismatch')
        job=dict(host=host,binary=measured_binary,vectors=remote_vectors,checkpoint_dir=index,shard_id=sid,source_id=prepared['source_id'],dimension=prepared['dimension'],row_count=slot['append_row_count'],global_id_start=slot['local_start'],id_stride=1,generation=generation,metric=prepared['metric'],threads=threads,checkpoint_rows=max(1,slot['append_row_count']),id_map=remote_map,boundary_hash=slot['routing_boundary_hash'])
        if previous:job['resume']=True
        if small:job.update(branching_factor=2,split_threshold=8,merge_threshold=2)
        jobs.append(job)
    transfer_s=time.monotonic()-transfer_started
    (case/'jobs.json').write_text(json.dumps(jobs,indent=2)+'\n')
    build_started_unix=time.time()
    report=run_jobs(jobs,case/'build',max_hosts=len(hosts),timeout=1800,jobs_per_host=jobs_per_host)
    warnings={str(p.name):p.read_text() for p in (case/'build').glob('*.stderr') if 'without full convergence' in p.read_text()}
    if not report['complete']:raise RuntimeError('subtree build incomplete: '+json.dumps(report))
    copied_started=time.monotonic();snapshot_paths=[]
    for sid,entry in entries:
        destination=case/'copies'/sid;destination.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['scp','-q','-r',entry['host']+':'+entry['index'],str(destination)],check=True,timeout=300)
        snapshot_paths.append(destination/('manifest-'+str(entry['generation'])+'.json'))
        entry['copy']=str(destination.resolve())
    copied_s=time.monotonic()-copied_started
    tree=assemble_tree(prepared_path,snapshot_paths,generation,case)
    tree_path=case/('tree-'+str(generation)+'.json');tree_path.write_bytes(canonical_bytes(tree))
    descriptors={slot['subtree_id']:dict(manifest=str(path),checkpoint_dir=str(path.parent),binary=binary,snapshot_sha256=slot['snapshot']['sha256']) for slot,path in zip(tree['subtrees'],snapshot_paths)}
    reopen_started=time.monotonic();backend=LocalBackend(descriptors);coordinator=TreeCoordinator(tree,{},backend)
    try:root_verification=coordinator.verify_roots()
    finally:coordinator.close()
    reopen_s=time.monotonic()-reopen_started
    pointer=publish(case/'current.json',tree,base=case)
    result=dict(binary=binary,measured_binary=measured_binary,case=str(case.resolve()),prepared=str(prepared_path.resolve()),tree=str(tree_path.resolve()),tree_sha256=content_hash(tree),entries=dict(entries),input_transfer_s=transfer_s,index_build=report,index_build_started_unix=build_started_unix,copy_for_local_reference_s=copied_s,reopen_verification_s=reopen_s,root_verification=root_verification,through_publication_s=time.monotonic()-started,fully_converged=not warnings,balance_warnings=warnings,pointer=pointer)
    (case/'case.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


def start_hosts(case_path,port,cohost=False):
    case_path=pathlib.Path(case_path);case=json.loads((case_path/'case.json').read_text());tree=json.loads(pathlib.Path(case['tree']).read_text())
    groups={};placement=dict(format_version=1,tree_sha256=case['tree_sha256'],placement_epoch=2 if cohost else 1,owners={})
    first=next(iter(case['entries'].values()))['host']
    for slot in tree['subtrees']:
        sid=slot['subtree_id'];entry=case['entries'][sid];host=first if cohost else entry['host'];index=entry['index']
        if cohost and host!=entry['host']:
            index='/mnt/spann/'+case_path.name+'/cohost/'+sid
            ssh(host,'import pathlib\npathlib.Path('+repr(str(pathlib.Path(index).parent))+').mkdir(parents=True,exist_ok=True)\n')
            subprocess.run(['scp','-q','-r',entry['copy'],host+':'+index],check=True,timeout=300)
        owner='owner-'+host.split('@')[-1]
        placement['owners'][sid]=dict(owner_id=owner,endpoint='http://'+host.split('@')[-1]+':'+str(port))
        config=groups.setdefault(host,dict(tree_sha256=case['tree_sha256'],owner_id=owner,timeout=30,subtrees={}))
        config['subtrees'][sid]=dict(manifest=index+'/manifest-'+str(entry['generation'])+'.json',checkpoint_dir=index,binary=case.get('binary','/mnt/spann/hierarchical_spann_shard-verified-v2'),snapshot_sha256=slot['snapshot']['sha256'])
    servers=[]
    for host,config in groups.items():
        directory='/mnt/spann/results/'+case_path.name;config_path=directory+'/host-'+str(port)+'.json';log=directory+'/host-'+str(port)+'.log'
        code='import pathlib,json,subprocess\np=pathlib.Path('+repr(directory)+');p.mkdir(parents=True,exist_ok=True)\npathlib.Path('+repr(config_path)+').write_text('+repr(json.dumps(config))+')\nf=open('+repr(log)+',"w")\np=subprocess.Popen(["python3","-m","tools.hierarchical_sharding.subtree_server","--config",'+repr(config_path)+',"--host","0.0.0.0","--port",'+repr(str(port))+'],cwd="/mnt/spann/source",stdout=f,stderr=f,start_new_session=True)\nprint(p.pid)\n'
        pid=int(ssh(host,code));servers.append(dict(host=host,pid=pid,config=config_path,log=log))
    placement_path=case_path/('placement-cohost.json' if cohost else 'placement-two-hosts.json');placement_path.write_text(json.dumps(placement,indent=2)+'\n')
    (case_path/('servers-cohost.json' if cohost else 'servers-two-hosts.json')).write_text(json.dumps(servers,indent=2)+'\n')
    coordinator=TreeCoordinator.from_files(case['tree'],placement_path)
    deadline=time.monotonic()+45;startup_started=time.monotonic()
    try:
        while True:
            try:verified=coordinator.verify_roots(timeout=min(30,max(.001,deadline-time.monotonic())));break
            except TreeError:
                if time.monotonic()>=deadline:raise
                time.sleep(.1)
    finally:coordinator.close()
    (case_path/('ready-cohost.json' if cohost else 'ready-two-hosts.json')).write_text(json.dumps(dict(status='passed',startup_s=time.monotonic()-startup_started,lower_roots=verified),indent=2)+'\n')
    return placement_path


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    b=sub.add_parser('build');b.add_argument('--prepared',required=True);b.add_argument('--case',required=True);b.add_argument('--hosts',nargs='+',required=True);b.add_argument('--generation',type=int,default=1);b.add_argument('--resume-case');b.add_argument('--threads',type=int,default=32);b.add_argument('--small',action='store_true');b.add_argument('--jobs-per-host',type=int,default=1);b.add_argument('--binary',default='/mnt/spann/hierarchical_spann_shard-verified-v2');b.add_argument('--measured-binary',default='/mnt/spann/measured_subtree_worker')
    s=sub.add_parser('serve');s.add_argument('--case',required=True);s.add_argument('--port',type=int,required=True);s.add_argument('--cohost',action='store_true');a=p.parse_args()
    if a.command=='build':print(json.dumps(build_case(a.prepared,a.case,a.hosts,a.generation,a.resume_case,a.threads,a.small,a.jobs_per_host,a.binary,a.measured_binary)))
    else:print(start_hosts(a.case,a.port,a.cohost))

if __name__=='__main__':main()
