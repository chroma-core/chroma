"""One admitted Linux native attempt. This supervisor never operates cloud resources."""
import argparse,hashlib,json,os,pathlib,signal,sys,time

def load(p):return json.loads(pathlib.Path(p).read_text())
def digest(p):
 h=hashlib.sha256()
 with open(p,'rb')as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def write(p,v):
 with open(p,'x')as f:json.dump(v,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def size(p):return sum(f.stat().st_size for f in pathlib.Path(p).rglob('*')if f.is_file() and not f.is_symlink())
def free(p):s=os.statvfs(p);return s.f_bavail*s.f_frsize
def rss(pid):
 try:
  for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines():
   if line.startswith('VmRSS:'):return int(line.split()[1])*1024
 except FileNotFoundError:pass
 return 0

def admit(p):
 for k in ['binary','binary_sha256','build_receipt','build_receipt_sha256','source_root','source_manifest_sha256','spec','spec_sha256','deadline_unix','shutdown_proof','shutdown_proof_sha256','receipt_dir','data_mount','source_prefix_sha256']:
  if p.get(k)is None:raise ValueError('Unfilled execution gate: '+k)
 l=p['limits'];assert p['deadline_unix']-time.time()>l['shutdown_reserve_seconds']
 for path,key in [('binary','binary_sha256'),('build_receipt','build_receipt_sha256'),('spec','spec_sha256'),('shutdown_proof','shutdown_proof_sha256')]:assert digest(p[path])==p[key],path
 proof=load(p['shutdown_proof']);assert proof['armed']is True and proof['deadline_unix']==p['deadline_unix']
 mpath=pathlib.Path(__file__).with_name('source-manifest.json');assert digest(mpath)==p['source_manifest_sha256'];m=load(mpath);root=pathlib.Path(p['source_root']).resolve()
 for rel,v in m['files'].items():
  
  if time.time()>=p['deadline_unix']-l['shutdown_reserve_seconds']:raise TimeoutError('Source binding exhausted reserve')
  q=root/rel;assert q.stat().st_size==v['bytes']and digest(q)==v['sha256'],rel
 for rel,target in m['symlinks'].items():assert os.readlink(root/rel)==target and (root/rel).resolve().is_relative_to(root)
 s=load(p['spec']);template=load(pathlib.Path(__file__).with_name('SPEC-TEMPLATE.json'))
 for k,v in template.items():
  if k not in ['output','prefix_sha256']:assert s[k]==v,k
 assert s['prefix_sha256']==p['source_prefix_sha256'] and len(s['prefix_sha256'])==64
 assert s['output'] and not pathlib.Path(s['output']).exists();assert not pathlib.Path(p['receipt_dir']).exists()
 build=load(p['build_receipt']);assert build['binary_sha256']==p['binary_sha256'] and build['source_manifest_sha256']==p['source_manifest_sha256'] and build['opt_level']=='3' and build['compiler_sha256']=='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1'
 assert free(p['source_root'])>=l['root_build_free_floor_bytes'];assert free(p['data_mount'])>=l['required_data_filesystem_free_bytes']
 available=next(int(x.split()[1])*1024 for x in pathlib.Path('/proc/meminfo').read_text().splitlines()if x.startswith('MemAvailable:'));assert available>=l['required_available_memory_bytes']
 # Verify the complete retained source and exact prefix in one bounded pass.
 h=hashlib.sha256();prefix=hashlib.sha256();n=0
 with open(s['vectors']['path'],'rb')as f:
  while True:
   if time.time()>=p['deadline_unix']-l['shutdown_reserve_seconds']:raise TimeoutError('Binding exhausted shutdown reserve')
   b=f.read(1048576)
   if not b:break
   h.update(b);prefix.update(b[:max(0,min(len(b),3072000000-n))]);n+=len(b)
 assert n==s['vectors']['bytes'] and h.hexdigest()==s['vectors']['sha256'] and prefix.hexdigest()==s['prefix_sha256']
 return s

def run(command,receipt,output,deadline,rss_limit,output_limit,log_limit,data_mount):
 import subprocess
 if deadline<=time.time():raise TimeoutError('Deadline expired before process creation')
 receipt=pathlib.Path(receipt);receipt.mkdir(parents=True,exist_ok=False);start=time.monotonic();stop=start+(deadline-time.time());reason=None;peak=0;scan_seconds=0.;nextscan=0.;child=None;usage=None;status=None
 env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4')
 with open(receipt/'stdout','xb')as stdout,open(receipt/'stderr','xb')as stderr:
  try:
   child=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True,env=env)
   while True:
    pid,status,usage=os.wait4(child.pid,os.WNOHANG)
    if pid:break
    peak=max(peak,rss(child.pid));now=time.monotonic()
    if now>=stop or time.time()>=deadline:reason='deadline'
    elif peak>rss_limit:reason='rss'
    elif stdout.tell()+stderr.tell()>log_limit:reason='logs'
    elif free(data_mount)<8589934592:reason='disk reserve'
    if now>=nextscan:
     a=time.monotonic();used=size(output)if pathlib.Path(output).exists()else 0;scan_seconds+=time.monotonic()-a;nextscan=now+5
     if used>output_limit:reason='output'
    if reason:os.killpg(child.pid,signal.SIGKILL);_,status,usage=os.wait4(child.pid,0);break
    time.sleep(.2)
  except BaseException:
   if child and usage is None:
    try:os.killpg(child.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    _,status,usage=os.wait4(child.pid,0)
   raise
 child.returncode=os.waitstatus_to_exitcode(status)
 terminal=usage.ru_maxrss*(1024 if sys.platform=='linux' else 1);peak=max(peak,terminal);used=size(output)if pathlib.Path(output).exists()else 0;logs=size(receipt)
 if peak>rss_limit:reason=reason or 'terminal rss'
 if used>output_limit:reason=reason or 'terminal output'
 if logs>log_limit:reason=reason or 'terminal logs'
 result=dict(exit_code=os.waitstatus_to_exitcode(status),guard=reason,seconds=time.monotonic()-start,user_seconds=usage.ru_utime,system_seconds=usage.ru_stime,peak_rss_bytes=peak,output_bytes=used,log_bytes=logs,monitor_tree_scan_seconds=scan_seconds,thread_environment={k:env[k]for k in ['TOKIO_WORKER_THREADS','RAYON_NUM_THREADS']},scope='Native child only. RSS/output are sampled stop guards with possible overshoot, not allocation quotas. Directory scans can interfere. Provider returns are not physical IO/network. External shutdown remains independently armed.')
 write(receipt/'process.json',result);return result

def main():
 a=argparse.ArgumentParser();a.add_argument('--plan',required=True);args=a.parse_args();p=load(args.plan)
 if sys.platform!='linux':raise RuntimeError('Linux execution only')
 s=admit(p);deadline=min(time.time()+p['limits']['science_seconds'],p['deadline_unix']-p['limits']['shutdown_reserve_seconds'])
 r=run([p['binary'],'reference-owner-diagnostic','--spec',p['spec']],p['receipt_dir'],s['output'],deadline,p['limits']['rss_kill_bytes'],p['limits']['fresh_output_store_cap_bytes'],p['limits']['metadata_log_cap_bytes'],p['data_mount'])
 # Native integrity is separate from the externally interpreted structural gates.
 r['native_passed']=r['exit_code']==0 and r['guard']is None;r['structural_gate_status']='Requires retained result review: cache crossing, moves followed by insertion, zero pending, census and bit checks.'
 write(pathlib.Path(p['receipt_dir'])/'summary.json',r);return 0 if r['native_passed']else 1
if __name__=='__main__':sys.exit(main())
