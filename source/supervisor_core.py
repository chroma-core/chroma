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

def clean_absolute(value):
 q=pathlib.Path(value)
 if not q.is_absolute() or '..' in q.parts:raise ValueError('Path must be absolute and normalized')
 for ancestor in [q,*q.parents]:
  if ancestor.is_symlink():raise ValueError('Symlink path is forbidden')
 return q

def policy_and_paths(p,s):
 template=load(pathlib.Path(__file__).with_name('PLAN-TEMPLATE.json'))
 if p['limits']!=template['limits'] or p['thread_environment']!=template['thread_environment']:raise ValueError('Immutable resource/thread policy differs')
 data=clean_absolute(p['data_output_root']);control=clean_absolute(p['control_root'])
 output=clean_absolute(s['output']);receipt=clean_absolute(p['receipt_dir'])
 if not data.is_relative_to(clean_absolute(p['data_mount'])):raise ValueError('Output root outside data mount')
 protected=[clean_absolute(p['source_root']),clean_absolute(s['vectors']['path']).parent]
 for a,b in [(data,control),(output,receipt)]:
  if a.is_relative_to(b) or b.is_relative_to(a):raise ValueError('Overlapping output/control paths')
 for q,root in [(output,data),(receipt,control)]:
  if q==root or not q.is_relative_to(root) or q.exists():raise ValueError('Output must be fresh and contained')
  for keep in protected:
   if q.is_relative_to(keep) or keep.is_relative_to(q):raise ValueError('Output overlaps retained input/source')
 return True

def terminate_group(pid):
 try:os.killpg(pid,signal.SIGKILL)
 except ProcessLookupError:pass

def enable_subreaper():
 if sys.platform=='linux':
  import ctypes
  if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0)!=0:raise OSError('Cannot enable child subreaper')

def reap_group(pid):
 # Linux subreaper adopts orphaned descendants in this owned process group.
 if sys.platform=='linux':
  until=time.monotonic()+3
  while True:
   try:
    got,_=os.waitpid(-pid,os.WNOHANG)
    if got:continue
   except ChildProcessError:return
   if time.monotonic()>=until:raise RuntimeError('Owned descendant did not reap')
   time.sleep(.01)

def host_policy(p,s):
 if not p.get('host_admission') or not p.get('host_admission_sha256'):raise ValueError('Unfilled hardware/path admission')
 assert digest(p['host_admission'])==p['host_admission_sha256']
 host=load(p['host_admission']);proof=load(p['shutdown_proof'])
 assert p.get('selected_instance_id') and host['instance_id']==p['selected_instance_id']==proof['instance_id']
 assert host['region']==p['region']=='us-east-1' and host['deadline_unix']==p['deadline_unix']==proof['deadline_unix']
 assert host['data_mount']==p['data_mount'];clean_absolute(host['data_mount'])
 assert host['account']=='079819354890' and host['physical_cores']==32 and host['input_path']==s['vectors']['path'] and host['fresh_identity_verified']is True and host['capacity_verified']is True

def admit(p):
 for k in ['binary','binary_sha256','build_receipt','build_receipt_sha256','source_root','source_manifest_sha256','spec','spec_sha256','deadline_unix','shutdown_proof','shutdown_proof_sha256','receipt_dir','data_mount','source_prefix_sha256','data_output_root','control_root']:
  if p.get(k)is None:raise ValueError('Unfilled execution gate: '+k)
 l=p['limits'];assert p['deadline_unix']-time.time()>l['shutdown_reserve_seconds']
 for path,key in [('binary','binary_sha256'),('build_receipt','build_receipt_sha256'),('spec','spec_sha256'),('shutdown_proof','shutdown_proof_sha256')]:assert digest(p[path])==p[key],path
 proof=load(p['shutdown_proof']);assert proof['armed']is True and proof['deadline_unix']==p['deadline_unix']
 mpath=pathlib.Path(__file__).with_name('source-manifest.json');assert digest(mpath)==p['source_manifest_sha256'];m=load(mpath);root=pathlib.Path(p['source_root']).resolve()
 for rel,v in m['files'].items():
  
  if time.time()>=p['deadline_unix']-l['shutdown_reserve_seconds']:raise TimeoutError('Source binding exhausted reserve')
  q=root/rel;assert q.stat().st_size==v['bytes']and digest(q)==v['sha256'],rel
 for rel,target in m['symlinks'].items():assert os.readlink(root/rel)==target and (root/rel).resolve().is_relative_to(root)
 s=load(p['spec']);policy_and_paths(p,s);template=load(pathlib.Path(__file__).with_name('SPEC-TEMPLATE.json'))
 for k,v in template.items():
  if k=='vectors':
   assert s[k]['bytes']==v['bytes'] and s[k]['sha256']==v['sha256'];clean_absolute(s[k]['path'])
  elif k not in ['output','prefix_sha256']:assert s[k]==v,k
 host_policy(p,s)
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
   h.update(b);prefix.update(b[:max(0,min(len(b),s['rows']*s['dimension']*4-n))]);n+=len(b)
 assert n==s['vectors']['bytes'] and h.hexdigest()==s['vectors']['sha256'] and prefix.hexdigest()==s['prefix_sha256']
 return s

SCAN_INTERVAL_SECONDS=60.
SCAN_TIMEOUT_SECONDS=30.
class OutputScan:
 """One owned, cancellable process; no file ledger survives a completed scan."""
 def __init__(self,paths,command=None):
  import subprocess
  self.started=time.monotonic();self.closed=False
  self.child=subprocess.Popen(command or [sys.executable,__file__,'--scan',*[str(p)for p in paths]],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,start_new_session=True)
  os.set_blocking(self.child.stdout.fileno(),False)
 def poll(self):
  if self.child.poll()is None:return None
  data=self.child.stdout.read(4097) or b''
  if self.child.returncode!=0 or len(data)>4096:raise RuntimeError('Output scan failed')
  values=json.loads(data)
  if not isinstance(values,list)or any(type(v)is not int or v<0 for v in values):raise RuntimeError('Malformed output scan')
  self.close();return values
 def close(self):
  if self.closed:return
  self.child.poll()
  try:terminate_group(self.child.pid)
  except PermissionError:
   # Darwin can reject killpg against an exited but unreaped session leader.
   # Reap only that owned child, then retry group cleanup; live denial still fails.
   self.child.wait(timeout=.1);terminate_group(self.child.pid)
  self.child.wait();self.child.stdout.close();reap_group(self.child.pid);self.closed=True

def run(command,receipt,output,deadline,rss_limit,output_limit,log_limit,data_mount,*,scan_interval=SCAN_INTERVAL_SECONDS,scan_timeout=SCAN_TIMEOUT_SECONDS,scan_factory=OutputScan):
 import subprocess
 enable_subreaper()
 if deadline<=time.time():raise TimeoutError('Deadline expired before process creation')
 receipt=pathlib.Path(receipt);receipt.mkdir(parents=True,exist_ok=False);start=time.monotonic();stop=start+(deadline-time.time());reason=None;peak=0;scan_seconds=0.;nextscan=start;child=None;usage=None;status=None;scanner=None;scans=0;used=None;observed_at=None;terminal_accounting=False
 env=dict(os.environ,TOKIO_WORKER_THREADS='32',RAYON_NUM_THREADS='32')
 previous=signal.getsignal(signal.SIGTERM)
 def terminated(signum,frame):raise SystemExit('Supervisor received TERM')
 signal.signal(signal.SIGTERM,terminated)
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
    if not reason and scanner is not None:
     try:
      values=scanner.poll()
      if values is not None:
       if len(values)!=1:raise RuntimeError('Output scan arity')
       used=values[0];observed_at=time.monotonic();scan_seconds+=observed_at-scanner.started;scanner=None;nextscan=observed_at+scan_interval
       if used>output_limit:reason='output'
      elif now-scanner.started>=scan_timeout:reason='output accounting timeout'
     except Exception:reason='output accounting failed'
    if not reason and scanner is None and now>=nextscan:
     scanner=scan_factory([output]);scans+=1
    if reason:os.killpg(child.pid,signal.SIGKILL);_,status,usage=os.wait4(child.pid,0);break
    time.sleep(.05)
  finally:
   try:
    try:
     if scanner is not None:scan_seconds+=time.monotonic()-scanner.started;scanner.close();scanner=None
    finally:
     # A scanner cleanup failure must never skip cleanup of the native group.
     if child:
      terminate_group(child.pid)
      if usage is None:_,status,usage=os.wait4(child.pid,0)
      child.returncode=os.waitstatus_to_exitcode(status);reap_group(child.pid)
   finally:signal.signal(signal.SIGTERM,previous)
 # Reconcile after the native group is gone. Terminal traversal is also cancellable.
 signal.signal(signal.SIGTERM,terminated)
 terminal_scan=None;terminal_start=time.monotonic();logs=None
 try:
  terminal_scan=scan_factory([output,receipt])
  while True:
   values=terminal_scan.poll()
   if values is not None:
    if len(values)!=2:raise RuntimeError('Terminal scan arity')
    used,logs=values;terminal_accounting=True;break
   if time.monotonic()-terminal_start>=scan_timeout:raise TimeoutError('Terminal scan timeout')
   time.sleep(.05)
 except Exception:reason=reason or 'terminal output accounting unavailable'
 finally:
  try:
   if terminal_scan is not None:terminal_scan.close()
  finally:signal.signal(signal.SIGTERM,previous)
 terminal_scan_seconds=time.monotonic()-terminal_start
 terminal=usage.ru_maxrss*(1024 if sys.platform=='linux' else 1);peak=max(peak,terminal)
 if peak>rss_limit:reason=reason or 'terminal rss'
 if terminal_accounting and used>output_limit:reason=reason or 'terminal output'
 if terminal_accounting and logs>log_limit:reason=reason or 'terminal logs'
 result=dict(exit_code=os.waitstatus_to_exitcode(status),guard=reason,seconds=time.monotonic()-start,user_seconds=usage.ru_utime,system_seconds=usage.ru_stime,peak_rss_bytes=peak,output_bytes=used,log_bytes=logs,monitor_tree_scan_seconds=scan_seconds,monitor_scan_count=scans,terminal_scan_seconds=terminal_scan_seconds,terminal_accounting_complete=terminal_accounting,last_live_output_observation_age_seconds=None if observed_at is None else time.monotonic()-observed_at,scan_interval_seconds=scan_interval,scan_timeout_seconds=scan_timeout,thread_environment={k:env[k]for k in ['TOKIO_WORKER_THREADS','RAYON_NUM_THREADS']},scope='Native child resource usage; supervisor elapsed includes bounded terminal reconciliation. One cancellable scanner; next scan begins after completion plus interval. RSS/output/free-space are sampled stopping guards, not quotas. With default policy output observation can lag roughly 90 seconds plus scheduling; byte overshoot has no finite bound without a write-rate bound. Failed scans stop the run. Directory scan work can still interfere; scanner CPU is excluded from native usage. External shutdown remains independently armed.')
 write(receipt/'process.json',result);return result

def main():
 a=argparse.ArgumentParser();a.add_argument('--plan',required=True);args=a.parse_args();p=load(args.plan)
 if sys.platform!='linux':raise RuntimeError('Linux execution only')
 s=admit(p);deadline=min(time.time()+p['limits']['science_seconds'],p['deadline_unix']-p['limits']['shutdown_reserve_seconds'])
 r=run([p['binary'],'reference-owner-diagnostic','--spec',p['spec']],p['receipt_dir'],s['output'],deadline,p['limits']['rss_kill_bytes'],p['limits']['fresh_output_store_cap_bytes'],p['limits']['metadata_log_cap_bytes'],p['data_mount'])
 # Native integrity is separate from the externally interpreted structural gates.
 r['native_passed']=r['exit_code']==0 and r['guard']is None;r['structural_gate_status']='Requires retained result review: cache crossing, moves followed by insertion, zero pending, census and bit checks.'
 write(pathlib.Path(p['receipt_dir'])/'summary.json',r);return 0 if r['native_passed']else 1
if __name__=='__main__':
 if len(sys.argv)>1 and sys.argv[1]=='--scan':print(json.dumps([size(p)if pathlib.Path(p).exists()else 0 for p in sys.argv[2:]]))
 else:sys.exit(main())
