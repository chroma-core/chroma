"""Local-only, one-attempt native supervision. Importing performs no operation."""
import hashlib,json,os,pathlib,signal,subprocess,sys,time,datetime

def pin(path):
 h=hashlib.sha256();size=0
 with pathlib.Path(path).open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk);size+=len(chunk)
 return dict(bytes=size,sha256=h.hexdigest())

def write(path,value):
 with pathlib.Path(path).open('x') as f:json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())

def tree_bytes(path):return sum(p.stat().st_size for p in pathlib.Path(path).rglob('*') if p.is_file()) if pathlib.Path(path).exists() else 0

def rss_bytes(pid):
 result=subprocess.run(['/bin/ps','-o','rss=','-p',str(pid)],capture_output=True,text=True,timeout=1)
 return int(result.stdout.strip())*1024 if result.returncode==0 and result.stdout.strip().isdigit() else None

def supervise(command,logs,output,deadline,rss_limit,output_limit,probe=rss_bytes,disk=tree_bytes):
 if time.monotonic()>=deadline:raise ValueError("Expired deadline before process creation")
 logs=pathlib.Path(logs);logs.mkdir();started=time.monotonic();started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat();peak=None;reason=None;usage=None;status=None;samples=0;output_peak=0
 env=os.environ.copy();env.update(TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4')
 with (logs/'stdout').open('wb') as stdout,(logs/'stderr').open('wb') as stderr:
  child=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True,env=env)
  try:
   while True:
    waited,status_now,usage_now=os.wait4(child.pid,os.WNOHANG)
    if waited:status=status_now;usage=usage_now;break
    value=probe(child.pid);samples+=1
    if value is not None:peak=value if peak is None else max(peak,value)
    output_peak=max(output_peak,disk(output)+tree_bytes(logs))
    if time.monotonic()>=deadline:reason='deadline'
    elif value is not None and value>rss_limit:reason='sampled-rss-limit'
    elif output_peak>output_limit:reason='sampled-output-limit'
    if reason:
     os.killpg(child.pid,signal.SIGKILL);_,status,usage=os.wait4(child.pid,0);break
    time.sleep(min(.1,max(0,deadline-time.monotonic())))
  except BaseException:
   try:os.killpg(child.pid,signal.SIGKILL)
   except ProcessLookupError:pass
   if status is None:_,status,usage=os.wait4(child.pid,0)
   child.returncode=os.waitstatus_to_exitcode(status);raise
  child.returncode=os.waitstatus_to_exitcode(status)
 output_peak=max(output_peak,disk(output)+tree_bytes(logs));reason=reason or ('final-output-limit' if output_peak>output_limit else None)
 terminal_max=usage.ru_maxrss*(1 if sys.platform=='darwin' else 1024)
 reason=reason or ('terminal-rss-limit' if terminal_max>rss_limit else None)
 result=dict(command=command,pid=child.pid,started_utc=started_utc,elapsed_seconds=time.monotonic()-started,returncode=child.returncode,guard_failure=reason,sampled_peak_rss_bytes=peak,rss_samples=samples,terminal_child_max_rss_bytes=terminal_max,child_user_seconds=usage.ru_utime,child_system_seconds=usage.ru_stime,retained_output_bytes=output_peak,thread_environment={'TOKIO_WORKER_THREADS':'4','RAYON_NUM_THREADS':'4'},scope='wait4 CPU/maximum RSS belong to the native child, not sampling subprocesses. RSS samples can miss short peaks; file-size scans are sampled and exclude filesystem allocation overhead. Parent supervision overhead contributes to elapsed pacing but not child CPU. No physical IO/network metric.')
 write(logs/'process.json',result);return result

def classify(result,live,cache_entries,rows):
 report=result['after_save'];shape=report['shape'];census=result['census'];checks={'pending_balance_zero':report['pending_balance']==0,'nodes_exceed_cache':shape['reachable_nodes']>cache_entries,'actual_splits':report['counts']['splits']>0,'full_current_census':census['current_rows']==rows,'vector_bits':census['vector_bits_verified'] is True,'query_bits':census['query_exact_distance_bits_verified'] is True}
 if live:
  store=report['live_topology'];checks.update(actual_evictions=store['evictions']>0,multiple_durable_commits=store['durable_root_commits']>=2)
 names=['input_seconds','setup_seconds','add_seconds','balance_seconds','placement_seconds','save_seconds','query_open_seconds','query_seconds','audit_seconds'];phase_sum=sum(result[n] for n in names);unbucketed=result['native_before_serialize_seconds']-phase_sum
 return dict(status='supported-mechanism' if all(checks.values()) and unbucketed>=0 else 'inconclusive',checks=checks,enclosing_phase_seconds=sum(result[n] for n in names),unbucketed_before_serialize_seconds=unbucketed,scope='Unbucketed elapsed includes provider initialization, diagnostic snapshots/hash/final descriptor work outside named timers; no clamping. Copy/hash/staging are nested in add. Reader64 is paged in BOTH construction arms. Placement is a greedy current-leaf heuristic; inspect actual load/skew, not a guaranteed threshold.')

def run_scope(plan):
 return dict(native_profile=plan['native_profile'],scope='Independent stochastic trees; the pinned plan reports the actual compiler profile. Within-run cost/work attribution only, no causal build speed ratio or quality comparison.')

def main():
 plan_path=pathlib.Path(sys.argv[1]).resolve();plan=json.loads(plan_path.read_text());root=pathlib.Path(plan['run_output']);root.mkdir();start=time.monotonic();deadline=start+plan['whole_seconds'];records=[];native_total=0.
 try:
  assert sys.platform=='darwin','This frozen admission supervises a local Mac run only.'
  for obj in [plan['native'],plan['input'],plan['source_manifest'],*plan['specs']]:assert pin(obj['path'])=={k:obj[k] for k in ['bytes','sha256']},obj['path']
  manifest=json.loads(pathlib.Path(plan['source_manifest']['path']).read_text())
  for rel,expected in manifest['files'].items():assert pin(pathlib.Path(plan['checkout'])/rel)==expected,rel
  for index,spec_desc in enumerate(plan['specs']):
   spec_path=pathlib.Path(spec_desc['path']);spec=json.loads(spec_path.read_text());assert not pathlib.Path(spec['output']).exists(),'No output reuse or retry.'
   remaining=plan['aggregate_native_seconds']-native_total;assert remaining>0
   process=supervise([plan['native']['path'],'reference-owner-diagnostic','--spec',str(spec_path)],root/('cell'+str(index)),spec['output'],min(deadline,time.monotonic()+plan['per_arm_seconds'],time.monotonic()+remaining),plan['rss_limit_bytes'],plan['output_limit_bytes_per_arm']);native_total+=process['elapsed_seconds'];records.append(dict(process=process))
   if process['returncode'] or process['guard_failure']:raise RuntimeError('Native process or guard failed; no second attempt.')
   result_path=pathlib.Path(spec['output'])/'result.json';result=json.loads(result_path.read_text());records[-1].update(result=dict(path=str(result_path),**pin(result_path)),structural_gate=classify(result,spec['live_topology'],spec['topology_cache_entries'],spec['rows']))
  summary=dict(status='completed' if all(v['structural_gate']['status']=='supported-mechanism' for v in records) else 'inconclusive',cells=records,elapsed_seconds=time.monotonic()-start,native_elapsed_sum=native_total,plan=dict(path=str(plan_path),**pin(plan_path)),**run_scope(plan))
 except BaseException as error:
  write(root/'summary.json',dict(status='failed',error=repr(error),cells=records,elapsed_seconds=time.monotonic()-start));raise
 write(root/'summary.json',summary)

if __name__=='__main__':main()
