"""One external sample during the first balance. No native instrumentation."""
import importlib.util,json,pathlib,subprocess,time,sys,os,signal

def main():
 path=pathlib.Path(sys.argv[1]);plan=json.loads(path.read_text());loader=importlib.util.spec_from_file_location('reviewed_supervisor',plan['supervisor']['path']);guard=importlib.util.module_from_spec(loader);loader.loader.exec_module(guard)
 for obj in [plan['supervisor'],plan['native'],plan['spec'],plan['input'],plan['source_manifest']]:assert guard.pin(obj['path'])=={k:obj[k] for k in ['bytes','sha256']},obj['path']
 manifest=json.loads(pathlib.Path(plan['source_manifest']['path']).read_text())
 for rel,expected in manifest['files'].items():assert guard.pin(pathlib.Path(plan['checkout'])/rel)==expected,rel
 spec=json.loads(pathlib.Path(plan['spec']['path']).read_text());assert not pathlib.Path(spec['output']).exists();logs=pathlib.Path(plan['logs']);assert not logs.exists();started=time.monotonic();sample=None;sample_started=None;sample_result={'status':'not-triggered'};armed=True
 def progress():
  file=logs/'stderr'
  if not file.exists():return []
  return [json.loads(line) for line in file.read_text(errors='replace').splitlines() if line.startswith('{') and 'progress_stage' in line]
 def finish_sample(force=False):
  nonlocal sample_result
  if sample is None:return
  if sample.poll() is None and (force or time.monotonic()-sample_started>=plan['sample_timeout_seconds']):sample.kill();sample.wait()
  if sample.poll() is not None and sample_result['status']=='running':sample_result.update(status='completed' if sample.returncode==0 else 'failed',returncode=sample.returncode,observed_elapsed_seconds=time.monotonic()-sample_started,last_progress_at_observed_completion=progress()[-1:] )
 def probe(pid):
  nonlocal sample,sample_started,sample_result,armed
  rss=guard.rss_bytes(pid);lines=progress();balance=[v for v in lines if v['progress_stage']=='balance']
  if armed and balance:
   armed=False
   if any(v['event']=='exit' for v in balance):sample_result={'status':'missed-first-balance;no-later-trigger'}
   else:
    command=[plan['sampler'],str(pid),'2','10','-file',str(logs/'sample.txt')];sample_started=time.monotonic()
    with (logs/'sample.stdout').open('wb') as out,(logs/'sample.stderr').open('wb') as err:sample=subprocess.Popen(command,stdout=out,stderr=err)
    sample_result={'status':'running','pid':sample.pid,'native_pid':pid,'command':command,'trigger_progress':balance[0],'started_since_supervision_seconds':sample_started-started}
  finish_sample();return rss
 try:
  process=guard.supervise([plan['native']['path'],'reference-owner-diagnostic','--spec',plan['spec']['path']],logs,spec['output'],started+60,plan['rss_limit_bytes'],plan['output_limit_bytes'],probe=probe)
 finally:
  if sample is not None:
   try:sample.wait(timeout=max(.001,plan['sample_timeout_seconds']-(time.monotonic()-sample_started)))
   except subprocess.TimeoutExpired:sample.kill();sample.wait()
   finish_sample(force=True)
  if logs.exists():guard.write(logs/'sample-process.json',sample_result)
 result_path=pathlib.Path(spec['output'])/'result.json';native=json.loads(result_path.read_text()) if result_path.exists() else None
 summary={'process':process,'sample':sample_result,'native_result':guard.pin(result_path) if native else None,'structural_gate':guard.classify(native,False,spec['topology_cache_entries'],spec['rows']) if native else None,'elapsed_seconds':time.monotonic()-started,'profile_scope':'Only first-balance2s/10ms sampled stacks. All threads include blocked/idle states; stack counts are not CPU percentages. Sampler overhead is included. Native work is independently stochastic; no causal wall comparison. Last progress at sampler completion documents phase overlap but has up to100ms observation delay.'}
 guard.write(logs/'summary.json',summary)

if __name__=='__main__':main()
