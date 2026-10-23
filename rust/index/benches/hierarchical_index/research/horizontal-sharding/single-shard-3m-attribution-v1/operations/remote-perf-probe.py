#!/usr/bin/env python3
"""Probe existing per-process userspace profiling after the absolute guard is active."""
import argparse,json,pathlib,shutil,subprocess,time
p=argparse.ArgumentParser();p.add_argument('--output-dir',required=True,type=pathlib.Path);p.add_argument('--window-id',required=True);a=p.parse_args()
assert a.window_id.replace('-','').isalnum()
subprocess.run(['systemctl','is-active','--quiet',f'spann-single-shard-3m-{a.window_id}-stop.timer'],check=True)
a.output_dir.mkdir(parents=True,exist_ok=False)
report={'observed_unix':time.time(),'perf_path':shutil.which('perf'),'gprofng_path':shutil.which('gprofng'),'event':'cpu-clock:u','frequency_hz':99,'clockid':'CLOCK_REALTIME','call_graph':'fp','scope':'Only the bounded childprocess; no system-wide collection','kernel':subprocess.check_output(['uname','-r'],text=True).strip(),'sysctl':{},'installed_dependencies':False,'permission_changes':False}
for key in ['perf_event_paranoid','kptr_restrict']:
 f=pathlib.Path('/proc/sys/kernel')/key;report['sysctl'][key]=f.read_text().strip() if f.exists() else None
if report['perf_path']:
 v=subprocess.run([report['perf_path'],'--version'],capture_output=True,text=True,timeout=5);report['perf_version']={'exit_code':v.returncode,'stdout':v.stdout,'stderr':v.stderr}
 if v.returncode==0:
  argv=[report['perf_path'],'record','--freq','99','--event','cpu-clock:u','--clockid','CLOCK_REALTIME','--call-graph','fp','--output',str(a.output_dir/'probe.perf.data'),'--','python3','-c','import time; end=time.monotonic()+0.5; x=0\nwhile time.monotonic()<end: x+=1']
  r=subprocess.run(argv,capture_output=True,text=True,timeout=15);report['record']={'argv':argv,'exit_code':r.returncode,'stdout':r.stdout,'stderr':r.stderr}
  if r.returncode==0:
   r=subprocess.run([report['perf_path'],'report','--stdio','--no-children','--input',str(a.output_dir/'probe.perf.data')],capture_output=True,text=True,timeout=15)
   (a.output_dir/'probe.report.txt').write_text(r.stdout);report['report']={'exit_code':r.returncode,'stderr':r.stderr};report['supported']=r.returncode==0
   if report['supported']:
    summary=subprocess.run(['python3',str(pathlib.Path(__file__).with_name('perf-phase-summary.py')),'--perf',report['perf_path'],'--profile',str(a.output_dir/'probe.perf.data'),'--output',str(a.output_dir/'probe.phases.json')],capture_output=True,text=True,timeout=135)
    report['phase_parser']={'exit_code':summary.returncode,'stdout':summary.stdout,'stderr':summary.stderr};report['supported']=summary.returncode==0
report.setdefault('supported',False)
(a.output_dir/'probe.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
