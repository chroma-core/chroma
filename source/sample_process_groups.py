"""Sample declared process groups and their descendants without inspecting data."""
import argparse,json,os,pathlib,time
p=argparse.ArgumentParser();p.add_argument('--groups',required=True);p.add_argument('--output',required=True);p.add_argument('--duration',type=float,default=3600);a=p.parse_args();groups=json.loads(pathlib.Path(a.groups).read_text());deadline=time.monotonic()+a.duration;ticks=os.sysconf('SC_CLK_TCK')
with open(a.output,'w') as out:
 while time.monotonic()<deadline:
  processes={}
  for path in pathlib.Path('/proc').iterdir():
   if not path.name.isdigit() or int(path.name)==os.getpid():continue
   try:
    stat=(path/'stat').read_text().rsplit(')',1)[1].split();status=dict(line.split(':',1) for line in (path/'status').read_text().splitlines() if ':' in line);processes[int(path.name)]=dict(pid=int(path.name),ppid=int(stat[1]),cpu_s=(int(stat[11])+int(stat[12]))/ticks,rss_kib=int(status.get('VmRSS','0 kB').split()[0]),peak_rss_kib=int(status.get('VmHWM','0 kB').split()[0]),main_thread_context_switches={key:int(status.get(key,'0')) for key in ('voluntary_ctxt_switches','nonvoluntary_ctxt_switches')},cmd=(path/'cmdline').read_bytes().replace(bytes([0]),b' ').decode(errors='replace'))
   except (FileNotFoundError,ProcessLookupError,PermissionError):pass
  selected={}
  for label,roots in groups.items():
   members=set(roots)
   while True:
    extra={pid for pid,row in processes.items() if row['ppid'] in members}-members
    if not extra:break
    members|=extra
   selected[label]=[row for pid,row in processes.items() if pid in members]
  out.write(json.dumps(dict(unix=time.time(),groups=selected))+'\n');out.flush();time.sleep(.5)
