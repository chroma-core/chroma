import pathlib,os,signal,shutil,time,json
r=pathlib.Path('/home/ubuntu/owner3m');deadline=1792281037
parents=[]
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:a=(p/'cmdline').read_bytes().split(b'\0')
 except OSError:continue
 if len(a)>1 and a[1]==b'/home/ubuntu/owner3m/run-cache1024.py':parents.append(int(p.name))
assert len(parents)==1,parents
parent=parents[0];(r/'disk-guard-ready.json').write_text(json.dumps(dict(parent_pid=parent,guard_pid=os.getpid(),minimum_free_bytes=10*1024**3,deadline_unix=deadline))+'\n')
while time.time()<deadline:
 try:os.kill(parent,0)
 except ProcessLookupError:break
 free=shutil.disk_usage(r).free
 if free<10*1024**3:
  killed=[]
  for p in pathlib.Path('/proc').iterdir():
   if not p.name.isdigit():continue
   try:a=(p/'cmdline').read_bytes().split(b'\0')
   except OSError:continue
   if a and a[0].startswith(b'/home/ubuntu/reference3m/source/target/release/deps/hierarchical_spann_shard-') and any(x.startswith(b'/home/ubuntu/owner3m/cache1024/owner-') and x.endswith(b'-spec.json') for x in a):
    try:os.kill(int(p.name),signal.SIGTERM);killed.append(int(p.name))
    except ProcessLookupError:pass
  (r/'disk-stop.json').write_text(json.dumps(dict(free_bytes=free,stopped_pids=killed,time_unix=time.time()))+'\n')
  if killed:break
 time.sleep(5)
(r/'disk-guard-closed.json').write_text(json.dumps(dict(time_unix=time.time()))+'\n')
