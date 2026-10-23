import pathlib,json,time,os
out=open('/mnt/spann/read-profile-v2/process-samples.jsonl','w');stop=time.monotonic()+240
while time.monotonic()<stop:
 rows=[]
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   cmd=(p/'cmdline').read_bytes().replace(bytes([0]),b' ').decode()
   if 'hierarchical_spann_shard-audited-v3 read --checkpoint-dir /mnt/spann/read-profile-v1/' not in cmd and 'tools.hierarchical_sharding.subtree_server' not in cmd:continue
   stat=(p/'stat').read_text().rsplit(')',1)[1].split();status=dict(line.split(':',1) for line in (p/'status').read_text().splitlines() if ':' in line)
   rows.append(dict(pid=int(p.name),cpu_s=(int(stat[11])+int(stat[12]))/os.sysconf('SC_CLK_TCK'),rss_kib=int(status.get('VmRSS','0 kB').split()[0]),cmd=cmd))
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass
 out.write(json.dumps(dict(unix=time.time(),processes=rows))+'\n');out.flush();time.sleep(.5)
