import hashlib,json,os,platform,shutil,subprocess,time
from pathlib import Path
R=Path('/mnt/spann/hardware-scorer-v1');O=Path('/mnt/spann/component-replay-v1/restored')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
 return h.hexdigest()
def write(n,v):(R/n).write_text(json.dumps(v,indent=2)+'\n')
deadline=json.loads((R/'deadline.json').read_text())['deadline_unix'];assert time.time()+600<deadline
subprocess.run(['systemctl','is-active','--quiet','spann-hardware-v1-stop.timer'],check=True)
assert platform.machine()=='x86_64'
raw=Path('/proc/cpuinfo').read_text();sets=[set(l.split(':',1)[1].split()) for l in raw.splitlines() if l.startswith('flags\t')];assert sets
flags=sorted(set.intersection(*sets));assert 'popcnt' in flags
write('cpu-features.json',dict(architecture=platform.machine(),flags=flags,logical_cpus=len(sets),observed_unix=time.time(),cpuinfo_sha256=hashlib.sha256(raw.encode()).hexdigest(),flag_policy='intersection across every reported logical CPU'))
(R/'cpuinfo.txt').write_text(raw)
assert shutil.disk_usage(R).free>12*1024**3
members=json.loads((R/'owner-members.json').read_text())
for name,v in members.items():
 p=O/name;assert p.stat().st_size==v['bytes'] and sha(p)==v['sha256'],name
assert len(members)==220
assert sha(O/'27/manifest-1.json')=='0d62eb96b71f873872c0685d0472c611e22e1f9a5ac9b6758f7bf8244aa589e7'
write('owner-verified.json',dict(files=len(members),logical_bytes=sum(v['bytes'] for v in members.values()),inventory_sha256=sha(R/'owner-members.json'),manifest_sha256=sha(O/'27/manifest-1.json'),observed_unix=time.time(),reused_without_download=True))
keys=['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','CARGO_BUILD_TARGET'];env={k:os.environ.get(k) for k in keys};assert not any(env.values()),env
write('build-preflight.json',dict(observed_unix=time.time(),deadline_unix=deadline,free_disk_bytes=shutil.disk_usage(R).free,environment=env,toolchain=subprocess.check_output(['/home/ubuntu/.cargo/bin/rustc','+1.92.0','-Vv'],text=True),timer=subprocess.check_output(['systemctl','show','spann-hardware-v1-stop.timer','-p','ActiveState','-p','NextElapseUSecRealtime'],text=True)))
