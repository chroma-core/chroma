"""Invalidate only the changed benchmark inputs, after immutable-byte validation."""
from pathlib import Path
import hashlib,json,os,time
BENCH_ENTRY='rust/index/benches/hierarchical_spann_shard.rs'
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def refresh(root,manifest,baseline,entry=BENCH_ENTRY,prefix='rust/index/benches/'):
 root=Path(root).resolve()
 if manifest['symlinks']!=baseline['symlinks']or set(manifest['files'])!=set(baseline['files']):raise ValueError('Source inventory changed beyond benchmark refresh scope')
 changed=[name for name,row in manifest['files'].items()if row!=baseline['files'][name]]
 if any(not name.startswith(prefix)for name in changed):raise ValueError('Changed source outside benchmark refresh scope')
 for name,row in manifest['files'].items():
  p=root/name
  if not p.resolve().is_relative_to(root)or p.is_symlink()or p.stat().st_size!=row['bytes']or digest(p)!=row['sha256']:raise ValueError('Source bytes differ before refresh: '+name)
 for name,target in manifest['symlinks'].items():
  p=root/name
  if os.readlink(p)!=target or not p.resolve().is_relative_to(root):raise ValueError('Source symlink differs')
 if entry not in manifest['files']:raise ValueError('Benchmark entry missing')
 stamp=time.time_ns();rows=[]
 for name in sorted(set(changed+[entry])):
  p=root/name;before=p.stat().st_mtime_ns;os.utime(p,ns=(stamp,stamp));after=p.stat().st_mtime_ns
  if digest(p)!=manifest['files'][name]['sha256']:raise ValueError('Source bytes changed during refresh')
  rows.append(dict(path=name,before_mtime_ns=before,after_mtime_ns=after,sha256=manifest['files'][name]['sha256']))
 return dict(changed_source_files=changed,refreshed=rows,source_root=str(root),clock_ns=stamp,scope='Validated benchmark-only changes plus consuming entrypoint mtime refresh. No content change, target deletion or dependency cache cleanup. Bench compiler-artifact must independently report fresh=false.')
def require_recompiled(artifact):
 if artifact.get('fresh')is not False:raise ValueError('Benchmark artifact reused or freshness absent; source compilation not proven')
 return artifact
