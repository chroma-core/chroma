"""Preserve all small10M construction records, excluding every checkpoint block."""
import pathlib,json,tarfile,hashlib
base=pathlib.Path('/mnt/spann');names=['fineweb10m128-h1-a16-c16-v1','fineweb10m128-h2-a16-c16-v2'];files=set()
for name in names:
 case=base/'results'/name;assert json.loads((case/'case.json').read_text())['status']=='passed'
 for p in case.rglob('*'):
  if p.is_file() and p.suffix in ('.json','.jsonl','.stdout','.stderr','.u64le'):assert not p.is_symlink();files.add(p)
 for p in (base/name).iterdir():
  if p.is_file() and (p.suffix in ('.json','.jsonl','.py') or p.name.startswith('producer')):files.add(p)
for name in ('collect10m_metrics.py','stage_guarded_initial.py','setup_read_epoch.py','read-grid-v1.json'):
 p=base/name
 if p.exists():files.add(p)
raw=sum(p.stat().st_size for p in files);assert raw<1024**3;assert all(p.stat().st_size<128*1024**2 for p in files)
out=base/'results/fineweb10m-completed-builds-small-v1.tar.gz'
with tarfile.open(out,'w:gz') as t:
 for p in sorted(files):t.add(p,arcname=str(p.relative_to(base)),recursive=False)
print(json.dumps(dict(files=len(files),raw_bytes=raw,archive_bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest())))
