"""Preserve bounded compaction receipts without checkpoint block payloads."""
import pathlib,json,tarfile,hashlib
base=pathlib.Path('/mnt/spann');names=['fineweb-compaction-guarded-int-h2-v1','fineweb-compaction-guarded-match-h1-v1']+[json.loads(x)['name'] for x in (base/'results/guarded-compaction-extra-v1/completed.jsonl').read_text().splitlines()];assert len(names)==6
files=set()
for name in names:
 case=base/'results'/name;assert json.loads((case/'case.json').read_text())['status']=='passed'
 for p in case.rglob('*'):
  if p.is_file() and p.suffix in ('.json','.jsonl','.stdout','.stderr','.u64le'):
   assert not p.is_symlink();files.add(p)
 for p in (base/name).iterdir():
  if p.is_file() and (p.suffix in ('.json','.jsonl','.py') or p.name.startswith('producer')):files.add(p)
for p in (base/'stage_guarded_compaction.py',base/'run_guarded_compaction_extra.py',base/'results/guarded-compaction-extra-v1/completed.jsonl'):
 if p.exists():files.add(p)
raw=sum(p.stat().st_size for p in files);assert raw<1024**3,raw;assert all(p.stat().st_size<64*1024**2 for p in files)
out=base/'results/guarded-compaction-six-arm-small-v1.tar.gz'
with tarfile.open(out,'w:gz') as t:
 for p in sorted(files):t.add(p,arcname=str(p.relative_to(base)),recursive=False)
assert out.stat().st_size<128*1024**2
print(json.dumps(dict(files=len(files),raw_bytes=raw,archive_bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest())))
