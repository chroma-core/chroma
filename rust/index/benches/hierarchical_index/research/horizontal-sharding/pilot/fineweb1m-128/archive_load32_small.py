"""Preserve only completed load journals and small provenance, never index data."""
import pathlib,tarfile,json,hashlib
root=pathlib.Path('/mnt/spann/results/fineweb1m-load32-v1');assert (root/'complete.json').exists();base=pathlib.Path('/mnt/spann')
for rep in (1,2,3):
 directory=root/f'repetition-{rep}';files=[p for p in directory.iterdir() if p.is_file() and (p.suffix=='.jsonl' or p.name.endswith('-readiness.json') or p.name=='summary.json')];assert all(not p.is_symlink() and p.stat().st_size<64*1024**2 for p in files);raw=sum(p.stat().st_size for p in files);assert raw<256*1024**2,raw
 out=base/'results'/f'load32-baseline-repetition-{rep}.tar.gz'
 with tarfile.open(out,'w:gz') as t:
  for p in files:t.add(p,arcname=str(p.relative_to(base)),recursive=False)
 assert out.stat().st_size<64*1024**2;print(json.dumps(dict(repetition=rep,raw_bytes=raw,archive_bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest())),flush=True)
files=[p for p in root.iterdir() if p.is_file() and p.suffix in ('.json','.jsonl')]+[base/p for p in ('run_load32.py','warm_load32.py','sample_process_groups.py','selection.json','load32-tools.tar.gz','results/resource-baseline.json','results/role-runtime-proof.json')];raw=sum(p.stat().st_size for p in files);assert raw<256*1024**2
out=base/'results/load32-baseline-provenance-resources.tar.gz'
with tarfile.open(out,'w:gz') as t:
 for p in files:t.add(p,arcname=str(p.relative_to(base)),recursive=False)
assert out.stat().st_size<64*1024**2;print(json.dumps(dict(role='driver-resources-source',raw_bytes=raw,archive_bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest())),flush=True)
