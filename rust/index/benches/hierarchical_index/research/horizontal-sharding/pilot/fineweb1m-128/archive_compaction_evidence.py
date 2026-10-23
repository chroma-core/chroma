import pathlib,json,tarfile,hashlib
base=pathlib.Path('/mnt/spann');names=['fineweb1m-append100k-integration-h1-v3']+[json.loads(x)['name'] for x in (base/'results/compaction-paired-v1/completed.jsonl').read_text().splitlines()];files=set()
for name in names:
 case=base/'results'/name;assert json.load(open(case/'case.json'))['status']=='passed'
 for p in case.rglob('*'):
  if p.is_file() and p.suffix in ('.json','.jsonl','.stdout','.stderr','.u64le'):
   assert not p.is_symlink();files.add(p)
 for p in (base/name).glob('*.json'):files.add(p)
files.add(base/'results/compaction-paired-v1/completed.jsonl')
for p in (base/'run_compaction_pairs.py',base/'collect_compaction_metrics.py'):files.add(p)
raw=sum(p.stat().st_size for p in files);assert raw<512*1024**2,raw;assert all(p.stat().st_size<64*1024**2 for p in files)
out=base/'results/compaction-baseline-small-v1.tar.gz'
with tarfile.open(out,'w:gz') as t:
 for p in sorted(files):t.add(p,arcname=str(p.relative_to(base)),recursive=False)
assert out.stat().st_size<64*1024**2
print(json.dumps(dict(files=len(files),raw_bytes=raw,archive_bytes=out.stat().st_size,sha256=hashlib.sha256(out.read_bytes()).hexdigest())))
