"""Package immutable metadata and the read binary without any vector blocks."""
import pathlib,hashlib,tarfile,json
base=pathlib.Path('/mnt/spann');case=base/'results/fineweb10m128-h2-a16-c16-v2';files=[p for p in case.glob('*.json') if p.name in ('case.json','current.json','tree-1.json','read-epoch.json') or p.name.startswith('tree-')];files+=list((case/'copies').rglob('*'));files=[p for p in files if p.is_file()];files.append(base/'hierarchical_spann_shard-memory-v4');assert all(not p.is_symlink() for p in files);assert sum(p.stat().st_size for p in files)<256*1024**2
manifest=base/'results/fineweb10m-read-immutable-metadata-manifest.json';manifest.write_text(json.dumps({str(p.relative_to(base)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files},indent=2));files.append(manifest)
out=base/'results/fineweb10m-read-immutable-metadata-v1.tar.gz'
with tarfile.open(out,'w:gz') as t:
 for p in sorted(files):t.add(p,arcname=str(p.relative_to(base)),recursive=False)
print(json.dumps(dict(files=len(files),archive_bytes=out.stat().st_size,archive_sha256=hashlib.sha256(out.read_bytes()).hexdigest())))
