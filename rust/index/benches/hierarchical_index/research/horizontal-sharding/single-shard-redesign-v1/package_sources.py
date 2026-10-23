"""Freeze source as regular files; validate and materialize internal source links."""
import argparse
import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('source', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
source = args.source.resolve()
if args.output.exists():
    raise SystemExit('Refusing to replace an existing source archive')
tracked = set(filter(None, subprocess.check_output(['git', '-C', str(source), 'ls-files', '-z']).decode().split('\0')))
additions = {str(p.relative_to(source)) for p in (source/'rust/index/benches/hierarchical_index').rglob('*.rs')}
additions.add('rust/index/benches/single_shard_matched.rs')
files = {}
links = {}
def visit(name, ancestors=frozenset()):
    p = source / name
    resolved = p.resolve()
    if not resolved.is_relative_to(source):
        raise SystemExit(f'External source link: {name}')
    if p.is_symlink():
        links[name] = str(resolved.relative_to(source))
    if resolved.is_dir():
        if resolved in ancestors:
            raise SystemExit(f'Cyclic source directory: {name}')
        for child in sorted(resolved.iterdir()):
            visit(str(Path(name)/child.name), ancestors | {resolved})
    elif resolved.is_file():
        actual = str(resolved.relative_to(source))
        if actual not in tracked and actual not in additions:
            raise SystemExit(f'Unreviewed file under source link: {name}')
        files[name] = resolved
    else:
        raise SystemExit(f'Missing or unexpected source file: {name}')
for name in sorted(tracked | additions):
    visit(name)
manifest = []
with tarfile.open(args.output, 'w:gz') as archive:
    for name, p in sorted(files.items()):
        content = p.read_bytes()
        item = tarfile.TarInfo(name)
        item.size = len(content)
        item.mode = p.stat().st_mode & 0o777
        item.mtime = 0
        archive.addfile(item, io.BytesIO(content))
        manifest.append([name, hashlib.sha256(content).hexdigest()])
result = {'source':str(source), 'base_commit':subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD']).decode().strip(),
    'archive':str(args.output.resolve()), 'archive_bytes':args.output.stat().st_size,
    'archive_sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),
    'materialized_internal_links':links, 'files':manifest,
    'source_manifest_sha256':hashlib.sha256(json.dumps(manifest,separators=(',', ':')).encode()).hexdigest()}
args.output.with_suffix(args.output.suffix + '.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='files'},indent=2))
