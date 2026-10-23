"""Overlay only accepted frozen benchmark bytes; preserve archive member types."""
from pathlib import Path
import hashlib,json,tarfile,copy,io,gzip
HERE=Path(__file__).resolve().parent
BASE=HERE.parent/'single-owner-local-scrub-runtime-v1/runtime-package.tar.gz'
sha=lambda b:hashlib.sha256(b).hexdigest()
with tarfile.open(BASE) as t:
 entries={m.name:(copy.copy(m),t.extractfile(m).read())for m in t.getmembers()}
with tarfile.open(fileobj=io.BytesIO(entries['source.tar.gz'][1]))as t:
 sources={m.name:(copy.copy(m),t.extractfile(m).read()if m.isfile()else None)for m in t.getmembers()}
h=json.loads((HERE.parent/'decoded-vector-cache-v1/HANDOFF.json').read_text())
replacements={r['path']:Path(r['frozen_path']).read_bytes()for r in h['sources']}
for row in h['sources']:
 data=replacements[row['path']];assert len(data)==row['bytes']and sha(data)==row['sha256']and sha(sources['source/'+row['path']][1])==row['before_sha256']
manifest=json.loads(entries['source-manifest.json'][1]);before=copy.deepcopy(manifest)
for name,data in replacements.items():
 archive_name = 'source/' + name
 assert archive_name in sources and sources[archive_name][0].isfile()
 info=copy.copy(sources[archive_name][0]);info.size=len(data);sources[archive_name]=(info,data);manifest['files'][name]={'bytes':len(data),'sha256':sha(data)}
assert len(manifest['files'])==957 and manifest['symlinks']=={'rust/types/idl':'../../idl'}
def pack(items):
 buf=io.BytesIO()
 with tarfile.open(fileobj=buf,mode='w')as t:
  for name,(m,data)in items.items():t.addfile(m,io.BytesIO(data)if data is not None else None)
 return gzip.compress(buf.getvalue(),mtime=0)
def put(name,data):
 info=copy.copy(entries[name][0])if name in entries else tarfile.TarInfo(name);info.size=len(data);info.mode=0o644;entries[name]=(info,data)
source=pack(sources);new_manifest=(json.dumps(manifest,indent=2)+'\n').encode();oldsha=sha(entries['source.tar.gz'][1]);oldbytes=len(entries['source.tar.gz'][1]);oldmanifest=sha(entries['source-manifest.json'][1]);newsha=sha(source);newmh=sha(new_manifest)
put('source.tar.gz',source);put('source-manifest.json',new_manifest)
# Update pinned source descriptors and extractor literal, without changing code semantics.
for name,(info,data)in list(entries.items()):
 if name.endswith('.json'):
  try:v=json.loads(data)
  except Exception:continue
  def change(x):
   if isinstance(x,dict):
    for k in x:
     if x[k]==oldsha:x[k]=newsha
     elif x[k]==oldmanifest:x[k]=newmh
     elif k=='bytes'and x[k]==oldbytes:x[k]=len(source)
     else:change(x[k])
   elif isinstance(x,list):
    for y in x:change(y)
  change(v);put(name,(json.dumps(v,indent=2)+'\n').encode())
 elif name=='operator/extract_and_test.py':put(name,data.replace(oldsha.encode(),newsha.encode()).replace(oldmanifest.encode(),newmh.encode()).replace(str(oldbytes).encode(),str(len(source)).encode()))
spec=json.loads(entries['SPEC-TEMPLATE.json'][1]);spec['full_records']['queue_cache_pages_per_home']=4;spec['immutable_handle_batching']=False;spec['owner_eligibility_prefix']=False;spec['single_owner_local_scrub']=True;spec.pop('decoded_vector_cache',None);put('SPEC-TEMPLATE.json',(json.dumps(spec,indent=2)+'\n').encode())
plan=json.loads(entries['PLAN-TEMPLATE.json'][1]);plan['limits']['fresh_window_seconds']=1800;plan['limits']['compute_usd']=2.016;put('PLAN-TEMPLATE.json',(json.dumps(plan,indent=2)+'\n').encode())
cache_baseline=json.loads(entries['operator/cache-baseline-source-manifest.json'][1])
config={'source_manifest_sha256':newmh,'source_archive_sha256':newsha,'changed_source_files':sorted(n for n,row in manifest['files'].items()if row!=cache_baseline['files'][n]),'old_binary_sha256':['afb126d7e5933d86e33b29ee3f50e3c687cea0dcd1b50c5e1d74f035ab1e4ea6','dc167fb08fb28ccee10af5e2647e295cc88842908aa19c5f7d78dd8237a8fb78','07761a4a9e44f6da2ffcaa19e529c231ce8366b1f2fd3e0b3b3a6eaad3fe7150','85f3e4d62c8cf084f60d42ac21da0a8b9bc289214e81dd0c3fdd4d3a77366740']}
put('operator/decoded-source.json',(json.dumps(config,indent=2)+'\n').encode())
(HERE/'operator/decoded-source.json').write_text(json.dumps(config,indent=2)+'\n')
for name in ['run_pair.py','test_pair_build_gate.py']:
 put('operator/'+name,(HERE/'operator'/name).read_bytes())
(HERE/'source.tar.gz').write_bytes(source);(HERE/'source-manifest.json').write_bytes(new_manifest);(HERE/'runtime-package.tar.gz').write_bytes(pack(entries));(HERE/'source-delta.json').write_text(json.dumps({'from_manifest_sha256':oldmanifest,'to_manifest_sha256':newmh,'changed_files':{n:manifest['files'][n]for n in replacements},'source_archive_sha256':newsha,'source_archive_bytes':len(source)},indent=2)+'\n')
print(json.dumps({'package_sha256':sha((HERE/'runtime-package.tar.gz').read_bytes()),'package_bytes':(HERE/'runtime-package.tar.gz').stat().st_size,'source_sha256':newsha,'manifest_sha256':newmh}))
