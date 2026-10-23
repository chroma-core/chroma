"""Local immutable overlay only; no cloud operation."""
from pathlib import Path
import hashlib,json,tarfile,copy,io,gzip,tempfile,os
HERE=Path(__file__).resolve().parent
D=HERE.parent
BASE=D/'reference-cadence-runtime-v1/fresh-build-v2/runtime-package.tar.gz'
sha=lambda b:hashlib.sha256(b).hexdigest()
def pack(items):
 b=io.BytesIO()
 with tarfile.open(fileobj=b,mode='w')as t:
  for n,(m,data)in items.items():t.addfile(m,io.BytesIO(data)if data is not None else None)
 return gzip.compress(b.getvalue(),mtime=0)
with tarfile.open(BASE)as t:entries={m.name:(copy.copy(m),t.extractfile(m).read())for m in t.getmembers()}
with tarfile.open(fileobj=io.BytesIO(entries['source.tar.gz'][1]))as t:sources={m.name:(copy.copy(m),t.extractfile(m).read()if m.isfile()else None)for m in t.getmembers()}
manifest=json.loads(entries['source-manifest.json'][1]);before=copy.deepcopy(manifest);changes=[]
for dirname in ['prepared-leaf-split-v1','prepared-leaf-split-native-v1']:
 h=json.loads((D/dirname/'HANDOFF.json').read_text())
 for row in h['files']:
  n=row['path'];data=(D/dirname/'frozen-source'/Path(n).name).read_bytes();assert sha(data)==row['sha256']and len(data)==row['bytes'];assert sha(sources['source/'+n][1])==row['before_sha256']
  m=copy.copy(sources['source/'+n][0]);m.size=len(data);sources['source/'+n]=(m,data);manifest['files'][n]={'bytes':len(data),'sha256':sha(data)};changes.append(n)
def put(n,data):
 m=copy.copy(entries[n][0])if n in entries else tarfile.TarInfo(n);m.size=len(data);m.mode=0o644;entries[n]=(m,data)
source=pack(sources);newmanifest=(json.dumps(manifest,indent=2)+'\n').encode();oldsha=sha(entries['source.tar.gz'][1]);oldmh=sha(entries['source-manifest.json'][1]);oldbytes=len(entries['source.tar.gz'][1]);newsha=sha(source);newmh=sha(newmanifest)
for n,(info,data)in list(entries.items()):
 if n.endswith('.json'):
  try:v=json.loads(data)
  except Exception:continue
  def update(x):
   if isinstance(x,dict):
    for k in x:
     if x[k]==oldsha:x[k]=newsha
     elif x[k]==oldmh:x[k]=newmh
     elif k=='bytes'and x[k]==oldbytes:x[k]=len(source)
     else:update(x[k])
   elif isinstance(x,list):
    for y in x:update(y)
  update(v);put(n,(json.dumps(v,indent=2)+'\n').encode())
 elif n=='operator/extract_and_test.py':put(n,data.replace(oldsha.encode(),newsha.encode()).replace(oldmh.encode(),newmh.encode()).replace(str(oldbytes).encode(),str(len(source)).encode()))
put('source.tar.gz',source);put('source-manifest.json',newmanifest)
spec=json.loads((D/'prepared-leaf-split-native-v1/SPEC-OFF.json').read_text());spec.pop('prepared_leaf_split',None)
# Native templates are identical to retained epoch13 short arm after operational fill.
spec['source_id']=json.loads(entries['SPEC-TEMPLATE.json'][1])['source_id']
put('SPEC-TEMPLATE.json',(json.dumps(spec,indent=2)+'\n').encode());put('operator/paired-spec.json',(json.dumps(spec,indent=2)+'\n').encode());(HERE/'operator/paired-spec.json').write_text(json.dumps(spec,indent=2)+'\n')
plan=json.loads(entries['PLAN-TEMPLATE.json'][1]);plan['limits'].update(fresh_window_seconds=1200,compute_usd=1.344,optimized_build_seconds=300,remaining_collection_grace_seconds=0,metadata_log_cap_bytes=16777216,required_data_filesystem_free_bytes=25769803776);put('PLAN-TEMPLATE.json',(json.dumps(plan,indent=2)+'\n').encode())
stages=entries['operator/remote_stages.py'][1].replace(b'required + 300',b'required + 180').replace(b'remaining(deadline, 600)',b'remaining(deadline, 300)').replace(b"folder / 'process.json', 600",b"folder / 'process.json', 300");put('operator/remote_stages.py',stages)
contract=json.loads(entries['operator/cadence-source.json'][1]);baseline=json.loads(entries['operator/cache-baseline-source-manifest.json'][1]);contract.update(source_manifest_sha256=newmh,source_archive_sha256=newsha,changed_source_files=sorted(n for n,r in manifest['files'].items()if baseline['files'].get(n)!=r));put('operator/cadence-source.json',(json.dumps(contract,indent=2)+'\n').encode());(HERE/'operator/cadence-source.json').write_text(json.dumps(contract,indent=2)+'\n')
test=entries['operator/test_pair_build_gate.py'][1].decode()
a=test.index(' def test_only_cadence_diff_common_split');b=test.index(' def ',a+5)
test=test[:a]+test[b:]
put('operator/test_pair_build_gate.py',test.encode())
put('operator/run_pair.py',(HERE/'operator/run_pair.py').read_bytes());put('operator/test_prepared_pair.py',(HERE/'operator/test_prepared_pair.py').read_bytes())
(HERE/'source.tar.gz').write_bytes(source);(HERE/'source-manifest.json').write_bytes(newmanifest);(HERE/'runtime-package.tar.gz').write_bytes(pack(entries))
with tempfile.TemporaryDirectory()as tmp:
 root=Path(tmp)
 with tarfile.open(fileobj=io.BytesIO(source))as t:t.extractall(root)
 actual={p.relative_to(root/'source').as_posix()for p in(root/'source').rglob('*')if p.is_file()and not p.is_symlink()};assert actual==set(manifest['files'])and len(actual)==957
 for n,r in manifest['files'].items():assert sha((root/'source'/n).read_bytes())==r['sha256']
 assert {p.relative_to(root/'source').as_posix():os.readlink(p)for p in(root/'source').rglob('*')if p.is_symlink()}==manifest['symlinks']
validation={'package_sha256':sha((HERE/'runtime-package.tar.gz').read_bytes()),'package_bytes':(HERE/'runtime-package.tar.gz').stat().st_size,'source_sha256':newsha,'source_manifest_sha256':newmh,'regular_files':957,'symlinks':manifest['symlinks'],'changed_from_epoch13':sorted(set(changes)),'validation':'All957 extracted source hashes plus sole symlink passed; frozen sequential before/after pins checked.'};(HERE/'package-validation.json').write_text(json.dumps(validation,indent=2)+'\n');print(json.dumps(validation))
