"""Repack immutable accepted bytes with manifest-declared symlink types."""
from pathlib import Path
import tarfile,io,gzip,json,hashlib,copy,importlib.util,tempfile,os
here=Path(__file__).resolve().parent;base=here.parent
sha=lambda data:hashlib.sha256(data).hexdigest()
def read_archive(path):
 with tarfile.open(path)as t:
  members=t.getmembers();assert len({m.name for m in members})==len(members)
  return {m.name:(copy.copy(m),t.extractfile(m).read()if m.isfile()else None)for m in members}
def archive(path,entries):
 with path.open('xb')as raw,gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0)as zipped,tarfile.open(fileobj=zipped,mode='w',format=tarfile.PAX_FORMAT,dereference=False)as t:
  for name,(info,data)in entries.items():t.addfile(info,io.BytesIO(data)if data is not None else None)
def replace(entries,name,data):
 info=copy.copy(entries[name][0]);info.size=len(data);entries[name]=(info,data)
manifest_bytes=(base/'source-manifest.json').read_bytes();manifest=json.loads(manifest_bytes)
old=read_archive(base/'source.tar.gz');expected={'source/'+p for p in manifest['files']}|{'source/'+p for p in manifest['symlinks']}
assert set(old)==expected
for rel,desc in manifest['files'].items():
 info,data=old['source/'+rel];assert info.isfile()and len(data)==desc['bytes']and sha(data)==desc['sha256']
new=dict(old);type_changes=[]
for rel,target in manifest['symlinks'].items():
 name='source/'+rel;info,data=old[name];assert info.isdir()and data is None
 assert not any(n.startswith(name+'/')for n in old),'Dereferenced descendants must not remain'
 info=copy.copy(info);info.type=tarfile.SYMTYPE;info.linkname=target;info.size=0;info.mode=0o777;new[name]=(info,None);type_changes.append(name)
archive(here/'source.tar.gz',new);source_bytes=(here/'source.tar.gz').read_bytes();source_binding={'bytes':len(source_bytes),'sha256':sha(source_bytes)}
# Reuse the exact accepted extraction function, including filter=data, then run
# every saved file/symlink assertion, not a hand-picked source subset.
module_path=base/'binding-revision-v3/operator/extract_and_test.py';spec=importlib.util.spec_from_file_location('accepted_extract',module_path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix='owner32-archive-verify-')as tmp:
 root=Path(tmp);module.extract(here/'source.tar.gz',root,allow_symlink=True);source=root/'source'
 for rel,desc in manifest['files'].items():
  p=source/rel;assert not p.is_symlink()and p.stat().st_size==desc['bytes']and module.digest(p)==desc['sha256']
 for rel,target in manifest['symlinks'].items():
  p=source/rel;assert os.readlink(p)==target and p.resolve().is_relative_to(source.resolve())and p.resolve().is_dir()
 # Walk without following links; directories created by extraction are structural.
 actual=set()
 for directory,dirs,files in os.walk(source,followlinks=False):
  for name in dirs+files:
   p=Path(directory)/name
   if p.is_symlink()or p.is_file():actual.add(p.relative_to(source).as_posix())
 assert actual==set(manifest['files'])|set(manifest['symlinks'])
 # Verify target closure files are explicitly manifested regular files.
 targets={rel:len([p for p in manifest['files']if (source/p).resolve().is_relative_to((source/rel).resolve())])for rel in manifest['symlinks']}
 assert all(n>0 for n in targets.values())
# Effective accepted bundle = base runtime package plus accepted binding revision3.
effective=read_archive(base/'runtime-package.tar.gz');effective.update(read_archive(base/'binding-revision-v3/binding-delta.tar.gz'));before=dict(effective)
replace(effective,'source.tar.gz',source_bytes)
for name,key in [('HANDOFF.json','source_archive'),('PLAN-TEMPLATE.json','source_archive'),('BINDING.json','source_archive'),('PREPARATION.json','source')]:
 data=json.loads(effective[name][1]);data[key]=source_binding;replace(effective,name,(json.dumps(data,indent=2)+'\n').encode())
name='operator/extract_and_test.py';data=effective[name][1];old_sha=sha((base/'source.tar.gz').read_bytes()).encode();assert data.count(old_sha)==1;replace(effective,name,data.replace(old_sha,source_binding['sha256'].encode()))
archive(here/'runtime-package.tar.gz',effective)
# Extract entire repaired bundle, then execute the same extractor on its source.
with tempfile.TemporaryDirectory(prefix='owner32-bundle-verify-')as tmp:
 root=Path(tmp);module.extract(here/'runtime-package.tar.gz',root)
 assert (root/'source-manifest.json').read_bytes()==manifest_bytes
 module.extract(root/'source.tar.gz',root,allow_symlink=True)
 for rel,desc in manifest['files'].items():
  p=root/'source'/rel;assert not p.is_symlink()and p.stat().st_size==desc['bytes']and module.digest(p)==desc['sha256']
 for rel,target in manifest['symlinks'].items():assert os.readlink(root/'source'/rel)==target
 assert source_binding['sha256'].encode() in (root/'operator/extract_and_test.py').read_bytes()
 assert json.loads((root/'PLAN-TEMPLATE.json').read_text())['source_archive']==source_binding
changed=[{'member':name,'before_sha256':sha(before[name][1]),'after_sha256':sha(data)}for name,(info,data)in effective.items()if data!=before[name][1]]
package=(here/'runtime-package.tar.gz').read_bytes();receipt={'status':'passed','source_archive':source_binding,'runtime_package':{'bytes':len(package),'sha256':sha(package)},'source_manifest':{'bytes':len(manifest_bytes),'sha256':sha(manifest_bytes)},'regular_files_verified':len(manifest['files']),'symlinks_verified':manifest['symlinks'],'target_closure_regular_file_counts':targets,'source_changed_members':type_changes,'source_regular_file_bytes_unchanged':True,'extra_missing_duplicate_members':False,'effective_bundle_changed_members':changed,'extraction':'Exact accepted binding-revision-v3 extract() with tar filter=data; all957 file lengths/SHA and all symlink targets verified twice after fresh extraction; source inventory rejects extra/missing members. No build/cloud/engine edits.'}
(here/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
