from pathlib import Path
import tarfile,io,gzip,copy,hashlib,json,importlib.util,tempfile,os,difflib
here=Path(__file__).resolve().parent;base=here.parents[1]/'immutable-handle-batching-runtime-v1/runtime-package.tar.gz';sha=lambda b:hashlib.sha256(b).hexdigest()
with tarfile.open(base)as t:entries={m.name:(copy.copy(m),t.extractfile(m).read())for m in t.getmembers()}
original=dict(entries);patch=[]
for name in ['operator/remote_stages.py','operator/test_build_profile.py','operator/build_validity.py','operator/test_build_validity.py','operator/cache-baseline-source-manifest.json']:
 data=(here/name).read_bytes()
 if name in entries:info=copy.copy(entries[name][0]);patch+=list(difflib.unified_diff(entries[name][1].decode().splitlines(True),data.decode().splitlines(True),fromfile='accepted/'+name,tofile='v7/'+name))
 else:info=tarfile.TarInfo(name);info.mode=0o644;info.mtime=0
 info.size=len(data);entries[name]=(info,data)
name='operator/extract_and_test.py';info=copy.copy(entries[name][0]);data=entries[name][1];assert b"'test_process_lifecycle']"in data;changed=data.replace(b"'test_process_lifecycle']",b"'test_process_lifecycle', 'test_build_profile', 'test_build_validity']");patch+=list(difflib.unified_diff(data.decode().splitlines(True),changed.decode().splitlines(True),fromfile='accepted/'+name,tofile='v7/'+name));info.size=len(changed);entries[name]=(info,changed)
(here/'change.patch').write_text(''.join(patch))
with (here/'runtime-package.tar.gz').open('xb')as raw,gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0)as gz,tarfile.open(fileobj=gz,mode='w',format=tarfile.PAX_FORMAT,dereference=False)as t:
 for name,(info,data)in entries.items():t.addfile(info,io.BytesIO(data))
(here/'extractor-for-validation.py').write_bytes(entries[name][1]if False else entries['operator/extract_and_test.py'][1]);ms=importlib.util.spec_from_file_location('extractor',here/'extractor-for-validation.py');module=importlib.util.module_from_spec(ms);ms.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix='owner32-build-validity-package-')as tmp:
 root=Path(tmp);module.extract(here/'runtime-package.tar.gz',root);module.extract(root/'source.tar.gz',root,allow_symlink=True);manifest=json.loads((root/'source-manifest.json').read_text());baseline=json.loads((root/'operator/cache-baseline-source-manifest.json').read_text())
 m=importlib.util.spec_from_file_location('validity',root/'operator/build_validity.py');validity=importlib.util.module_from_spec(m);m.loader.exec_module(validity)
 refreshed=validity.refresh(root/'source',manifest,baseline)
 assert refreshed['changed_source_files']==['rust/index/benches/hierarchical_index/writer/reference_owner_adapter.rs']
 for name,row in manifest['files'].items():
  p=root/'source'/name;assert not p.is_symlink()and p.stat().st_size==row['bytes']and module.digest(p)==row['sha256']
 for name,target in manifest['symlinks'].items():assert os.readlink(root/'source'/name)==target
 with tarfile.open(root/'source.tar.gz')as t:assert set(t.getnames())=={'source/'+n for n in manifest['files']}|{'source/'+n for n in manifest['symlinks']}
 (here/'full-source-refresh-validation.json').write_text(json.dumps(refreshed,indent=2)+'\n')
package=(here/'runtime-package.tar.gz').read_bytes();r={'status':'passed','package':{'bytes':len(package),'sha256':sha(package)},'base_package_sha256':sha(base.read_bytes()),'source_archive_sha256':sha(entries['source.tar.gz'][1]),'source_manifest_sha256':sha(entries['source-manifest.json'][1]),'files_verified':len(manifest['files']),'symlinks_verified':manifest['symlinks'],'changed_or_added_members':[{'path':name,'before_sha256':sha(original[name][1])if name in original else None,'after_sha256':sha(data)}for name,(info,data)in entries.items()if name not in original or data!=original[name][1]],'scope':'Source bytes unchanged. Only validated benchmark inputs+consumer timestamp refresh; mandatory fresh=false artifact. No target deletion/GC, Cargo workspace build or cloud action.'};(here/'package-validation.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
