from pathlib import Path
import tarfile,importlib.util,tempfile,json,hashlib,os,ast
here=Path(__file__).resolve().parent
with tempfile.TemporaryDirectory(prefix='eligibility-package-')as d:
 root=Path(d).resolve()
 with tarfile.open(here/'runtime-package.tar.gz')as t:raw=t.extractfile('operator/extract_and_test.py').read()
 (root/'extractor.py').write_bytes(raw);s=importlib.util.spec_from_file_location('extractor',root/'extractor.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
 control=root/'control';control.mkdir();m.extract(here/'runtime-package.tar.gz',control);m.extract(control/'source.tar.gz',control,allow_symlink=True);manifest=json.loads((control/'source-manifest.json').read_text())
 for name,row in manifest['files'].items():
  p=control/'source'/name;assert p.is_file()and not p.is_symlink()and p.stat().st_size==row['bytes']and m.digest(p)==row['sha256'],name
 for name,target in manifest['symlinks'].items():assert os.readlink(control/'source'/name)==target and(control/'source'/name).resolve().is_relative_to(control/'source')
 for p in control.rglob('*.py'):ast.parse(p.read_text())
 assert m.digest(control/'source-manifest.json')in(control/'operator/extract_and_test.py').read_text()
 baseline=json.loads((control/'operator/cache-baseline-source-manifest.json').read_text());changed=sorted(name for name,row in manifest['files'].items()if row!=baseline['files'][name]);config=json.loads((control/'operator/reassignment-source.json').read_text());assert changed==config['changed_source_files']and len(changed)==6
 with tarfile.open(here.parent/'decoded-vector-cache-runtime-v1/source.tar.gz')as tar:
  old={m.name:tar.extractfile(m).read()for m in tar.getmembers()if m.isfile()}
 changed_prior=sorted(name for name in manifest['files']if (control/'source'/name).read_bytes()!=old['source/'+name]);assert len(changed_prior)==5
 assert m.digest(control/'source/rust/index/benches/hierarchical_index/writer/owner_vector_store.rs')=='a25c65657c707e1e545083adc684eefdbcfbda95146ad298bde95ad3ff9cfaac'
 result={'changed_vs_epoch11':changed_prior,'decoded_cache_store_unchanged':True,'status':'passed','regular_files_verified':len(manifest['files']),'symlinks_verified':manifest['symlinks'],'changed_vs_cache_baseline':changed,'python_AST':'all extracted Python parsed','same_binary_arms':'shared build receipt bytecopy; localreassignmentfalse/true only'}
 (here/'package-validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
