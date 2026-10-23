"""Make a future-build-only revision from the accepted immutable bundle."""
from pathlib import Path
import tarfile,copy,io,gzip,json,hashlib,tempfile,os,importlib.util,subprocess,sys
here=Path(__file__).resolve().parent;base=here.parent/'build-parallelism-v5';sha=lambda b:hashlib.sha256(b).hexdigest()
with tarfile.open(base/'runtime-package.tar.gz')as t:
 members=t.getmembers();assert len({v.name for v in members})==len(members)
 entries={v.name:(copy.copy(v),t.extractfile(v).read())for v in members}
old=dict(entries)
name='operator/remote_stages.py';info,data=entries[name];old_target=b"TARGET = '/mnt/spann-owner32-scratch/retained-target-copy'";new_target=b"TARGET = '/home/ubuntu/spann-owner32-build-v6/target'";assert data.count(old_target)==1;data=data.replace(old_target,new_target);info=copy.copy(info);info.size=len(data);entries[name]=(info,data)
name='BINDING.json';info,data=entries[name];binding=json.loads(data);binding['target']='/home/ubuntu/spann-owner32-build-v6/target';binding['operational_paths'].update(build_base='/home/ubuntu/spann-owner32-build-v6',control='/home/ubuntu/spann-owner32-build-v6/<fresh-epoch>',source='/home/ubuntu/spann-owner32-build-v6/<fresh-epoch>/source',target='/home/ubuntu/spann-owner32-build-v6/target',TMPDIR='/home/ubuntu/spann-owner32-build-v6/<fresh-epoch>/tmp',scope='Build target, fresh control/source and temporary files are on authorized durable EBS. Actual epoch/control/source/TMPDIR remain operator-bound and verified. Science output/data remain on verified scratch; retained EBS input is unchanged and read-only. Source-filesystem build free floor remains20GiB; no admission is inferred.');data=(json.dumps(binding,indent=2)+'\n').encode();info=copy.copy(info);info.size=len(data);entries[name]=(info,data)
# Preserve all metadata/member bytes except the two explicit operator changes.
with (here/'runtime-package.tar.gz').open('xb')as raw,gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0)as gz,tarfile.open(fileobj=gz,mode='w',format=tarfile.PAX_FORMAT,dereference=False)as t:
 for name,(info,data)in entries.items():t.addfile(info,io.BytesIO(data))
for name in ['operator/remote_stages.py','BINDING.json']:
 p=here/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(entries[name][1])
extract_bytes=entries['operator/extract_and_test.py'][1];module_path=here/'extractor-for-validation.py';module_path.write_bytes(extract_bytes);spec=importlib.util.spec_from_file_location('accepted_extractor',module_path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix='owner32-build32-verify-')as tmp:
 root=Path(tmp);module.extract(here/'runtime-package.tar.gz',root)
 manifest=json.loads((root/'source-manifest.json').read_text());source_tar=(root/'source.tar.gz').read_bytes();assert sha(source_tar)=='bdee8c4ad3ff8ebe683ca02d16b9a57a9f38330eb6441de563802f03918fcfe1'
 assert sha((root/'source-manifest.json').read_bytes())=='edf371fcab03dcbb879e99fe622e7ec8ac323523605c68756dfbdbd7a94bacd2'
 with tarfile.open(root/'source.tar.gz')as t:
  names=t.getnames();expected={'source/'+p for p in manifest['files']}|{'source/'+p for p in manifest['symlinks']};assert len(names)==len(set(names))and set(names)==expected
 module.extract(root/'source.tar.gz',root,allow_symlink=True);source=root/'source'
 for rel,d in manifest['files'].items():
  p=source/rel;assert not p.is_symlink()and p.stat().st_size==d['bytes']and module.digest(p)==d['sha256']
 for rel,target in manifest['symlinks'].items():assert os.readlink(source/rel)==target and(source/rel).resolve().is_relative_to(source.resolve())and(source/rel).resolve().is_dir()
 actual=set()
 for d,dirs,files in os.walk(source,followlinks=False):
  for n in dirs+files:
   p=Path(d)/n
   if p.is_symlink()or p.is_file():actual.add(p.relative_to(source).as_posix())
 assert actual==set(manifest['files'])|set(manifest['symlinks'])
 plan=json.loads((root/'PLAN-TEMPLATE.json').read_text());specification=json.loads((root/'SPEC-TEMPLATE.json').read_text());assert plan['source_archive']['sha256']==sha(source_tar)and plan['source_archive']['bytes']==len(source_tar);assert specification['threads']==32 and specification['owner_slots']==1
 assert sha(source_tar).encode()in extract_bytes
 with(here/'tests.stdout').open('wb')as stdout,(here/'tests.stderr').open('wb')as stderr:
  result=subprocess.run([sys.executable,'-m','unittest','-v','test_build_profile'],cwd=root/'operator',stdout=stdout,stderr=stderr,timeout=30)
 assert result.returncode==0
 assert b"'-j32'" in entries['operator/remote_stages.py'][1]
 sys.path.insert(0,str(root/'operator'))
 from remote_stages import TARGET,environment
 assert TARGET=='/home/ubuntu/spann-owner32-build-v6/target'and environment()['CARGO_TARGET_DIR']==TARGET
 assert json.loads((root/'BINDING.json').read_text())['operational_paths']['input']=='/home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32'
 assert plan['limits']['root_build_free_floor_bytes']==21474836480
 for name in ['PLAN-TEMPLATE.json','SPEC-TEMPLATE.json','supervise.py','source.tar.gz','source-manifest.json']:
  assert entries[name][1]==old[name][1]
package=(here/'runtime-package.tar.gz').read_bytes();receipt={'status':'passed','base_runtime_package':{'bytes':(base/'runtime-package.tar.gz').stat().st_size,'sha256':sha((base/'runtime-package.tar.gz').read_bytes())},'runtime_package':{'bytes':len(package),'sha256':sha(package)},'source_archive_sha256':sha(entries['source.tar.gz'][1]),'source_manifest_sha256':sha(entries['source-manifest.json'][1]),'verified_regular_files':len(manifest['files']),'verified_symlinks':manifest['symlinks'],'missing_extra_duplicate_source_members':False,'changed_members':[{'path':n,'before_sha256':sha(old[n][1]),'after_sha256':sha(v[1])}for n,v in entries.items()if v[1]!=old[n][1]],'scope':'Future target changes from ephemeral scratch to authorized durable EBS build base; BINDING describes operator-bound fresh control/source/TMPDIR. Jobs32/release/opt3/debug0/offline/locked unchanged. Science scratch/input/limits/engine unchanged. No Cargo/cloud invocation or active build change.'}
(here/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
