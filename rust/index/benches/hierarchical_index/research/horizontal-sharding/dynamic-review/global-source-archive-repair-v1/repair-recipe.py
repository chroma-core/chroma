from pathlib import Path,PurePosixPath
import os,json,tarfile,hashlib,copy,re,stat,tomllib
R=Path('/private/tmp/chroma-hierarchical-spann-sharding');W=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding');A=W/'dynamic-review/global-source-archive-repair-v1';A.mkdir();BASE=W/'dynamic-review/dynamic-global-native-construction-v1/repair-v3';H=lambda b:hashlib.sha256(b).hexdigest();baseline=json.loads((BASE/'manifest.json').read_text());pins=baseline['files'];assert len(pins)==934;assert all(H((R/p).read_bytes())==s for p,s in pins.items())
failed_bundle=W/'fleet/global-runner-integration-v1/upload-bundle-v1.tar.gz';failure=W/'fleet/global-runner-integration-v1/linux-build-attempt1/process';
with tarfile.open(failed_bundle) as t:
 original_remote=json.load(t.extractfile('source-artifacts.json'));source_entries={m.name[7:]:m for m in t.getmembers() if m.name.startswith('source/') and m.isfile()};assert set(source_entries)==set(pins)
 for p,m in source_entries.items():assert H(t.extractfile(m).read())==pins[p]
assert (R/'rust/types/idl').is_symlink();target=os.readlink(R/'rust/types/idl');assert target=='../../idl'
link={'path':'rust/types/idl','target':target,'target_text_sha256':H(target.encode()),'resolved_relative_target':'idl','kind':'relative-directory-symlink'}
# Preserve all original regular-file tar entries and bytes; add only the omitted link.
with tarfile.open(BASE/'source.tar.gz') as old,tarfile.open(A/'source.tar.gz','w:gz',format=tarfile.PAX_FORMAT,dereference=False) as new:
 members=old.getmembers();assert len(members)==934 and all(m.isfile() for m in members)
 for m in members:new.addfile(copy.copy(m),old.extractfile(m))
 m=tarfile.TarInfo(link['path']);m.type=tarfile.SYMTYPE;m.linkname=target;m.mode=0o777;m.mtime=0;new.addfile(m)
# Extraction validates links against the destination and never follows an external target.
D=A/'extracted-layout';D.mkdir()
with tarfile.open(A/'source.tar.gz') as t:
 names=t.getnames();assert len(names)==len(set(names))==935
 for m in t.getmembers():
  p=PurePosixPath(m.name);assert not p.is_absolute() and '..' not in p.parts
  if m.issym():assert m.name==link['path'] and m.linkname==target
  else:assert m.isfile() and m.name in pins
 t.extractall(D,filter='data')
assert os.readlink(D/link['path'])==target;assert (D/link['path']).resolve()==(D/'idl').resolve();assert (D/link['path']).resolve().is_relative_to(D.resolve())
extracted={p:H((D/p).read_bytes()) for p in pins};assert extracted==pins
# Conservative Cargo path dependency closure: index regular/build/dev; normal/build
# transitive deps including optional and target-specific declarations, without executing Cargo.
workspace=tomllib.loads((D/'Cargo.toml').read_text())['workspace'];ws=workspace['dependencies'];todo=['rust/index'];seen=set();edges=[]
while todo:
 p=todo.pop()
 if p in seen:continue
 seen.add(p);m=tomllib.loads((D/p/'Cargo.toml').read_text());tables=[m.get('dependencies',{}),m.get('build-dependencies',{})]
 if p=='rust/index':tables.append(m.get('dev-dependencies',{}))
 for v in m.get('target',{}).values():tables.extend([v.get('dependencies',{}),v.get('build-dependencies',{})])
 for table in tables:
  for name,d in table.items():
   if not isinstance(d,dict):continue
   shared=d.get('workspace',False);effective=ws.get(name,{}) if shared else d
   if isinstance(effective,dict) and 'path' in effective:
    q=((D if shared else D/p)/effective['path']).resolve();assert q.is_relative_to(D.resolve());rel=str(q.relative_to(D));assert (q/'Cargo.toml').is_file();todo.append(rel);edges.append({'from':p,'dependency':name,'to':rel})
# All source files and active custom build scripts in the conservative local closure.
sources=[];builds=[];literal_includes=[];embed_folders=[]
for p in sorted(seen):
 for x in (R/p/'src').rglob('*.rs'):
  n=str(x.relative_to(R));assert n in pins;sources.append(n)
  text=x.read_text()
  for raw in re.findall(r'include(?:_str|_bytes)?!\(\s*"([^"]+)"',text):
   q=(x.parent/raw).resolve();nq=str(q.relative_to(R));assert nq in pins;assert H((D/nq).read_bytes())==H(q.read_bytes());literal_includes.append({'source':n,'literal':raw,'resolved':nq,'sha256':pins[nq]})
  for raw in re.findall(r'#\[folder\s*=\s*"([^"]+)"',text):
   folder=(R/p/raw).resolve();files=[str(q.relative_to(R)) for q in sorted(folder.rglob('*')) if q.is_file()];assert all(q in pins for q in files);embed_folders.append({'source':n,'folder':str(folder.relative_to(R)),'files':files})
 x=R/p/'build.rs'
 if x.exists():assert str(x.relative_to(R)) in pins;builds.append(str(x.relative_to(R)))
# Resolve every literal types proto input via the repaired link, and local imports.
build=(D/'rust/types/build.rs').read_text();proto_paths=re.findall(r'"(idl/chromadb/proto/[^\"]+\.proto)"',build);protos=[];external=set()
for raw in proto_paths:
 resolved=D/'rust/types'/raw;rel=str(resolved.resolve().relative_to(D.resolve()));assert rel in pins;assert H(resolved.read_bytes())==pins[rel]
 imports=re.findall(r'^import\s+"([^"]+)";',resolved.read_text(),re.M)
 for name in imports:
  if name.startswith('google/protobuf/'):external.add(name)
  else:assert (D/'rust/types/idl'/name).is_file();assert str((D/'rust/types/idl'/name).resolve().relative_to(D.resolve())) in pins
 protos.append({'build_input':str(PurePosixPath('rust/types')/raw),'resolved':rel,'sha256':pins[rel],'imports':imports})
assert len(protos)==8
idx=json.loads((W/'dynamic-review/dynamic-global-default-authority-v1/index-before.json').read_text());assert H(Path(idx['path']).read_bytes())==idx['sha256']
failhash={p.name:H(p.read_bytes()) for p in failure.iterdir() if p.is_file()}
closure={'entry':'cargo build --release --locked --offline -j2 -p chroma-index --bench hierarchical_spann_shard','method':'static conservative path dependency traversal; no Cargo/build invocation','local_crates':sorted(seen),'edges':edges,'local_src_count':len(sources),'local_src_files':sources,'custom_build_scripts':builds,'literal_includes':literal_includes,'embedded_folders':embed_folders,'types_proto_inputs':protos,'external_protoc_include_requirements':sorted(external),'repository_symlinks':{'required':[link],'excluded_nonbuild':[{'path':'docs/mintlify/CLAUDE.md','target':'AGENTS.md'}]},'limits':['No Linux build or compiler executed','Rust cfg/feature-specific standard library and registry/git dependency native build assets are external and not bundled','External protobuf well-known definitions belong to Linux protoc installation; their bytes are not validated here','Conservative local closure is target-specific to this index bench, not a universal workspace build package','Source/archive extraction and static literal includes cannot prove every arbitrary dynamic path computed by a future build script','Cargo lock pins external dependency identities but their cached source completeness/toolchain/headers/linker remain next actual Linux build prerequisites']}
(A/'closure-audit.json').write_text(json.dumps(closure,indent=2)+'\n')
manifest={'regular_files':pins,'regular_file_count':934,'symlinks':[link],'archive_entries':935,'archive_sha256':H((A/'source.tar.gz').read_bytes()),'archive_bytes':(A/'source.tar.gz').stat().st_size,'baseline_archive':str(BASE/'source.tar.gz'),'baseline_archive_sha256':H((BASE/'source.tar.gz').read_bytes()),'original_failed_bundle':str(failed_bundle),'original_failed_bundle_sha256':H(failed_bundle.read_bytes()),'source_byte_changes':[],'layout_delta':{'added':[link],'removed':[],'changed_regular_files':[]},'failed_process_evidence_sha256':failhash,'git_index_sha256':idx['sha256']}
(A/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');(A/'extracted-validation.json').write_text(json.dumps({'status':'PASS layout/static only','extracted_root':str(D),'regular_files':extracted,'symlinks':[link],'proto_inputs_verified':len(protos),'local_crates_verified':len(seen),'embed_sql_files':sum(len(x['files'])for x in embed_folders),'index_unchanged':True,'no_build_or_runtime':True},indent=2)+'\n')
print(json.dumps({'artifact_root':str(A),'archive_sha256':manifest['archive_sha256'],'entries':935,'local_crates':len(seen),'source_files_in_closure':len(sources),'embedded_files':sum(len(x['files'])for x in embed_folders)}))
