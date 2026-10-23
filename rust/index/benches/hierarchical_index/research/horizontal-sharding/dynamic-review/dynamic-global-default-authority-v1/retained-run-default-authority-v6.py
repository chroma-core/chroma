import json,hashlib,tarfile,difflib,subprocess,os,time,shutil
from pathlib import Path
r=Path('/private/tmp/chroma-hierarchical-spann-sharding');base_root=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/dynamic-global-default-authority-v1');b=base_root/'repair-v5';a=base_root/'repair-v6';a.mkdir();h=lambda b:hashlib.sha256(b).hexdigest();base=json.loads((b/'manifest.json').read_text());changed=['rust/index/benches/hierarchical_index/writer/authoritative_dynamic_tests.rs','rust/index/benches/hierarchical_index/writer/authoritative_dynamic.rs','rust/index/benches/hierarchical_index/writer/authoritative_structure.rs','rust/index/benches/hierarchical_index/writer/authoritative_owner.rs'];new=[];files={f:h((r/f).read_bytes()) for f in list(base['files'])+new};assert all(files[f]==s for f,s in base['files'].items() if f not in changed)
patch=''
with tarfile.open(b/'source.tar.gz') as t:
 for f in changed:patch+=''.join(difflib.unified_diff(t.extractfile(f).read().decode().splitlines(True),(r/f).read_text().splitlines(True),fromfile='a/'+f,tofile='b/'+f))
for f in new:patch+=''.join(difflib.unified_diff([], (r/f).read_text().splitlines(True),fromfile='a/'+f,tofile='b/'+f))
(a/'isolated-change.patch').write_text(patch)
with tarfile.open(a/'source.tar.gz','w:gz') as t:
 for f in sorted(files):t.add(r/f,arcname=f)
(a/'manifest.json').write_text(json.dumps({'files':files,'file_count':len(files),'archive_sha256':h((a/'source.tar.gz').read_bytes()),'patch_sha256':h(patch.encode()),'baseline':str(b/'manifest.json'),'changed':changed,'new':new},indent=2)+'\n')
out=a/'local-tests-attempt1';out.mkdir();tool=Path('/Users/chroma/.rustup/toolchains/1.92.0-aarch64-apple-darwin/bin');env=os.environ.copy();canonical='/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/representative-compaction-v3/runtime-attempt1/append/private-writers/canonical.f32';sha='69ce5f9fa914cd9ed6b05c860f36ef29d68600b30bdc57891d2bb2bd635838cc';assert h(Path(canonical).read_bytes())==sha;env.update(CARGO_TARGET_DIR='/private/tmp/spann-write-check',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0',RUSTC=str(tool/'rustc'),TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2',SPANN_DYNAMIC_GLOBAL_OUTPUT=str(out),SPANN_AUTHORITY_CANONICAL=canonical,SPANN_AUTHORITY_CANONICAL_SHA256=sha);common=['--locked','--offline','-j2','-p','chroma-index'];stages=[['test']+common+['--test','hierarchical_dynamic_global','dynamic_default_','--','--test-threads=1','--nocapture'],['check']+common+['--bench','hierarchical_spann_shard']]
for i,args in enumerate(stages,1):
 assert shutil.disk_usage(r).free>=20*1024**3;assert all(h((r/f).read_bytes())==s for f,s in files.items());p=out/f'stage-{i}';cmd=[str(tool/'cargo')]+args;start=time.monotonic()
 with p.with_suffix('.stdout').open('wb') as stdout,p.with_suffix('.stderr').open('wb') as stderr:
  try:rc=subprocess.run(cmd,cwd=r,env=env,stdout=stdout,stderr=stderr,timeout=1200).returncode
  except subprocess.TimeoutExpired:rc=124
 result={'command':cmd,'exit_code':rc,'elapsed_seconds':time.monotonic()-start,'stdout_sha256':h(p.with_suffix('.stdout').read_bytes()),'stderr_sha256':h(p.with_suffix('.stderr').read_bytes()),'compiler_sha256':h((tool/'rustc').read_bytes()),'canonical_sha256':h(Path(canonical).read_bytes()),'end_pins':{f:h((r/f).read_bytes()) for f in files}};p.with_suffix('.result.json').write_text(json.dumps(result,indent=2)+'\n');print(i,rc,result['elapsed_seconds'],flush=True)
 if rc:break
