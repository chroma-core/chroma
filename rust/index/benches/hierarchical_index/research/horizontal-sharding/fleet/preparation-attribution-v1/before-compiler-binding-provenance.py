import hashlib,json,os,re,subprocess
from pathlib import Path
R=Path('/mnt/spann/preparation-attribution-v1'); S=R/'source'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def run(args):return subprocess.check_output(args,text=True,cwd=S).strip()
m=json.loads((R/'source-manifest.json').read_text())['files']
inputs={}
for d in (R/'target/release').rglob('*.d'):
 for name in re.findall(r'[^\s:]+',d.read_text(errors='replace')):
  p=Path(name)
  if not p.is_absolute():p=S/p
  p=p.resolve()
  if p.is_file() and p.is_relative_to(S):inputs[str(p.relative_to(S))]=sha(p)
configs={}
for root in [S,*S.parents,Path.home()]:
 for name in ['.cargo/config','.cargo/config.toml']:
  p=root/name
  if p.is_file():configs[str(p)]={'sha256':sha(p),'text':p.read_text()}
r={'rustc':run(['/home/ubuntu/.cargo/bin/rustc','+1.92.0','-Vv']),'cargo':run(['/home/ubuntu/.cargo/bin/cargo','+1.92.0','-V']),'configs':configs,'environment':{k:v for k,v in os.environ.items() if k in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','CARGO_HOME','RUSTUP_HOME']},'build_environment':{'CARGO_PROFILE_RELEASE_DEBUG':'0','CARGO_INCREMENTAL':'0','RAYON_NUM_THREADS':'2','TOKIO_WORKER_THREADS':'2'},'local_dependency_inputs':inputs,'unpinned_local_dependency_inputs':{k:v for k,v in inputs.items() if k not in m},'mismatched_pins':{k:v for k,v in inputs.items() if k in m and m[k]!=v},'all_source_pins_match':all(sha(S/k)==v for k,v in m.items()),'build_controller_sha256':sha(R/'build.py'),'binary_sha256':sha(R/'native-reader')}
(R/'build-provenance.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps({k:v for k,v in r.items() if k not in ['local_dependency_inputs','configs']},indent=2))
