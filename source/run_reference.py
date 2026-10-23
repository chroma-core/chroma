"""One isolated optimized historical reference; no cloud operations."""
import argparse,hashlib,importlib.util,json,os,re,shutil,time
from pathlib import Path
from process_lifecycle import run_owned
from supervisor_core import run as supervised_run
HERE=Path(__file__).resolve().parent
CONTRACT=HERE/'SOURCE-CONTRACT.json'
COMPILER=Path('/home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu/bin/rustc')
COMPILER_SHA='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1'
INPUT=Path('/home/ubuntu/spann-fineweb100k-retained-v1/fineweb100k.f32')
INPUT_SHA='5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c'
SAVED_DIGEST='6373f1fe63982f854fd8cde678c0f6e02ba213cd7a426e98b93f42badfa253da'
def digest(path):
 h=hashlib.sha256()
 with Path(path).open('rb')as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def save(path,value):
 with Path(path).open('x')as f:json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def readiness():
 c=json.loads(CONTRACT.read_text())
 if c.get('accepted')is not True or not c.get('source_manifest_sha256')or not c.get('baseline_manifest_sha256')or not c.get('changes')or not c.get('membership_uniqueness_review_sha256')or not c.get('protocol_sha256'):raise ValueError('Historical source/unique-membership review bindings unfilled')
 if digest(HERE/'PROTOCOL.json')!=c['protocol_sha256']:raise ValueError('Historical CLI protocol differs')
 return c
def clean(p):
 if not p.is_absolute()or'..'in p.parts or any(x.is_symlink()for x in[p,*p.parents]):raise ValueError('Real normalized absolute paths required')
 return p
def remaining(deadline,seconds):
 if deadline-time.time()<seconds+300:raise TimeoutError('Phase allowance plus collection/shutdown reserve cannot fit')
def native_command(binary,output):
 values=json.loads((HERE/'PROTOCOL.json').read_text())['command_template']
 if '--validate-postings'not in values or '--verify-valid-postings'not in values or '--no-commit'in values:raise ValueError('Membership/commit CLI policy differs')
 values=[str(INPUT)if x=='RETAINED_INPUT'else str(output)if x=='FRESH_OUTPUT'else x for x in values]
 return[str(binary)]+values
def validate_summary(text):
 records=[json.loads(line.split(' ',1)[1])for line in text.splitlines()if line.startswith('RETAINED_POSTING_VALIDATION ')]
 if len(records)!=2 or {r.get('phase')for r in records}!={'before commit','after reopen'}:raise ValueError('Exactly two structured posting validations required')
 for r in records:
  if any(type(r.get(k))is not int or r[k]!=v for k,v in [('unique_current_ids',100000),('current_postings',100000),('duplicate_current_postings',0)]) or not isinstance(r.get('seconds'),(int,float)) or r['seconds']<0:raise ValueError('Current posting membership/uniqueness differs')
 for phase in ['before commit','after reopen']:
  expected=f'[posting validation] {phase}: 100000 root-reachable valid ids; all child links resolve'
  if expected not in text:raise ValueError('Missing successful posting-membership validation: '+phase)
 if 'Pre-commit reachable valid IDs: 100000 | indexed embeddings without one: 0 | missing child nodes: 0'not in text:raise ValueError('Pre-commit orphan diagnostics differ')
 if 'Post-reopen reachable valid IDs: 100000 | missing child nodes: 0'not in text:raise ValueError('Post-reopen orphan diagnostics differ')
 rows=[json.loads(line.split('RETAINED_SAVED_BITS ',1)[1])for line in text.splitlines()if line.startswith('RETAINED_SAVED_BITS ')]
 if len(rows)!=1 or rows[0]['rows']!=100000 or rows[0]['all_current_vector_bits_sha256']!=SAVED_DIGEST:raise ValueError('Saved-vector bit verification differs')
 return{'posting_membership':records,'saved_bits':rows[0],'queries_or_gt_executed':False}
def verify_source(control,deadline):
 c=readiness();mp=control/'source-manifest.json'
 if digest(mp)!=c['source_manifest_sha256']:raise ValueError('Historical source manifest differs')
 m=json.loads(mp.read_text());root=control/'source'
 if len(m['files'])>5000 or sum(v['bytes']for v in m['files'].values())>536870912:raise ValueError('Source closure exceeds declared bound')
 actual_files=set();actual_links=set()
 for base,dirs,files in os.walk(root,followlinks=False):
  for name in list(dirs)+files:
   q=Path(base)/name;relative=q.relative_to(root).as_posix()
   if q.is_symlink():actual_links.add(relative)
   elif q.is_file():actual_files.add(relative)
 if actual_files!=set(m['files'])or actual_links!=set(m['symlinks']):raise ValueError('Exact source inventory differs')
 for name,row in m['files'].items():
  remaining(deadline,0);relative=Path(name)
  if relative.is_absolute()or'..'in relative.parts:raise ValueError('Unsafe source manifest member')
  q=root/name
  if q.is_symlink()or q.stat().st_size!=row['bytes']or digest(q)!=row['sha256']:raise ValueError('Source member differs: '+name)
 for name,target in m['symlinks'].items():
  if os.readlink(root/name)!=target or not(root/name).resolve().is_relative_to(root.resolve()):raise ValueError('Source symlink differs')
 return c,m
def environment(control,target):
 if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):raise ValueError('Assertions required')
 forbidden=['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','MALLOC_CONF','JEMALLOC_SYS_WITH_MALLOC_CONF']
 if any(os.environ.get(k)for k in forbidden):raise ValueError('Ambient compiler/allocator overrides forbidden')
 if any(k.startswith('CARGO_PROFILE_')and k!='CARGO_PROFILE_RELEASE_DEBUG'for k in os.environ):raise ValueError('Unreviewed profile override')
 env=dict(os.environ,RUSTC=str(COMPILER),CARGO_TARGET_DIR=str(target),CARGO_PROFILE_RELEASE_DEBUG='0',TMPDIR=str(control/'tmp'),TOKIO_WORKER_THREADS='32',RAYON_NUM_THREADS='32',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1')
 return env
def build(control,target,deadline):
 remaining(deadline,600);contract,manifest=verify_source(control,deadline)
 if digest(COMPILER)!=COMPILER_SHA:raise ValueError('Compiler differs')
 baseline=control/'baseline-source-manifest.json'
 if digest(baseline)!=contract['baseline_manifest_sha256']:raise ValueError('Historical baseline differs')
 old=json.loads(baseline.read_text())
 if old['symlinks']!=manifest['symlinks']:raise ValueError('Unreviewed symlink inventory change')
 actual={n for n in set(old['files'])|set(manifest['files'])if old['files'].get(n)!=manifest['files'].get(n)}
 if actual!=set(contract['changes']):raise ValueError('Exact reviewed historical source delta differs')
 entry='rust/index/benches/hierarchical_spann_profile_quantized.rs'
 for name in sorted(actual|{entry}):
  if name not in manifest['files']:raise ValueError('Source removal unsupported')
  path=control/'source'/name;stamp=time.time_ns();os.utime(path,ns=(stamp,stamp))
  if digest(path)!=manifest['files'][name]['sha256']:raise ValueError('Source changed during timestamp refresh')
 folder=control/'build';folder.mkdir();env=environment(control,target);command=[str(COMPILER.with_name('cargo')),'build','--release','--locked','--offline','-j32','-p','chroma-index','--bench','hierarchical_spann_profile_quantized','--message-format=json']
 with(folder/'stdout.jsonl').open('x')as out,(folder/'stderr').open('x')as err:r=run_owned(command,folder/'process.json',600,cwd=control/'source',env=env,stdout=out,stderr=err)
 if r.returncode:raise RuntimeError('Reference build failed; no retry')
 values=[json.loads(line)for line in(folder/'stdout.jsonl').read_text().splitlines()];art=[v for v in values if v.get('reason')=='compiler-artifact'and v.get('target',{}).get('name')=='hierarchical_spann_profile_quantized'and v.get('executable')];dep=[v for v in values if v.get('reason')=='compiler-artifact'and v.get('target',{}).get('name')=='chroma_blockstore'and'lib'in v.get('target',{}).get('kind',[])]
 if len(art)!=1 or len(dep)!=1 or any(v.get('fresh')is not False or v['profile']['opt_level']!='3'or v['profile']['debuginfo']!=0 for v in art+dep):raise ValueError('Fresh optimized reference+storage build not proven')
 if Path(dep[0]['target']['src_path']).resolve()!=(control/'source/rust/blockstore/src/lib.rs').resolve()or Path(art[0]['target']['src_path']).resolve()!=(control/'source/rust/index/benches/hierarchical_spann_profile_quantized.rs').resolve():raise ValueError('Compiled source roots differ')
 binary=folder/'native';shutil.copy2(art[0]['executable'],binary);save(folder/'result.json',{'binary':str(binary),'binary_sha256':digest(binary),'source_manifest_sha256':contract['source_manifest_sha256'],'compiler_sha256':COMPILER_SHA,'profile':art[0]['profile'],'command':command,'environment':{k:env.get(k)for k in ['RUSTC','CARGO_TARGET_DIR','CARGO_PROFILE_RELEASE_DEBUG','TMPDIR','RUSTFLAGS','MALLOC_CONF']},'allocator':'Historical entrypoint jemalloc retained','bench_fresh':False,'blockstore_fresh':False})
def science(control,target,output,host_path,proof_path,deadline):
 remaining(deadline,300);verify_source(control,deadline);env=environment(control,target);host=json.loads(host_path.read_text());proof=json.loads(proof_path.read_text())
 if host['account']!='079819354890'or host['instance_id']!='i-011ff180f03584f40'or host['region']!='us-east-1'or host['physical_cores']!=32 or host['deadline_unix']!=deadline or proof['deadline_unix']!=deadline or proof['instance_id']!=host['instance_id']or proof['armed']is not True:raise ValueError('Fresh identity/absolute guards differ')
 if INPUT.stat().st_size!=307200000 or digest(INPUT)!=INPUT_SHA:raise ValueError('Retained input differs')
 if output.exists()or not output.parent.is_dir():raise ValueError('Output must be fresh under declared real parent')
 for keep in[control,INPUT.parent]:
  if output.is_relative_to(keep)or keep.is_relative_to(output):raise ValueError('Output overlaps retained/control roots')
 for root,floor in[(control,21474836480),(Path(host['data_mount']),17179869184)]:
  stat=os.statvfs(root)
  if stat.f_bavail*stat.f_frsize<floor:raise ValueError('Filesystem floor not met')
 if next(int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()if line.startswith('MemAvailable:'))<34359738368:raise ValueError('Available memory below admission')
 receipt=json.loads((control/'build/result.json').read_text());binary=Path(receipt['binary'])
 if digest(binary)!=receipt['binary_sha256']or receipt['source_manifest_sha256']!=readiness()['source_manifest_sha256']or receipt['bench_fresh']is not False or receipt['blockstore_fresh']is not False:raise ValueError('Build binding differs')
 save(control/'native-command.json',{'command':native_command(binary,output),'host_sha256':digest(host_path),'proof_sha256':digest(proof_path),'deadline':deadline});os.environ.update(env)
 result=supervised_run(native_command(binary,output),control/'measurement',output,min(time.time()+300,deadline-300),25769803776,4294967296,268435456,host['data_mount'])
 if result['exit_code']or result['guard']is not None:raise RuntimeError('Reference native failed/stopped; preserve evidence')
 text=(control/'measurement/stdout').read_text()+'\n'+(control/'measurement/stderr').read_text();save(control/'integrity-summary.json',validate_summary(text))
def guard_admission(control,host_path,proof_path,deadline):
 host=json.loads(host_path.read_text());proof=json.loads(proof_path.read_text())
 if host['account']!='079819354890'or host['instance_id']!='i-011ff180f03584f40'or host['region']!='us-east-1'or host['physical_cores']!=32 or host['deadline_unix']!=deadline or proof['deadline_unix']!=deadline or proof['instance_id']!=host['instance_id']or proof['armed']is not True or host.get('fresh_identity_verified')is not True or host.get('capacity_verified')is not True:raise ValueError('Fresh guarded host admission differs')
 stat=os.statvfs(control)
 if stat.f_bavail*stat.f_frsize<21474836480:raise ValueError('Build/source floor below20GiB')
 return host

def isolate_paths(control,target,output):
 for a,b in [(control,target),(control,output),(target,output),(INPUT.parent,target),(INPUT.parent,output),(INPUT.parent,control)]:
  if a.is_relative_to(b)or b.is_relative_to(a):raise ValueError('Target/control/output/input paths overlap')

def main():
 a=argparse.ArgumentParser();a.add_argument('stage',choices=['build','science'])
 for n in['control','target','output','host','proof']:a.add_argument('--'+n,type=Path,required=True)
 a.add_argument('--deadline',type=float,required=True);v=a.parse_args()
 for path in[v.control,v.target,v.output,v.host,v.proof]:clean(path)
 isolate_paths(v.control,v.target,v.output)
 readiness();guard_admission(v.control,v.host,v.proof,v.deadline)
 if v.stage=='build':build(v.control,v.target,v.deadline)
 else:science(v.control,v.target,v.output,v.host,v.proof,v.deadline)
if __name__=='__main__':main()
