"""One new binary, cadence4096 then100000 with freshly compiled dependency; no cloud lifecycle operations."""
import argparse,hashlib,importlib.util,json,os,shutil,sys,time
from pathlib import Path
CONFIG=Path(__file__).with_name('cadence-source.json')
def digest(p):
 h=hashlib.sha256()
 with Path(p).open('rb')as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def save(p,v):
 with Path(p).open('x')as f:json.dump(v,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def module(c):
 sys.path.insert(0,str(c/'operator'));s=importlib.util.spec_from_file_location('stages_'+c.name,c/'operator/remote_stages.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
def clean(p):
 if not p.is_absolute()or'..'in p.parts or any(x.is_symlink()for x in[p,*p.parents]):raise ValueError('Real normalized absolute path required')
 return p
def arm_spec(original,enabled):
 if type(enabled)is not bool:raise ValueError('Fixed boolean arm required')
 expected=json.loads((Path(__file__).with_name('paired-spec.json')).read_text())
 if original!=expected:raise ValueError('Exact fixed scientific baseline differs')
 spec=json.loads(json.dumps(original));spec['prepared_leaf_split']=enabled
 return spec

def configuration_ready():
 expected=json.loads(CONFIG.read_text())
 if not isinstance(expected.get('source_manifest_sha256'),str)or len(expected['source_manifest_sha256'])!=64 or not isinstance(expected.get('changed_source_files'),list)or not expected['changed_source_files']:
  raise ValueError('Frozen source bindings are unfilled; no build or science admitted')
 required={'rust/blockstore/src/arrow/block/delta/single_column_storage.rs','rust/blockstore/src/arrow/block/delta/ordered_block_delta.rs','rust/blockstore/src/arrow/ordered_blockfile_writer.rs'}
 if not required.issubset(expected['changed_source_files']):raise ValueError('Required exact dependency delta missing')
 return expected

def validate_pair_build(control,stages):
 expected=json.loads(CONFIG.read_text());receipt=json.loads((control/'build/result.json').read_text())
 if receipt.get('bench_artifact_fresh')is not False:raise ValueError('Build freshness invalid before either arm')
 if receipt['source_manifest_sha256']!=expected['source_manifest_sha256']:raise ValueError('Build source differs')
 binary=control/'build/native';actual=digest(binary)
 if receipt['binary']!=str(binary)or actual!=receipt['binary_sha256']or actual in expected['old_binary_sha256']:raise ValueError('New binary identity differs or is an old implementation')
 artifacts=[]
 dependency_artifacts=[]
 for line in(control/'build/stdout.jsonl').read_text().splitlines():
  value=json.loads(line)
  if value.get('reason')=='compiler-artifact'and value.get('target',{}).get('name')=='hierarchical_spann_shard'and value.get('executable'):artifacts.append(value)
  if value.get('reason')=='compiler-artifact'and value.get('target',{}).get('name')=='chroma_blockstore'and 'lib'in value.get('target',{}).get('kind',[]):dependency_artifacts.append(value)
 stages.validate_artifact(artifacts)
 if len(dependency_artifacts)!=1 or dependency_artifacts[0].get('fresh')is not False:raise ValueError('Changed blockstore dependency did not compile freshly')
 dependency=dependency_artifacts[0]
 if dependency.get('profile',{}).get('opt_level')!='3'or dependency.get('profile',{}).get('debuginfo')!=0:raise ValueError('Dependency profile differs')
 if Path(dependency['target']['src_path']).resolve()!=(control/'source/rust/blockstore/src/lib.rs').resolve():raise ValueError('Dependency source root differs')
 if digest(artifacts[0]['executable'])!=actual:raise ValueError('Raw compiler executable differs')
 refreshed=json.loads((control/'build/source-refresh.json').read_text())
 if refreshed!=receipt['source_refresh']or sorted(refreshed['changed_source_files'])!=expected['changed_source_files']:raise ValueError('Exact reviewed frozen-source refresh differs')
 evidence={'status':'passed-before-either-arm','binary_sha256':actual,'bench_artifact_fresh':False,'blockstore_artifact_fresh':False,'source_manifest_sha256':receipt['source_manifest_sha256'],'source_refresh_sha256':digest(control/'build/source-refresh.json')};save(control/'paired-build-validity.json',evidence);return evidence

def clone_arm(build,control,enabled):
 if control.exists()or control.is_relative_to(build)or build.is_relative_to(control):raise ValueError('Fresh disjoint arm control required')
 control.mkdir()
 for name in ['supervise.py','source-manifest.json','PLAN-TEMPLATE.json','input-binding.json']:
  shutil.copy2(build/name,control/name)
 shutil.copytree(build/'source',control/'source',symlinks=True);shutil.copytree(build/'operator',control/'operator');(control/'build').mkdir();shutil.copy2(build/'build/result.json',control/'build/result.json');(control/'tmp').mkdir()
 save(control/'SPEC-TEMPLATE.json',arm_spec(json.loads((build/'SPEC-TEMPLATE.json').read_text()),enabled))
 save(control/'shared-build-provenance.json',{'original_build_receipt':str(build/'build/result.json'),'original_build_receipt_sha256':digest(build/'build/result.json'),'copied_receipt_sha256':digest(control/'build/result.json'),'shared_binary':json.loads((build/'build/result.json').read_text())['binary'],'prepared_leaf_split':enabled})

def main():
 p=argparse.ArgumentParser()
 for flag in ['build-control','short-control','long-control','science-parent','proof','host']:p.add_argument('--'+flag,type=Path,required=True)
 p.add_argument('--deadline',type=float,required=True);a=p.parse_args();controls=[a.build_control,a.short_control,a.long_control];outcome={'order':['off','on'],'arms':[],'deadline':a.deadline}
 try:
  if not __debug__ or os.environ.get('PYTHONOPTIMIZE'):raise ValueError('Assertions required')
  configuration_ready()
  for path in controls+[a.science_parent,a.proof,a.host]:clean(path)
  if any(x.is_relative_to(y)or y.is_relative_to(x)for i,x in enumerate(controls)for y in controls[i+1:]):raise ValueError('Controls overlap')
  if a.deadline-time.time()<1080:raise TimeoutError('Build300, arms600 and preservation180 cannot fit')
  host=json.loads(a.host.read_text());proof=json.loads(a.proof.read_text())
  if host['deadline_unix']!=a.deadline or proof['deadline_unix']!=a.deadline:raise ValueError('Absolute guards differ')
  shutil.copy2(a.host,a.build_control/'host-admission.json');(a.build_control/'tmp').mkdir(exist_ok=True);os.environ['TMPDIR']=str(a.build_control/'tmp')
  stages=module(a.build_control);stages.build(a.build_control,a.deadline);outcome['build_validity']=validate_pair_build(a.build_control,stages)
  if a.science_parent.exists()or not a.science_parent.parent.is_dir():raise ValueError('Fresh science parent under existing real parent required')
  a.science_parent.mkdir()
  for label,control,enabled in [('off',a.short_control,False),('on',a.long_control,True)]:
   if a.deadline-time.time()<(600 if label=='off'else 300)+180:raise TimeoutError('Arm caps and preservation cannot fit')
   clone_arm(a.build_control,control,enabled);os.environ['TMPDIR']=str(control/'tmp');rc=module(control).fill_and_run(control,a.deadline,a.science_parent/label,a.proof,a.host);outcome['arms'].append({'label':label,'returncode':rc,'control':str(control)})
   if rc:raise RuntimeError(label+' failed; no retry')
  outcome['status']='completed'
 except BaseException as error:outcome.update(status='failed',error=repr(error));raise
 finally:save(a.build_control/'paired-result.json',outcome)
if __name__=='__main__':main()
