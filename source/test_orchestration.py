"""Small local subprocess fixture; fake restore/binder/driver, real collector.

The production CLI fixes the2Mcohort. A temporary local shim invokes the same
core with an explicit tiny contract; it cannot start AWS, watchdogs or native work.
"""
import copy
import hashlib
import importlib.util
import io
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from orchestration_contract import descriptor
loader=importlib.util.spec_from_file_location('new_orchestrator', HERE/'orchestrate-restored.py')
orchestrator=importlib.util.module_from_spec(loader);loader.loader.exec_module(orchestrator)


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')
    return descriptor(path)


class Fixture:
    def __init__(self, root, mode='success'):
        root=root.resolve()
        self.root=root
        self.mode=mode
        now=time.time();stop=now+1000
        self.cohort=dict(rows=3,dimension=2,metric='ip',source_id='fake-three')
        config=dict(write_navigation='fp',fp_npa=True,branching_factor=2,split_threshold=8,merge_threshold=0)
        self.expected=dict(cohort=self.cohort,config=config,query_ids=[str(i) for i in range(16)],
            restore_objects=1,storage_chunk=2,balance_every=2,owner_slots=2,owner_vector_threshold=2,
            max_nodes=100,max_leaf_rows=100,max_owner_physical_rows=100)
        self.scripts={}
        restore='''import json,sys,hashlib
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());p=Path(s['attempt_dir']);p.mkdir();
if s.get('fake_exit'):raise SystemExit(s['fake_exit'])
c=p/'canonical.f32';c.write_bytes(bytes(range(24)));d=dict(path=str(c),bytes=24,sha256=hashlib.sha256(c.read_bytes()).hexdigest());r=dict(status='complete',canonical=d,rows=3,dimension=2,expected_bytes=24,deadline_unix=s['deadline_unix'],min_free_bytes=s['min_free_bytes']);(p/'receipt.json').write_text(json.dumps(r));(p/'runtime-provenance.json').write_text(json.dumps(dict(r,fixture=True)));(p/'object-0000.json').write_text('{}')
'''
        binder='''import json,sys,hashlib
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());base=json.loads(Path(s['base']['path']).read_text());inputs=json.loads(Path(s['matched_inputs']['path']).read_text());r=json.loads(Path(s['restore_runtime']['path']).read_text());out=Path(s['output']);out.mkdir();n=dict(inputs['fake_native'],vectors=r['canonical']);
if inputs.get('fake_bad_config'):n['config']['fp_npa']=False
prepare=dict(base,vectors=r['canonical'],queries=s['queries'],rows=n['rows'],dimension=n['dimension'],output=str(out/'prepared'))
for name,v in [('native-common.json',n),('prepare-stage.json',prepare),('restore-bindings.json',s)]:(out/name).write_text(json.dumps(v))
files=[]
for name in ['native-common.json','prepare-stage.json','restore-bindings.json']:
 p=out/name;b=p.read_bytes();files.append(dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
(out/'bound-inputs.json').write_text(json.dumps(dict(format_version=1,files=files)))
'''
        driver='''import json,sys
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());out=Path(s['integration_output']);out.joinpath('driver-entered.json').write_text('{}');original=json.loads(Path(s['fake_base_template']).read_text());assert {k:v for k,v in s.items() if k!='bound_inputs'}=={k:v for k,v in original.items() if k!='bound_inputs'};assert s['bound_inputs']['sha256'];
if s.get('fake_exit'):raise SystemExit(s['fake_exit'])
for name in s['fake_results']:
 p=Path(s['fake_experiment'])/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('{}')
'''
        for name,source in [('restore',restore),('binder',binder),('driver',driver)]:
            p=root/(name+'.py');p.write_text(source);self.scripts[name]=descriptor(p)
        query=root/'matched-queries16.jsonl';query.write_text(''.join(json.dumps(dict(query_id=str(i),vector=[0.,1.]))+'\n' for i in range(16)))
        queries=descriptor(query)
        inputs=save(root/'matched-inputs.json',dict(config=config,queries=queries,query_ids=self.expected['query_ids'],fake_native=dict(self.cohort,format_version=1,config=config,**{k:self.expected[k] for k in ('storage_chunk','balance_every','owner_slots','owner_vector_threshold','max_nodes','max_leaf_rows','max_owner_physical_rows')}),fake_bad_config=mode=='bad-generated'))
        self.budget=dict(deadline_unix=now+300,threads=2,min_free_bytes=1,max_process_rss_bytes=1000000,max_log_bytes=1000000,guard_timer='fake-fixed.timer')
        plan=save(root/'fake-plan.json',dict(scope='FAKE local plan, not runtime cohort'))
        restorespec=save(root/'restore-spec.json',dict(plan=plan,deadline_unix=self.budget['deadline_unix'],min_free_bytes=1,attempt_dir=str(root/'restore'),fake_exit=9 if mode=='restore-fail' else 0))
        sources=[HERE/'orchestrate-restored.py',HERE/'orchestration_contract.py',Path(self.scripts['binder']['path']),Path(self.scripts['driver']['path']),Path(self.scripts['restore']['path']),HERE/'collect-evidence.py']
        collector=descriptor(HERE/'collect-evidence.py')
        if mode=='collection-fail':
            p=root/'collector-fail.py';p.write_text('raise SystemExit(5)\n');collector=descriptor(p);sources.append(p)
        setup=save(root/'setup-artifacts.json',dict(format_version=1,files=[descriptor(p) for p in sources]))
        runner_dir=root/'fake-runner';runner_dir.mkdir()
        reviewed=HERE.parent.parent/'dynamic-review/matched-2m-quality-plan-v1/runner'
        for name in ['artifact_bindings.py','project_rounds.py']:(runner_dir/name).write_bytes((reviewed/name).read_bytes())
        runner=save(root/'runner-artifacts.json',dict(format_version=1,files=[descriptor(p) for p in runner_dir.glob('*.py')]))
        checkout=root/'fake-checkout';package=checkout/'tools/hierarchical_sharding';package.mkdir(parents=True)
        for name in ['spatial_prepare.py','exact_blocked.py']:(package/name).write_text('# fake source, not executed\n')
        binary=root/'fake-native';binary.write_bytes(b'fake native is never executed')
        compiler=root/'fake-compiler';compiler.write_bytes(b'fake compiler is never executed')
        source_manifest=save(root/'source-artifacts.json',dict(format_version=1,files=[descriptor(p) for p in package.glob('*.py')]))
        provenance=save(root/'build-provenance.json',dict(source_manifest=source_manifest,compiler=descriptor(compiler),binary=descriptor(binary),compiler_version='FAKE local fixture',build_command=['FAKE-not-run']))
        implementation=save(root/'implementation-artifacts.json',dict(format_version=1,files=[descriptor(p) for p in package.glob('*.py')]+[source_manifest,descriptor(compiler),descriptor(binary),provenance]))
        base=dict(self.budget,runner_directory=str(root/'fake-runner'),integration_output=str(root/'integration'),bound_inputs=None,
            setup_artifacts=setup,runner_artifacts=runner,implementation_artifacts=implementation,build_provenance=provenance,
            implementation_checkout=str(checkout),binary=str(binary),binary_sha256=descriptor(binary)['sha256'],fake_base_template=str(root/'base.json'),
            fake_experiment=str(root),fake_results=sorted(orchestrator.REQUIRED_RESULTS),fake_exit=7 if mode=='driver-fail' else 0)
        base_artifact=save(root/'base.json',base)
        approval=save(root/'resource-approval.json',dict(decision='approved',epoch='matched-2m-dynamic-v1',account='079819354890',instance_id='i-fake',instance_type='r6i.2xlarge',volume_id='vol-fake',required_free_bytes=1,machine_stop_unix=stop))
        start=save(root/'start-attempt.json',dict(identity=dict(Account='079819354890'),instance=dict(InstanceId='i-fake',InstanceType='r6i.2xlarge'),experiment_volume=dict(VolumeId='vol-fake',Size=64,VolumeType='gp3',Encrypted=True),deadline_unix=stop,watchdog_pid=123))
        ready=save(root/'watchdog-ready.json',dict(pid=123,deadline=stop))
        mount=save(root/'mount-verification.json',dict(volume_id='vol-fake',mount=str(root),free_bytes=1000000))
        raw=root/'remote-guard.log';raw.write_text('fake guard evidence only\n')
        guard=save(root/'remote-guard.json',dict(active=True,timer='fake-fixed.timer',deadline_unix=stop,instance_id='i-fake',raw=descriptor(raw)))
        evidence=dict(resource_approval=approval,start_attempt=start,watchdog_ready=ready,mount_verification=mount,remote_guard=guard,remote_guard_raw=descriptor(raw))
        files={str(Path(x['path']).relative_to(root)) for x in [base_artifact,restorespec,inputs,queries,setup,runner,implementation,provenance]+list(evidence.values())}
        files.update({'admission.json','collection.json','restore/receipt.json','restore/runtime-provenance.json','restore/failure.json','restore/object-0000.json','orchestration/restore.stdout','orchestration/restore.stderr','orchestration/binder.stdout','orchestration/binder.stderr','orchestration/work.stdout','orchestration/work.stderr','orchestration/binding-spec.json','orchestration/generated-base.json','orchestration/pre-collection.json','integration/native-common.json','integration/prepare-stage.json','integration/restore-bindings.json','integration/bound-inputs.json'})
        files.update({'orchestration/closure/build-source-artifacts.json','orchestration/closure/restore-plan.json','orchestration/closure/restore-materializer.py','orchestration/closure/copies.json'})
        files.update(orchestrator.REQUIRED_RESULTS)
        for stage in orchestrator.STAGES:files.update(f'integration/{stage}{tail}' for tail in ('-execution.json','-admission.json','-exit.json','.stdout','.stderr'))
        # Fake driver emits all required scientific metadata paths, not native work.
        base['fake_results']=sorted(set(base['fake_results'])|{f'integration/{stage}{tail}' for stage in orchestrator.STAGES for tail in ('-execution.json','-admission.json','-exit.json','.stdout','.stderr')});base_artifact=save(root/'base.json',base)
        collection=save(root/'collection.json',dict(root=str(root),deadline_unix=now+600,files=sorted(files)))
        artifacts=dict(base_template=base_artifact,restore_script=self.scripts['restore'],restore_spec=restorespec,binder=self.scripts['binder'],driver=self.scripts['driver'],collector=collector,collection=collection,matched_inputs=inputs,queries=queries,setup_artifacts=setup,runner_artifacts=runner,implementation_artifacts=implementation,build_provenance=provenance,native_binary=descriptor(binary))
        self.admission=dict(format_version=1,decision='approved',epoch='matched-2m-dynamic-v1',experiment_root=str(root),machine_stop_unix=stop,artifacts=artifacts,evidence=evidence,budgets=self.budget,expected=self.expected,generated_fields=['bound_inputs'],retrieval_limits=dict(max_wire_bytes=1024*1024,max_archive_bytes=400000,max_sidecar_bytes=100000))
        self.admit()
        shim=root/'local-test-shim.py';shim.write_text(f"import sys,json,importlib.util\nsys.path.insert(0,{str(HERE)!r})\np=importlib.util.spec_from_file_location('o',{str(HERE/'orchestrate-restored.py')!r});m=importlib.util.module_from_spec(p);p.loader.exec_module(m)\ns=json.load(open(sys.argv[1]));r=m.orchestrate(s,_cohort={self.cohort!r});print(json.dumps(r));raise SystemExit(not r['success'])\n")
        self.shim=shim

    def admit(self):
        self.spec=dict(format_version=1,admission=save(self.root/'admission.json',self.admission),output=str(self.root/'orchestration'))
        save(self.root/'orchestration-spec.json',self.spec)

    def run(self):
        command=[sys.executable,str(self.shim),str(self.root/'orchestration-spec.json')]
        result=subprocess.run(command,capture_output=True,timeout=20)
        (self.root/'test-shim.stdout').write_bytes(result.stdout)
        (self.root/'test-shim.stderr').write_bytes(result.stderr)
        save(self.root/'test-shim-result.json',dict(command=command,returncode=result.returncode,scope='FAKE sources and local subprocesses only'))
        evidence=os.environ.get('MATCHED_LOCAL_TEST_EVIDENCE_ROOT')
        if evidence:
            parent=Path(evidence);parent.mkdir(parents=True,exist_ok=True)
            target=parent/(str(len(list(parent.iterdir())))+'-'+self.mode)
            shutil.copytree(self.root,target,ignore=shutil.ignore_patterns('__pycache__'))
            save(target/'test-retained-copy.json',dict(original_root=str(self.root),retained_root=str(target),scope='FAKE local fixture copy; absolute origin paths are intentionally historical test paths, not runnable admission'))
        return result


class OrchestrationIntegration(unittest.TestCase):
    def test_success_restore_binding_admission_work_collection_and_actual_retrieval(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Fixture(Path(tmp));p=f.run();self.assertEqual(p.returncode,0,p.stderr.decode())
            root=f.root;result=json.loads((root/'orchestration/result.json').read_text());self.assertTrue(result['success']);self.assertEqual([result[k] for k in ('restore','binder','work','collection')],[0,0,0,0]);self.assertTrue((root/'integration/driver-entered.json').is_file())
            generated=json.loads((root/'orchestration/generated-base.json').read_text());self.assertEqual(generated['bound_inputs'],descriptor(root/'integration/bound-inputs.json'))
            manifest=root/'orchestration/retrieval-manifest.json';m=json.loads(manifest.read_text());payload=io.BytesIO()
            with tarfile.open(fileobj=payload,mode='w',format=tarfile.USTAR_FORMAT) as tar:
                for item in m['files']:tar.add(item['path'],arcname=item['name'],recursive=False)
            spec=dict(manifest=descriptor(manifest),admission=f.spec['admission'],admission_copy=f.spec['admission'],limits=m['limits'],deadline_unix=time.time()+30,output=str(root/'retrieved'));save(root/'retrieval-spec.json',spec)
            p=subprocess.run([sys.executable,str(HERE/'retrieve-evidence.py'),'--spec',str(root/'retrieval-spec.json')],input=payload.getvalue(),capture_output=True,timeout=20);self.assertEqual(p.returncode,0,p.stderr.decode())
            self.assertTrue((root/'retrieved/retrieval-complete.json').is_file())
            for item in m['files']:self.assertEqual((root/'retrieved'/item['name']).read_bytes(),Path(item['path']).read_bytes())

    def test_restore_driver_binding_and_collection_failures_still_produce_bounded_terminal_retrieval(self):
        for mode in ['restore-fail','driver-fail','bad-generated','collection-fail']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                f=Fixture(Path(tmp),mode);p=f.run();self.assertNotEqual(p.returncode,0)
                result=json.loads((f.root/'orchestration/result.json').read_text());self.assertFalse(result['success']);self.assertTrue((f.root/'orchestration/pre-collection.json').exists());m=json.loads((f.root/'orchestration/retrieval-manifest.json').read_text());names={x['name']for x in m['files']};self.assertIn('result.json',names)
                if mode in ('restore-fail','bad-generated'):self.assertIsNone(result['work']);self.assertFalse((f.root/'integration/driver-entered.json').exists())
                if mode!='collection-fail':self.assertEqual(result['collection'],0);self.assertIn('evidence.tar',names)
                else:self.assertEqual(result['collection'],5);self.assertNotIn('evidence.tar',names)

    def test_missing_external_approval_and_wrong_guard_or_query_block_before_restore(self):
        for mode in ['draft','guard','query','closure']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                f=Fixture(Path(tmp))
                if mode=='draft':f.admission['decision']='draft'
                elif mode=='guard':
                    path=Path(f.admission['evidence']['remote_guard']['path']);v=json.loads(path.read_text());v['deadline_unix']+=1;f.admission['evidence']['remote_guard']=save(path,v)
                elif mode=='query':f.admission['expected']['query_ids'].reverse()
                else:
                    path=Path(f.admission['artifacts']['collection']['path']);v=json.loads(path.read_text());v['files'].remove('restore/runtime-provenance.json');f.admission['artifacts']['collection']=save(path,v)
                f.admit();p=f.run();self.assertNotEqual(p.returncode,0);self.assertFalse((f.root/'restore').exists());self.assertFalse((f.root/'orchestration').exists())


if __name__=='__main__':unittest.main(verbosity=2)
