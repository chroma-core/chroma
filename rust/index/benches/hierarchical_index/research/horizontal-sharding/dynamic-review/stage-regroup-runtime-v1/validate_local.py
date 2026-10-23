import ast,tarfile,tempfile,json,sys,importlib.util,subprocess,hashlib,os,time
from unittest.mock import patch
from pathlib import Path
H=Path(__file__).resolve().parent
with tempfile.TemporaryDirectory()as tmp:
 c=Path(tmp)/'control'
 # Exercise exact packaged command-line extraction/count/hash checks; skip only its
 # subprocess test dispatch, which is independently run below on this host.
 with tarfile.open(H/'runtime-package.tar.gz')as t:
  code=t.extractfile('operator/extract_and_test.py').read()
 namespace={'__name__':'packaged_extractor'};exec(compile(code,'packaged_extract_and_test.py','exec'),namespace)
 argv=['extract_and_test.py','--package',str(H/'runtime-package.tar.gz'),'--package-sha256',hashlib.sha256((H/'runtime-package.tar.gz').read_bytes()).hexdigest(),'--package-bytes',str((H/'runtime-package.tar.gz').stat().st_size),'--control',str(c),'--deadline',str(time.time()+3600)]
 with patch.object(sys,'argv',argv),patch.object(subprocess,'run')as dispatch:
  namespace['main']();assert dispatch.call_count==2
 for p in c.rglob('*.py'):ast.parse(p.read_text())
 sys.path.insert(0,str(c/'operator'))
 import build_validity
 manifest=json.loads((c/'source-manifest.json').read_text());baseline=json.loads((c/'operator/cache-baseline-source-manifest.json').read_text())
 refresh=build_validity.refresh(c/'source',manifest,baseline)
 for n,r in manifest['files'].items():assert hashlib.sha256((c/'source'/n).read_bytes()).hexdigest()==r['sha256']
 import run_pair
 spec=json.loads((c/'SPEC-TEMPLATE.json').read_text());off=run_pair.arm_spec(spec,False);on=run_pair.arm_spec(spec,True);assert {k for k in off if off[k]!=on[k]}=={'stage_regroup'}
 for label,v in [('OFF',off),('ON',on)]:(H/('SPEC-'+label+'.json')).write_text(json.dumps(v,indent=2)+'\n')
 r=subprocess.run([sys.executable,'-m','unittest','-v','test_regroup_pair','test_build_profile','test_build_validity','test_pair_build_gate'],cwd=c/'operator',capture_output=True,text=True,timeout=60);(H/'tests.stdout').write_text(r.stdout);(H/'tests.stderr').write_text(r.stderr);assert r.returncode==0,r.stderr
 result={'status':'pass','source_refresh_changed_files':refresh['changed_source_files'],'exact_source_bytes_preserved_after_refresh':True,'all_packaged_python_AST':'pass','focused_tests':r.stderr,'only_scientific_delta':'stage_regroup','cloud_actions':0};(H/'controller-validation.json').write_text(json.dumps(result,indent=2)+'\n');print(r.stderr)
