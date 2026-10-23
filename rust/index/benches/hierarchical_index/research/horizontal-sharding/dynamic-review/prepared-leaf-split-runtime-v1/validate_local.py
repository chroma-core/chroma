import ast,tarfile,tempfile,json,sys,importlib.util,subprocess,hashlib,os
from pathlib import Path
H=Path(__file__).resolve().parent
with tempfile.TemporaryDirectory()as tmp:
 c=Path(tmp)/'control';c.mkdir()
 with tarfile.open(H/'runtime-package.tar.gz')as t:t.extractall(c)
 with tarfile.open(c/'source.tar.gz')as t:t.extractall(c)
 for p in c.rglob('*.py'):ast.parse(p.read_text())
 sys.path.insert(0,str(c/'operator'))
 import build_validity
 manifest=json.loads((c/'source-manifest.json').read_text());baseline=json.loads((c/'operator/cache-baseline-source-manifest.json').read_text())
 refresh=build_validity.refresh(c/'source',manifest,baseline)
 for n,r in manifest['files'].items():assert hashlib.sha256((c/'source'/n).read_bytes()).hexdigest()==r['sha256']
 import run_pair
 spec=json.loads((c/'SPEC-TEMPLATE.json').read_text());off=run_pair.arm_spec(spec,False);on=run_pair.arm_spec(spec,True);assert {k for k in off if off[k]!=on[k]}=={'prepared_leaf_split'}
 for label,v in [('OFF',off),('ON',on)]:(H/('SPEC-'+label+'.json')).write_text(json.dumps(v,indent=2)+'\n')
 r=subprocess.run([sys.executable,'-m','unittest','-v','test_prepared_pair','test_build_profile','test_build_validity','test_pair_build_gate'],cwd=c/'operator',capture_output=True,text=True,timeout=60);(H/'tests.stdout').write_text(r.stdout);(H/'tests.stderr').write_text(r.stderr);assert r.returncode==0,r.stderr
 result={'status':'pass','source_refresh_changed_files':refresh['changed_source_files'],'exact_source_bytes_preserved_after_refresh':True,'all_packaged_python_AST':'pass','focused_tests':r.stderr,'only_scientific_delta':'prepared_leaf_split','cloud_actions':0};(H/'controller-validation.json').write_text(json.dumps(result,indent=2)+'\n');print(r.stderr)
