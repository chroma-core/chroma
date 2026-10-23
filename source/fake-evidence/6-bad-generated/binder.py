import json,sys,hashlib
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());base=json.loads(Path(s['base']['path']).read_text());inputs=json.loads(Path(s['matched_inputs']['path']).read_text());r=json.loads(Path(s['restore_runtime']['path']).read_text());out=Path(s['output']);out.mkdir();n=dict(inputs['fake_native'],vectors=r['canonical']);
if inputs.get('fake_bad_config'):n['config']['fp_npa']=False
prepare=dict(base,vectors=r['canonical'],queries=s['queries'],rows=n['rows'],dimension=n['dimension'],output=str(out/'prepared'))
for name,v in [('native-common.json',n),('prepare-stage.json',prepare),('restore-bindings.json',s)]:(out/name).write_text(json.dumps(v))
files=[]
for name in ['native-common.json','prepare-stage.json','restore-bindings.json']:
 p=out/name;b=p.read_bytes();files.append(dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
(out/'bound-inputs.json').write_text(json.dumps(dict(format_version=1,files=files)))
