"""One persistence integration check; not a save performance study."""
from pathlib import Path
import os,sys,json,subprocess
p=Path(__file__).resolve().parent;profile=json.loads((p/'candidate-profile.json').read_text())
env=dict(os.environ,**profile['env']);name='33b-save-smoke'
subprocess.run([sys.executable,str(p/'run.py'),name,profile['writer_kind'],'save'],env=env,check=True)
d=json.loads((p/name/'owner1-receipts.json').read_text());r=json.loads(next(v for k,v in d.items() if k.endswith('/result.json')))
assert r['status']=='construction_save_and_fresh_reader_validated' and r['validation_complete']
(p/'save-validation.json').write_text(json.dumps({'status':r['status'],'validation_complete':r['validation_complete'],'save_seconds':r['save_seconds'],'repeat_save':r['repeat_save'],'fresh_reader_validation_seconds':r['fresh_reader_validation_seconds']},indent=2)+'\n');print('SAVE_VALIDATED')
