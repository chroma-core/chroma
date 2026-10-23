"""One transport smoke and three predeclared navigation pairs; never overlap runs."""
import os,sys,json,subprocess
from pathlib import Path
p=Path(__file__).resolve().parent

def run(name,enabled,flags=()):
 env=dict(os.environ,SPANN_ACCEPT_MODE='readiness',SPANN_INSERTION_WORKERS='64',SPANN_SERIALIZE_BOUNDARY_REFRESH='1',SPANN_REFRESH_PREPARE_UNDER_TURN='0',SPANN_INSERTION_NAVIGATION_WINDOW='64',SPANN_SAVE_WORKERS='32',SPANN_NAVIGATION_PARENT_CACHE=str(int(enabled)))
 folder=p/name
 if not (folder/'owner1-receipts.json').exists():
  assert not folder.exists(),f'Partial attempt {name}: inspect and recover, never silently rerun'
  subprocess.run([sys.executable,str(p/'run.py'),name,'epoch33v1',*flags],env=env,check=True)
 assert json.loads((folder/'exit.json').read_text())['exit_code']==0
 d=json.loads((folder/'owner1-receipts.json').read_text())
 if 'replay' in name:
  old=json.loads((p.parent/'development30/baseline-replay-v4/owner1-receipts.json').read_text())
  extract=lambda x:next(v for k,v in x.items() if k.endswith('/replay-results.json'))
  assert extract(d)==extract(old),'fixed navigation replay mismatch'
 else:
  r=json.loads(next(v for k,v in d.items() if k.endswith('/result.json')))
  assert r['status']=='writer_validated',r['status']
 print('VALIDATED',name,flush=True)

run('33-replay-off',False)
run('33-replay-on',True)
run('33-transport-smoke',False,('detailed',))
for i,arms in enumerate([(False,True),(True,False),(False,True)],1):
 for enabled in arms:run(f'33-nav-{"on" if enabled else "off"}-{i}',enabled)
run('33-navigation-detailed',True,('detailed',))
subprocess.run([sys.executable,str(p/'analyze.py')],check=True)
