import subprocess,sys,os,json
from pathlib import Path
p=Path(__file__).resolve().parent
env=dict(os.environ,SPANN_ACCEPT_MODE='readiness',SPANN_INSERTION_WORKERS='64',SPANN_SERIALIZE_BOUNDARY_REFRESH='1',SPANN_REFRESH_PREPARE_UNDER_TURN='0',SPANN_INSERTION_NAVIGATION_WINDOW='64',SPANN_SAVE_WORKERS='32',SPANN_NAVIGATION_PARENT_CACHE='1')
for i in range(1,4):
 name=f'33-size-diagnostic-{i}';r=subprocess.run([sys.executable,str(p/'run.py'),name,'epoch33v2'],env=env)
 d=p/name
 if not (d/'exit.json').exists():raise SystemExit('No exit receipt; inspect execution failure')
 status=json.loads((d/'exit.json').read_text());print('DIAGNOSTIC',name,status,flush=True)
 receipts=json.loads((d/'owner1-receipts.json').read_text());stderr='\n'.join(v for k,v in receipts.items() if k.endswith('/stderr'));print(stderr,flush=True)
 if status['exit_code']!=0:break
