import os,sys,json,subprocess
from pathlib import Path
p=Path(__file__).resolve().parent
plan=json.loads((p/'experiment-design.json').read_text())
for i,order in enumerate(plan['order'],1):
 for arm in order.split('-'):
  name=f'32-gate-{arm}-{i}'
  env=dict(os.environ,SPANN_ACCEPT_MODE='readiness',SPANN_INSERTION_WORKERS='64',SPANN_SERIALIZE_BOUNDARY_REFRESH='1' if arm=='on' else '0',SPANN_INSERTION_NAVIGATION_WINDOW='64',SPANN_SAVE_WORKERS='32')
  if not (p/name/'exit.json').exists():subprocess.run([sys.executable,str(p/'run.py'),name,'epoch31v4'],env=env,check=True)
  assert json.loads((p/name/'exit.json').read_text())['exit_code']==0
  data=json.loads((p/name/'owner1-receipts.json').read_text());r=json.loads(next(v for k,v in data.items() if k.endswith('/result.json')))
  assert r['status']=='writer_validated' and r['rows']==3000000
  print('VALIDATED',name,flush=True)
