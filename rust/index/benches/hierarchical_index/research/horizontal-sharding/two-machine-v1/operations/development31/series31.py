import subprocess,os,sys,json
from pathlib import Path
p=Path(__file__).resolve().parent
kind='epoch31v3'
def run(name,mode='readiness',window='64',save_workers='32',flags=[]):
 env=dict(os.environ,SPANN_ACCEPT_MODE=mode,SPANN_INSERTION_NAVIGATION_WINDOW=window,SPANN_SAVE_WORKERS=save_workers)
 r=subprocess.run([sys.executable,str(p/'run.py'),name,kind,*flags],env=env)
 if r.returncode:raise SystemExit(r.returncode)
 receipt=json.loads((p/name/'exit.json').read_text());assert receipt['exit_code']==0,(name,receipt)
 data=json.loads((p/name/'owner1-receipts.json').read_text())
 if 'replay' in name:
  base=json.loads((p.parent/'development30/baseline-replay-v4/owner1-receipts.json').read_text())
  extract=lambda d:next(v for k,v in d.items() if k.endswith('/replay-results.json'))
  assert extract(data)==extract(base),'replay differs'
 else:
  result=json.loads(next(v for k,v in data.items() if k.endswith('/result.json')))
  assert result['status'] in ['writer_validated','construction_save_and_fresh_reader_validated'],result['status']
 print('VALIDATED',name,flush=True)
mode=sys.argv[1]
if mode=='construction':
 run('combined-replay')
 for i in range(1,4):
  arms=[('control','poll_1ms','64'),('readiness','readiness','64'),('window256','readiness','256')]
  if i==2:arms.reverse()
  for name,accept,window in arms:run(name+'-'+str(i),accept,window)
 run('readiness-detailed',flags=['detailed'])
elif mode=='saving':
 run('save-serial',save_workers='1',flags=['save'])
 run('save-parallel',save_workers='32',flags=['save'])
