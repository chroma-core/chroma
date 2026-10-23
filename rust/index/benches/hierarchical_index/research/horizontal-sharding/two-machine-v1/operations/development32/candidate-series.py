import os,sys,json,subprocess
from pathlib import Path
p=Path(__file__).resolve().parent
plan=json.loads((p/'experiment-design.json').read_text())
def run(name,arm='on',flags=[]):
 env=dict(os.environ,SPANN_ACCEPT_MODE='readiness',SPANN_INSERTION_WORKERS='64',SPANN_SERIALIZE_BOUNDARY_REFRESH='0' if arm=='off' else '1',SPANN_REFRESH_PREPARE_UNDER_TURN='1' if arm=='early' else '0',SPANN_INSERTION_NAVIGATION_WINDOW='64',SPANN_SAVE_WORKERS='32')
 if not (p/name/'exit.json').exists():
  result=subprocess.run([sys.executable,str(p/'run.py'),name,'epoch32v2',*flags],env=env)
  if result.returncode:
   if (p/name/'exit.json').exists() and json.loads((p/name/'exit.json').read_text())['exit_code']==0:subprocess.run([sys.executable,str(p/'recollect.py'),name],check=True)
   else:raise SystemExit(result.returncode)
 assert json.loads((p/name/'exit.json').read_text())['exit_code']==0
 data=json.loads((p/name/'owner1-receipts.json').read_text())
 if 'replay' in name:
  baseline=json.loads((p.parent/'development30/baseline-replay-v4/owner1-receipts.json').read_text());extract=lambda d:next(v for k,v in d.items() if k.endswith('/replay-results.json'));assert extract(data)==extract(baseline)
 else:
  r=json.loads(next(v for k,v in data.items() if k.endswith('/result.json')))
  assert r['status']==('writer_checkpoint_complete' if 'checkpoint-after-add' in flags else 'writer_validated'),r.get('status')
  if 'balance-only' in flags:assert r['mode']=='balance_only' and r['resume_input']==3000000 and r['cycles'][0]['add_seconds']==0
 print('VALIDATED',name,flush=True)
run('32-instrumented-replay')
for i in range(1,4):run(f'32-instrumented-control-{i}')
run('32-post-insertion-checkpoint',flags=['checkpoint-after-add'])
subprocess.run([sys.executable,str(p/'verify-checkpoint.py')],check=True)
for i,arms in enumerate(plan['fixed_state_blocks'],1):
 for arm in arms:run(f'32-fixed-{arm}-{i}',arm,['balance-only'])
for arm in plan['fixed_state_detailed_arms']:run(f'32-fixed-{arm}-detailed',arm,['balance-only','detailed'])
for arm in plan['fixed_state_profile_arms']:run(f'32-fixed-{arm}-profile',arm,['balance-only','profile'])
