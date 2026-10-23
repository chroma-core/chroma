"""Exact replays, three predeclared interleaved pairs and separate detailed validation."""
import os,sys,json,subprocess
from pathlib import Path
p=Path(__file__).resolve().parent

def run(name,concurrency,flags=()):
 profile=json.loads((p/'accepted-profile.json').read_text());env=dict(os.environ,**profile['env'],SPANN_EXPANSION_GROUP_CONCURRENCY=str(concurrency));folder=p/name
 if not (folder/'owner1-receipts.json').exists():
  assert not folder.exists(),f'Partial attempt {name}: inspect and recover, never silently rerun'
  subprocess.run([sys.executable,str(p/'run.py'),name,'epoch34v2',*flags],env=env,check=True)
 assert json.loads((folder/'exit.json').read_text())['exit_code']==0
 d=json.loads((folder/'owner1-receipts.json').read_text())
 if 'replay' in name:
  old=json.loads((p.parent/'development30/baseline-replay-v4/owner1-receipts.json').read_text());extract=lambda x:next(v for k,v in x.items() if k.endswith('/replay-results.json'));assert extract(d)==extract(old),'fixed navigation replay mismatch'
 else:
  r=json.loads(next(v for k,v in d.items() if k.endswith('/result.json')));assert r['status']=='writer_validated',r['status']
 print('VALIDATED',name,flush=True)
run('34-replay-off',1)
run('34-replay-on',2)
for i,arms in enumerate([(1,2),(2,1),(1,2)],1):
 for concurrency in arms:run(f'34-group-{"on" if concurrency==2 else "off"}-{i}',concurrency)
run('34-groups-detailed',2,('detailed',))
subprocess.run([sys.executable,str(p/'summarize.py')],check=True)
subprocess.run([sys.executable,str(p/'inspect-diagnostic.py'),'34-groups-detailed'],check=True)
