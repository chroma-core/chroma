import subprocess,sys,json
from pathlib import Path
p=Path(__file__).resolve().parent
kind=sys.argv[1]
for item in sys.argv[2:]:
 parts=item.split(':');name=parts[0];flags=parts[1:]
 r=subprocess.run([sys.executable,str(p/'run.py'),name,kind,*flags])
 if r.returncode:raise SystemExit(r.returncode)
 state=json.loads((p/name/'exit.json').read_text())
 if state['exit_code']!=0:raise SystemExit('Preserved failed arm: '+name)
 if 'replay' in name and kind in ['candidate','transport1']:
  def result(folder):
   receipts=json.loads((p/folder/'owner1-receipts.json').read_text())
   return next(v for k,v in receipts.items() if k.endswith('/replay-results.json'))
  assert result(name)==result('baseline-replay-v4'), 'Fixed navigation replay differs from baseline'
  (p/name/'comparison.json').write_text(json.dumps({'exact_replay_match':True,'queries':256,'baseline':'baseline-replay-v4'}))
 print('VALIDATED_PROCESS',name,flush=True)
