from pathlib import Path
import json,os,subprocess,sys
p=Path(__file__).resolve().parent;profile=json.loads((p/'accepted-profile.json').read_text());env=dict(os.environ,**profile['env']);name='34-diagnostic'
subprocess.run([sys.executable,str(p/'run.py'),name,'epoch34v1','detailed'],env=env,check=True)
r=json.loads((p/name/'owner1-receipts.json').read_text());result=json.loads(next(v for k,v in r.items() if k.endswith('/result.json')));assert result['status']=='writer_validated';print('DIAGNOSTIC_VALIDATED')
