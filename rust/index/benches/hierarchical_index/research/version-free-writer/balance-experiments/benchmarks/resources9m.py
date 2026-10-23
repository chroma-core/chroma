import json,re,sys
from pathlib import Path
p=Path(sys.argv[1]);out=[]
for f in sorted(p.glob('wikipedia-*-9m-*.result.json')):
 d=json.loads(f.read_text());name=f.name.removesuffix('.result.json');log=(p/f'{name}.log').read_bytes();offset=log.index(b'  Serial insertion preflight:')
 markers=[json.loads(x) for x in (p/f'{name}.lines.jsonl').read_text().splitlines()];end=next(x['unix'] for x in markers if x['offset']==offset)
 data=next(x for x in json.loads((p/'analysis.json').read_text()) if x['name']==name);wall=data['phases_seconds']['Balance'];start=end-wall
 samples=[json.loads(x) for x in (p/f'{name}.resources.jsonl').read_text().splitlines()];samples=[s for s in samples if start<=s['unix']<=end and 'stat' in s]
 def cpu(s):
  z=s['stat'].rsplit(')',1)[1].split();return (int(z[11])+int(z[12]))/100
 steps=[{'unix':z['unix'],'cpu_cores':(cpu(z)-cpu(a))/(z['unix']-a['unix'])} for a,z in zip(samples,samples[1:])]
 record={'name':name,'balance_seconds':wall,'window_method':'Output timestamp immediately after joined add/balance, minus exclusive balance duration; one resumed checkpoint/batch. Sampling window endpoint has output latency.','sampled_seconds':samples[-1]['unix']-samples[0]['unix'] if len(samples)>1 else 0,'sample_cpu_seconds':cpu(samples[-1])-cpu(samples[0]) if len(samples)>1 else 0,'steps':steps}
 if steps:record['steps_below_2cores']=sum(x['cpu_cores']<2 for x in steps);record['mean_cores']=sum(x['cpu_cores'] for x in steps)/len(steps)
 out.append(record)
(p/'resources9m-analysis.json').write_text(json.dumps(out,indent=2))
for x in out:print(x['name'],x['balance_seconds'],x['sample_cpu_seconds'],x.get('mean_cores'),x.get('steps_below_2cores'))
