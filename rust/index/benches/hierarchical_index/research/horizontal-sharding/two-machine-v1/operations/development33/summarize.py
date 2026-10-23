from pathlib import Path
import json,statistics,math,subprocess,sys
p=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(p/'analyze.py')],check=True,stdout=subprocess.DEVNULL)
rows=json.loads((p/'comparison.json').read_text());by={r['name']:r for r in rows};out={'pairs':3,'transport_in_both_arms':True,'coordination_enabled':True,'metrics':{},'note':'Three paired repetitions; tree evolution changes operation counts. Inclusive CPU scopes overlap. No isolated transport performance claim.'}
pairs=[(by[f'33-nav-off-{i}'],by[f'33-nav-on-{i}']) for i in range(1,4)]
for key in ['insertion_s','balance_s','construction_s','balance_cpu_s','insertion_cpu_s']:
 a=[x[key] for x,y in pairs];b=[y[key] for x,y in pairs];d=[y-x for x,y in zip(a,b)];margin=4.30265272975*statistics.stdev(d)/math.sqrt(3)
 out['metrics'][key]={'control_mean':statistics.mean(a),'candidate_mean':statistics.mean(b),'control_range':[min(a),max(a)],'candidate_range':[min(b),max(b)],'paired_differences':d,'mean_difference':statistics.mean(d),'approximate_95pct_paired_t_interval':[statistics.mean(d)-margin,statistics.mean(d)+margin]}
out['mean_work']={k:{'control':statistics.mean(x['work'][k] for x,y in pairs),'candidate':statistics.mean(y['work'][k] for x,y in pairs)} for k in pairs[0][0]['work'] if not k.startswith('Boundary')}
(p/'navigation-summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['metrics'],indent=2))
