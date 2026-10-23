from pathlib import Path
import json,statistics,math,subprocess,sys
p=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(p/'analyze.py')],check=True,stdout=subprocess.DEVNULL)
rows=json.loads((p/'comparison.json').read_text());by={r['name']:r for r in rows};out={'pairs':6,'same_binary_and_diagnostics_in_both_arms':True,'coordination_enabled':True,'metrics':{},'note':'Six exploratory paired repetitions; verification extension declared after observing pairs1–2; tree evolution changes operation counts. Inclusive CPU scopes overlap. Control concurrency1 versus candidate2; detailed disabled.'}
pairs=[(by[f'34-group-off-{i}'],by[f'34-group-on-{i}']) for i in range(1,7)]
for key in ['insertion_s','balance_s','construction_s','balance_cpu_s','insertion_cpu_s']:
 a=[x[key] for x,y in pairs];b=[y[key] for x,y in pairs];d=[y-x for x,y in zip(a,b)];margin=2.57058183564*statistics.stdev(d)/math.sqrt(6)
 out['metrics'][key]={'control_mean':statistics.mean(a),'candidate_mean':statistics.mean(b),'control_range':[min(a),max(a)],'candidate_range':[min(b),max(b)],'paired_differences':d,'mean_difference':statistics.mean(d),'approximate_95pct_paired_t_interval':[statistics.mean(d)-margin,statistics.mean(d)+margin]}
out['mean_work']={k:{'control':statistics.mean(x['work'][k] for x,y in pairs),'candidate':statistics.mean(y['work'][k] for x,y in pairs)} for k in pairs[0][0]['work'] if not k.startswith('Boundary')}
out['subsets']={label:{key:{'control_mean':statistics.mean(x[key] for x,y in subset),'candidate_mean':statistics.mean(y[key] for x,y in subset),'paired_differences':[y[key]-x[key] for x,y in subset]} for key in out['metrics']} for label,subset in [('initial',pairs[:3]),('verification',pairs[3:])]}
(p/'group-six-summary.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out['metrics'],indent=2))
