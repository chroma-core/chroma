from pathlib import Path
import re,json

def seconds(x):
 x=x.strip()
 if x=='-': return 0.
 m=re.fullmatch(r'([0-9.]+)(ns|us|ms|s|m)',x)
 if not m: raise ValueError(x)
 return float(m[1])*{'ns':1e-9,'us':1e-6,'ms':.001,'s':1,'m':60}[m[2]]
def table(text,title):
 block=text.split('--- '+title+' ---\n')[1].split('\n\n')[0]
 lines=[line for line in block.splitlines() if line.startswith('|')]
 head=[x.strip() for x in lines[0].strip('|').split('|')]
 return [dict(zip(head,[x.strip() for x in line.strip('|').split('|')])) for line in lines[2:]]
for path in sorted(Path(__file__).parent.glob('*.log')):
 t=path.read_text()
 if 'Parallel Balance Exclusive Worker Time' not in t: continue
 rows=table(t,'Parallel Balance Exclusive Worker Time')
 phases=table(t,'Parallel Balance Phase Time')
 print(path.name)
 for selection, selected in [('all',rows),('after-first',rows[1:])] if len(rows)>2 else [('all',rows)]:
  totals={k:sum(seconds(row[k]) for row in selected) for k in rows[0] if k!='CP'}
  stages={k:v for k,v in totals.items() if k not in ['worker time','slowest workers','assigned slack']}
  denom=sum(stages.values())
  print(selection,'phase wall',sum(seconds(row['phase wall']) for row in (phases[1:] if selection=='after-first' else phases)), 'exclusive seconds',round(denom,3))
  print({k:(round(v,3),round(v/denom*100,2)) for k,v in sorted(stages.items(),key=lambda kv:-kv[1])})
 print('reassignment per checkpoint',[(r['CP'],r['reassign_navigation'],r['register_posting_load'],r['reassign_register'],r['reassign_embedding'],r['reassign_version']) for r in rows])
