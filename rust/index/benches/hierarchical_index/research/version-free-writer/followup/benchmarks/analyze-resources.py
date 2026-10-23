import json,re,sys
from pathlib import Path
r=Path(__file__).parent/'artifacts';name=sys.argv[1];log=(r/f'{name}.log').read_text();samples=[json.loads(x) for x in (r/f'{name}.resources.jsonl').read_text().splitlines()];samples=[s for s in samples if 'stat' in s]
def fields(s):
 v=s['stat'].rsplit(')',1)[1].split();io=dict((k,int(n)) for k,n in re.findall(r'(\w+): (\d+)',s['io']));return {'cpu':(int(v[11])+int(v[12]))/100,'minor':int(v[7]),'major':int(v[9]),'read':io['read_bytes'],'write':io['write_bytes']}
intervals=[]
for enter,leave in zip(re.findall(r'BENCHMARK_BALANCE_ENTER unix=([\d.]+).*?checkpoint=(\d+)',log),re.findall(r'BENCHMARK_BALANCE_EXIT unix=([\d.]+).*?checkpoint=(\d+)',log)):
 t0,cp=enter;t1,cp1=leave;assert cp==cp1;t0=float(t0);t1=float(t1)
 ss=[s for s in samples if t0<=s['unix']<=t1]
 if len(ss)<2:continue
 b=fields(ss[0]);e=fields(ss[-1]);steps=[]
 for a,z in zip(ss,ss[1:]):
  x,y=fields(a),fields(z);dt=z['unix']-a['unix'];threads=z.get('thread_stats',[]);states={}
  for t in threads:
   state=t['stat'].rsplit(')',1)[1].split()[0];states[state]=states.get(state,0)+1
  steps.append({'unix':z['unix'],'cpu_cores':(y['cpu']-x['cpu'])/dt,'read_bytes':y['read']-x['read'],'major_faults':y['major']-x['major'],'thread_states':states})
 intervals.append({'checkpoint':int(cp),'balance_wall':t1-t0,'sampled_wall':ss[-1]['unix']-ss[0]['unix'],'cpu_seconds':e['cpu']-b['cpu'],'major_faults':e['major']-b['major'],'read_bytes':e['read']-b['read'],'steps':steps})
(r/f'{name}.resource-analysis.json').write_text(json.dumps(intervals,indent=2)+'\n')
for d in intervals:print(d['checkpoint'],round(d['balance_wall'],3),'sampleCPU',round(d['cpu_seconds'],2),'majfault',d['major_faults'],'readMiB',round(d['read_bytes']/1024**2,1))
