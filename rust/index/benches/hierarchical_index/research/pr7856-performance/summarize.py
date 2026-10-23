import json,re,sys
from pathlib import Path
root=Path(__file__).parent
units={'ns':1e-9,'us':1e-6,'ms':1e-3,'s':1,'m':60,'h':3600}
def duration(s):
 if s.strip()=="-": return 0.0
 m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m|h)',s.strip())
 if not m: raise ValueError(s)
 return float(m[1])*units[m[2]]
pattern=re.compile(r'Indexed .*? vec in (\S+) \([^)]*\) \| balance (\S+) \((\d+) iterations\) \| load (\S+) \| commit (\S+) \| reopen (\S+) \| total (\S+)')
rows=[]
for p in sorted(root.glob('*-*-*.log')):
 text=p.read_text()
 matches=pattern.findall(text)
 if len(matches)!=10 or 'Total vectors:' not in text: continue
 cp=[]
 for m in matches:
  cp.append(dict(zip(['add','balance','iterations','load','commit','reopen','checkpoint_total'],[duration(m[0]),duration(m[1]),int(m[2]),*[duration(x) for x in m[3:]]])))
 dataset,variant,rep=p.stem.split('-')
 throughput=float(re.search(r'Overall: (\d+) vec/s',text)[1])
 summary={'name':p.stem,'dataset':dataset,'variant':variant,'rep':int(rep),'build_seconds_approx':1e6/throughput,**{k:sum(c[k] for c in cp) for k in cp[0]}}
 summary['add_balance']=summary['add']+summary['balance']
 summary['checkpoint_total_no_load']=summary['checkpoint_total']-summary['load']
 tables=re.search(r'--- Task Total Time ---\n(.*?)\n---',text,re.S)
 if tables:
  data=[]
  for line in tables[1].splitlines():
   cells=[s.strip() for s in line.split('|')[1:-1]]
   if cells and cells[0].isdigit(): data.append(cells)
  summary['merge_worker_seconds']=sum(duration(c[5]) for c in data)
  summary['reassign_worker_seconds']=sum(duration(c[6]) for c in data)
 counts=re.search(r'--- Task Counts ---\n(.*?)\n---',text,re.S)
 if counts:
  summary['merge_calls']=sum(int(line.split('|')[6].strip().replace(',','')) for line in counts[1].splitlines() if re.match(r'\|\s*\d+',line))
 recall=re.findall(r'\|\s*1\.50\s*\|\s*1x\s*\|\s*cold\s*\|.*',text)
 summary['recall_row']=recall[-1] if recall else None
 final=re.search(r'Total entries: (.*)',text)
 summary['tree_entries']=final[1] if final else None
 timing=p.with_suffix('.time')
 if timing.exists():
  t=timing.read_text()
  summary['max_rss_kib']=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',t)[1])
  summary['exit_code']=int(re.search(r'Exit status: (\d+)',t)[1])
 rows.append(summary)
(root/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
 print(r['name'], 'build',round(r['build_seconds_approx'],2),'add',round(r['add'],3),'balance',round(r['balance'],3),'load',round(r['load'],3),'merge_worker',round(r.get('merge_worker_seconds',0),3),'merge_calls',r.get('merge_calls'))
