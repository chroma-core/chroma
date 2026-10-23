import re,json,pathlib,sys
r=pathlib.Path(sys.argv[1])
def seconds(s):
 s=s.strip()
 if s.endswith('µs'): return float(s[:-2])/1e6
 if s.endswith('ms'): return float(s[:-2])/1000
 if s.endswith('s'): return float(s[:-1])
 if s.endswith('m'): return float(s[:-1])*60
 raise ValueError(s)
rows=[]
for p in sorted(r.glob('*.log')):
 txt=p.read_text(errors='replace')
 cps=re.findall(r'Indexed .*? vec in (.*?) \(.*? vec/s\) \| balance (.*?) \((\d+) iterations\) \| load (.*?) \| commit (.*?) \| reopen (.*?) \| total (.*?)\n',txt)
 if not cps: continue
 totals=[sum(seconds(c[i]) for c in cps) for i in [0,1,3,4,5,6]]
 summary=re.search(r'Total vectors: .*? \| Total time: (.*?) \| Overall: ([0-9]+) vec/s',txt)
 timing=p.with_suffix('.time').read_text() if p.with_suffix('.time').exists() else ''
 wall=re.search(r'Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([0-9:.]+)',timing)
 wall_seconds=None
 if wall:
  wall_seconds=0.0
  for v in wall.group(1).split(':'): wall_seconds=wall_seconds*60+float(v)
 commit_phases=re.findall(r'Commit phases: fork (.*?) \(rss .*? \| flush (.*?) \(rss',txt)
 rss=re.search(r'Maximum resident set size \(kbytes\): (\d+)',p.with_suffix('.time').read_text() if p.with_suffix('.time').exists() else '')
 recall=[line.strip() for line in txt.splitlines() if re.search(r'cold|warm',line) and ('|' in line)]
 rows.append(dict(run=p.stem,wall=wall_seconds,fork=sum(seconds(x[0]) for x in commit_phases),flush=sum(seconds(x[1]) for x in commit_phases),checkpoints=len(cps),add=totals[0],balance=totals[1],load=totals[2],commit=totals[3],reopen=totals[4],phase_total=totals[5],build=1000000/int(summary.group(2)) if summary else None,reported_build=summary.group(1) if summary else None,index_phases=sum(totals[:2])+sum(totals[3:5]),peak_rss_gib=int(rss.group(1))/1048576 if rss else None,cp1_add=seconds(cps[0][0]),cp1_balance=seconds(cps[0][1]),recall=recall))
print(json.dumps(rows,indent=2))
