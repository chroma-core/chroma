import json,re,statistics
from pathlib import Path
root=Path(__file__).parent

def row(text, section):
    block=text.split('--- '+section+' ---')[1]
    return [x.strip() for x in re.search(r'^\|  1 \|(.+)\|$',block,re.M)[1].split('|')]
def number(x):
    return float(x.replace(',','').rstrip('KM')) * (1000 if x.endswith('K') else 1000000 if x.endswith('M') else 1)
def seconds(x):
    return float(re.search(r'[0-9.]+',x)[0]) * (0.001 if x.endswith('ms') else 60 if x.endswith('m') else 1)
results=[]
for p in sorted(root.glob('*.log')):
    if not re.match(r'(baseline|candidate)-[0-9]+\.log',p.name):continue
    text=p.read_text()
    m=re.search(r'Indexed .*? in ([\d.]+s).*?balance ([\d.]+s) \((\d+) iterations\).*?load ([\d.]+s).*?commit ([\d.]+s).*?reopen ([\d.]+m?s).*?total ([\d.]+s)',text)
    if not m:continue
    counts=row(text,'Task Counts');npa=row(text,'split() NPA Neighbor Stats')
    result=dict(run=p.stem,add_s=seconds(m[1]),balance_s=seconds(m[2]),rounds=int(m[3]),load_s=seconds(m[4]),commit_s=seconds(m[5]),reopen_s=seconds(m[6]),total_s=seconds(m[7]),splits=number(counts[3]),merges=number(counts[4]),reassigns=number(counts[5]),scrubs=number(counts[6]),avg_depth=float(npa[0]),warnings='without full convergence' in text)
    timing=(root/(p.stem+'.time')).read_text()
    result['peak_gib']=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',timing)[1])/1024**2
    warm=next(line for line in text.splitlines() if '|  warm |' in line)
    cells=[cell.strip() for cell in warm.split('|')][1:-1]
    result['recall_percent']=float(cells[17].rstrip('%'))
    result['warm_latency']=cells[18]
    results.append(result)
(root/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
for r in results:print(r)
