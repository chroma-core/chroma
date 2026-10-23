import json,re
from pathlib import Path
root=Path(__file__).parent
text=(root/'run.log').read_text()
units={'ns':1e-9,'us':1e-6,'ms':1e-3,'s':1,'m':60,'h':3600}
def duration(s):
    if s.strip()=='-': return 0.
    m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m|h)',s.strip())
    if not m: raise ValueError(s)
    return float(m[1])*units[m[2]]
def size(s):
    if s=='-': return 0
    m=re.fullmatch(r'([\d.]+)(B|KB|MB|GB|TB)',s)
    return float(m[1])*1024**['B','KB','MB','GB','TB'].index(m[2])
pattern=re.compile(r'Indexed .*? vec in (\S+) \((\d+) vec/s\) \| balance (\S+) \((\d+) iterations\) \| load (\S+) \| commit (\S+) \| reopen (\S+) \| total (\S+)')
rows=[]
for i,block in enumerate(re.split(r'--- Checkpoint \d+ \([^\n]+---',text)[1:],1):
    m=pattern.search(block)
    if not m: continue
    a,t,b,r,l,c,o,total=m.groups()
    row={'checkpoint':i,'vectors':i*1_000_000,'add_s':duration(a),'add_vectors_per_s':int(t),'balance_s':duration(b),'rounds':int(r),'load_s':duration(l),'commit_s':duration(c),'open_s':duration(o),'total_s':duration(total)}
    fork=re.search(r'Commit phases: fork (\S+) .*?\| flush (\S+)',block)
    if fork: row.update(commit_prepare_s=duration(fork[1]),flush_s=duration(fork[2]))
    peak=re.search(r'cp peak (\S+)',block)
    if peak: row['peak_rss_bytes']=size(peak[1])
    init=re.search(r'Add navigation initialization: (\S+)',block)
    if init: row['navigation_init_s']=duration(init[1])
    for label in ['Lazy IO:','Writer mem:','Reader pins (balanced):','GC:']:
        line=next((l.strip() for l in block.splitlines() if l.strip().startswith(label)),None)
        row[label[:-1]]=line
    rows.append(row)
summary={'revision':(root/'revision').read_text().strip(),'checkpoints':rows,'cap_warnings':[l for l in text.splitlines() if 'warn' in l.lower() or 'limit' in l.lower()],'read_rows':[l for l in text.splitlines() if re.search(r'\|\s*(cold|warm)\s*\|',l)]}
for line in text.splitlines():
    if line.startswith('Total vectors:') or line.startswith('Total entries:') or 'Reader: ' in line: print(line)
    if 'Tree depth' in line: print(line)
summary['phase_totals']={k:sum(r[k] for r in rows) for k in ['add_s','balance_s','load_s','commit_prepare_s','flush_s','open_s','total_s']}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for r in rows: print(r['checkpoint'],r['add_s'],r['balance_s'],r['rounds'],r['load_s'],r['commit_prepare_s'],r['flush_s'],r['open_s'],round(r['peak_rss_bytes']/2**30,2))
