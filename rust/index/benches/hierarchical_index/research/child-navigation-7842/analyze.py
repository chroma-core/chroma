import glob,re,json
from pathlib import Path

def seconds(v):
    if v=='-': return 0
    m=re.fullmatch(r'([0-9.]+)(ms|us|ns|s|m)',v.strip())
    return float(m[1])*{'m':60,'s':1,'ms':.001,'us':1e-6,'ns':1e-9}[m[2]]
rows=[]
for f in sorted(glob.glob('replay-*.log')+glob.glob('full-*.log')):
    s=Path(f).read_text()
    cps=re.findall(r'Indexed 100.0K vec in (\S+) .*?\| balance (\S+) .*?\| load (\S+) \| commit (\S+) \| reopen (\S+) \| total (\S+)',s)
    totals=dict(zip(['add','balance','source_load','commit','reopen','total'],[round(sum(seconds(c[i]) for c in cps),3) for i in range(6)]))
    stages=[]
    for l in s.splitlines():
        if re.match(r'\|\s+\d+\s+\|',l):
            a=[x.strip() for x in l.split('|')[1:-1]]
            if len(a)==18:
                stages.append(dict(zip(['worker','scrub','registration_load'],[seconds(a[i]) for i in [1,5,17]])))
    totals.update({k:round(sum(x[k] for x in stages),3) for k in ['worker','scrub','registration_load']})
    recall=[]
    for l in s.splitlines():
        if '|  cold |' in l:
            p=re.findall(r'([0-9.]+)%',l)
            recall.append(float(p[4]))
    t=Path(f.replace('.log','.time'))
    rss=re.search(r'Maximum resident set size \(kbytes\): (\d+)',t.read_text()) if t.exists() else None
    rows.append({'run':f[:-4],'checkpoints':len(cps),**totals,'cold_recall':recall,'peak_rss_gib':round(int(rss[1])/1024**2,3) if rss else None})
print(json.dumps(rows,indent=2))
Path('summary.json').write_text(json.dumps(rows,indent=2)+'\n')
