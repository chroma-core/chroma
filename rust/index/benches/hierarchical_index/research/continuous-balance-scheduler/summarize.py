import json,re,statistics
from pathlib import Path
root=Path(__file__).parent

def seconds(s):
    if s.endswith('ms'): return float(s[:-2])/1000
    if s.endswith('us'): return float(s[:-2])/1000000
    if s.endswith('s'): return float(s[:-1])
    if s.endswith('m'): return float(s[:-1])*60
    raise ValueError(s)

rows=[]
for p in sorted((root/'raw').glob('*.log')):
    text=p.read_text(errors='replace')
    match=re.search(r'Indexed .*? in (\S+) .*?balance (\S+) \((\d+) (\w+)\) \| load (\S+) \| commit (\S+) \| reopen (\S+) \| total (\S+)',text)
    if not match: continue
    add,balance,work,kind,load,commit,reopen,total=match.groups()
    row=dict(run=p.stem,add=seconds(add),balance=seconds(balance),work=int(work),work_unit=kind,load=seconds(load),commit=seconds(commit),reopen=seconds(reopen),checkpoint=seconds(total))
    memory=re.search(r'cp peak ([\d.]+)GB',text)
    if memory: row['peak_gib']=float(memory[1])
    for line in text.splitlines():
        if re.search(r'\|\s+warm\s+\|',line):
            cols=[c.strip() for c in line.split('|')]
            row['recall']=float(cols[18].rstrip('%'))
            row['warm_ms']=seconds(cols[19])*1000
    occupancy=re.search(r'worker task time ([\d.]+)s / ([\d.]+)s available; (\d+) claim conflicts; (\d+) capped leaves; total budget exhausted: (\w+)',text)
    if occupancy:
        busy,available,conflicts,capped,exhausted=occupancy.groups()
        row.update(occupancy=float(busy)/float(available),conflicts=int(conflicts),capped=int(capped),exhausted=exhausted)
    counts=re.search(r'--- Task Counts ---\n.*\n.*\n(.*)',text)
    if counts:
        cols=[c.strip() for c in counts[1].split('|')]
        row.update(splits=cols[5],merges=cols[6],reassigns=cols[7],scrubs=cols[8])
    rows.append(row)
(root/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for row in rows: print(row)
