import json,re
from pathlib import Path
root=Path(__file__).parent
units={'ns':1e-6,'us':1e-3,'ms':1,'s':1000,'m':60000}
def latency(s):
    s=s.split()[0]
    m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m)',s)
    return float(m[1])*units[m[2]]
rows=[]
for n in range(1,11):
    p=root/f'read-{n}.log'
    text=p.read_text()
    match=re.search(r'Reader: (.*?) nodes \| open (\S+)',text)
    depth=re.search(r'Recall \(.*?depth=(\d+)\)',text)
    row={'checkpoint':n,'vectors':n*1000000,'nodes_rounded':match[1],'reader_open_ms':latency(match[2]),'depth':int(depth[1])}
    for phase in ['cold','warm']:
        line=next(l for l in text.splitlines() if re.search(r'\|\s*'+phase+r'\s*\|',l))
        cells=[c.strip() for c in line.split('|')[1:-1]]
        times=dict(zip(['navigation_ms','query_quantization_ms','posting_scoring_ms','selection_ms','rerank_ms'],map(latency,cells[-6:-1])))
        total=latency(cells[-7])
        row[phase]={'total_ms':total,**times,'unattributed_ms':total-sum(times.values()),'recall_at100_pct':float(cells[-8].removesuffix('%')),'scanned_rounded':cells[-12],'raw_row':line}
    timing=p.with_suffix('.time').read_text()
    row['max_rss_kib']=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',timing)[1])
    row['exit_status']=int(re.search(r'Exit status: (\d+)',timing)[1])
    assert row['exit_status']==0
    rows.append(row)
(root/'read-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
    print(r['checkpoint'],r['depth'],r['nodes_rounded'],r['reader_open_ms'],r['cold']['total_ms'],r['warm']['total_ms'],r['warm']['navigation_ms'],r['warm']['posting_scoring_ms'],r['warm']['scanned_rounded'],r['warm']['recall_at100_pct'])
