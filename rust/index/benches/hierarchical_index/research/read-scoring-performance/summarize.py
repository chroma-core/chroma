from pathlib import Path
import re, json, statistics
root=Path(__file__).parent
rows=[]
def ms(value):
    m=re.search(r'([\d.]+)(ns|us|ms|s)',value)
    return float(m[1])*{'ns':1e-6,'us':1e-3,'ms':1,'s':1e3}[m[2]]
for path in sorted(root.glob('*-*-*-*.log')):
    m=re.fullmatch(r'(wikipedia|msmarco)-(\d+)-(baseline|candidate)-(\d+)\.log',path.name)
    if not m: continue
    for line in path.read_text().splitlines():
        if not re.match(r'\s*\|\s*1\.50',line): continue
        fields=[x.strip() for x in line.split('|')[1:-1]]
        row=dict(dataset=m[1], workers=int(m[2]), version=m[3],repeat=int(m[4]), phase=fields[2], recall=fields[14],total_ms=ms(fields[15]), navigation_ms=ms(fields[16]), quantize_ms=ms(fields[17]),scoring_ms=ms(fields[18]),sort_ms=ms(fields[19]),scanned=fields[10])
        rows.append(row)
summary=[]
for dataset in ['wikipedia','msmarco']:
    for workers in [32,1]:
        for phase in ['cold','warm']:
            means={}
            for version in ['baseline','candidate']:
                values=[r for r in rows if r['dataset']==dataset and r['workers']==workers and r['phase']==phase and r['version']==version]
                means[version]={key:statistics.mean(r[key] for r in values) for key in ['total_ms','scoring_ms']}
            summary.append(dict(dataset=dataset,workers=workers,phase=phase,**means,improvement_pct=100*(1-means['candidate']['total_ms']/means['baseline']['total_ms'])))
            print(summary[-1])
checks=[]
for dataset in ['wikipedia','msmarco']:
    reference=sorted(l for l in (root/f'{dataset}-32-baseline-1.stderr').read_text().splitlines() if l.startswith('READ_RESULT'))
    for path in sorted(root.glob(f'{dataset}-*.stderr')):
        actual=sorted(l for l in path.read_text().splitlines() if l.startswith('READ_RESULT'))
        assert actual == reference, path.name
        checks.append(dict(file=path.name,query_results=len(actual),exact=True))
print('Exact result comparisons:',len(checks),'x',len(reference),'queries,100scoredreturnedneighbors each')
(root/'summary.json').write_text(json.dumps(dict(rows=rows,summary=summary,exact_results=checks),indent=2)+'\n')
