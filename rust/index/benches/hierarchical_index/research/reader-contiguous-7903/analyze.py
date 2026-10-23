from pathlib import Path
from collections import defaultdict
import json, re, statistics, sys
root=Path(__file__).parent

def ms(value):
    match=re.search(r'([0-9.]+)\s*(ns|us|µs|ms|s|m)', value)
    if not match: return None
    return float(match[1])*{'ns':1e-6,'us':.001,'µs':.001,'ms':1,'s':1000,'m':60000}[match[2]]

def count(value):
    match=re.match(r'\s*([0-9.]+)([KMG]?)',value)
    return float(match[1])*{'':1,'K':1000,'M':1e6,'G':1e9}[match[2]]

rows=[]; runs=[]
for p in sorted(root.glob('*-*.log')):
    if not re.fullmatch(r'(fixed-)?(baseline|candidate)-\d+',p.stem): continue
    text=p.read_text(); variant=p.stem.split('-')[-2]; cohort='fixed' if p.stem.startswith('fixed-') else 'build'
    run={'run':p.stem,'variant':variant,'cohort':cohort}
    for line in text.splitlines():
        if 'Reader:' in line and '| open ' in line:
            run['reader_open_ms']=ms(line.split('| open ')[1])
        if 'Indexed 1.00M vec in ' in line:
            for key,expr in [('add_ms','vec in '),('balance_ms','| balance '),('load_ms','| load '),('commit_ms','| commit '),('writer_reopen_ms','| reopen '),('checkpoint_ms','| total ')]:
                run[key]=ms(line.split(expr)[1])
        if 'Reader after open:' in line:run['reader_memory']=line.strip()
        if 'Build wall' in line:run['build_wall']=line.strip()
        if not re.match(r'\s*\|\s*[0-9]+\.[0-9]+\s*\|',line): continue
        c=[s.strip() for s in line.split('|')[1:-1]]
        if len(c)!=25 or c[2] not in ('cold','warm'):continue
        rows.append(dict(run=p.stem,variant=variant,cohort=cohort,tau=float(c[0]),rerank=int(c[1].rstrip('x')),phase=c[2],writer_diagnostic_reachable_recall_pct=float(c[10].rstrip('%')),final_recall_pct=float(c[17].rstrip('%')),leaf_beam=c[9],scanned_codes=count(c[13]),latency_ms=ms(c[18]),navigation_ms=ms(c[19]),posting_distance_ms=ms(c[21]),rerank_ms=ms(c[23])))
    section=re.search(r'--- Checkpoint 40 wall time \(exclusive seconds\) ---\n(.*?)\n--- Build',text,re.S)
    if section:
        total=re.search(r'  Total: ([0-9.]+)s',section[1])
        if total:run['checkpoint_wall_ms']=float(total[1])*1000
    measured=re.search(r'Build wall time reconciliation: measured=([0-9.]+)s',text)
    if measured:run['build_with_diagnostics_ms']=float(measured[1])*1000
    time=root/(p.stem+'.time')
    if time.exists():
        t=time.read_text()
        m=re.search(r'Maximum resident set size \(kbytes\): (\d+)',t)
        if m:run['process_peak_rss_gib']=int(m[1])/1024**2
    runs.append(run)
groups=defaultdict(list)
for r in rows:groups[(r['cohort'],r['variant'],r['tau'],r['rerank'],r['phase'])].append(r)
means=[]
for key,rs in sorted(groups.items()):
    item=dict(zip(['cohort','variant','tau','rerank','phase'],key));item['runs']=len(rs)
    for field in ['writer_diagnostic_reachable_recall_pct','final_recall_pct','scanned_codes','latency_ms','navigation_ms','posting_distance_ms','rerank_ms']:
        item[field]=statistics.mean(r[field] for r in rs)
    item['latency_range_ms']=[min(r['latency_ms'] for r in rs),max(r['latency_ms'] for r in rs)]
    means.append(item)
(root/'results.json').write_text(json.dumps(dict(runs=runs,rows=rows,means=means),indent=2)+'\n')
print(json.dumps(dict(runs=runs,means=means),indent=2))
