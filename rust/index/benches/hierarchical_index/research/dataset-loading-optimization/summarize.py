import json, re, statistics
from pathlib import Path
root=Path(__file__).parent
out={}
for dataset in ['msmarco','wikipedia']:
    out[dataset]={}
    for variant in ['baseline','candidate']:
        runs=[]
        for trial in range(1,4):
            log=(root/f'{dataset}-{variant}-{trial}.log').read_text()
            match=re.search(r'total .*seconds=([0-9.]+) digest=([0-9a-f]+)',log)
            assert match, (dataset,variant,trial)
            timing=(root/f'{dataset}-{variant}-{trial}.time').read_text()
            rss=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',timing)[1])
            runs.append({'seconds':float(match[1]),'digest':match[2],'peak_rss_kib':rss})
        out[dataset][variant]={'runs':runs,'median_seconds':statistics.median(r['seconds'] for r in runs),'median_peak_rss_mib':statistics.median(r['peak_rss_kib'] for r in runs)/1024}
    assert len({r['digest'] for v in ['baseline','candidate'] for r in out[dataset][v]['runs']})==1
    old=out[dataset]['baseline']['median_seconds'];new=out[dataset]['candidate']['median_seconds']
    out[dataset]['reduction_pct']=100*(1-new/old)
    out[dataset]['speedup']=old/new
(root/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
