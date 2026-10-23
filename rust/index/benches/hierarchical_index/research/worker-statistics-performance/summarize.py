import json,re,statistics
from pathlib import Path
root=Path(__file__).parent
units={'ns':1e-9,'us':1e-6,'ms':.001,'s':1,'m':60}
def seconds(value):
    match=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m)',value)
    return float(match[1])*units[match[2]]
runs=[]
for variant in ('baseline','candidate'):
    for repeat in range(1,4):
        path=root/f'{variant}-{repeat}.log'
        if not path.exists(): continue
        text=path.read_text()
        checkpoint=re.search(r'Indexed 1.00M vec in (\S+) \([^\n]+?\) \| balance (\S+) \((\d+) iterations\) \| load (\S+) \| commit (\S+) \| reopen (\S+) \| total (\S+)',text)
        if not checkpoint: continue
        values=checkpoint.groups()
        row={'variant':variant,'repeat':repeat}
        for name,value in zip(['add_s','balance_s','rounds','load_s','commit_s','reopen_s','checkpoint_s'],values):
            row[name]=int(value) if name=='rounds' else seconds(value)
        collection=re.search(r'Buffered add: collect ([\d.]+)s \| regroup and flush ([\d.]+)s \| total add ([\d.]+)s',text)
        row.update(collection_s=float(collection[1]),regroup_flush_s=float(collection[2]),precise_add_s=float(collection[3]))
        commit=re.search(r'Commit phases: fork (\S+) .*?\| flush (\S+)',text)
        row.update(prepare_s=seconds(commit[1]),flush_s=seconds(commit[2]))
        for phase in ['cold','warm']:
            line=next((l for l in text.splitlines() if re.search(r'\|\s*'+phase+r'\s*\|',l)),None)
            if line:
                columns=[s.strip() for s in line.split('|')[1:-1]]
                row[phase+'_ms']=seconds(columns[-7].split()[0])*1000
                row[phase+'_recall_pct']=float(columns[-8].removesuffix('%'))
        row['valid_ids_complete']='Unique vectors: 10.00M (0 orphaned)' in text
        timing=root/f'{variant}-{repeat}.time'
        if timing.exists():
            data=timing.read_text()
            peak=re.search(r'Maximum resident set size \(kbytes\): (\d+)',data)
            if peak: row['peak_gib']=int(peak[1])/(1024*1024)
            row['exit_ok']='Exit status: 0' in data
        runs.append(row)
medians={variant:{metric:statistics.median(row[metric] for row in runs if row['variant']==variant and metric in row) for metric in ['precise_add_s','collection_s','regroup_flush_s','balance_s','load_s','prepare_s','flush_s','checkpoint_s','peak_gib'] if any(row['variant']==variant and metric in row for row in runs)} for variant in ['baseline','candidate']}
summary={'runs':runs,'medians':medians}
if medians['baseline'] and medians['candidate']:
    summary['reductions_pct']={metric:100*(1-medians['candidate'][metric]/medians['baseline'][metric]) for metric in medians['baseline'].keys()&medians['candidate'].keys()}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
