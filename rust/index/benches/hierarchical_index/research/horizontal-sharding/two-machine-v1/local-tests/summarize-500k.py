"""Summarize retained-binary construction cycles; keep overlapping scopes separate."""
import json, sys
from pathlib import Path

def summarize(path):
    data=json.loads(Path(path).read_text())
    reference='arm' in data
    rows=[]
    for cycle in data['cycles']:
        if reference:
            balance=cycle['balance']; kernel=cycle['clustering_delta']
            row=dict(rows=cycle['rows_after'],add_seconds=cycle['add']['wall_seconds'],balance_seconds=balance['wall_seconds'],cpu_seconds=balance['process_cpu_seconds'],kernel=kernel,placement_seconds=0,peak_rss_bytes=balance['process_peak_rss_bytes'],shape=cycle['shape'],work=cycle['balance_work'])
        else:
            before={o['owner']:o for o in cycle['before_balance']}
            after={o['owner']:o for o in cycle['before_placement']}
            # Process CPU and kernel counters are local, so reject distributed construction.
            assert all(o['vectors']==0 for owner,o in after.items() if owner!=1)
            counters={};scopes={}
            for owner in [1]:
                for k,v in after[owner]['work']['counters'].items(): counters[k]=v-before[owner]['work']['counters'].get(k,0)
                for k,v in after[owner]['work']['inclusive_scopes'].items():
                    previous=before[owner]['work']['inclusive_scopes'].get(k,{})
                    scopes[k]={s:n-previous.get(s,0) for s,n in v.items() if not s.startswith('max_')}
            kernel=cycle['clustering_local_process_delta']
            row=dict(rows=cycle['rows_after'],add_seconds=cycle['add_seconds'],balance_seconds=cycle['balance_seconds'],cpu_seconds=cycle['coordinator_balance_cpu_seconds'],kernel=kernel,placement_seconds=cycle['placement_seconds'],peak_rss_bytes=cycle['peak_rss'],work=counters,inclusive_scopes=scopes,owners_before_placement={o['owner']:o['vectors'] for o in after.values()},owners_after_placement={o['owner']:o['vectors'] for o in cycle['after_placement']})
        row['kernel_cpu_seconds']=kernel['thread_cpu_nanos']/1e9
        row['non_kernel_cpu_seconds']=row['cpu_seconds']-row['kernel_cpu_seconds']
        row['effective_cpu_cores']=row['cpu_seconds']/row['balance_seconds']
        rows.append(row)
    totals={k:sum(c[k] for c in rows) for k in ['add_seconds','balance_seconds','cpu_seconds','kernel_cpu_seconds','non_kernel_cpu_seconds','placement_seconds']}
    totals['kernel']={k:sum(c['kernel'][k] for c in rows) for k in rows[0]['kernel']}
    totals['effective_cpu_cores']=totals['cpu_seconds']/totals['balance_seconds']
    tail={k:sum(c[k] for c in rows[1:]) for k in ['balance_seconds','cpu_seconds','kernel_cpu_seconds','non_kernel_cpu_seconds']}
    return dict(input_result=str(path),rows=data['rows'],reference=reference,cycles=rows,totals=totals,after_first_interval=tail,validation=data.get('validation',data.get('validation_report')),settings=data['settings'],final_shape=data.get('shape',data.get('routing_shape')),gross_build_save=data.get('build_and_save_total',data.get('construction_and_save_seconds')),save_seconds=(data['phases']['commit']['wall_seconds']+data['phases']['flush']['wall_seconds']) if reference else data['save_seconds'])

if __name__=='__main__':
    print(json.dumps(summarize(sys.argv[1]),indent=2))
