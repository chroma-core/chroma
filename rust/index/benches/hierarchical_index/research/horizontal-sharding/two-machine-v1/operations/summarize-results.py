#!/usr/bin/env python3
"""Summarize retained receipts without accessing or modifying remote resources."""
import json
import argparse
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--window',default='window-epoch2',choices=['window-epoch2','window-epoch3','window-epoch4','window-epoch5','window-epoch6','window-epoch7','window-epoch8','window-epoch9']);args=parser.parse_args()
P=Path(__file__).resolve().parent/args.window
out=[]
for folder in sorted(P.iterdir()):
 if not folder.is_dir() or not (folder/'run-receipts.json').exists():continue
 files=json.loads((folder/'run-receipts.json').read_text());result=next((json.loads(v) for k,v in files.items() if k.endswith('/result.json')),None);process=next((json.loads(v) for k,v in files.items() if k.endswith('/exit.json')),None)
 row={'attempt':folder.name,'process':process}
 if result is None:
  row['status']='no_validated_result';row['stderr']=[v for k,v in files.items() if k.endswith('/stderr')];out.append(row);continue
 r=result;row['rows']=r['rows'];row['status']=r.get('status','reference_result');row['settings']=r['settings']
 if 'phases' in r:
  phases=r['phases'];row.update(add_seconds=phases['add']['wall_seconds'],balance_seconds=phases['balance']['wall_seconds'],placement_seconds=0,save_seconds=phases['commit']['wall_seconds']+phases['flush']['wall_seconds'],construction_save_seconds=r['build_and_save_total']['wall_seconds'],construction_cpu_seconds=phases['add']['process_cpu_seconds']+phases['balance']['process_cpu_seconds'],shape=r['shape'],validation=r['validation'],work=r['work'])
 else:
  cycles=r['cycles'];row.update(add_seconds=sum(c['add_seconds'] for c in cycles),balance_seconds=sum(c['balance_seconds'] for c in cycles),placement_seconds=sum(c['placement_seconds'] for c in cycles),save_seconds=r['save_seconds'],construction_save_seconds=r['construction_and_save_seconds'],construction_seconds=r['construction_seconds'],shape=r['routing_shape'],owners=r['owners'],minimum_owner_share=r['minimum_owner_share'],rpc=r['rpc_accounting'])
  row['cycles']=[{k:c[k] for k in ['rows_before','rows_after','add_seconds','balance_seconds','placement_seconds','before_add','after_add','before_placement','after_placement','rpc_phases']} for c in cycles]
  row['phase_process_cpu_seconds']={}
  row['phase_rpc_framed_bytes']={}
  for phase,before_key,after_key in [('add','before_add','after_add'),('balance','after_add','before_placement'),('placement','before_placement','after_placement')]:
   deltas=[]
   for cycle in cycles:
    before={v['owner']:v for v in cycle[before_key]};after={v['owner']:v for v in cycle[after_key]}
    if set(before)!=set(after):raise ValueError('Owner coverage changed between phase snapshots')
    for owner in before:
     first=before[owner]['process_cpu_seconds'];last=after[owner]['process_cpu_seconds']
     if first is None or last is None:raise ValueError('Linux process CPU measurement missing')
     if last<first:raise ValueError('Process CPU counter went backwards')
     deltas.append(last-first)
   row['phase_process_cpu_seconds'][phase]=sum(deltas)
   row['phase_rpc_framed_bytes'][phase]=sum(int(c['rpc_phases'][phase]['after']['application_framed_bytes_total'])-int(c['rpc_phases'][phase]['before']['application_framed_bytes_total']) for c in cycles)
  row['cpu_accounting']='Sum of per-owner process CPU snapshot differences; includes between-snapshot bookkeeping and counts RPC. These are interval measurements, not exclusive kernel attribution.'
  row['rpc_accounting_scope']='Coordinator endpoint counts both directions once, excluding TCP/IP headers and retransmissions. Do not add peer endpoint counters.'

 out.append(row)
(P/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
for r in out:
 print(json.dumps({k:v for k,v in r.items() if k in ['attempt','rows','status','add_seconds','balance_seconds','placement_seconds','save_seconds','construction_seconds','construction_save_seconds','minimum_owner_share','stderr']}))
