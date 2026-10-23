from pathlib import Path
import json,re,statistics,sys
root=Path(sys.argv[1])

def seconds(s):
 m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m|h)',s.strip()); assert m,s
 return float(m[1])*{'ns':1e-9,'us':1e-6,'ms':1e-3,'s':1,'m':60,'h':3600}[m[2]]

def summary(xs):
 return {'n':len(xs),'mean':statistics.mean(xs),'median':statistics.median(xs),'min':min(xs),'max':max(xs)}

out={'opens':{},'component_commits':{},'increments':[]}
for variant in ['baseline','candidate']:
 rows=[]
 for p in sorted(root.glob(f'open-{variant}-*.jsonl')):
  for line in p.read_text().splitlines():
   if line.startswith('{'):rows.append(json.loads(line))
 out['opens'][variant]={'rows':rows,'metrics':{k:summary([r[k] for r in rows]) for k in ['open_seconds','first_query_seconds','open_plus_first_query_seconds','navigation_payload_bytes']}}
 rows=[json.loads(line) for line in (root/f'commit-{variant}.jsonl').read_text().splitlines() if line.startswith('{')]
 out['component_commits'][variant]=rows
for run in ['baseline-1','candidate-upgrade-1','candidate-steady-1','candidate-steady-2','candidate-upgrade-2','baseline-2']:
 if not (root/f'{run}.exit-code').exists():continue
 assert (root/f'{run}.exit-code').read_text().strip()=='0',run
 log=(root/f'{run}.log').read_text()
 m=re.search(r'Indexed 1.00M vec in (\S+) .*?balance (\S+) .*?load (\S+) \| commit (\S+) \| reopen (\S+)',log);assert m,run
 row={'run':run,**dict(zip(['add_s','balance_s','load_s','commit_s','writer_reopen_s'],map(seconds,m.groups())))}
 m=re.search(r'Reader:.*?\| open (\S+) \|',log);assert m,run
 row['reader_open_s']=seconds(m[1])
 m=re.search(r'--- Checkpoint 40 wall time \(exclusive seconds\) ---\n(.*?)(?=\n---)',log,re.S);assert m,run
 row['checkpoint_wall_s']=float(re.search(r'  Total: ([\d.]+)s',m[1])[1])
 row['wall_phases']={k:float(v) for k,v in re.findall(r'^  (.+?): ([\d.]+)s$',m[1],re.M)}
 assert 'Unique vectors: 40.00M (0 orphaned)' in log,run
 row['recall_rows']=[]
 for line in log.splitlines():
  cells=[c.strip() for c in line.split('|')[1:-1]]
  if len(cells)==25 and re.fullmatch(r'\d\.\d\d',cells[0]):
   row['recall_rows'].append({'tau':float(cells[0]),'rerank':int(cells[1].strip('x')),'phase':cells[2],'scanned':cells[13],'recall':float(cells[17].strip('%')),'latency_s':seconds(cells[18]),'navigation_s':seconds(cells[19].split()[0])})
 assert len(row['recall_rows'])==12,run
 row['high_recall']=[r for r in row['recall_rows'] if r['tau']==2 and r['rerank']==8]
 row['checkpoint_metadata']=json.loads((root/f'{run}.checkpoint.json').read_text())
 assert row['checkpoint_metadata']['total_vectors']==40000000
 out['increments'].append(row)
for group in ['baseline','candidate-upgrade','candidate-steady']:
 rows=[r for r in out['increments'] if r['run'].startswith(group+'-')]
 out[group]={k:summary([r[k] for r in rows]) for k in ['add_s','balance_s','load_s','commit_s','writer_reopen_s','reader_open_s','checkpoint_wall_s']} if rows else {}
 if rows:
  out[group]['warm_latency_s']=summary([x['latency_s'] for r in rows for x in r['high_recall'] if x['phase']=='warm'])
  out[group]['cold_latency_s']=summary([x['latency_s'] for r in rows for x in r['high_recall'] if x['phase']=='cold'])
  out[group]['recall']=summary([x['recall'] for r in rows for x in r['high_recall'] if x['phase']=='warm'])
all_open=[r for variant in out['opens'].values() for r in variant['rows']]
out['identical_first_result_ids']=all(r['first_result_ids']==all_open[0]['first_result_ids'] for r in all_open)
out['identical_first_scan_counts']=all(r['scanned']==all_open[0]['scanned'] for r in all_open)
(root/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k not in ['opens','component_commits','increments']},indent=2))
