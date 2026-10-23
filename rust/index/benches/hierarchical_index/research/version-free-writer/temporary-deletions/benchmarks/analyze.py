import json,re,sys
from pathlib import Path
p=Path(sys.argv[1]);results=[]
for d in json.loads((p/'comparison.json').read_text()):
 name=d['name'];s=(p/f'{name}.log').read_text();rounds=d['balance_rounds'];d['round_totals']={k:sum(float(x.get(k,0)) for x in rounds) for k in ['retry_s','workers_wall_s','workers_sum_s','retry_coalesced_ids','retry_current_ids','retry_superseded_ids']}
 d['counter_totals']={k:sum(x.get(k,0) for x in d['transfer_metrics']) for k in ['conflicts','retries','vectors','source_entries_scanned']}
 d['round_cap_messages']=[l for l in s.splitlines() if any(t in l.lower() for t in ['round cap','iteration limit','round limit','residual','maximum balance','eight rounds','stopped after 8 rounds'])]
 d['audit']={}
 audit=p/f'{name}.audit.log'
 if audit.exists():d['audit']={'lines':[l for l in audit.read_text().splitlines() if 'BENCHMARK_AUDIT' in l]}
 results.append(d)
(p/'analysis.json').write_text(json.dumps(results,indent=2))
for d in results:print(d['name'],d.get('writer_and_commit_seconds'),d['phases_seconds'].get('Balance'),d['round_totals'],d['counter_totals'],d['recall_percent'])
