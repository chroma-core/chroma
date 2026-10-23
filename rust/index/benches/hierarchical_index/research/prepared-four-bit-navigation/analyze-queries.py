from pathlib import Path
from collections import defaultdict
import json,statistics,csv
rows=[]
for p in sorted(Path('queries').glob('run-*.jsonl')):
 for line in p.read_text().splitlines():
  r=json.loads(line);r['run']=p.stem;rows.append(r)
groups=defaultdict(list)
for r in rows:groups[(r['navigation'],r['internal_beam'],r['leaf_beam'],r['rerank_factor'],r['phase'])].append(r)
summary=[]
for key,rs in sorted(groups.items()):
 r=dict(zip(['navigation','internal_beam','leaf_beam','rerank_factor','phase'],key));r['runs']=len(rs);r['kernel']=rs[0]['kernel']
 for field in ['recall_at_100','codes_scanned','vectors_reranked','latency_ms','navigation_ms','query_quantization_ms','posting_distance_ms','rerank_ms','selection_ms','preparation_ms','scoring_cache_allocation_bytes','filesystem_read_bytes_per_query']:
  r[field]=statistics.mean(s[field] for s in rs)
 r['reachable_recall']=rs[0]['reachable_recall']
 r['latency_range']=[min(s['latency_ms'] for s in rs),max(s['latency_ms'] for s in rs)]
 summary.append(r)
Path('query-summary.json').write_text(json.dumps(summary,indent=2))
with Path('queries.csv').open('w') as f:
 writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
lines=['# Prepared four-bit query comparison','','Each row averages three runs on the saved 40M Wikipedia index with 100 exact L2 queries and 32 workers. Cold clears reader postings and block pins; provider and OS caches remain. Filesystem bytes are averaged over the concurrent query batch and do not measure production network traffic.','','| Centroid / query implementation | Internal beam | Leaves | Reranked | Reachable recall | Final recall | Codes scanned | Warm mean ms | Navigation ms | Cold mean ms | Cold filesystem MiB/query |','|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in summary:
 if r['phase']!='warm':continue
 cold=next(c for c in summary if c['phase']=='cold' and all(c[k]==r[k] for k in ['navigation','internal_beam','leaf_beam','rerank_factor']))
 lines.append(f"| {r['navigation']} | {r['internal_beam']} | {r['leaf_beam']} | {r['vectors_reranked']:.0f} | {r['reachable_recall'][-1]:.2%} | {r['recall_at_100']:.2%} | {r['codes_scanned']:,.0f} | {r['latency_ms']:.2f} | {r['navigation_ms']:.2f} | {cold['latency_ms']:.2f} | {cold['filesystem_read_bytes_per_query']/2**20:.2f} |")
Path('QUERY_RESULTS.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
