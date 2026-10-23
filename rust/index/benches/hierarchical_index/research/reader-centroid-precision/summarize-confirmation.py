import json,statistics
from collections import defaultdict
from pathlib import Path
rows=[json.loads(line) for p in Path('confirmation').glob('*.jsonl') for line in p.read_text().splitlines() if line.strip()]
groups=defaultdict(list)
for r in rows:groups[(r['navigation'],r['internal_beam'],r['leaf_beam'],r['rerank_factor'],r['phase'])].append(r)
summary=[]
for key,rs in sorted(groups.items()):
 row=dict(zip(['navigation','internal_beam','leaf_beam','rerank_factor','phase'],key));row['runs']=len(rs)
 for field in ['recall_at_100','reachable_recall','codes_scanned','vectors_reranked','latency_ms','navigation_ms','filesystem_read_bytes_per_query']:
  row[field]=statistics.mean(r[field][-1] if field=='reachable_recall' else r[field] for r in rs)
 row['latency_min_ms']=min(r['latency_ms'] for r in rs);row['latency_max_ms']=max(r['latency_ms'] for r in rs)
 summary.append(row)
Path('confirmation-summary.json').write_text(json.dumps(summary,indent=2))
lines=['# Repeated reader precision measurements','','Each row averages three runs of 100 fixed queries on the saved 40M Wikipedia index. Query workers: 32. The internal beam keeps this many nodes before selecting the final leaf beam.','','| Centroid / query | Internal beam | Leaves | Reranked | Reachable recall | Final recall | Warm mean ms (range) | Cold mean ms | Cold filesystem MiB/query |','|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for r in summary:
 if r['phase']!='warm':continue
 cold=next(c for c in summary if c['phase']=='cold' and all(c[k]==r[k] for k in ['navigation','internal_beam','leaf_beam','rerank_factor']))
 lines.append(f"| {r['navigation']} | {r['internal_beam']} | {r['leaf_beam']} | {r['vectors_reranked']:.0f} | {r['reachable_recall']:.2%} | {r['recall_at_100']:.2%} | {r['latency_ms']:.2f} ({r['latency_min_ms']:.2f}–{r['latency_max_ms']:.2f}) | {cold['latency_ms']:.2f} | {cold['filesystem_read_bytes_per_query']/2**20:.2f} |")
Path('CONFIRMATION.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
