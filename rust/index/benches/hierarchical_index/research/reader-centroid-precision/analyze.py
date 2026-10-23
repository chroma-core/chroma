import argparse,json,csv
from pathlib import Path

p=argparse.ArgumentParser();p.add_argument('results');p.add_argument('--out',required=True);a=p.parse_args()
rows=[json.loads(line) for line in Path(a.results).read_text().splitlines() if line.strip()]
out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
with (out/'results.csv').open('w') as f:
 writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
modes=['1bit-4bit','4bit-4bit','4bit-fp']
warm=[r for r in rows if r['phase']=='warm']
chosen=[]
lines=['# Reader centroid precision comparison','',f'{len(rows)} measured rows. Index: 40M Wikipedia, 100 fixed exact L2 queries, 32 query workers.','',
'The modes vary navigation precision only. All modes use the same saved tree, one-bit posting codes, and full-precision reranking. The root beam keeps all children. The next internal beam is 64 or 256; the final leaf beam is fixed independently.','',
'## Lowest warm latency at each recall target','',
'| Target recall | Mode | Internal beam | Leaf beam | Rerank factor | Actual recall | Leaf reachability | Codes scanned | Vectors reranked | Mean warm ms | Navigation ms | Cold filesystem MiB/query |',
'|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
for target in [.8,.9,.94]:
 for mode in modes:
  candidates=[r for r in warm if r['navigation']==mode and r['recall_at_100']>=target]
  if not candidates:
   lines.append(f'| {target:.0%} | {mode} | Not reached | | | | | | | | | |');continue
  r=min(candidates,key=lambda r:r['latency_ms']);chosen.append(r)
  cold=next(c for c in rows if c['phase']=='cold' and all(c[k]==r[k] for k in ['navigation','internal_beam','leaf_beam','rerank_factor','repeat']))
  reads=cold.get('filesystem_read_bytes_per_query');read_text='Unavailable' if reads is None else f'{reads/2**20:.2f}'
  lines.append(f"| {target:.0%} | {mode} | {r['internal_beam']} | {r['leaf_beam']} | {r['rerank_factor']} | {r['recall_at_100']:.2%} | {r['reachable_recall'][-1]:.2%} | {r['codes_scanned']:.0f} | {r['vectors_reranked']:.0f} | {r['latency_ms']:.2f} | {r['navigation_ms']:.2f} | {read_text} |")
lines+=['','## Lowest scan count at each recall target','',
'| Target recall | Mode | Internal beam | Leaf beam | Rerank factor | Recall | Codes scanned | Vectors reranked | Mean warm ms |',
'|---|---|---:|---:|---:|---:|---:|---:|---:|']
for target in [.8,.9,.94]:
 for mode in modes:
  candidates=[r for r in warm if r['navigation']==mode and r['recall_at_100']>=target]
  if not candidates:continue
  r=min(candidates,key=lambda r:(r['codes_scanned'],r['vectors_reranked'],r['latency_ms']))
  lines.append(f"| {target:.0%} | {mode} | {r['internal_beam']} | {r['leaf_beam']} | {r['rerank_factor']} | {r['recall_at_100']:.2%} | {r['codes_scanned']:.0f} | {r['vectors_reranked']:.0f} | {r['latency_ms']:.2f} |")
lines+=['','## Storage and interpretation','']
for mode in modes:
 r=next(r for r in warm if r['navigation']==mode)
 lines.append(f"- {mode}: {r['logical_centroid_bytes']/2**20:.2f} MiB of logical centroid code payload.")
lines+=['',
'The experiment derives four-bit codes once from the saved original centroids and keeps them in reader memory. It retains the old codes too; reported logical centroid bytes describe replacing the one-bit storage payload, not the complete experimental reader RSS. Setup is excluded from query latency.',
'',
'Cold clears reader postings and block pins; the 1 GiB provider cache and OS file cache can still contain data. Filesystem read traffic includes full storage blocks served through the OS cache, averaged across the concurrent 100-query batch. It is not production network traffic or isolated per-query cold bytes. Warm immediately repeats the same queries. The fixed query set and single exploratory samples do not establish general recall or latency variance.',
'']
(out/'RESULTS.md').write_text('\n'.join(lines))
(out/'selected-configurations.json').write_text(json.dumps(chosen,indent=2))
print('\n'.join(lines))
