import json,re,sys
from pathlib import Path
p=Path(sys.argv[1] if len(sys.argv)>1 else '.')
results=[]
for f in sorted(p.glob('*.result.json')):
 if '.audit-' in f.name:continue
 d=json.loads(f.read_text());name=f.name.removesuffix('.result.json');s=(p/f'{name}.log').read_text();d['name']=name
 if '--- Build wall time (exclusive seconds) ---' in s:
  section=s.split('--- Build wall time (exclusive seconds) ---')[1].split('Build wall time reconciliation')[0]
  d['phases_seconds']={k:float(v) for k,v in re.findall(r'^  (.+): ([0-9.]+)s$',section,re.M)}
 d['validation_before_count']=s.count('[posting validation] before commit:');d['validation_after_count']=s.count('[posting validation] after reopen:')
 d['recall_rows']=[]
 for line in s.splitlines():
  cells=[v.strip() for v in line.split('|')][1:-1]
  if len(cells)>5 and cells[0] in ['1.50','2.00'] and cells[2] in ['cold','warm']:
   d['recall_rows'].append(cells)
 t=(p/f'{name}.time').read_text();m=re.search(r'Maximum resident set size \(kbytes\): (\d+)',t);d['peak_rss_kib']=int(m[1]) if m else None
 d['transfer_metrics']=[dict(zip(['conflicts','retries','hold_ns','batches','vectors','source_entries_scanned'],map(int,v))) for v in re.findall(r'Leaf transfers: conflicts=(\d+) retries=(\d+) hold_ns=(\d+) batches=(\d+) vectors=(\d+) source_entries_scanned=(\d+)',s)]
 results.append(d)
(p/'comparison.json').write_text(json.dumps(results,indent=2)+'\n')
for d in results:print(d['name'],d['returncode'],'process',round(d['elapsed_seconds'],3),'build',d.get('phases_seconds',{}).get('Total'),'rssGiB',round((d['peak_rss_kib'] or 0)/1024**2,3),'checks',d['validation_after_count'])
