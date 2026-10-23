import json,re,statistics
from pathlib import Path
r=Path(__file__).parent
units={'ns':1e-6,'us':1e-3,'ms':1,'s':1000,'m':60000}
def ms(s):
 m=re.fullmatch(r'([\d.]+)(ns|us|ms|s|m)',s.split()[0]); return float(m[1])*units[m[2]]
rows=[]
for variant in ['baseline','candidate']:
 for repeat in range(1,4):
  text=(r/f'read-{variant}-{repeat}.log').read_text()
  row={'variant':variant,'repeat':repeat}
  for phase in ['cold','warm']:
   line=next(l for l in text.splitlines() if re.search(r'\|\s*'+phase+r'\s*\|',l))
   c=[x.strip() for x in line.split('|')[1:-1]]
   row[phase]={'total_ms':ms(c[-7]),'navigation_ms':ms(c[-6]),'quantization_ms':ms(c[-5]),'scoring_ms':ms(c[-4]),'selection_ms':ms(c[-3]),'recall_pct':float(c[-8].removesuffix('%')),'scanned':c[-12],'raw':line}
  time=(r/f'read-{variant}-{repeat}.time').read_text()
  row['peak_rss_kib']=int(re.search(r'Maximum resident set size \(kbytes\): (\d+)',time)[1])
  assert 'Exit status: 0' in time
  rows.append(row)
means={v:{p:{k:statistics.mean(x[p][k] for x in rows if x['variant']==v) for k in ['total_ms','navigation_ms','quantization_ms','scoring_ms','selection_ms','recall_pct']} for p in ['cold','warm']} for v in ['baseline','candidate']}
summary={'runs':rows,'means':means,'reductions_pct':{p:100*(1-means['candidate'][p]['total_ms']/means['baseline'][p]['total_ms']) for p in ['cold','warm']}}
(r/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(means,indent=2)); print(summary['reductions_pct'])
