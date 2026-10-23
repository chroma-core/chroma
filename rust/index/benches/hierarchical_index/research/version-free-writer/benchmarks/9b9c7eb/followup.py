subprocess.run(['python3',str(r/'summarize.py'),str(a)],check=True)
rows=json.loads((a/'comparison.json').read_text()); decisions=[]
for ds in ['wikipedia','msmarco']:
 b=next(x for x in rows if x['name']==f'{ds}-baseline-1');c=next(x for x in rows if x['name']==f'{ds}-candidate-1')
 gain=1-c['phases_seconds']['Total']/b['phases_seconds']['Total']
 recalls=lambda x:{(v[0],v[1]):float(v[14].strip('%')) for v in x['recall_rows'] if v[2]=='warm'}
 br,cr=recalls(b),recalls(c);assert len(br)==len(cr)==4
 max_drop=max(br[k]-cr[k] for k in br)
 promising=gain>=0.05 and max_drop<1.0
 decisions.append(dict(dataset=ds,build_reduction=gain,max_recall_drop_percentage_points=max_drop,promising=promising))
 if promising:
  run(ds,'candidate',2)
  run(ds,'baseline',2)
(a/'repeat-decisions.json').write_text(json.dumps(decisions,indent=2))
subprocess.run(['bash',str(r/'audit.sh')],check=True)
subprocess.run(['python3',str(r/'summarize.py'),str(a)],check=True)
(a/'audits-complete').write_text(str(time.time()))
while not (r/'scale-ready').exists():time.sleep(10)
exec((r/'scale.py').read_text())
