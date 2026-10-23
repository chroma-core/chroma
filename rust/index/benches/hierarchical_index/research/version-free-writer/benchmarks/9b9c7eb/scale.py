# This stage is released only after the coordinator checks the repeated 1M results and audits.
selected=json.loads((a/'scale-decisions.json').read_text())
with (a/'gt-verification-10m.log').open('w') as log:
 subprocess.run([str(r/'venv/bin/python'),str(r/'prepare-10m.py')],stdout=log,stderr=subprocess.STDOUT,check=True)
source=(r/'run.py').read_text().split('\nfor dataset in ')[0]
source=source.replace('def run(dataset,variant,repeat=1):','def run10m(dataset,variant,repeat=1):').replace("name=f'{dataset}-{variant}-{repeat}'", "name=f'{dataset}-10m-{variant}-{repeat}'").replace("'--checkpoint-size','100000'", "'--checkpoint-size','1000000'").replace("ck['total_vectors']==1000000", "ck['total_vectors']==10000000")
exec(source)
for ds in selected:
 for variant in ['baseline','candidate']:run10m(ds,variant)
subprocess.run([str(r/'venv/bin/python'),str(r/'audit.py')],check=True)
subprocess.run(['python3',str(r/'summarize.py'),str(a)],check=True)
(a/'all-complete').write_text(str(time.time()))
