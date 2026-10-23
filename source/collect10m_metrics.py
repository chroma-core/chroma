import json,pathlib,subprocess,sys
name=sys.argv[1];root=pathlib.Path('/mnt/spann/results')/name;case=json.load(open(root/'case.json'));rows=[]
for i,host in enumerate(sorted({x['host'] for x in case['entries'].values()})):
 code='''import pathlib,json
prefix=PREFIX
out=[]
for p in pathlib.Path('/mnt/spann/results').glob('*/process.json'):
 try:
  a=json.load(open(p))
  if a.get('checkpoint_dir','').startswith(prefix+'/'):out.append(a)
 except (OSError,ValueError):pass
print(json.dumps(out))
'''.replace('PREFIX',repr('/mnt/spann/'+(sys.argv[2] if len(sys.argv)>2 else name)+'/indexes'))
 r=subprocess.run(['ssh',host,'python3 -'],input=code,text=True,capture_output=True,check=True);a=json.loads(r.stdout);(root/f'wrapper-metrics-host-{i}.json').write_text(json.dumps(a));rows+=a
assert len(rows)==128,len(rows)
s=dict(name=name,owner_jobs=len(rows),child_cpu_s=sum(x['user_cpu_s']+x['system_cpu_s'] for x in rows),maximum_individual_owner_rss_kib=max(x['ru_maxrss_kib'] for x in rows),failed_jobs=sum(x['returncode']!=0 for x in rows));(root/'resource-summary.json').write_text(json.dumps(s));print(json.dumps(s))
