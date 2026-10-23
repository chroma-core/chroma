from pathlib import Path
import subprocess,json,hashlib,re,time
root=Path('/home/ubuntu/spann-single-shard-3m-attribution-v1/epoch2/run')
assert json.loads((root/'complete.json').read_text())['all_processes_passed']
out=root/'annotation';out.mkdir(exist_ok=False)
rows=[]
for name in ['01-reference','02-candidate']:
 profile=root/(name+'.perf.data');symbols=out/(name+'.symbols.txt')
 cmd=['/usr/bin/timeout','--kill-after=5','30','perf','report','--stdio','--no-children','--no-inline','--call-graph','none','--sort','symbol','--percent-limit','0.01','--input',str(profile)]
 with symbols.open('w') as fd:
  r=subprocess.run(cmd,stdout=fd,stderr=subprocess.PIPE,text=True)
 assert r.returncode==0,r.stderr
 options=[]
 for line in symbols.read_text().splitlines():
  if 'navigate_with_scratch' in line:
   m=re.search(r'\[\.\]\s+(.*)$',line)
   if m:options.append(m.group(1).strip())
 assert options,'No physical navigate_with_scratch symbol; no speculative annotation'
 symbol=options[0];full=out/(name+'.annotate.full.txt');err=out/(name+'.annotate.stderr')
 command=['/usr/bin/timeout','--kill-after=5','60','perf','annotate','--stdio','--symbol',symbol,'--percent-type','local-period','--input',str(profile)]
 started=time.time()
 with full.open('w') as fd,err.open('w') as ef:r=subprocess.run(command,stdout=fd,stderr=ef)
 row={'name':name,'physical_symbol':symbol,'command':command,'exit_code':r.returncode,'seconds':time.time()-started,'full_bytes':full.stat().st_size,'full_sha256':hashlib.sha256(full.read_bytes()).hexdigest()}
 text=full.read_text();lines=text.splitlines()
 if full.stat().st_size<=2*1024**2:excerpt=text;row['excerpt']='full bounded text'
 else:
  selected=set(range(min(25,len(lines))))
  for i,line in enumerate(lines):
   if re.search(r'\block\b|cmpxchg|xadd|node_store|sync.rs|raw_rwlock|clone|fmadd',line):selected.update(range(max(0,i-4),min(len(lines),i+5)))
  excerpt='\n'.join(lines[i] for i in sorted(selected));excerpt=excerpt.encode()[:2*1024**2].decode(errors='ignore');row['excerpt']='bounded matching instructions/source context; full retained remotely'
 (out/(name+'.annotate.excerpt.txt')).write_text(excerpt)
 row['instruction_excerpt_lines']=[line for line in lines if re.search(r'\block\b|cmpxchg|xadd|node_store|sync.rs|raw_rwlock',line)][:80]
 rows.append(row)
(out/'summary.json').write_text(json.dumps({'same_recorded_profiles':True,'no_new_benchmark':True,'rows':rows},indent=2)+'\n')
print(json.dumps({'annotation_complete':rows},indent=2))
