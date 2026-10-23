import pathlib,hashlib,struct,json,subprocess,os,signal,time
base=pathlib.Path(__file__).resolve().parent
root=base/'native-runtime';root.mkdir(exist_ok=False)
binary='/private/tmp/spann-write-check/debug/deps/hierarchical_spann_profile_quantized-4dd25938422731d8'
raw=b''.join(struct.pack('<f',-0.0 if d==2 else ((i*17+d*13)%127)/31.0) for i in range(129) for d in range(32))
p=root/'input.f32';p.write_bytes(raw);sha=hashlib.sha256(raw).hexdigest()
expected=hashlib.sha256(b''.join(struct.pack('<Q',i)+raw[i*128:(i+1)*128] for i in range(129))).hexdigest()
common=['--retained-f32',str(p),'--retained-rows','129','--retained-sha256',sha,'--dim','32','--checkpoint','1','--checkpoint-size','129','--metric','l2','--threads','4','--balance-threads','4','--branching-factor','4','--split-threshold','16','--merge-threshold','4','--write-navigation','fp','--fp-npa','--write-beam-tau','1.5','--write-beam-min','10','--write-beam-max','16','--write-rng-epsilon','0','--write-rng-factor','1','--max-replicas','1','--num-queries','0','--brute-force-gt','false','--compute-gt-clusters','false','--gc-blockfiles','false','--max-cache-bytes','1073741824','--save','--validate-postings']
cases=[('valid',common,True)]
bad=common.copy();bad[bad.index('--retained-sha256')+1]='0'*64;cases.append(('bad-sha',bad,False))
bad=common.copy();bad[bad.index('--retained-rows')+1]='130';cases.append(('bad-length',bad,False))
cases.append(('no-commit',common+['--no-commit'],False))
results=[]
for name,args,ok in cases:
 out=root/name;cmd=[binary,*args,'--save-dir',str(out)]
 with (root/(name+'.stdout')).open('wb') as stdout,(root/(name+'.stderr')).open('wb') as stderr:
  start=time.monotonic();proc=subprocess.Popen(cmd,stdout=stdout,stderr=stderr,start_new_session=True,env={**os.environ,'HF_HUB_OFFLINE':'1','TOKIO_WORKER_THREADS':'4','RAYON_NUM_THREADS':'4'})
  try:rc=proc.wait(timeout=30)
  except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait();raise
 elapsed=time.monotonic()-start
 if ok:
  assert rc==0,(name,rc)
  lines=(root/(name+'.stdout')).read_text().splitlines();marker=[json.loads(l.split(' ',1)[1]) for l in lines if l.startswith('RETAINED_SAVED_BITS ')]
  assert len(marker)==1 and marker[0]['rows']==129 and marker[0]['all_current_vector_bits_sha256']==expected,marker
  assert any('[posting validation] before commit: 129 ' in l for l in lines)
  assert any('[posting validation] after reopen: 129 ' in l for l in lines)
 else:assert rc!=0 and not out.exists(),(name,rc,out.exists())
 results.append({'case':name,'exit':rc,'seconds':elapsed,'command':cmd})
(base/'native-result.json').write_text(json.dumps({'cases':results,'input_sha256':sha,'expected_bits_sha256':expected,'binary_sha256':hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest(),'scope':'Tiny unoptimized native wiring and integrity only; root-reachable set equality does not reject duplicate current posting rows.'},indent=2)+'\n')
print(json.dumps(results))
