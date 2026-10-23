import hashlib,json,math,os,signal,struct,subprocess,time
from pathlib import Path
root=Path('/private/tmp/chroma-hierarchical-spann-sharding')
out=Path(__file__).resolve().parent
env=dict(os.environ,CARGO_TARGET_DIR='/private/tmp/chroma-single-shard-target')
def run(name,argv,cwd,seconds=180):
 start=time.time()
 with (out/(name+'.log')).open('w') as log:
  p=subprocess.Popen(argv,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  try: code=p.wait(timeout=seconds)
  except subprocess.TimeoutExpired:
   os.killpg(p.pid,signal.SIGTERM);code=p.wait(timeout=10)
 receipt={'command':argv,'cwd':str(cwd),'exit_code':code,'seconds':time.time()-start}
 (out/(name+'.json')).write_text(json.dumps(receipt,indent=2)+'\n')
 print(name,json.dumps(receipt),flush=True)
 if code:print((out/(name+'.log')).read_text()[-7000:],flush=True);raise SystemExit(code)
 return (out/(name+'.log')).read_text()
run('packed-centroid-library',['cargo','test','-p','chroma-index','--lib','packed_centroid','--','--test-threads=4'],root/'single-shard')
fixture=Path('/private/tmp/spann-redesign-synthetic64x8.f32')
fixture.write_bytes(b''.join(struct.pack('<f',math.sin(i*.17+j*.13)) for i in range(64) for j in range(8)))
fixture_hash=hashlib.sha256(fixture.read_bytes()).hexdigest()
for arm,source,flags in [('candidate',root/'single-shard',['--','--cfg','single_shard_owner']),('reference',root/'matched-reference',[])]:
 log=run(arm+'-artifact-build',['cargo','rustc','-p','chroma-index','--bench','single_shard_matched','--profile','dev','--message-format=json']+flags,source)
 binaries=[]
 for line in log.splitlines():
  try:item=json.loads(line)
  except ValueError:continue
  if item.get('reason')=='compiler-artifact' and item.get('target',{}).get('name')=='single_shard_matched' and item.get('executable'):binaries.append(item['executable'])
 if len(binaries)!=1:raise SystemExit(f'Expected one exact compiler artifact, got {binaries}')
 executable=Path(binaries[0])
 (out/(arm+'-native-binary.json')).write_text(json.dumps({'path':str(executable),'sha256':hashlib.sha256(executable.read_bytes()).hexdigest()},indent=2)+'\n')
 result=Path('/private/tmp')/('spann-redesign-native-smoke-'+arm)
 run(arm+'-native-smoke',[str(executable),'--input',str(fixture),'--output-dir',str(result),'--rows','64','--dim','8','--workers','4','--input-sha256',fixture_hash],source,30)
 (out/(arm+'-native-smoke-result.json')).write_bytes((result/'result.json').read_bytes())
print('All compilation, library and synthetic harness checks passed; native smoke timings are not benchmark evidence.',flush=True)
