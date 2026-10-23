"""A bounded paired component replay; no construction or index modification."""
import hashlib,json,math,os,select,signal,struct,subprocess,time
from pathlib import Path
ROOT=Path('/mnt/spann/component-replay-v1')
OUT=ROOT/'runtime-attempt1'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
 return h.hexdigest()
def check_inventory():
 for name,item in json.loads((ROOT/'owner-members.json').read_text()).items():
  p=ROOT/'restored'/name
  assert p.stat().st_size==item['bytes'] and sha(p)==item['sha256'],name
check_inventory()
binary=json.loads((ROOT/'binary.json').read_text())
assert sha(ROOT/'native-reader')==binary['sha256']
assert sha(ROOT/'requests.json')=='dabf838ac51e62028d6204b8d74c3ee66c7042a36c154836bbee4966ee296874'
requests=json.loads((ROOT/'requests.json').read_text());assert len(requests)==16
deadline=json.loads((ROOT/'deadline.json').read_text())['deadline_unix']
assert time.time()+330<deadline
subprocess.run(['systemctl','is-active','--quiet','spann-component-v1-stop.timer'],check=True)
OUT.mkdir(exist_ok=False)
fields=('load_ns','quantize_ns','distance_ns','dedup_ns','select_ns','sort_ns')
children=[];results=[];cleanup=[];started=time.monotonic();output_bytes=0
def timeout(*_):raise TimeoutError('300-second measurement deadline')
signal.signal(signal.SIGALRM,timeout);signal.alarm(300)
env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2')
def rss(pid):
 try:
  for line in Path(f'/proc/{pid}/status').read_text().splitlines():
   if line.startswith('VmRSS:'):return int(line.split()[1])*1024
 except FileNotFoundError:return 0
 return 0
def call(proc,payload):
 global output_bytes
 begin=time.monotonic();wire=(json.dumps(payload,separators=(',',':'))+'\n').encode()
 proc.stdin.write(wire);proc.stdin.flush();buf=bytearray();peak=0
 while True:
  peak=max(peak,rss(proc.pid));assert peak<=2*1024**3,'observed child RSS exceeds2GiB'
  left=30-(time.monotonic()-begin)
  if left<=0:raise TimeoutError('30-second request deadline')
  ready,_,_=select.select([proc.stdout],[],[],min(left,.05))
  if not ready:continue
  b=os.read(proc.stdout.fileno(),65536)
  if not b:raise RuntimeError('reader closed without response')
  buf.extend(b);assert len(buf)<=8*1024**2,'response frame cap'
  if b'\n' in buf:
   assert buf.endswith(b'\n') and buf.count(b'\n')==1,'unexpected trailing frame'
   break
 elapsed=time.monotonic()-begin;output_bytes+=len(buf)
 assert output_bytes<50*1024**2,'raw response output cap'
 return json.loads(buf),elapsed,peak
def normalized(response):
 rows=response['candidates'];seen=set();out=[]
 for row in rows:
  idx=row['local_id'];distance=row['distance']
  assert type(idx)is int and 0<=idx<64406 and idx not in seen
  assert math.isfinite(distance);seen.add(idx)
  out.append((idx,struct.pack('<f',distance).hex()))
 return sorted(out)
status='failed';failure=None
try:
 with (OUT/'raw.jsonl').open('x') as raw:
  for repetition in range(3):
   pair={}
   order=('full-sort-inclusive','partition-inclusive') if repetition%2==0 else ('partition-inclusive','full-sort-inclusive')
   for mode in order:
    args=[str(ROOT/'native-reader'),'read','--checkpoint-dir',str(ROOT/'restored/27'),'--generation','1','--local-ids-only','--profile-posting-selection']
    if mode=='partition-inclusive':args.append('--partition-posting-candidates')
    err=(OUT/f'{repetition}-{mode}.stderr').open('x')
    proc=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,env=env,start_new_session=True)
    children.append((proc,err));pair[mode]=[]
    for number,request in enumerate(requests):
     payload=dict(request['payload'],request_id=f'{repetition}:{mode}:{number}')
     response,elapsed,peak=call(proc,payload)
     raw.write(json.dumps(dict(repetition=repetition,mode=mode,query_id=request['query_id'],request=payload,response=response,pipe_elapsed_s=elapsed,observed_child_rss_peak_bytes=peak))+'\n');raw.flush()
     assert 'error' not in response,response.get('error')
     assert response['request_id']==payload['request_id'] and response['generation']==1 and response['shard_id']=='27'
     assert response['id_mode']=='local-only' and response['posting_selection_policy']==mode
     profile=response['posting_selection_profile'];assert profile['policy']==mode
     assert response['unique_candidates_scanned']==request['expected_unique_candidates']==profile['unique_rows']
     assert response['leaves_scanned']==len(payload['nodes']) and response['candidate_budget']==1000
     assert all(type(profile[k])is int and profile[k]>=0 for k in fields)
     assert profile['retained_rows']==response['actual_candidate_count']==len(response['candidates'])
     assert profile['retained_rows']<=profile['sorted_rows']<=profile['unique_rows']
     native_ns=round(response['elapsed_ms']*1e6);summed=sum(profile[k] for k in fields);assert summed<=native_ns
     pair[mode].append(normalized(response))
     results.append(dict(repetition=repetition,mode=mode,query_id=request['query_id'],sequence=number,cache_label='fresh reader first request; OS uncontrolled' if number==0 else 'same reader subsequent request; partially warmed',native_ns=native_ns,profile=profile,unassigned_ns=native_ns-summed,pipe_elapsed_s=elapsed,observed_child_rss_peak_bytes=peak))
     assert sum(p.stat().st_size for p in OUT.iterdir() if p.is_file())<50*1024**2
    proc.stdin.close();proc.wait(timeout=10);assert proc.returncode==0
    err.close();cleanup.append(dict(pid=proc.pid,returncode=proc.returncode))
   assert pair['full-sort-inclusive']==pair['partition-inclusive'],'paired exact candidate parity failed'
 status='passed'
except Exception as error:
 failure=repr(error)
finally:
 signal.alarm(0)
 for proc,err in children:
  if proc.poll() is None:
   os.killpg(proc.pid,signal.SIGTERM)
   try:proc.wait(timeout=5)
   except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
   cleanup.append(dict(pid=proc.pid,returncode=proc.returncode))
  err.close()
 try:check_inventory()
 except Exception as error:status='failed';failure=repr(error)
 (OUT/'result.json').write_text(json.dumps(dict(status=status,failure=failure,seconds=time.monotonic()-started,scoring_calls=len(results),samples=results,cleanup=cleanup,binary=binary,scope='one owner component; no full-query or recall claim; fresh application reader is not OS-cold',thread_configuration=2,raw_response_bytes=output_bytes),indent=2)+'\n')
if status!='passed':raise SystemExit(1)
