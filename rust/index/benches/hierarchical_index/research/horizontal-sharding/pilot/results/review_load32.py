"""Independently recompute preserved load outcomes and sampled resource limits."""
import hashlib,json,re,statistics,tarfile
from pathlib import Path
BASE=Path(__file__).resolve().parent
import sys
rep=int(sys.argv[1])
archive=BASE/f'load32-baseline-repetition-{rep}.tar.gz'
held=json.loads((BASE/'fineweb1m-heldout-independent-review.json').read_text())['query_hashes']
records={};barriers={};readies={};driver=[];summary=None;plan=None;contexts={}
def samples(stream):
 result=[]
 for line in stream:
  row=json.loads(line);groups={}
  for key,processes in row['groups'].items():
   groups[key]=[dict({k:p[k] for k in ('pid','cpu_s','rss_kib','main_thread_context_switches') if k in p},role='adapter' if 'subtree_server' in p.get('cmd','') else 'reader' if 'hierarchical_spann_shard' in p.get('cmd','') else 'other') for p in processes]
  result.append(dict(unix=row['unix'],groups=groups))
 return result
with tarfile.open(archive,mode='r|gz') as tar:
 for member in tar:
  name=Path(member.name).name
  match=re.fullmatch(r'(cohost|two-hosts)-c(\d+)-client-(\d+)\.jsonl',name)
  if match:
   label,n,index=match.groups();key=(label,int(n));good=[];ready=[]
   for line in tar.extractfile(member):
    row=json.loads(line)
    if row['status']=='ready':ready.append(row);continue
    assert row['status']=='ok' and row['trace_sha256']==held[row['query_id']]
    assert len(row['ids'])==len(set(row['ids']))==100
    expected_hosts={'ip-172-31-69-252'} if label=='cohost' else {'ip-172-31-69-252','ip-172-31-65-187'}
    assert set(row['actual_hosts'])==expected_hosts and {t['actual_host'] for t in row['transport']}==expected_hosts
    good.append(row)
   assert len(ready)==1 and ready[0]['client']==int(index)
   assert int(index) not in records.setdefault(key,{})
   records[key][int(index)]=good;readies[(key,int(index))]=ready[0]
  elif name.endswith('-readiness.json'):
   match=re.fullmatch(r'(cohost|two-hosts)-c(\d+)-readiness.json',name)
   if match:barriers[(match[1],int(match[2]))]=json.load(tar.extractfile(member))
  elif False:plan=json.load(tar.extractfile(member))
  elif name=='summary.json':summary=json.load(tar.extractfile(member))
  elif member.name=='results/fineweb1m-read-load-resources-v1/driver.jsonl':driver=samples(tar.extractfile(member))
with tarfile.open(BASE/'load32-baseline-provenance-resources.tar.gz') as tar:
 plan=json.load(tar.extractfile('results/fineweb1m-load32-v1/plan.json'))
 driver=samples(tar.extractfile('results/fineweb1m-load32-v1/driver.jsonl'))
 assert hashlib.sha256(tar.extractfile('selection.json').read()).hexdigest()==plan['selection_sha256']
workers={}
for host in (0,1):
 with tarfile.open(BASE/f'load32-worker{host}-resources.tar.gz') as tar:
  workers[host]=samples(tar.extractfile('results/fineweb1m-load32-worker-resources-v1/processes.jsonl'))
def resource(rows,group,start,end,role=None):
 inside=[r for r in rows if start<=r['unix']<=end]
 assert inside and rows[0]['unix']<=start and rows[-1]['unix']>=end
 cpu=voluntary=nonvoluntary=0.;covered=0.
 for a,b in zip(rows,rows[1:]):
  dt=min(end,b['unix'])-max(start,a['unix'])
  if dt<=0:continue
  ratio=dt/(b['unix']-a['unix']);covered+=dt
  x={p['pid']:p for p in a['groups'].get(group,[]) if role is None or p['role']==role};z={p['pid']:p for p in b['groups'].get(group,[]) if role is None or p['role']==role}
  for pid in set(x)&set(z):
   cpu+=(z[pid]['cpu_s']-x[pid]['cpu_s'])*ratio
   if 'main_thread_context_switches'in x[pid] and 'main_thread_context_switches'in z[pid]:
    voluntary+=(z[pid]['main_thread_context_switches']['voluntary_ctxt_switches']-x[pid]['main_thread_context_switches']['voluntary_ctxt_switches'])*ratio
    nonvoluntary+=(z[pid]['main_thread_context_switches']['nonvoluntary_ctxt_switches']-x[pid]['main_thread_context_switches']['nonvoluntary_ctxt_switches'])*ratio
 assert abs(covered-(end-start))<.01
 return dict(samples=len(inside),sampled_cpu_cores=cpu/(end-start),peak_sum_rss_kib=max(sum(p['rss_kib'] for p in r['groups'].get(group,[]) if role is None or p['role']==role) for r in inside),main_thread_voluntary_switches_per_s=voluntary/(end-start),main_thread_nonvoluntary_switches_per_s=nonvoluntary/(end-start))
assert plan['tree_sha256']=='9635309389f9b57694f936effc66296d75c1521ae07f2d28d6ddf35601d63fc5'
assert plan['binary_sha256']=='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158'
points=[];all_queries=set();total=0
for expected in summary['results']:
 key=(expected['placement'],expected['client_processes']);label,n=key;byclient=records[key];assert set(byclient)==set(range(n))
 barrier=barriers[key];assert barrier['all_ready'] and len(barrier['clients'])==n and all(r['status']=='ready' for r in barrier['clients'])
 start=barrier['started_unix'];assert start==expected['started_unix']
 for index in range(n):assert readies[(key,index)]['unix']<start
 good=[r for part in byclient.values() for r in part];assert len(good)==expected['queries']==expected['outcome_counts']['ok']
 assert expected['outcome_counts']==dict(ok=len(good),error=0,client_failure=0,ready=n,invalid=0)
 assert all(r['started_unix']>=start-.001 for r in good)
 end=max(r['finished_unix'] for r in good);window=max(expected['offered_window_s'],end-start)
 assert abs(window-expected['completion_window_s'])<.002
 assert abs(len(good)/window-expected['qps'])<.002
 mean=statistics.mean(r['elapsed_s'] for r in good);assert abs(mean-expected['mean_s'])<1e-12
 wire={field:sum(t[field] for r in good for t in r['transport']) for field in ('request_bytes','response_bytes')}
 maximum=max(t['response_bytes'] for r in good for t in r['transport']);assert maximum<=64*1024**2
 active={str(h):resource(workers[h],label,start,end) for h in ((0,) if label=='cohost' else (0,1))}
 for h,entry in active.items():
  entry['roles']={role:resource(workers[int(h)],label,start,end,role) for role in ('adapter','reader')}
 points.append(dict(placement=label,clients=n,queries=len(good),qps=expected['qps'],mean_s=mean,started_unix=start,query_completion_unix=end,wire_body_bytes=wire,max_owner_response_bytes=maximum,mean_candidate_contributions=statistics.mean(r['candidate_contributions'] for r in good),driver=resource(driver,'coordinator',start,end),workers=active))
 total+=len(good);all_queries.update(r['query_id'] for r in good)
assert len(points)==10 and total>0
report=dict(status='passed',plan_bindings=plan,windows=len(points),successful_queries=total,failed_outcomes=0,distinct_queries=len(all_queries),all_load_trace_hashes_match_heldout=True,points=points,archives={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [archive]},limits=['The driver has 32 vCPUs for at most16 clients; sampled headroom does not establish server capacity.','Sampled CPU/context deltas count PIDs present in adjacent samples; process birth/exit edges can be missed.','Context switches cover main threads only.','Summed RSS counts shared pages multiple times; per-host peaks are not synchronized.','Wire counters measure successful HTTP JSON bodies, excluding headers and TCP.','Fifteen-second windows do not establish sustained throughput or reliable tail percentiles.'])
(BASE/f'load32-repetition-{rep}-independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(rep,total,[(x['placement'],x['clients'],round(x['qps'],3)) for x in points])
