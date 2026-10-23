"""Keep sampling separate; align each host's monotonic samples to balance events."""
from run import P,H,B,ssh,worker,peer
import json,shlex,concurrent.futures
for arm in ['off','on']:
 name=f'32-fixed-{arm}-profile';folder=P/name
 if not (folder/'owner1-receipts.json').exists():continue
 d=json.loads((folder/'owner1-receipts.json').read_text());events=[json.loads(line) for k,v in d.items() if k.endswith('/events.jsonl') for line in v.splitlines()];start=next(x for x in events if x.get('event')=='balance_started');end=next(x for x in events if x.get('event')=='balance_complete');metadata=[]
 def one(pair):
  iid,owner=pair;r=json.loads((folder/f'owner{owner}-receipts.json').read_text());clock=json.loads(next(v for k,v in r.items() if k.endswith('/clock-start.json')));finish=json.loads(next(v for k,v in r.items() if k.endswith('/clock-end.json')))
  if owner==1:a=start['monotonic_ns']/1e9;b=end['monotonic_ns']/1e9
  else:a=(clock['monotonic_ns']+start['realtime_ns']-clock['realtime_ns'])/1e9;b=(clock['monotonic_ns']+end['realtime_ns']-clock['realtime_ns'])/1e9
  delta=(finish['realtime_ns']-finish['monotonic_ns'])-(clock['realtime_ns']-clock['monotonic_ns']);assert abs(delta)<100000000,'clock offset drift exceeds100ms'
  remote=B+'/'+name+f'-process{owner}/perf.data';cmd=['sudo','perf','report','--stdio','--no-children','--percent-limit','0.5','--sort','dso,symbol','--time',f'{a:.9f},{b:.9f}','-i',remote]
  report=ssh(iid,shlex.join(cmd),timeout=180);(folder/f'owner{owner}-balance-perf.txt').write_text(report)
  return {'owner':owner,'remote_perf_data':remote,'start_monotonic_seconds':a,'end_monotonic_seconds':b,'clock_offset_drift_ns':delta,'alignment':'coordinator event clock directly' if owner==1 else 'peer monotonic clock offset through realtime; residual inter-host realtime skew not independently measured'}
 with concurrent.futures.ThreadPoolExecutor(2) as pool:metadata=list(pool.map(one,[(worker,1),(peer,2)]))
 (folder/'profile-phase-alignment.json').write_text(json.dumps(metadata,indent=2)+'\n');print('PROFILE_COLLECTED',name,flush=True)
