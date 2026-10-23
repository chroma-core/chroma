"""Attribute held-out resources to the post-restart serving PID groups."""
import json,tarfile
from pathlib import Path
base=Path(__file__).resolve().parent
review=json.loads((base/'fineweb1m-heldout-independent-review.json').read_text())
workers={}
for host in (0,1):
 with tarfile.open(base/f'fineweb1m-small-worker{host}.tar.gz') as archive:
  workers[host]=[json.loads(line) for line in archive.extractfile('results/fineweb1m-read-test-v1/resources/processes.jsonl')]
with tarfile.open(base/'fineweb1m-small-driver.tar.gz') as archive:
 driver=[json.loads(line) for line in archive.extractfile('results/fineweb1m-read-test-v1/resources/driver.jsonl')]
def bracket(rows,key,start,end):
 before=max((r for r in rows if r[key]<=start),key=lambda r:r[key]);after=min((r for r in rows if r[key]>=end),key=lambda r:r[key])
 inside=[r for r in rows if before[key]<=r[key]<=after[key]]
 return before,after,inside
points={}
for label,point in review['points'].items():
 start,end=point['started_unix'],point['finished_unix'];hosts={}
 for host in ((0,) if label=='cohost' else (0,1)):
  first,last,inside=bracket(workers[host],'unix',start,end)
  a={p['pid']:p for p in first['groups'][label]};z={p['pid']:p for p in last['groups'][label]};assert set(a)==set(z)
  cpu=sum(z[p]['cpu_s']-a[p]['cpu_s'] for p in a)/(last['unix']-first['unix'])
  peaks=max(sum(p['rss_kib'] for p in r['groups'][label]) for r in inside)
  hosts[str(host)]=dict(samples=len(inside),start_unix=first['unix'],end_unix=last['unix'],processes=len(a),cpu_cores=cpu,peak_group_rss_kib=peaks)
 first,last,inside=bracket(driver,'unix_time_s',start,end)
 a=first['host_cpu_jiffies'];z=last['host_cpu_jiffies'];busy=sum(z[i]-a[i] for i in (0,1,2,5,6,7));cores=busy/first['clock_ticks_per_second']/(last['unix_time_s']-first['unix_time_s'])
 points[label]=dict(hosts=hosts,active_serving_cpu_cores=sum(h['cpu_cores'] for h in hosts.values()),sum_host_peak_rss_kib=sum(h['peak_group_rss_kib'] for h in hosts.values()),driver_host_busy_cores=cores,driver_samples=len(inside))
report=dict(status='passed',points=points,limits=['Two-host sum of host RSS peaks is an upper bound from separately sampled peaks, not an exact synchronized aggregate.','Serving group RSS excludes other inactive-placement processes on those hosts.','Driver host CPU includes unrelated processes; this is one sequential query stream, not a load saturation test.','Endpoint CPU counters use bracketing samples and include small pre/post query margins.'])
(base/'fineweb1m-heldout-resource-independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
