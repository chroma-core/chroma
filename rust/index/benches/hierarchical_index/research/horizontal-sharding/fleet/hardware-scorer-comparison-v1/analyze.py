"""Summarize retained measurements without running a reader."""
from pathlib import Path
import hashlib,json,statistics
R=Path(__file__).resolve().parent
p=R/'runtime-attempt1/result.json';r=json.loads(p.read_text())
assert r['status']=='passed' and r['scoring_calls']==96
samples=r['samples']; modes=['portable','runtime'];fields=['load_ns','quantize_ns','distance_ns','dedup_ns','select_ns','sort_ns','unassigned_ns','native_ns']
def value(s,k):return s[k] if k in s else s['profile'][k]
def group(seq):
 out={}
 for mode in modes:
  rows=[s for s in seq if s['mode']==mode];total=sum(s['native_ns'] for s in rows)
  out[mode]={'calls':len(rows),'median_native_ms':statistics.median(s['native_ns'] for s in rows)/1e6,'median_pipe_ms':statistics.median(s['pipe_elapsed_s'] for s in rows)*1000,'max_observed_child_rss_bytes':max(s['observed_child_rss_peak_bytes'] for s in rows),'stage_total_ns':{k:sum(value(s,k) for s in rows) for k in fields},'stage_share_of_summed_native':{k:sum(value(s,k) for s in rows)/total for k in fields},'median_selection_plus_sort_ms':statistics.median(s['profile']['select_ns']+s['profile']['sort_ns'] for s in rows)/1e6}
 a={(s['repetition'],s['query_id']):s for s in seq if s['mode']==modes[0]};b={(s['repetition'],s['query_id']):s for s in seq if s['mode']==modes[1]}
 out['paired_native_speedup_ratios']=[a[k]['native_ns']/b[k]['native_ns'] for k in a]
 out['median_paired_distance_speedup']=statistics.median(a[k]['profile']['distance_ns']/b[k]['profile']['distance_ns'] for k in a)
 out['median_paired_native_speedup']=statistics.median(out['paired_native_speedup_ratios'])
 out['median_paired_selection_plus_sort_speedup']=statistics.median((a[k]['profile']['select_ns']+a[k]['profile']['sort_ns'])/(b[k]['profile']['select_ns']+b[k]['profile']['sort_ns']) for k in a)
 return out
seen=set();no_new=set()
for n,request in enumerate(json.loads((R/'upload/requests.json').read_text())):
 nodes=set(request['payload']['nodes'])
 if not nodes-seen:no_new.add(n)
 seen|=nodes
result={'input_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'scope':r['scope'],'all_calls':group(samples),'first_request_per_reader_OS_uncontrolled':group([s for s in samples if s['sequence']==0]),'subsequent_partially_warmed':group([s for s in samples if s['sequence']>0]),'no_new_leaf_sequence_subset':group([s for s in samples if s['sequence'] in no_new]),'limitations':['Portable means the explicit batch policy in the same new binary, not the omitted-flag legacy path.','New-leaf classification is inferred from ordered requests, not physical cache-miss counters.','Three repetitions of sixteen fixed queries on one owner; observations are paired and correlated, not independent workload draws.','Stage shares use summed times only to describe this serial component workload, not end-to-end query latency.','Setup, startup, network fanout, global routing, refinement and recall are outside native envelope.','Observed RSS is sampled, not a hard peak bound.']}
(R/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
