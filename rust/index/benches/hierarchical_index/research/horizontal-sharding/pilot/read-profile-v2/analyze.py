import collections,hashlib,json,pathlib,statistics
ROOT=pathlib.Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/pilot/results')
SAMPLES=ROOT/'read-profile-v2/worker-process-samples-combined.jsonl'
samples=[json.loads(s) for s in SAMPLES.open()]
all_signatures={};summary=[]
for epoch,adapters in [('read-profile-v2',1),('read-profile-four-v2',4)]:
 root=ROOT/epoch
 for c in [1,4,8]:
  result=json.loads((root/f'c{c}-summary.json').read_text());rows=[json.loads(l) for p in root.glob(f'c{c}-client*.jsonl') for l in p.open()];ok=[r for r in rows if r['status']=='ok']
  for row in ok:
   q=row['query_id'];signature=row['trace_sha256']
   assert q not in all_signatures or all_signatures[q]==signature,(epoch,c,q)
   all_signatures[q]=signature
  s=[r for r in samples if result['started_unix']<=r['unix']<=result['finished_unix']]
  resources=None
  if len(s)>=2:
   a,b=s[0],s[-1];elapsed=b['unix']-a['unix']
   cpu=lambda r,server:sum(v['cpu_s'] for v in r['processes'] if not server or 'subtree_server' in v['cmd'])
   resources=dict(sampled_seconds=elapsed,total_cores=(cpu(b,False)-cpu(a,False))/elapsed,adapter_cores=(cpu(b,True)-cpu(a,True))/elapsed,peak_rss_mib=max(sum(v['rss_kib'] for v in r['processes']) for r in s)/1024,process_count=len(a['processes']))
  phase_http=[];outside=[]
  for row in ok:
   cursor=0;by=collections.defaultdict(list)
   for trace in row['transport']:
    op=row['operations'][cursor]['op'];cursor+=trace['operation_count'];by[op].append(trace)
   # This fixed tree has one expand, one scoring, and one reranking round.
   assert set(by)=={'expand','score_selected_leaves','rerank_selected'}
   assert cursor==len(row['operations'])
   critical=sum(max(t['elapsed_s'] for t in values) for values in by.values())
   phase_http.append(critical*1000);outside.append(row['elapsed_s']*1000-critical*1000)
  summary.append(dict(epoch=epoch,physical_hosts=1,adapters=adapters,readers=16,admission_per_adapter=16,concurrency=c,queries=result['successes'],failures=result['failures'],qps=result['qps'],mean_ms=result['mean_s']*1000,mean_sum_phase_max_http_ms=statistics.mean(phase_http),mean_outside_phase_max_http_ms=statistics.mean(outside),resources=resources))
old=ROOT/'target-fineweb100k-read-curve/test-cohost.jsonl'
for line in old.open():
 row=json.loads(line);r=row['result'];signature={k:r[k] for k in ['ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count']};sha=hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':')).encode()).hexdigest();assert all_signatures[row['query_id']]==sha,row['query_id']
out=dict(scope='Single 15-second windows, warm persistent readers and OS cache; profiling, not publishable capacity curves.',physical_host='ip-172-31-65-187',tree_sha256='29f311e75a3cc48fd4d1ff1d2e4b7682e820ebd4b0b7c228be7f883362269e76',binary_sha256='d159552ebcc1ce9e3aabef22f679cf5b1d74e9b09c3bbca790bd7a9aef667bf3',beam=64,candidate_budget=1000,distinct_queries_with_identical_full_trace_hashes=len(all_signatures),matches_preserved_v3_traces=True,runs=summary)
(ROOT/'read-profile-comparison.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
