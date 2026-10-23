"""Stream preserved calibration evidence without extracting its large traces."""
import hashlib,json,statistics,tarfile
from pathlib import Path
BASE=Path(__file__).resolve().parent
ROOT=BASE.parents[1]
archive=BASE/'fineweb1m-calibration-small.tar.gz'
refsdir=ROOT/'subtree/fineweb/fineweb-stream1m-reports-v1'
queriesdir=ROOT/'subtree/fineweb/fineweb-pilot-preparation-v1/queries'
refs={r['query_id']:r for r in map(json.loads,(refsdir/'calibration.exact.jsonl').read_text().splitlines())}
queries={r['query_id']:r for r in map(json.loads,(queriesdir/'calibration.jsonl').read_text().splitlines())}
held={r['query_id'] for r in map(json.loads,(queriesdir/'test.jsonl').read_text().splitlines())}
assert set(queries)==set(refs) and not(set(queries)&held)
points={}; plan=selection=None; retained={}; maxpins=0; member_times={}
with tarfile.open(archive,mode='r|gz') as tar:
 for member in tar:
  name=Path(member.name).name
  if name=='plan.json':plan=json.load(tar.extractfile(member))
  elif name=='selection.json':selection=json.load(tar.extractfile(member));member_times['selection_mtime']=member.mtime
  elif name.startswith('calibration-b') and name.endswith('.jsonl'):
   records=[];seen=set();requests=[];responses=[];max_owner_response=0;max_owner_request=0;contributions=[]
   for line in tar.extractfile(member):
    row=json.loads(line);assert row['status']=='ok';qid=row['query_id'];assert qid in refs and qid not in seen;seen.add(qid)
    result=row['result'];ids=result['ids'];assert len(ids)==100 and len(set(ids))==100
    recall=len(set(ids)&set(refs[qid]['ids'][:100]))/100
    assert abs(recall-row['recall'])<1e-12
    records.append((recall,result['elapsed_s']))
    requests.append(sum(t['request_bytes'] for t in result['transport']));responses.append(sum(t['response_bytes'] for t in result['transport']))
    max_owner_response=max(max_owner_response,max(t['response_bytes'] for t in result['transport']));max_owner_request=max(max_owner_request,max(t['request_bytes'] for t in result['transport']))
    contributions.append(result['candidate_contributions'])
    for worker in result['worker_metrics']:
     metrics=worker['metrics'];assert metrics['generation']==1
     cache=metrics['retained_postings'];maxpins=max(maxpins,cache['posting_block_pins'],cache['vector_block_pins'])
     sid=worker['subtree_id']
     if sid not in retained or cache['payload_bytes']>retained[sid]['payload_bytes']:retained[sid]=cache
   assert seen==set(refs)
   points[name]=dict(queries=len(records),recall_at_100=statistics.mean(x[0] for x in records),mean_s=statistics.mean(x[1] for x in records),total_request_bytes=sum(requests),total_response_bytes=sum(responses),total_candidate_contributions=sum(contributions),mean_request_bytes=statistics.mean(requests),mean_response_bytes=statistics.mean(responses),max_query_response_bytes=max(responses),max_owner_response_bytes=max_owner_response,max_owner_request_bytes=max_owner_request,mean_candidate_contributions=statistics.mean(contributions))
assert len(points)==10
for name,expected in plan['input_sha256'].items():
 path=(refsdir if '.exact.' in name else queriesdir)/name
 assert hashlib.sha256(path.read_bytes()).hexdigest()==expected
metadata=json.loads((BASE/'fineweb1m-publication-metadata/fineweb1m128-h2-a16-c16/tree-1.json').read_text())
canonical=lambda d:(json.dumps(d,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
assert hashlib.sha256(canonical(metadata)).hexdigest()==plan['tree_sha256']
for item in selection['calibration']:
 key=f"calibration-b{item['beam_width']}-m{item['candidate_budget']}.jsonl";point=points[key]
 assert item['queries']==point['queries'] and item['status']=='passed'
 assert abs(item['recall_at_100']-point['recall_at_100'])<1e-12 and abs(item['mean_s']-point['mean_s'])<1e-12
eligible=[(p['mean_s'],name) for name,p in points.items() if p['recall_at_100']>=plan['primary_recall_target']]
chosen=min(eligible)[1];s=selection['selected'];assert chosen==f"calibration-b{s['beam_width']}-m{s['candidate_budget']}.jsonl"
assert plan['binary_sha256']=='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158'
report=dict(status='passed',archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),points=points,selected=chosen,calibration_queries=len(queries),held_out_queries=len(held),query_sets_disjoint=True,tree_sha256=plan['tree_sha256'],input_hashes_verified=plan['input_sha256'],selection_mtime=member_times['selection_mtime'],calibration_finished_unix=max(p['finished_unix'] for p in selection['calibration']),retained_per_owner_maxima=dict(owners=len(retained),payload_bytes=sum(x['payload_bytes'] for x in retained.values()),leaf_owned_bytes_including_centroids=sum(x['leaf_owned_bytes_including_centroids'] for x in retained.values()),entries=sum(x['entries'] for x in retained.values()),materialized_leaves=sum(x['materialized_leaves'] for x in retained.values()),max_block_pins=maxpins),limits=['Held-out raw data/timestamps not yet received, so selection-before-held-out execution remains to be verified.','Retained cache totals sum observed per-owner maxima; they are not a simultaneous RSS measurement.','HTTP byte counts are JSON bodies, excluding headers/TCP.','Sequential calibration has warm application/OS caches and is not a serving capacity benchmark.'])
(BASE/'fineweb1m-calibration-independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
