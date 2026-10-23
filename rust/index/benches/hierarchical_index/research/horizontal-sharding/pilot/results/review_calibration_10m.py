"""Independently stream the frozen 10M calibration without extracting traces."""
import hashlib,json,statistics,tarfile
from pathlib import Path
BASE=Path(__file__).resolve().parent
archive=BASE/'fineweb10m-calibration-small-v1.tar.gz'
small={}; times={}
with tarfile.open(archive,'r|gz') as tar:
 for member in tar:
  name=Path(member.name).name
  if name in {'plan.json','selection.json','calibration.jsonl','calibration.exact.jsonl'}:
   small[name]=tar.extractfile(member).read();times[name]=member.mtime
plan=json.loads(small['plan.json']);selection=json.loads(small['selection.json'])
assert plan==selection['plan']
assert hashlib.sha256(small['selection.json']).hexdigest()=='ae76af1a02203a5433c186c1d4411eeb8d53b6cc488293660fc79f8b8c85b5b0'
for name in ('calibration.jsonl','calibration.exact.jsonl'):
 assert hashlib.sha256(small[name]).hexdigest()==plan['input_sha256'][name]
refs={r['query_id']:r for r in map(json.loads,small['calibration.exact.jsonl'].splitlines())}
queries={r['query_id']:r for r in map(json.loads,small['calibration.jsonl'].splitlines())}
assert len(refs)==len(queries)==100 and set(refs)==set(queries)
points={};retained={};pins=0
with tarfile.open(archive,'r|gz') as tar:
 for member in tar:
  name=Path(member.name).name
  if not(name.startswith('calibration-b') and name.endswith('.jsonl')):continue
  rows=[];seen=set();req=[];resp=[];logical=[];wire=[];max_owner=0
  for line in tar.extractfile(member):
   row=json.loads(line);assert row['status']=='ok';qid=row['query_id'];assert qid in refs and qid not in seen;seen.add(qid)
   result=row['result'];ids=result['ids'];assert len(ids)==100 and len(set(ids))==100 and len(result['distances'])==100
   recall=len(set(ids)&set(refs[qid]['ids'][:100]))/100;assert abs(recall-row['recall'])<1e-12
   rows.append((recall,result['elapsed_s']));req.append(sum(x['request_bytes'] for x in result['transport']));resp.append(sum(x['response_bytes'] for x in result['transport']));max_owner=max(max_owner,max(x['response_bytes'] for x in result['transport']))
   logical.append(result['candidate_contributions']);wire.append(result['wire_candidate_contributions']);assert wire[-1]<=logical[-1]
   for worker in result['worker_metrics']:
    metrics=worker['metrics'];assert metrics['generation']==1
    cache=metrics['retained_postings'];pins=max(pins,cache['posting_block_pins'],cache['vector_block_pins']);sid=worker['subtree_id']
    if sid not in retained or cache['payload_bytes']>retained[sid]['payload_bytes']:retained[sid]=cache
  assert seen==set(refs)
  points[name]={'queries':len(rows),'recall_at_100':statistics.mean(x[0] for x in rows),'mean_s':statistics.mean(x[1] for x in rows),'mean_request_bytes':statistics.mean(req),'mean_response_bytes':statistics.mean(resp),'max_query_response_bytes':max(resp),'max_owner_response_bytes':max_owner,'mean_logical_contributions':statistics.mean(logical),'mean_wire_contributions':statistics.mean(wire)}
assert len(points)==8
for item in selection['calibration']:
 p=points[f"calibration-b{item['beam_width']}-m{item['candidate_budget']}.jsonl"]
 assert item['status']=='passed' and item['failure'] is None and item['queries']==p['queries']==100
 assert abs(item['recall_at_100']-p['recall_at_100'])<1e-12 and abs(item['mean_s']-p['mean_s'])<1e-12
chosen=min((p['mean_s'],name) for name,p in points.items() if p['recall_at_100']>=plan['primary_recall_target'])[1]
s=selection['selected'];assert chosen==f"calibration-b{s['beam_width']}-m{s['candidate_budget']}.jsonl"
with tarfile.open(BASE/'fineweb10m-read-immutable-metadata-v1.tar.gz','r|gz') as tar:
 for member in tar:
  if member.name.endswith('/tree-1.json'):
   tree_bytes=tar.extractfile(member).read();assert hashlib.sha256(tree_bytes).hexdigest()==plan['tree_sha256'];break
 else:raise AssertionError('missing tree')
assert plan['binary_sha256']=='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158'
report={'status':'passed','archive_sha256':hashlib.file_digest(archive.open('rb'),'sha256').hexdigest(),'records':800,'failures':0,'selected':chosen,'points':points,'tree_sha256':plan['tree_sha256'],'selection_sha256':hashlib.sha256(small['selection.json']).hexdigest(),'selection_mtime':times['selection.json'],'calibration_finished_unix':max(x['finished_unix'] for x in selection['calibration']),'retained_per_owner_observed_maxima':{'owners':len(retained),'payload_bytes':sum(x['payload_bytes'] for x in retained.values()),'leaf_owned_bytes_including_centroids':sum(x['leaf_owned_bytes_including_centroids'] for x in retained.values()),'entries':sum(x['entries'] for x in retained.values()),'max_block_pins':pins},'limits':['Held-out input files and execution chronology require the separate held-out handoff.','Observed per-owner cache maxima are not simultaneous RSS.','HTTP byte counts cover JSON bodies only.','Sequential warm-cache calibration is not a serving capacity measurement.','Binary/source identities are preserved declared bindings; this local review does not execute the binary.']}
(BASE/'fineweb10m-calibration-independent-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
