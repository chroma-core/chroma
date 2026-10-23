"""Stream held-out traces and verify calibration selection separation."""
import argparse,hashlib,json,statistics,tarfile
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('archive');args=parser.parse_args()
base=Path(__file__).resolve().parent;root=base.parents[1]
refdir=root/'subtree/fineweb/fineweb-stream1m-reports-v1';qdir=root/'subtree/fineweb/fineweb-pilot-preparation-v1/queries'
references={r['query_id']:r for r in map(json.loads,(refdir/'test.exact.jsonl').read_text().splitlines())}
with tarfile.open(base/'fineweb1m-calibration-small.tar.gz') as archive:
 selection=json.load(archive.extractfile('results/fineweb1m-read-calibration-v1/selection.json'))
 selection_mtime=archive.getmember('results/fineweb1m-read-calibration-v1/selection.json').mtime
canonical=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
points={};signatures={};summary_docs={}
with tarfile.open(args.archive,mode='r|gz') as archive:
 for member in archive:
  name=Path(member.name).name
  if not member.isfile():continue
  if name in ('test-cohost.jsonl','test-two-hosts.jsonl'):
   label=name[5:-6];seen=set();recalls=[];latencies=[];earliest=float('inf');latest=0;signatures[label]={};request_bytes=response_bytes=0
   for line in archive.extractfile(member):
    row=json.loads(line);assert row['status']=='ok';qid=row['query_id'];assert qid in references and qid not in seen;seen.add(qid)
    result=row['result'];assert len(result['ids'])==len(set(result['ids']))==100
    recall=len(set(result['ids'])&set(references[qid]['ids'][:100]))/100;assert abs(recall-row['recall'])<1e-12
    recalls.append(recall);latencies.append(result['elapsed_s']);earliest=min(earliest,row['started_unix']);latest=max(latest,row['finished_unix'])
    signature={key:result[key] for key in ('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions')}
    signatures[label][qid]=hashlib.sha256(canonical(signature)).hexdigest()
    request_bytes+=sum(t['request_bytes'] for t in result['transport']);response_bytes+=sum(t['response_bytes'] for t in result['transport'])
   assert seen==set(references) and earliest>selection_mtime and earliest>max(r['finished_unix'] for r in selection['calibration'])
   points[label]=dict(queries=len(seen),recall_at_100=statistics.mean(recalls),mean_s=statistics.mean(latencies),median_s=statistics.median(latencies),started_unix=earliest,finished_unix=latest,request_bytes=request_bytes,response_bytes=response_bytes)
  elif name.endswith('.json') and ('summary' in name or 'plan' in name):
   summary_docs[member.name]=json.load(archive.extractfile(member))
assert set(points)=={'cohost','two-hosts'} and signatures['cohost']==signatures['two-hosts']
for name,expected in selection['plan']['input_sha256'].items():
 path=(refdir if '.exact.' in name else qdir)/name
 assert hashlib.sha256(path.read_bytes()).hexdigest()==expected
report=dict(status='passed',raw_archive=str(args.archive),archive_sha256=hashlib.sha256(Path(args.archive).read_bytes()).hexdigest(),selected=selection['selected'],selection_archive_mtime=selection_mtime,points=points,all_deterministic_trace_hashes_equal=True,query_hashes=signatures['cohost'],limits=['File modification timestamp and raw query timestamps establish ordering in preserved evidence; no claim of tamper-proof wall clocks.','Host utilization and RSS require separate sampler review.','One sequential query stream per placement is not a serving capacity measurement.'])
(base/'fineweb1m-heldout-independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='query_hashes'},indent=2))
