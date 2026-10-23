"""Bind the10M ownership, selected-tree storage, and held-out parity gates."""
import pathlib,json,tarfile,hashlib,struct,statistics,time
base=pathlib.Path(__file__).resolve().parents[1]/'results';tree_sha='6fe60910493a4f875bca39b2f729b619d24ec2f0eb0298ccf0a77c3b46dd1659';selection_sha='ae76af1a02203a5433c186c1d4411eeb8d53b6cc488293660fc79f8b8c85b5b0';artifacts=['fineweb10m-calibration-small-v1.tar.gz','fineweb10m-heldout-v1-small.tar.gz','fineweb10m-read-immutable-metadata-v1.tar.gz','fineweb10m-selected-storage-small-v1.tar.gz','fineweb10m-completed-builds-small-v1.tar.gz'];bindings={n:dict(bytes=(base/n).stat().st_size,sha256=hashlib.file_digest((base/n).open('rb'),'sha256').hexdigest()) for n in artifacts}
with tarfile.open(base/artifacts[2]) as t:
 tree=json.load(t.extractfile('results/fineweb10m128-h2-a16-c16-v2/tree-1.json'));assert hashlib.sha256((json.dumps(tree,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()).hexdigest()==tree_sha
 bitmap=bytearray((10000000+7)//8);count=0
 for s in tree['subtrees']:
  desc=s['local_to_global'];name='results/fineweb10m128-h2-a16-c16-v2/'+desc['path'];raw=t.extractfile(name).read();assert len(raw)==desc['bytes'] and hashlib.sha256(raw).hexdigest()==desc['sha256']
  for (ident,) in struct.iter_unpack('<Q',raw):
   assert ident<10000000;byte,bit=divmod(ident,8);mask=1<<bit;assert not bitmap[byte]&mask;bitmap[byte]|=mask;count+=1
 assert count==10000000
with tarfile.open(base/artifacts[1]) as t:
 def read(name):return json.load(t.extractfile('results/fineweb10m-heldout-v1/'+name))
 summary=read('summary.json');plan=read('plan.json');assert summary['status']=='passed' and summary['tree_sha256']==tree_sha and plan['selection_sha256']==selection_sha
 oracle={r['query_id']:set(r['ids'][:100]) for r in map(json.loads,t.extractfile('fineweb10m-pilot-data/test.exact.jsonl'))};records={label:list(map(json.loads,t.extractfile('results/fineweb10m-heldout-v1/test-'+label+'.jsonl'))) for label in ('cohost','two-hosts')}
 recalls={};keys=('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions')
 for label,rows in records.items():
  assert len(rows)==100 and all(r['status']=='ok' and r['started_unix']>plan['selection_mtime'] for r in rows);recalls[label]=statistics.mean(len(set(r['result']['ids'])&oracle[r['query_id']])/100 for r in rows)
  assert abs(recalls[label]-.9587)<1e-12
 other={r['query_id']:r for r in records['two-hosts']}
 for r in records['cohost']:
  for k in keys:assert r['result'][k]==other[r['query_id']]['result'][k]
storage=json.loads((base/'fineweb10m-selected-storage-summary-v1.json').read_text());assert storage['status']=='passed' and storage['tree_sha256']==tree_sha and storage['rows']==10000000
receipt=dict(status='passed-for-bounded-100M-preparation-job-materialization',checked_unix=time.time(),tree_sha256=tree_sha,prepared_sha256='739c0b520b204c9ade837aa92a501c6ccd8c87d735248a5f395b0df39202cbcb',model_sha256='e644f2af720d6952b9105668773809c7f6719c287aefb3fdaf686efb02016a81',construction_binary_sha256='d159552ebcc1ce9e3aabef22f679cf5b1d74e9b09c3bbca790bd7a9aef667bf3',read_binary_sha256='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158',selection_sha256=selection_sha,beam_width=2048,global_candidate_budget=1000,checkpoint_rows=10000,global_map_rows_checked=count,heldout_queries_per_placement=100,heldout_recall=recalls,full_logical_trace_matches=100,query_errors=0,latency_valid=False,latency_limitation='V1 overlaps a closed-calibration archive download. A clean fresh-reader V2 repeat is required; these latencies are excluded.',selected_store=storage,artifact_bindings=bindings,scope='This gate binds actual10M periodic-checkpoint construction, bounded coverage audits, complete disjoint global maps, actual pinned reopens, storage inventories and held-out quality/placement consistency. It authorizes no100M provisioning or indexing by itself; the concrete job/runtime/guards/capacity and parent launch gates still apply. Full raw vector content is not independently reread by this receipt.')
(base/'fineweb10m-preparation-qualification-v1.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({k:receipt[k] for k in ('status','tree_sha256','global_map_rows_checked','heldout_recall','full_logical_trace_matches','latency_valid')}))
