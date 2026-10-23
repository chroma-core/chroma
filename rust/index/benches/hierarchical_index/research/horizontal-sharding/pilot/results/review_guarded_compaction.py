"""Stream retained compaction evidence and verify immutable append bindings."""
import hashlib,json,tarfile
from pathlib import Path
import numpy as np
BASE=Path(__file__).resolve().parent
archive=BASE/'guarded-compaction-six-arm-small-v1.tar.gz'
old=json.loads((BASE/'fineweb1m-publication-metadata/fineweb1m128-h2-a16-c16/tree-1.json').read_text());parents={s['subtree_id']:s for s in old['subtrees']}
with tarfile.open(BASE/'fineweb1m-load-small-worker0.tar.gz') as tar:
 payload=tar.extractfile('fineweb-append100k-128-v1/input.json').read();prepared=json.loads(payload)
assert hashlib.sha256(payload).hexdigest()=='aa9bcbc44a3006ab8933ccfc7d27a26033b29c5853c5b18e9afc12739816e64c'
targets={s['subtree_id']:s for s in prepared['subtrees']};cases={};outer={};configs={};clone_signatures={};signatures={}
canonical=lambda v:(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
with tarfile.open(archive,mode='r|gz') as tar:
 for member in tar:
  if not member.isfile():continue
  parts=Path(member.name).parts;name=parts[-1]
  if parts[0]!='results':
   if name=='summary.json':outer[parts[0]]=json.load(tar.extractfile(member))
   elif name=='config.json':configs[parts[0]]=json.load(tar.extractfile(member))
   elif name=='clone-inventory.json':
    rows=json.load(tar.extractfile(member));seen=set()
    if rows and 'owners' not in rows[0]:rows=[dict(owners=rows)]
    for host in rows:
     for owner in host['owners']:
      sid=Path(owner['index']).name;seen.add(sid);files=owner['files']
      if not isinstance(files,list):continue  # Initial integration has counts only; paired arms retain SHA inventories.
      lookup={r['path']:r for r in files}
      assert lookup['manifest-1.json']['sha256']==lookup['manifest.json']['sha256']==parents[sid]['snapshot']['sha256']
      assert lookup['id-map-1.u64le']['sha256']==parents[sid]['local_to_global']['sha256']
      signature=hashlib.sha256(canonical(sorted(files,key=lambda r:r['path']))).hexdigest()
      if sid in clone_signatures:assert clone_signatures[sid]==signature
      else:clone_signatures[sid]=signature
    assert seen==set(parents)
   continue
  case=cases.setdefault(parts[1],dict(maps={},manifests={},bitmap=np.zeros(1100000,dtype=np.bool_),balance_warnings=0))
  if name in ('case.json','tree-2.json','current.json','remote-reopen.json','old-reader-startup.json','config.json') and len(parts)==3:
   case[name]=json.load(tar.extractfile(member))
  elif name=='journal.jsonl' and len(parts)==3:case['journal']=[json.loads(line) for line in tar.extractfile(member)]
  elif name=='id-map-2.u64le':
   sid=parts[-2];data=tar.extractfile(member).read();ids=np.frombuffer(data,dtype='<u8');target=targets[sid]
   assert len(ids)==target['vector_count'] and np.all(ids<1100000) and len(np.unique(ids))==len(ids) and not case['bitmap'][ids].any();case['bitmap'][ids]=True
   assert hashlib.sha256(data).hexdigest()==target['local_to_global']['sha256']
   assert hashlib.sha256(data[:parents[sid]['vector_count']*8]).hexdigest()==parents[sid]['local_to_global']['sha256']
   case['maps'][sid]=hashlib.sha256(data).hexdigest()
  elif name=='manifest-2.json' and 'copies' in parts:
   data=tar.extractfile(member).read();row=json.loads(data);case['manifests'][row['shard_id']]=(hashlib.sha256(data).hexdigest(),row)
  elif name.endswith('.stderr') and 'build' in parts:
   case['balance_warnings']+=tar.extractfile(member).read().count(b'without full convergence')
reports={}
for name,case in cases.items():
 if 'case.json'not in case:continue
 result=case['case.json'];tree=case['tree-2.json'];current=case['current.json'];entries=configs[name]['entries']
 assert result['status']=='passed' and tree['tree_generation']==2 and tree['source']['row_count']==1100000
 assert tree['model']['sha256']==old['model']['sha256'] and tree['upper_root']==old['upper_root']
 assert hashlib.sha256(canonical(tree)).hexdigest()==result['tree_sha256']==current['tree_sha256']
 assert case['bitmap'].all() and set(case['maps'])==set(parents)==set(case['manifests']) and not case['balance_warnings']
 for slot in tree['subtrees']:
  sid=slot['subtree_id'];digest,row=case['manifests'][sid];proof=row['integrity_audit']
  assert row['generation']==slot['generation']==2 and row['vector_count']==targets[sid]['vector_count']
  assert digest==slot['snapshot']['sha256'] and row['id_mapping']['sha256']==case['maps'][sid]==slot['local_to_global']['sha256']
  assert proof['generation']==2 and proof['block_ids']==row['block_ids'] and proof['id_map_sha256']==case['maps'][sid]
  assert proof['coverage']['expected_rows']==proof['coverage']['unique_live_rows']==row['vector_count']
  startup=case['old-reader-startup.json'][sid]
  assert startup['generation']==1 and startup['shard_id']==sid and startup['vector_count']==parents[sid]['vector_count'] and startup['id_mapping']['sha256']==parents[sid]['local_to_global']['sha256']
 inspections=case['remote-reopen.json'];assert len(inspections)==128
 for inspected in inspections:
  sid=inspected['subtree_id'];digest,row=case['manifests'][sid]
  assert inspected['snapshot_sha256']==digest and inspected['mapping_sha256']==case['maps'][sid] and inspected['root_response']['root']['node_id']==row['root_ref']['id']
 journal=case['journal'];start=next(r['time_unix'] for r in journal if r['phase']=='writers-start');finish=next(r['time_unix'] for r in journal if r['phase']=='writers-finish');published=next(r['time_unix'] for r in journal if r['phase']=='published');reads=[r for r in journal if r['phase']=='old-reader']
 assert any(start<=r['started_unix']<=finish for r in reads) and any(start<=r['finished_unix']<=finish for r in reads) and any(r['started_unix']>=published for r in reads)
 for read in reads:
  key=read['query_index'];digest=hashlib.sha256(canonical(read['signature'])).hexdigest()
  if key in signatures:assert signatures[key]==digest
  else:signatures[key]=digest
 count_delta=len(reads)-result['old_reader_checks']
 assert count_delta==0
 assert sum(r['owners'] for r in journal if r['phase']=='old-generation-reopened')==128
 host_batch=result['index_build']['host_batch']
 assert host_batch['complete'] and not host_batch['missing_terminal_hosts']
 for terminal in host_batch['hosts'].values():
  assert terminal['status']=='passed' and terminal['reconciled'] and all(o['status']=='ok' and o['returncode']==0 for o in terminal['guard_report']['outcomes'])
 assert result['index_build']['complete'] and len(result['index_build']['jobs'])==128 and all(r['status']=='ok' for r in result['index_build']['jobs'])
 reports[name]=dict(status='passed',tree_sha256=result['tree_sha256'],construction_s=result['construction_s'],publication_s=result['publication_s'],through_publication_s=result['through_publication_s'],old_reader_checks=len(reads),reported_old_reader_checks=result['old_reader_checks'],reader_count_delta=count_delta,owners=128,full_dense_rows=1100000,hosts=len({e['host'] for e in entries.values()}),balance_warnings=0)
assert len(reports)==6
report=dict(status='passed',cases=reports,validated_owner_maps=128*len(reports),full_dense_rows_per_case=1100000,old_reader_trace_queries=len(signatures),archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),limits=['Old-reader summary counts reconcile exactly with raw journals in this execution epoch.','No raw index blocks are present locally; committed coverage relies on bound recorded full audits and pinned remote reopen responses.','Reader overlap uses the driver construction interval, including launch/control, and does not certify individual Rust mutation intervals.','Internal split work is randomized; inputs/configuration match, not bit-identical work.','Different constructed gen2 trees must not be used as one-vs-two-host read parity arms.'])
(BASE/'guarded-compaction-independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
