import tarfile,json,hashlib
from pathlib import Path
import numpy as np
p=Path(__file__).resolve().parent;a=p/'fineweb10m-recovery-small-v1.tar.gz';base='fineweb10m128-h2-publication-recovery-v1/';old='fineweb10m128-h2-a16-c16-v1/'
with tarfile.open(a) as t:
 raw=lambda n:t.extractfile(n).read()
 load=lambda n:json.loads(raw(n))
 receipt=load(base+'independent-admitted-input-and-source.json');pins=load(base+'recovery-inputs.json')
 assert receipt['admitted_prepared_sha256']==pins['prepared']['sha256']=='739c0b520b204c9ade837aa92a501c6ccd8c87d735248a5f395b0df39202cbcb'
 for name,desc in pins['original'].items():assert hashlib.sha256(raw(old+name)).hexdigest()==desc['sha256']
 for name in ['publication_recovery.py','guarded_compaction_backend.py']:
  expected=next(v for k,v in receipt['files'].items() if k.endswith('/'+name));assert hashlib.sha256(raw(base+'launched-'+name)).hexdigest()==expected
 report=load(base+'case.json');assert report['status']=='passed' and report['writer_rerun'] is False
 original_events=[json.loads(x) for x in raw(old+'journal.jsonl').splitlines()];assert original_events[-1]['phase']=='failed'
 terminal=load(old+'build/host-batch/report.json');batch=load(old+'build/host-batch/config.json')
 for i,entry in enumerate(batch['hosts']):
  fresh=load(base+f'build/host-{i}-read-terminal.json');assert fresh==terminal['hosts'][entry['host']] and fresh['reconciled'] and fresh['status']=='passed'
 tree=load(base+'tree-1.json');pointer=load(base+'current.json');assert hashlib.sha256(raw(base+'tree-1.json')).hexdigest()==pointer['tree_sha256']==report['tree_sha256']
 inspections={x['subtree_id']:x for x in load(base+'remote-reopen.json')};seen=np.zeros(10000000,dtype=np.bool_);logbytes=0;warnings=0
 for slot in tree['subtrees']:
  sid=slot['subtree_id'];m=load(base+f'copies/{sid}/manifest-1.json');data=raw(base+f'copies/{sid}/id-map-1.u64le');ids=np.frombuffer(data,dtype='<u8')
  assert len(ids)==m['vector_count'] and int(ids.max())<len(seen) and len(np.unique(ids))==len(ids) and not seen[ids].any();seen[ids]=True
  assert hashlib.sha256(data).hexdigest()==slot['local_to_global']['sha256']==m['id_mapping']['sha256']
  assert hashlib.sha256(raw(base+f'copies/{sid}/manifest-1.json')).hexdigest()==slot['snapshot']['sha256']==inspections[sid]['snapshot_sha256']
  proof=m['integrity_audit'];assert proof['block_ids']==m['block_ids'] and proof['coverage']['unique_live_rows']==len(ids) and proof['id_map_sha256']==m['id_mapping']['sha256']
 for n in t.getnames():
  if n.startswith(base+'build/') and n.endswith('.raw-log-metadata.json'):
   for suffix,meta in load(n).items():
    assert meta['full_raw_retained_remotely'] and len(meta['sha256'])==64 and meta['classification_occurrences']['balance_nonconvergence']==0
    logbytes+=meta['bytes'];warnings+=meta['classification_occurrences']['navigation_precision_warning']
 assert seen.all() and len(inspections)==128
 result=dict(status='passed',owners=128,rows=10000000,recovery_s=report['recovery_s'],tree_sha256=report['tree_sha256'],raw_log_bytes=logbytes,navigation_warning_occurrences=warnings,writer_rerun=False,launched_prepared_pin='external independently hashed receipt; launched helper predates mandatory CLI pin',source_receipt=receipt,archive_sha256=hashlib.sha256(a.read_bytes()).hexdigest(),limitations=['Full raw logs remain remote and need version-pinned archive receipts before stop.','Block payloads are not copied here; publication binds recorded full audits and actual pinned reopens.','Original construction attempt remains failed; recovery timing is separate.'])
(p/'fineweb10m-recovery-independent-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
