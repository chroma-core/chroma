import json,hashlib,statistics,datetime,time
from pathlib import Path
root=Path(__file__).resolve().parents[3];p=root/'two-machine-v1/operations/development31';tests=root/'two-machine-v1/local-tests'
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
rows=json.loads((p/'comparison.json').read_text());by={x['name']:x for x in rows}
for name in ['combined-1','combined-2','combined-3','save-serial','save-parallel','combined-save']:assert name in by,name
runs=[];validators=[];bindings={}
for folder in sorted(p.iterdir()):
 f=folder/'owner1-receipts.json'
 if not f.exists():continue
 d=json.loads(f.read_text());rs=[json.loads(v) for k,v in d.items() if k.endswith('/result.json')]
 if rs:
  r=rs[0];assert r['status'] in ['writer_validated','construction_save_and_fresh_reader_validated'];assert r['rows']==3000000
  runs.append({'name':folder.name,'status':r['status'],'rows':r['rows']})
  if r['status']=='construction_save_and_fresh_reader_validated':
   vals=[json.loads(v) for k,v in d.items() if k.endswith('durable-validation.json')]
   assert len(vals)==2
   assert [(q['input_id'],q['results']) for q in vals[0]['queries']]==[(q['input_id'],q['results']) for q in vals[1]['queries']]
   assert sum(x['posting_blocks_materialized'] for x in r['repeat_save']['save_profile']['owners'].values())==0
  for k,v in d.items():
   if k.endswith('durable-validation.json'):
    x=json.loads(v)
    for check in ['exact_membership','all_posting_codes_recomputed','all_centroid_codes_recomputed','vector_sha256_verified']:assert x[check],(folder.name,check)
    assert x['validated_posting_codes']==3000000
    validators.append({'run':folder.name,'report':Path(k).name,**{key:x[key] for key in ['status','rows','validated_posting_codes','elapsed_seconds','membership_and_code_validation_seconds','peak_rss']}})
 for name in ['run.json','serve.json','exit.json','owner1-receipts.json','owner2-receipts.json']:
  f=folder/name
  if f.exists():bindings[str(f.relative_to(root))]=sha(f)
for f in [*p.glob('*manifest.json'),p/'epoch31v4-binary.json',p/'reader-selection.json',p/'accepted-run.py',p/'run.py',p/'experiment-design.json',p/'final-cloud-audit.json',p/'comparison.json',tests/'EPOCH31-CONSTRUCTION-AND-SAVING.md',p.parent/'development30/checkpoint-manifest.json',p.parent.parent/'packages/source-epoch31v4.tar.gz',p/'reader-source.tar.gz']:
 assert f.exists(),f;bindings[str(f.relative_to(root))]=sha(f)
med=lambda prefix:{k:statistics.median(by[prefix+'-'+str(i)][k] for i in [1,2,3]) for k in ['insert','balance','construction','insert_cpu','balance_cpu']}
profile={'writer_kind':'epoch31v4','writer_sha256':json.loads((p/'epoch31v4-binary.json').read_text())['sha256'],'env':{'SPANN_ACCEPT_MODE':'poll_1ms','SPANN_INSERTION_NAVIGATION_WINDOW':'64','SPANN_INSERTION_WORKERS':'64','SPANN_SERIALIZE_BOUNDARY_REFRESH':'1','SPANN_SAVE_WORKERS':'32'},'reader':json.loads((p/'reader-selection.json').read_text()),'balance_workers_per_owner':32,'source_archive':'two-machine-v1/packages/source-epoch31v4.tar.gz','decision':'Accepted combined latency profile with higher CPU demand; bounded parallel durable saving is accepted design. Preserve compact navigation and conditional routing-view transport.'}
(p/'accepted-profile.json').write_text(json.dumps(profile,indent=2)+'\n')
bindings[str((p/'accepted-profile.json').relative_to(root))]=sha(p/'accepted-profile.json')
now=datetime.datetime.now(datetime.timezone.utc).isoformat();receipt={'completed_at':now,'status':'complete','accepted':profile,'medians':{k:med(k) for k in ['control','workers64','refreshgate','combined']},'validated_runs':runs,'fresh_readers':validators,'artifacts_sha256':bindings,'native_validation':{'writer_suite_passed':284,'reader_suite_passed':287,'ignored':3,'known_unrelated_exclusion':1},'automation':'PAUSED','machines':'left running under current guards per user','limits':['Three construction repetitions per arm; single serial/parallel save comparison.','Tree evolution and work counts vary with scheduling.','Reader binary differs between serial and parallel save arms; save timings finish before readers run.','No claim of scalability to100B from these3M experiments.']}
(tests/'EPOCH31-COMPLETION.json').write_text(json.dumps(receipt,indent=2)+'\n')
state=json.loads((root/'CONTINUATION-STATE.json').read_text());state['updated_at']=now;state['active_execution'].update(updated_at=now,status='epoch31_complete_machines_left_running',source='/private/tmp/spann-research31',next_action='Use the accepted epoch31 profile for future baseline measurements. Further construction research should target remaining publication serialization and excess distance work. Keep recurring follow-up paused; inspect current machine lease before reuse.',automation='PAUSED_BY_USER',report='two-machine-v1/local-tests/EPOCH31-CONSTRUCTION-AND-SAVING.md',completion_receipt='two-machine-v1/local-tests/EPOCH31-COMPLETION.json',accepted_profile='two-machine-v1/operations/development31/accepted-profile.json');(root/'CONTINUATION-STATE.json').write_text(json.dumps(state,indent=2)+'\n')
ledger=json.loads((root/'SPEND-LEDGER.json').read_text());guard=json.loads((p.parent/'window-epoch29c/start-intent.json').read_text())
for row in ledger['active_compute_reservations']:
 if row['window']=='window-epoch29c':row.update(status='experiments complete; machines running per user with local and remote guards',snapshot_observed_unix=time.time(),elapsed_compute_upper_bound_usd_at_snapshot=(time.time()-guard['observed_start_unix'])*8.064/3600)
(root/'SPEND-LEDGER.json').write_text(json.dumps(ledger,indent=2)+'\n');print(json.dumps({'runs':len(runs),'fresh_readers':len(validators),'medians':receipt['medians']},indent=2))
