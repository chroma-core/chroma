from pathlib import Path
import json,subprocess,hashlib,shutil,time,os
O=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/phased-construction-v1');P=O.parent/'build-speed-diagnostic-v1/local-native-final';r=O/'local-native';r.mkdir()
binary=max((p for p in Path('/private/tmp/spann-write-check/debug/deps').glob('hierarchical_spann_shard-*') if p.is_file() and p.suffix==''),key=lambda p:p.stat().st_mtime);kept=O/'local-native-binary';shutil.copyfile(binary,kept);kept.chmod(0o755)
results=[]
for label,workers in [('legacy',0),('phased',2)]:
 spec=json.load(open(P/'authoritative-spec.json'));spec.update(output=str(r/label),lazy_disabled_evaluation_trace=True,profile_before_scrub=True,lazy_disabled_before_scrub_trace=True,current_embedding_view=True,storage_chunk_rows=8,vector_probe_rounds=0,balance_every=32,phased_owner_workers=workers)
 if workers==0:spec.pop('phased_owner_workers')
 path=r/(label+'-spec.json');path.write_text(json.dumps(spec,indent=2)+'\n');before=time.monotonic()
 with (r/(label+'.stdout')).open('wb') as out,(r/(label+'.stderr')).open('wb') as err:p=subprocess.run([str(kept),'build-speed-diagnostic','--spec',str(path)],stdout=out,stderr=err,timeout=30,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2'))
 result=dict(policy=label,returncode=p.returncode,elapsed_seconds=time.monotonic()-before);results.append(result)
 if p.returncode:break
 v=json.load(open(r/label/'result.json'));assert v['spec']==spec;assert v['census']['unique_live_rows']==64 and v['census']['duplicate_current_entries']==0 and v['census']['unreachable_nodes']==0
 assert len(v['cuts'])==2
 if workers:
  result['phase_report']=v['cuts'][-1]['construction_phase'];assert result['phase_report']['status']=='Ready' and result['phase_report']['completed_phases']==2 and result['phase_report']['peak_outstanding_owner_requests']==1 and result['phase_report']['peak_active_owner_registrations']==1 and result['phase_report']['active_owner_registrations']==0 and result['phase_report']['owner_registration_calls']==63 and result['phase_report']['valid'] is True
  assert all(c['reservation_seconds']>0 and c['publication_seconds']>0 and c['reshard_seconds']>0 for c in v['cuts'])
 else:assert v['cuts'][-1]['construction_phase'] is None
(r/'test-result.json').write_text(json.dumps(dict(status='passed' if len(results)==2 and all(x['returncode']==0 for x in results) else 'failed',results=results,binary=dict(path=str(kept),bytes=kept.stat().st_size,sha256=hashlib.sha256(kept.read_bytes()).hexdigest()),scope='Synthetic64x32 ARM dev entry-point correctness only. Native harness has1owner, so configured2workers produces1effective owner request. Full two-owner fixed-tape parity is the ordinary Rust test. Independent stochastic native arms have no quality/performance ratio.'),indent=2)+'\n');print(results)
