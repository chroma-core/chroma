"""Generated64-row ARM wiring check; no retained corpus, cloud or scale measurement."""
import json,math,os,struct,subprocess,time
from pathlib import Path
from harness_common import descriptor,write,require
from navigation_work import validate_query_work_counts
HERE=Path(__file__).resolve().parent
binary=Path(json.loads((HERE/'build-result.json').read_text())['binary']['path']);out=HERE/'local-native';out.mkdir(exist_ok=False)
raw=b''.join(struct.pack('<32f',*[math.sin((i+1)*(d+1)*.37) for d in range(32)]) for i in range(64));vectors=out/'generated.f32';vectors.write_bytes(raw)
config=json.loads((HERE/'runtime-spec.template.json').read_text())['config'];config.update(split_threshold=8,merge_threshold=2)
env=dict(os.environ,TOKIO_WORKER_THREADS='2',RAYON_NUM_THREADS='2');results={};started=time.monotonic()
for arm in ['baseline','split-input']:
    owner=True;spec=dict(protocol='reference-owner-parallel-v1',source_id='generated64-local-wiring-only',vectors=descriptor(vectors),prefix_sha256=descriptor(vectors)['sha256'],rows=64,dimension=32,metric='ip',config=config,treatment='owner' if owner else 'reference',threads=2,balance_every=32,owner_slots=1 if arm=='dynamic1' else 4,move_above=16,predicate_batching=owner,construction_observation=True,skip_placement=arm=='dynamic1',direct_current_reads=False,batched_scrub_validation=owner,live_prefix_validation=True,batched_split_input_validation=arm=='split-input',publish_checkpoint=True,output=str(out/arm))
    path=out/(arm+'-spec.json');write(path,spec);command=[str(binary),'reference-owner-diagnostic','--spec',str(path)];t=time.monotonic()
    with (out/(arm+'.stdout')).open('xb') as stdout,(out/(arm+'.stderr')).open('xb') as stderr:p=subprocess.run(command,stdout=stdout,stderr=stderr,env=env,timeout=30)
    write(out/(arm+'-process.json'),dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-t));require(p.returncode==0,'localnative failed')
    value=json.loads((out/(arm+'.stdout')).read_text());require(value['spec']==spec and value['spec_sha256']==descriptor(path)['sha256'],'nativebinding');require(value['census']['unique_live_rows']==64 and value['census']['duplicate_current_entries']==value['census']['unreachable_nodes']==0,'census');require(value['source_read_calls']==1 and value['source_read_chunk_rows']==8192,'sourcechunkbinding')
    a=value['timing_accounting'];parts=value['input_seconds']+a['setup_seconds']+a['phase_elapsed_seconds']+value['save_seconds']+value['audit_seconds']+a['publication_seconds']+a['other_before_serialize_seconds'];require(abs(parts-a['native_before_serialize_seconds'])<1e-6,'timereconciliation');results[arm]=value
    require(value['owner_report']['placement_moves']>0,'placement');work=value['owner_report']['split_input_validation'];require(work['enabled'] is (arm=='split-input'),'split echo');require(work['physical_rows']>0,'split work');require(work['scalar_current_calls']==(work['physical_rows'] if arm=='baseline' else 0),'split scalar accounting');require(arm=='baseline' or work['owner_calls']>0,'grouped work')

    store=out/arm/'store';before=descriptor(store/'manifest.json');mapping=descriptor(store/'id-map-1.u64le');require(mapping['bytes']==64*8,'mapwrite')
    requests=[dict(request_id=label,generation=1,op='query',query=[.25]*32,k=4,beam_min=2,beam_max=2,rerank_factor=2) for label in ['first','repeat']];qpath=out/(arm+'-queries.jsonl');qpath.write_text(''.join(json.dumps(v)+'\n' for v in requests));qcommand=[str(binary),'read','--checkpoint-dir',str(store),'--profile-query-work','--profile-rerank-pins','--trace-query-stages','--query-navigation','four-bit'];t=time.monotonic()
    with qpath.open('rb') as stdin,(out/(arm+'-query.stdout')).open('xb') as stdout,(out/(arm+'-query.stderr')).open('xb') as stderr:p=subprocess.run(qcommand,stdin=stdin,stdout=stdout,stderr=stderr,env=env,timeout=10)
    write(out/(arm+'-query-process.json'),dict(command=qcommand,returncode=p.returncode,elapsed_seconds=time.monotonic()-t));require(p.returncode==0,'localquery failed')
    replies=[json.loads(v) for v in (out/(arm+'-query.stdout')).read_text().splitlines()];require(len(replies)==2 and replies[0]['ids']==replies[1]['ids'] and replies[0]['query_work']['returned_distance_bits']==replies[1]['query_work']['returned_distance_bits'],'repeatedquerybits')
    cache=replies[0]['compressed_navigation_cache'];internal_count=cache['nodes']-len([v for v in value['census'].get('leaves',[])]) if 'leaves' in value['census'] else None
    # The opened saved reader reports actual expansions; the original full audit
    # supplies total reachable leaves/nodes where supported. No structural pass
    # or16K pruning claim is made from this small wiring fixture.
    for reply in replies:
        c=reply['query_work']['counts'];trace=c['navigation_level_trace'];require(trace['complete'] is True and reply['query_stage_trace']['id_cap']==16384,'newtraceconsumerbinding')
        require(sum(v['expanded_internal'] for v in trace['levels'])==c['expanded_internal_nodes'] and sum(v['offered_internal']+v['offered_leaf'] for v in trace['levels'])==c['centroid_distance_evaluations'],'counterequations')
    require(descriptor(store/'manifest.json')==before and descriptor(store/'id-map-1.u64le')==mapping,'originalcheckpointmutation')
require(all(v['all_id_exact_fp_distance_bits']==results['baseline']['all_id_exact_fp_distance_bits'] for v in results.values()),'all-IDexactbits')
bad=dict(spec);bad.update(live_prefix_validation=False,batched_split_input_validation=True,direct_current_reads=True,output=str(out/'rejected'),vectors=dict(path='/does-not-exist',bytes=1,sha256='0'*64));path=out/'rejected-spec.json';write(path,bad)
negative=subprocess.run([str(binary),'reference-owner-diagnostic','--spec',str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=5)
(out/'rejected.stdout').write_bytes(negative.stdout);(out/'rejected.stderr').write_bytes(negative.stderr);require(negative.returncode!=0 and b'split input batching requires owner treatment' in negative.stderr and not (out/'rejected').exists(),'premutationpolicyrejection')
write(HERE/'local-native-result.json',dict(status='passed',elapsed_seconds=time.monotonic()-started,arms=list(results),rows=64,dimension=32,all_id_exact_distance_bits_match=True,scopes='GeneratedsmallARMentrywiring/caps/census/querycounterbindings only;no16Kperformance,ANNorinternalpruningclaim.'))
