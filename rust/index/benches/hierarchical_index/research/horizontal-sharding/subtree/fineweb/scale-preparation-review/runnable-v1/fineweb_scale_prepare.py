"""Execute only in the isolated fleet slot under the external resource guard."""
import hashlib,json,os,resource,shutil,sys,time,copy,random
from pathlib import Path
import numpy as np
from tools.hierarchical_sharding.fineweb_parallel import ingest
from tools.hierarchical_sharding.fineweb_scale_runtime import validate_scale_job,MINIMUM_STAGE_ESTIMATES
from tools.hierarchical_sharding.preparation_attempt import new_attempt,save_attempt
from tools.hierarchical_sharding.mapped_rows import mapped_rows,copy_rows,relative_rows
from tools.hierarchical_sharding.spatial_prepare import distances
from tools.hierarchical_sharding.fineweb_stream import _whole_prefix_model,_route
from tools.hierarchical_sharding.exact_blocked import exact_chunks
from tools.hierarchical_sharding.benchmark import read_jsonl,write_jsonl
from tools.hierarchical_sharding.tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write,content_hash

def audit_membership(chunks,subtrees,out,model,count):
    """Prove every logical row, owner assignment and original float32 value."""
    seen=np.zeros(count,dtype=np.bool_);starts=np.array([c['start_id'] for c in chunks],dtype=np.uint64);source_paths=[checked_artifact(c['vectors'],out) for c in chunks]
    for owner in subtrees:
        ids=np.fromfile(checked_artifact(owner['local_to_global'],out),dtype='<u8')
        assert len(ids)==owner['vector_count'] and np.all(ids<count) and len(np.unique(ids))==len(ids) and not seen[ids].any()
        seen[ids]=True
        with checked_artifact(owner['vectors'],out).open('rb') as stream:
            for first in range(0,len(ids),4096):
                subset=ids[first:first+4096];source_slots=np.searchsorted(starts,subset,side='right')-1
                values=np.frombuffer(stream.read(len(subset)*768*4),dtype='<f4').reshape(-1,768)
                assert np.all(np.argmin(distances(values,model['centroids'],'ip'),axis=1)==owner['centroid_index'])
                for slot in np.unique(source_slots):
                    selected=source_slots==slot
                    expected=copy_rows(source_paths[slot],(chunks[slot]['row_count'],768),relative_rows(subset[selected],starts[slot],chunks[slot]['row_count']))
                    np.testing.assert_array_equal(values[selected].view('<u4'),expected.view('<u4'))
    assert seen.all()
    return starts,source_paths


def prepare(job_path):
    job_path=Path(job_path);job=json.loads(job_path.read_text());root=Path(job['owned_root']);report_root=Path(job['reports']);reports=new_attempt(report_root)
    plan_bundle=json.loads(checked_artifact(job['source_plan'],job_path.parent).read_text());plan=plan_bundle['plan'];validate_scale_job(job,plan,job_path.parent)
    seed_inodes={(p.stat().st_dev,p.stat().st_ino) for p in Path(job['ingest']['seed_previous']).parent.rglob('*') if p.is_file()} if job['ingest'].get('seed_previous') else set()
    started=time.monotonic();report=dict(status='running',job=artifact(job_path),stages=[],code_epoch='100m_instrumented_verified_workflow_v1',producer_sources=[artifact(Path(__file__))]+[artifact(Path(__file__).with_name(name)) for name in ['fineweb_parallel.py','fineweb_stream.py','spatial_prepare.py','exact_blocked.py','mapped_rows.py','resource_guard.py','fineweb_scale_runtime.py','fineweb_scale_job.py']])
    def save():save_attempt(reports,report)
    def stage(name,began,**values):
     report['stages'].append(dict(stage=name,wall_s=time.monotonic()-began,**values));save();print(json.dumps(report['stages'][-1]),flush=True)
     if time.time()>=job['stop_epoch']-1800:raise TimeoutError('preservation reserve reached')
     estimates=job['stage_estimates_s'];names=list(MINIMUM_STAGE_ESTIMATES)
     if name in names and job['stop_epoch']-time.time()-1800<sum(estimates[k] for k in names[names.index(name)+1:]):raise TimeoutError('remaining stage projection no longer fits fixed deadline')
    def output_accounting():
     logical=allocated=new=0;inodes=set();attempts=[]
     for folder in (Path(job['ingest']['output']),Path(job['spatial']['output']),reports):
      if not folder.exists():continue
      for p in folder.rglob('*'):
       if not p.is_file():continue
       stat=p.stat();logical+=stat.st_size;key=(stat.st_dev,stat.st_ino)
       if key not in inodes:
        allocated+=stat.st_blocks*512
        if key not in seed_inodes:new+=stat.st_blocks*512
        inodes.add(key)
       if 'attempt' in p.name or 'incomplete' in str(p.relative_to(folder)):attempts.append(str(p))
     return dict(logical_path_bytes=logical,allocated_unique_inode_bytes=allocated,new_allocated_excluding_seed_inode_bytes=new,retained_attempt_files=attempts,scope='preparation only; excludes index/checkpoint writes')
    def disk_gate(initial=False):
     free=shutil.disk_usage(root).free;needed=job['resources']['minimum_start_free_allocated_bytes' if initial else 'minimum_free_allocated_bytes']
     if free<needed:raise ValueError('allocated free-space floor violated')
     if output_accounting()['logical_path_bytes']>job['resources']['max_new_logical_output_bytes']:raise ValueError('preparation logical output cap violated')
     return free
    save()
    try:
     initial_free=disk_gate(True);source=checked_artifact(job['source_manifest'],job_path.parent);manifest=json.loads(source.read_text())
     assert content_hash(manifest)==job['source_manifest_sha256']
     for pool,desc in job['query_pools'].items():
      assert artifact(Path(desc['path']))['sha256']==desc['sha256']
      query_rows=read_jsonl(desc['path']);assert len(query_rows)==desc['count'] and len({q['query_id'] for q in query_rows})==desc['count']
     assert plan['status']=='passed' and plan['count']==job['count'] and plan['manifest_sha256']==content_hash(manifest)
     options=dict(job['ingest']);ingest_root=Path(options.pop('output'));options['resume']=ingest_root.exists();options['max_wall_s']=min(options['max_wall_s'],int(job['stop_epoch']-time.time()-1800-job['stage_estimates_s']['archive-and-restore']));began=time.monotonic()
     if options['max_wall_s']<=0:raise TimeoutError('no ingestion time remains before archive reserve')
     reused_ingestion=(ingest_root/'complete.json').exists()
     if reused_ingestion:
      result=json.loads((ingest_root/'ingest.json').read_text());assert result['status']=='passed' and result['global_row_count']==job['count']
      for chunk in result['chunks']:
       checked_artifact(chunk['vectors'],ingest_root);checked_artifact(chunk['uuids'],ingest_root)
     else:result=ingest(manifest,ingest_root,job['count'],**options)
     assert result['config']['manifest_sha256']==content_hash(manifest) and result['config']['count']==job['count'] and result['config']['decoder_epoch']=='arrow_primitive_f16_v2'
     stage('ingestion',began,reused_complete_ingestion=reused_ingestion,ingest=artifact(ingest_root/'ingest.json'),rows=result['global_row_count'],resources=result.get('resources'),free_allocated_bytes=disk_gate())
     out=Path(job['spatial']['output'])/reports.name;out.mkdir(parents=True,exist_ok=False);chunks=copy.deepcopy(result['chunks'])
     for chunk in chunks:
      for key in ('vectors','uuids'):chunk[key]=artifact(checked_artifact(chunk[key],ingest_root))
     spatial=job['spatial'];config=dict(count=job['count'],children=spatial['children'],epoch=spatial['construction_epoch'],metric=job['metric'],sample_size=spatial['sample_size'],seed=spatial['seed'],iterations=spatial['iterations'])
     if (out/'complete.json').exists():raise ValueError('prepared100M output is already immutable; review preserved report instead')
     began=time.monotonic();model=_whole_prefix_model(out,chunks,config);stage('whole-prefix-model',began,model=artifact(out/'model.json'),sample=artifact(out/'sample.f32'),rows=len(model['sample_row_ids']))
     began=time.monotonic();subtrees=_route(out,chunks,config,model,spatial['batch_rows']);stage('routing',began,counts=[s['vector_count'] for s in subtrees],free_allocated_bytes=disk_gate())
     prepared=dict(format_version=1,construction_epoch=config['epoch'],dimension=768,metric='ip',source_id=job['dataset']+'@'+job['revision']+':lex-prefix100m:preserved-f16-to-f32',start_id=0,input_row_count=job['count'],global_row_count=job['count'],source=dict(kind='verified_contiguous_chunks',manifest=artifact(source),chunks=chunks),model=artifact(out/'model.json',out),upper_root=dict(kind='centroid_root',children=[dict(subtree_id=str(i),centroid_index=i) for i in range(spatial['children'])]),subtrees=subtrees,uuid_index=artifact(checked_artifact(result['uuid_index'],ingest_root)),ingest_receipt=artifact(ingest_root/'ingest.json'))
     began=time.monotonic();starts,source_paths=audit_membership(chunks,subtrees,out,model,job['count']);stage('full-ID-and-bitwise-routing-audit',began,rows=job['count'],all_owner_vectors_bitwise_match=True,all_owner_memberships_recomputed=True)
     # Independently reconstruct the seeded sample IDs and inspect corresponding UUIDs.
     assert model['sample_row_ids']==sorted(random.Random(config['seed']).sample(range(job['count']),config['sample_size']))
     sample=np.fromfile(out/'sample.f32',dtype='<f4').reshape(-1,768);sample_ids=np.array(model['sample_row_ids'],dtype=np.uint64);positions=np.searchsorted(starts,sample_ids,side='right')-1
     for slot in np.unique(positions):
      selected=positions==slot;np.testing.assert_array_equal(sample[selected].view('<u4'),copy_rows(source_paths[slot],(chunks[slot]['row_count'],768),relative_rows(sample_ids[selected],starts[slot],chunks[slot]['row_count'])).view('<u4'))
     # Spot-check UUID identity/provenance independently of the ingest UUID index.
     import sqlite3
     uuid_began=time.monotonic();sampled=set(random.Random(981).sample(range(job['count']),256));found=0
     db=sqlite3.connect('file:'+str(checked_artifact(result['uuid_index'],ingest_root))+'?mode=ro',uri=True)
     jobs=json.loads((ingest_root/'plan.json').read_text())['jobs'];job_starts=np.array([j['start_id'] for j in jobs])
     for chunk in chunks:
      selected={i for i in sampled if chunk['start_id']<=i<chunk['start_id']+chunk['row_count']}
      if not selected:continue
      with checked_artifact(chunk['uuids'],out).open() as stream:
       for line in stream:
        row=json.loads(line)
        if row['global_id'] in selected:
         i=row['global_id'];slot=int(np.searchsorted(job_starts,i,side='right')-1);source_job=jobs[slot]
         assert row['source_file']==source_job['file']['path'] and row['source_row']==i-source_job['start_id']
         assert db.execute('SELECT source_uuid FROM documents WHERE global_id=?',(i,)).fetchone()==(row['source_uuid'],);found+=1
     db.close();assert found==256
     stage('whole-prefix-sample-and-UUID-spotcheck',uuid_began,sample_rows=len(sample_ids),UUID_rows=found,sample_vectors_bitwise_match=True)
     _atomic_write(out/'input.json',canonical_bytes(prepared));_atomic_write(out/'complete.json',canonical_bytes(dict(status='passed',scope='routing and ownership only; final attempt run.status=passed required for index admission',input=artifact(out/'input.json'),config=config)))
     chunks_for_exact=[(checked_artifact(c['vectors'],out),c['start_id'],c['row_count']) for c in chunks]
     for pool in ('calibration','test'):
      began=time.monotonic();queries=Path(job['query_pools'][pool]['path']);rows=exact_chunks(chunks_for_exact,read_jsonl(queries),768,100,'ip');dest=reports/(pool+'.exact.jsonl');write_jsonl(dest,rows);stage('exact-'+pool,began,corpus_rows=job['count'],queries=artifact(queries),output=artifact(dest))
     began=time.monotonic();queries=read_jsonl(job['query_pools']['test']['path'])[:3];expected=read_jsonl(reports/'test.exact.jsonl')[:3]
     for query,reference in zip(queries,expected):
      q=np.asarray(query['vector'],dtype=np.float64);scores=np.empty(job['count'],dtype=np.float64)
      assert query['query_id']==reference['query_id']
      for source_path,chunk in zip(source_paths,chunks):
       with source_path.open('rb') as stream:
        for first in range(0,chunk['row_count'],4096):
         block=np.frombuffer(stream.read(min(4096,chunk['row_count']-first)*768*4),dtype='<f4').reshape(-1,768).astype(np.float64);scores[chunk['start_id']+first:chunk['start_id']+first+len(block)]=1-np.einsum('ij,j->i',block,q,optimize=False)
      ids=np.arange(job['count'],dtype=np.uint64);order=np.lexsort((ids,scores))[:100];assert reference['ids']==order.tolist();np.testing.assert_allclose(reference['distances'],scores[order],rtol=0,atol=1e-12)
     stage('independent-full100m-reference-check',began,queries=3,method='independent f64 elementwise reduction over every row, complete global lexsort')
     report.update(status='passed',input=artifact(out/'input.json'),free_allocated_bytes=disk_gate())
    except Exception as error:report.update(status='failed',error_type=type(error).__name__,error=str(error));raise
    finally:
     usage=resource.getrusage(resource.RUSAGE_SELF);report.update(wall_s=time.monotonic()-started,cpu_user_s=usage.ru_utime,cpu_system_s=usage.ru_stime,max_rss_kib=usage.ru_maxrss,outputs=output_accounting());save()
    
    return report

def main():
 import argparse
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--job',required=True);a=p.parse_args();print(json.dumps(prepare(a.job)))
if __name__=='__main__':main()
