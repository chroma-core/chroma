"""Use the reviewed scan API for the frozen calibration pool only if time admits."""
import sys,json,time,os
from pathlib import Path
os.environ['CUBLAS_WORKSPACE_CONFIG']=':4096:8'
os.environ['OPENBLAS_NUM_THREADS']='4'
os.environ['OMP_NUM_THREADS']='4'
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.publication_gpu_job import admit_work
from tools.hierarchical_sharding.reference_device_engine import CudaDeviceScorer,preflight_chunks
from tools.hierarchical_sharding.reference_device_filter import memory_projection,admit_memory
from tools.hierarchical_sharding.publication_reference import certified_scan
from tools.hierarchical_sharding.tree_manifest import artifact,checked_artifact,canonical_bytes,_atomic_write,file_hash
ROOT=Path('/mnt/spann');OUT=ROOT/'calibration-fallback-v1';OUT.mkdir(exist_ok=False);started=time.monotonic();report=dict(status='running');scorer=None
try:
 launch=ROOT/'publication-results';pilot=json.loads((launch/'reference11000/report.json').read_text())
 if pilot.get('status')!='failed' or 'admission' in pilot or 'pilot' not in pilot or not pilot.get('error','').startswith('TimeoutError('):raise ValueError('fullpool must decline at measured time admission after a passed pilot')
 launch_report=json.loads((launch/'report.json').read_text());job_path=checked_artifact(launch_report['config'],ROOT);job=json.loads(job_path.read_text())
 deadline=json.loads((ROOT/'deadline.json').read_text())['stop_epoch']
 if deadline!=job['stop_epoch'] or not 0<deadline-job['started_epoch']<=3600:raise ValueError('fixed original launch deadline differs')
 frozen_path=Path('/var/lib/spann-publication-evidence/frozen-pools-11000.json');frozen=json.loads(frozen_path.read_text());ids=frozen['calibration_ids']
 if len(ids)!=1000 or len(set(ids))!=1000 or len(frozen['test_ids'])!=10000 or len(set(frozen['test_ids']))!=10000 or set(ids)&set(frozen['test_ids']):raise ValueError('frozen disjoint1000cal pool required')
 embedding=launch/'embedding11000';complete=json.loads((embedding/'complete.json').read_text());query_path=checked_artifact(complete['output'],embedding)
 rows=[json.loads(line) for line in query_path.read_text().splitlines()];lookup={r['query_id']:r for r in rows}
 if len(rows)!=11000 or len(lookup)!=11000 or set(lookup)!=set(ids+frozen['test_ids']):raise ValueError('complete frozen embedding identities differ')
 if frozen['source_text']['sha256']!=complete['source_text']['sha256'] or complete['source_text']['sha256']!=job['inputs']['texts11000']['sha256']:raise ValueError('frozen text source differs')
 queries=[lookup[qid] for qid in ids]
 # Include measured fresh input-validation and init work, independent oracle,
 # safety margin and preservation reserve; no kernel-only extrapolation.
 input_cost=sum(json.loads((launch/phase/'report.json').read_text())['input_validation_wall_s'] for phase in ['integration1m','integration10m'])
 reserve=300+1.5*(pilot['pilot_oracle_s']+input_cost+10)
 report['admission']=admit_work(pilot['pilot']['wall_s']/32,1000,deadline-time.time(),reserve_s=reserve)
 report.update(frozen=artifact(frozen_path),embedding_complete=artifact(embedding/'complete.json'),pilot=artifact(launch/'reference11000/report.json'),producer=artifact(Path(__file__)),scope='1000 frozen calibration queries only;10000 test references remain pending')
 prepared=checked_artifact(job['inputs']['prepared10m'],ROOT);tree=json.loads(prepared.read_text());chunks=[(checked_artifact(c['vectors'],prepared.parent),c['start_id'],c['row_count']) for c in tree['source']['chunks']];preflight_chunks(chunks,768,10000000)
 scorer=CudaDeviceScorer();report['memory_admission']=admit_memory(memory_projection(768,32768,32,1000,100),int(scorer.torch.cuda.mem_get_info()[0]))
 result=certified_scan(chunks,queries,768,100,scorer,OUT/'certified',[launch/'integration1m/report.json',launch/'integration10m/report.json'],[file_hash(checked_artifact(job['inputs']['expected1m'],ROOT)),file_hash(checked_artifact(job['inputs']['expected10m'],ROOT))],backend=scorer.backend,to_numpy=scorer.to_numpy)
 report.update(status='passed',wall_s=time.monotonic()-started,completion=artifact(OUT/'certified/complete.json',OUT));_atomic_write(OUT/'complete.json',canonical_bytes(report))
except BaseException as error:report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
finally:
 try:report['runtime']=scorer.metadata() if scorer else dict(not_initialized=True)
 except BaseException as error:report['runtime']=dict(metadata_failed=True,error=repr(error))
 _atomic_write(OUT/'report.json',canonical_bytes(report))
