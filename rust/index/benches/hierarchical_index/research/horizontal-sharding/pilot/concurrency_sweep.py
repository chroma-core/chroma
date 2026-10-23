import json,pathlib,sys,time,subprocess
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.build import run_jobs
out=pathlib.Path('/mnt/spann/results/target-real1m-concurrency');out.mkdir(exist_ok=True)
for hosts,cap in [(1,2),(1,4),(1,8),(1,16),(2,2),(2,4),(2,8)]:
 name=f'h{hosts}-c{cap}'
 base=pathlib.Path(f'/mnt/spann/results/target-real1m-{"one" if hosts==1 else "two"}-r1')
 jobs=json.loads((base/'jobs.json').read_text())
 for j in jobs:j['checkpoint_dir']=f'/mnt/spann/target-real1m-concurrency/{name}/indexes/'+j['shard_id']
 directory=out/name;directory.mkdir(exist_ok=False)
 (directory/'jobs.json').write_text(json.dumps(jobs,indent=2))
 report=run_jobs(jobs,directory/'build',max_hosts=hosts,timeout=1800,jobs_per_host=cap)
 warnings={p.name:p.read_text() for p in (directory/'build').glob('*.stderr') if 'without full convergence' in p.read_text()}
 result=dict(name=name,hosts=hosts,jobs_per_host=cap,balancing_threads=1,runtime_threads=2,report=report,fully_converged=not warnings,balance_warnings=warnings,boundary='Pre-staged identical vectors and maps; construction through worker publication. Input transfer and local-reference copies excluded.')
 (directory/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(dict(name=name,elapsed_s=report['elapsed_s'],complete=report['complete'],fully_converged=not warnings)),flush=True)
 if not report['complete'] or warnings:break
