import pathlib,json,sys,time,hashlib
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator
case=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');out=pathlib.Path('/mnt/spann/results/load32-warmup-v1');out.mkdir(exist_ok=False);queries=[json.loads(x) for x in pathlib.Path('/mnt/spann/fineweb1m-pilot-data/test.jsonl').read_text().splitlines()];tree=json.load(open(case/'case.json'))['tree']
for label in ('cohost','two-hosts'):
 start=time.monotonic();c=TreeCoordinator.from_files(tree,case/('placement-'+label+'.json'));count=0
 try:
  c.verify_roots(timeout=60)
  for q in queries:
   if time.monotonic()-start>600:raise TimeoutError('Warmup wall cap')
   r=c.query(q.get('vector',q.get('query')),k=100,beam_width=512,candidate_budget=1000,timeout=30);count+=1
 finally:c.close()
 row=dict(placement=label,status='passed',queries=count,elapsed_s=time.monotonic()-start,scope='Untimed100-query warmup with fixed selected policy; OS cache uncontrolled.');(out/(label+'.json')).write_text(json.dumps(row));print(json.dumps(row),flush=True)
