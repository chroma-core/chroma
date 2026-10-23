import pathlib,json,sys
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_orchestrate import ssh
case=pathlib.Path('/mnt/spann/results/fineweb10m128-h2-a16-c16-v2');out=case;groups={};samplers=[]
for label in ('cohost','two-hosts'):
 for r in json.load(open(case/('servers-'+label+'.json'))):groups.setdefault(r['host'],{}).setdefault(label,[]).append(r['pid'])
for host,group in groups.items():
 code='import pathlib,json,subprocess,hashlib\np=pathlib.Path("/mnt/spann/results/fineweb10m-read-heldout-resources-v2");p.mkdir(exist_ok=False)\n(p/"groups.json").write_text('+repr(json.dumps(group))+')\nf=open(p/"stderr","w");s=subprocess.Popen(["python3","/mnt/spann/sample_process_groups.py","--groups",str(p/"groups.json"),"--output",str(p/"processes.jsonl"),"--duration","1800"],stdout=subprocess.DEVNULL,stderr=f,start_new_session=True)\n(p/"source.json").write_text(json.dumps({n:hashlib.sha256((pathlib.Path("/mnt/spann/owner-reduction-source-v2/tools/hierarchical_sharding")/n).read_bytes()).hexdigest() for n in ("server.py","subtree_server.py","subtree_transport.py")}))\nprint(s.pid)\n'
 samplers.append(dict(host=host,pid=int(ssh(host,code))))
(out/'worker-samplers.json').write_text(json.dumps(samplers));print(json.dumps(samplers))
