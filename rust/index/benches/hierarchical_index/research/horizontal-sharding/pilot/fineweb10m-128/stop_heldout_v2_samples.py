import pathlib,json,sys
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_orchestrate import ssh
case=pathlib.Path('/mnt/spann/results/fineweb10m128-h2-a16-c16-v2');out=[]
for r in json.loads((case/'worker-samplers.json').read_text()):
 code="import pathlib,os,signal,json\np=pathlib.Path('/proc')/str("+repr(r['pid'])+")\nif p.exists():\n a=(p/'cmdline').read_bytes();assert b'sample_process_groups.py' in a and b'fineweb10m-read-heldout-resources-v2' in a;os.kill("+repr(r['pid'])+",signal.SIGTERM)\nprint(json.dumps({'status':'stopped'}))\n"
 out.append(dict(host=r['host'],outcome=json.loads(ssh(r['host'],code))))
(case/'stopped-heldout-v2-samplers.json').write_text(json.dumps(out));print(json.dumps(out))
