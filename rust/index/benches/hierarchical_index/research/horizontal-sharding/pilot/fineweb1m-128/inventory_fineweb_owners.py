import sys,pathlib,json,subprocess,time,hashlib,concurrent.futures
host=sys.argv[1];base=pathlib.Path('/mnt/spann/fineweb1m128-h2-a16-c16/indexes');out=pathlib.Path('/mnt/spann/results/fineweb1m-storage-v1');out.mkdir(exist_ok=True);binary='/mnt/spann/hierarchical_spann_storage_audit-v1';assert hashlib.file_digest(open(binary,'rb'),'sha256').hexdigest()=='a6827c10288e54cc994b34d8ff644b1d85b0e3d5f0b9675e7bd3434a443c9174'
def run(index):
 sha=hashlib.sha256((index/'manifest-1.json').read_bytes()).hexdigest();command=[binary,'--checkpoint-dir',str(index),'--expected-current-sha256',sha];began=time.time();p=subprocess.run(command,capture_output=True,text=True,timeout=30);record=dict(subtree_id=index.name,command=command,expected_current_sha256=sha,exit_status=p.returncode,started_unix=began,finished_unix=time.time(),stderr=p.stderr)
 if p.returncode==0:record['report']=json.loads(p.stdout)
 else:record['stdout']=p.stdout
 (out/(host+'-'+index.name+'.json')).write_text(json.dumps(record,indent=2));return record
began=time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:records=list(pool.map(run,sorted(p for p in base.iterdir() if p.is_dir())))
summary=dict(host=host,started_unix=began,finished_unix=time.time(),owners=len(records),failed=sum(r['exit_status']!=0 for r in records));(out/(host+'-summary.json')).write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
